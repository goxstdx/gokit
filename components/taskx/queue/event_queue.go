package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
)

// EventConsumer 事件队列消费管理器，为单个 Runner 管理 N 个消费协程
type EventConsumer struct {
	runner            core.QueueRunner
	option            core.RunnerOption
	driver            driver.EventQueueDriver
	lock              driver.LockDriver
	prefix            string
	logger            core.Logger
	onAlert           core.AlertFunc
	onHeartbeat       core.ListenerHeartbeatFunc
	traceKey          string
	lockTTL           time.Duration
	procTimeout       time.Duration
	internalOpTimeout time.Duration
	popTimeout        time.Duration

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewEventConsumer 创建事件队列消费器
func NewEventConsumer(
	runner core.QueueRunner,
	opt core.RunnerOption,
	eq driver.EventQueueDriver,
	lk driver.LockDriver,
	prefix string,
	lockTTL time.Duration,
	procTimeout time.Duration,
	internalOpTimeout time.Duration,
	popTimeout time.Duration,
	logger core.Logger,
	onAlert core.AlertFunc,
	onHeartbeat core.ListenerHeartbeatFunc,
	traceKey string,
) *EventConsumer {
	return &EventConsumer{
		runner:            runner,
		option:            opt,
		driver:            eq,
		lock:              lk,
		prefix:            prefix,
		lockTTL:           lockTTL,
		procTimeout:       procTimeout,
		internalOpTimeout: internalOpTimeout,
		popTimeout:        popTimeout,
		logger:            logger,
		onAlert:           onAlert,
		onHeartbeat:       onHeartbeat,
		traceKey:          traceKey,
	}
}

func (c *EventConsumer) pendingKey() string {
	return fmt.Sprintf("%s:event:{%s}:pending", c.prefix, c.runner.GetName())
}

func (c *EventConsumer) processingKey() string {
	return fmt.Sprintf("%s:event:{%s}:processing", c.prefix, c.runner.GetName())
}

func (c *EventConsumer) deadKey() string {
	return fmt.Sprintf("%s:event:{%s}:dead", c.prefix, c.runner.GetName())
}

func (c *EventConsumer) recoveryLockKey() string {
	return fmt.Sprintf("%s:lock:recover:event:{%s}", c.prefix, c.runner.GetName())
}

func (c *EventConsumer) alert(data core.AlertData) {
	if c.onAlert != nil {
		if data.Source == "" {
			data.Source = core.AlertSourceEvent
		}
		c.onAlert(data)
	}
}

func (c *EventConsumer) beat() {
	if c.onHeartbeat == nil {
		return
	}
	c.onHeartbeat(
		core.ListenerHeartbeat{
			Kind: core.ListenerKindEvent,
			Name: c.runner.GetName(),
			At:   time.Now(),
		},
	)
}

func (c *EventConsumer) internalOpContext() (context.Context, context.CancelFunc) {
	timeout := c.internalOpTimeout
	if timeout <= 0 {
		timeout = defaults.InternalOpTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (c *EventConsumer) recoverMaxDuration(lockTTL time.Duration) time.Duration {
	maxDuration := c.procTimeout + lockTTL
	minDuration := lockTTL + defaults.RecoveryLockMargin
	if maxDuration < minDuration {
		maxDuration = minDuration
	}
	return maxDuration
}

// Start 启动消费者
func (c *EventConsumer) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)
	c.beat()

	// 启动消费协程
	for i := 0; i < c.option.ConsumerCount; i++ {
		c.wg.Add(1)
		go c.consume(ctx, i)
	}

	// 启动一次性恢复协程：等待 processingTimeout 后恢复残留消息
	c.wg.Add(1)
	go c.startupRecover(ctx)

	c.logger.Infof("taskx: event[%s] started with %d consumers", c.runner.GetName(), c.option.ConsumerCount)
	return nil
}

// Stop 停止消费者
func (c *EventConsumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.logger.Infof("taskx: event[%s] stopped", c.runner.GetName())
}

// startupRecover 启动时一次性恢复：抢锁 → 等待 processingTimeout → 恢复残留消息 → 退出
func (c *EventConsumer) startupRecover(ctx context.Context) {
	defer c.wg.Done()

	if c.lock == nil {
		return
	}

	lockKey := c.recoveryLockKey()
	// 锁 TTL 覆盖整个等待+恢复过程
	lockTTL := c.procTimeout + defaults.RecoveryLockMargin
	if c.lockTTL > lockTTL {
		lockTTL = c.lockTTL
	}
	ok, err := c.lock.Lock(ctx, lockKey, lockTTL)
	if err != nil {
		c.logger.Warnf("taskx: event[%s] recovery lock error: %v", c.runner.GetName(), err)
		return
	}
	if !ok {
		// 其他实例已经在执行恢复
		return
	}
	defer func() {
		unlockCtx, unlockCancel := c.internalOpContext()
		defer unlockCancel()
		_ = c.lock.Unlock(unlockCtx, lockKey)
	}()
	stopRenew, lockLost := c.startRecoverRenewLoop(lockKey, lockTTL)
	defer stopRenew()

	// 等待 processingTimeout，让正在处理中的消息有足够时间完成
	c.logger.Infof(
		"taskx: event[%s] recovery waiting %v for in-flight messages to finish",
		c.runner.GetName(),
		c.procTimeout,
	)
	select {
	case <-ctx.Done():
		return
	case <-time.After(c.procTimeout):
	}

	// 等待结束后，将 processing 中残留消息视为可恢复消息并分批移回 pending。
	// EventQueue 的 processing 使用 List，无法按单条消息进入 processing 的时间过滤；
	// 若业务处理耗时超过 processingTimeout，可能发生重复投递，业务侧需保持幂等。
	// 后续优化可考虑：processing 改为带时间戳结构、引入租约续期，或增加实例心跳辅助判断。
	var totalRecovered int64
	startedAt := time.Now()
	maxDuration := c.recoverMaxDuration(lockTTL)
	for {
		select {
		case <-ctx.Done():
			return
		case <-lockLost:
			return
		default:
		}
		if time.Since(startedAt) > maxDuration {
			err := fmt.Errorf("taskx: event[%s] recovery exceeded max duration %v", c.runner.GetName(), maxDuration)
			c.logger.Warnf("%v", err)
			c.alert(
				core.AlertData{
					Source:       core.AlertSourceEvent,
					AlertType:    core.AlertRecoveryExceeded,
					RunnerName:   c.runner.GetName(),
					RunnerResult: core.RunnerFuncResult{IsOk: false, Err: err},
				},
			)
			return
		}
		recovered, err := c.driver.RecoverProcessing(ctx, c.processingKey(), c.pendingKey(), c.procTimeout)
		if err != nil {
			c.logger.Warnf("taskx: event[%s] recover processing error: %v", c.runner.GetName(), err)
			break
		}
		totalRecovered += recovered
		if recovered == 0 {
			break
		}
	}
	if totalRecovered > 0 {
		c.logger.Infof("taskx: event[%s] recovered %d orphaned messages from processing", c.runner.GetName(), totalRecovered)
	}
}

func (c *EventConsumer) startRecoverRenewLoop(lockKey string, lockTTL time.Duration) (func(), <-chan struct{}) {
	lost := make(chan struct{})
	interval := lockTTL / defaults.LockRenewIntervalDivisor
	if interval <= 0 {
		interval = defaults.DefaultLockRenewInterval
	}
	if interval < defaults.MinLockRenewInterval {
		interval = defaults.MinLockRenewInterval
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(lost)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := c.internalOpContext()
				ok, err := c.lock.Renew(renewCtx, lockKey, lockTTL)
				renewCancel()
				if err != nil {
					c.logger.Warnf("taskx: event[%s] recovery lock renew error: %v", c.runner.GetName(), err)
					continue
				}
				if !ok {
					err := fmt.Errorf(
						"taskx: event[%s] recovery lock lost, duplicate recovery may happen",
						c.runner.GetName(),
					)
					c.logger.Warnf("%v", err)
					c.alert(
						core.AlertData{
							Source:       core.AlertSourceEvent,
							AlertType:    core.AlertRecoveryLockLost,
							RunnerName:   c.runner.GetName(),
							RunnerResult: core.RunnerFuncResult{IsOk: false, Err: err},
						},
					)
					return
				}
			}
		}
	}()

	return func() {
		cancel()
		wg.Wait()
	}, lost
}

func (c *EventConsumer) consume(ctx context.Context, id int) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		popTimeout := c.popTimeout
		if popTimeout <= 0 {
			popTimeout = defaults.EventPopTimeout
		}
		raw, err := c.driver.PopToProcessing(ctx, c.pendingKey(), c.processingKey(), popTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Errorf("taskx: event[%s][%d] pop error: %v", c.runner.GetName(), id, err)
			time.Sleep(defaults.EventPopErrorBackoff)
			continue
		}
		c.beat()
		if raw == "" {
			continue
		}

		c.process(ctx, raw, id)
	}
}

func (c *EventConsumer) process(ctx context.Context, raw string, id int) {
	env, err := core.DecodeEnvelope(raw)
	if err != nil {
		// Envelope 已损坏时，重复投递通常仍无法修复，且可能形成 poison message 无限循环。
		// 因此这里告警后直接 Ack 删除；调用方应通过 OnAlert 排查生产端或历史脏数据。
		c.logger.Errorf("taskx: event[%s][%d] decode error: %v, raw: %s", c.runner.GetName(), id, err, raw)
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceEvent,
				AlertType:    core.AlertCorruptMessage,
				RunnerName:   c.runner.GetName(),
				RunnerResult: core.RunnerFuncResult{IsOk: false, Err: err},
			},
		)
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if ackErr := c.driver.Ack(opCtx, c.processingKey(), raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack(decode-err) failed: %v", c.runner.GetName(), id, ackErr)
		}
		return
	}

	runCtx := ctx
	if c.traceKey != "" {
		runCtx = context.WithValue(runCtx, c.traceKey, env.ID)
	}
	result := c.runner.Run(runCtx, env.Payload)

	if result.IsOk {
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if ackErr := c.driver.Ack(opCtx, c.processingKey(), raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack failed: %v", c.runner.GetName(), id, ackErr)
		}
		return
	}

	c.logger.Warnf("taskx: event[%s][%d] run failed: %v", c.runner.GetName(), id, result.Err)
	if result.NextTime > 0 {
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceEvent,
				AlertType:    core.AlertEventNextTimeIgnored,
				RunnerName:   c.runner.GetName(),
				Envelope:     env,
				RunnerResult: result,
			},
		)
		// NextTime 属于业务改道信号；告警后直接 ack，不再回到 event 重试链路。
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if ackErr := c.driver.Ack(opCtx, c.processingKey(), raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack(next-time) failed: %v", c.runner.GetName(), id, ackErr)
		}
		return
	}
	env.RetryCount++

	if env.RetryCount > *c.option.MaxRetry {
		c.logger.Warnf(
			"taskx: event[%s][%d] max retry reached (%d), moving to dead letter",
			c.runner.GetName(),
			id,
			*c.option.MaxRetry,
		)
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceEvent,
				AlertType:    core.AlertMaxRetryExhausted,
				RunnerName:   c.runner.GetName(),
				Envelope:     env,
				RunnerResult: result,
			},
		)
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if err := c.driver.MoveToDead(opCtx, c.processingKey(), c.deadKey(), raw); err != nil {
			c.logger.Errorf("taskx: event[%s][%d] move-to-dead failed: %v", c.runner.GetName(), id, err)
		}
		return
	}

	newRaw := env.Encode()
	opCtx, opCancel := c.internalOpContext()
	defer opCancel()
	if err := c.driver.RetryRequeue(opCtx, c.processingKey(), c.pendingKey(), raw, newRaw); err != nil {
		c.logger.Errorf("taskx: event[%s][%d] retry-requeue failed: %v", c.runner.GetName(), id, err)
	}
}
