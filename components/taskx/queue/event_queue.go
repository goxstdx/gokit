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

// EventConsumer 事件队列消费管理器，为一个队列组管理 N 个消费协程。
// 同组内可包含多个 Runner，消费者根据 Envelope.RunnerName 路由到对应 Runner。
type EventConsumer struct {
	runners       map[string]EventRunnerEntry
	groupName     string
	consumerCount int
	driver        driver.EventQueueDriver
	lock          driver.LockDriver
	logger        core.Logger

	keys                QueueKeySet
	onAlert             core.AlertFunc
	onHeartbeat         core.ListenerHeartbeatFunc
	traceKey            string
	lockTTL             time.Duration
	recoveryGracePeriod time.Duration
	internalOpTimeout   time.Duration
	popTimeout          time.Duration
	recoveryMode        RecoveryMode

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewEventConsumer 创建事件队列消费器
func NewEventConsumer(cfg EventConsumerConfig) *EventConsumer {
	groupName := cfg.Keys.Meta.Name
	if groupName == "" {
		groupName = cfg.Keys.Pending
	}

	return &EventConsumer{
		runners:             cfg.Runners,
		groupName:           groupName,
		consumerCount:       cfg.ConsumerCount,
		driver:              cfg.Driver,
		lock:                cfg.Lock,
		keys:                cfg.Keys,
		lockTTL:             cfg.LockTTL,
		recoveryGracePeriod: cfg.RecoveryGracePeriod,
		recoveryMode:        cfg.RecoveryMode.Normalize(),
		internalOpTimeout:   cfg.InternalOpTimeout,
		popTimeout:          cfg.PopTimeout,
		logger:              cfg.Logger,
		onAlert:             cfg.OnAlert,
		onHeartbeat:         cfg.OnHeartbeat,
		traceKey:            cfg.TraceKey,
	}
}

func (c *EventConsumer) BuildKey() string {
	return c.keys.Pending
}

func (c *EventConsumer) logName() string {
	return c.groupName
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
			Name: c.groupName,
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

func (c *EventConsumer) gracePeriod() time.Duration {
	if c.recoveryGracePeriod > 0 {
		return c.recoveryGracePeriod
	}
	return defaults.RecoveryGracePeriod
}

func (c *EventConsumer) recoverMaxDuration(lockTTL time.Duration) time.Duration {
	gp := c.gracePeriod()
	maxDuration := gp + lockTTL
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

	for i := 0; i < c.consumerCount; i++ {
		c.wg.Add(1)
		go c.consume(ctx, i)
	}

	if c.recoveryMode.WithStartupRecover() {
		c.wg.Add(1)
		go c.startupRecover(ctx)
	}
	if c.recoveryMode.WithPeriodicRecover() {
		c.wg.Add(1)
		go c.periodicRecover(ctx)
	}

	c.logger.Infof("taskx: event[%s] started with %d consumers", c.logName(), c.consumerCount)
	return nil
}

// Stop 停止消费者
func (c *EventConsumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	c.logger.Infof("taskx: event[%s] stopped", c.logName())
}

// startupRecover 启动时一次性恢复：抢锁 → 按 gracePeriod 过滤恢复超时消息 → 退出
func (c *EventConsumer) startupRecover(ctx context.Context) {
	defer c.wg.Done()
	c.doRecover(ctx, "startup")
}

// periodicRecover 后台定期恢复协程，兜底处理因快速重启或多实例 crash 遗漏的 processing 消息。
func (c *EventConsumer) periodicRecover(ctx context.Context) {
	defer c.wg.Done()

	if c.lock == nil {
		return
	}

	ticker := time.NewTicker(c.gracePeriod())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("taskx: event[%s] periodicRecover exiting, ctx cancelled: %v", c.logName(), ctx.Err())
			return
		case <-ticker.C:
			c.doRecover(ctx, "periodic")
		}
	}
}

// doRecover 通用恢复逻辑：抢锁 → 按 gracePeriod 过滤恢复 processing 超时消息 → 释放锁
func (c *EventConsumer) doRecover(ctx context.Context, label string) {
	if c.lock == nil {
		return
	}

	gp := c.gracePeriod()
	lockKey := c.keys.RecoveryLock
	lockTTL := gp + defaults.RecoveryLockMargin
	if c.lockTTL > lockTTL {
		lockTTL = c.lockTTL
	}
	ok, err := c.lock.Lock(ctx, lockKey, lockTTL)
	if err != nil {
		c.logger.Warnf("taskx: event[%s] %s recovery lock error: %v", c.logName(), label, err)
		return
	}
	if !ok {
		c.logger.Debugf("taskx: event[%s] %s recovery lock not acquired (held by another), skipping", c.logName(), label)
		return
	}
	defer func() {
		unlockCtx, unlockCancel := c.internalOpContext()
		defer unlockCancel()
		_ = c.lock.Unlock(unlockCtx, lockKey)
	}()
	stopRenew, lockLost := c.startRecoverRenewLoop(lockKey, lockTTL)
	defer stopRenew()

	c.logger.Infof("taskx: event[%s] %s recovery started (gracePeriod=%v)", c.logName(), label, gp)

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
			err := fmt.Errorf("taskx: event[%s] %s recovery exceeded max duration %v", c.logName(), label, maxDuration)
			c.logger.Warnf("%v", err)
			c.alert(
				core.AlertData{
					Source:       core.AlertSourceEvent,
					AlertType:    core.AlertRecoveryExceeded,
					RunnerName:   c.logName(),
					RunnerResult: core.RunnerFuncResult{IsOk: false, Err: err},
				},
			)
			return
		}
		c.logger.Debugf(
			"taskx: event[%s] %s recovering, processingKey=%s, pendingKey=%s, gracePeriod=%v",
			c.logName(), label, c.keys.Processing, c.keys.Pending, gp,
		)
		recovered, err := c.driver.RecoverProcessing(ctx, c.keys.Processing, c.keys.Pending, gp)
		if err != nil {
			c.logger.Warnf("taskx: event[%s] %s recover processing error: %v", c.logName(), label, err)
			break
		}
		totalRecovered += recovered
		if recovered == 0 {
			break
		}
		c.logger.Infof("taskx: event[%s] %s batch recovered %d messages", c.logName(), label, recovered)
	}

	if totalRecovered > 0 {
		c.logger.Infof(
			"taskx: event[%s] %s recovery finished, recovered %d orphaned messages from processing",
			c.logName(),
			label,
			totalRecovered,
		)
	} else {
		c.logger.Debugf(
			"taskx: event[%s] %s recovery finished, recovered %d orphaned messages from processing",
			c.logName(),
			label,
			totalRecovered,
		)
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
					c.logger.Warnf("taskx: event[%s] recovery lock renew error: %v", c.logName(), err)
					continue
				}
				if !ok {
					err := fmt.Errorf(
						"taskx: event[%s] recovery lock lost, duplicate recovery may happen",
						c.logName(),
					)
					c.logger.Warnf("%v", err)
					c.alert(
						core.AlertData{
							Source:       core.AlertSourceEvent,
							AlertType:    core.AlertRecoveryLockLost,
							RunnerName:   c.logName(),
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

	pollInterval := c.popTimeout
	if pollInterval <= 0 {
		pollInterval = defaults.EventPopTimeout
	}

	// 启动后立即消费一次，避免首次等待完整 pollInterval
	c.beat()
	c.fetchAndProcess(ctx, id)

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("taskx: event[%s][%d] consume loop exiting, ctx cancelled: %v", c.logName(), id, ctx.Err())
			return
		case <-ticker.C:
		}

		c.beat()
		c.fetchAndProcess(ctx, id)
	}
}

func (c *EventConsumer) fetchAndProcess(ctx context.Context, id int) {
	popTimeout := c.popTimeout
	if popTimeout <= 0 {
		popTimeout = defaults.EventPopTimeout
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("taskx: event[%s][%d] fetchAndProcess exiting, ctx cancelled: %v", c.logName(), id, ctx.Err())
			return
		default:
		}

		c.logger.Debugf(
			"taskx: event[%s][%d] pop pending items, pending key: %s, processing key: %s, timeout: %v",
			c.logName(), id, c.keys.Pending, c.keys.Processing, popTimeout,
		)
		raw, err := c.driver.PopToProcessing(ctx, c.keys.Pending, c.keys.Processing, popTimeout)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Infof("taskx: event[%s][%d] pop interrupted, ctx cancelled: %v", c.logName(), id, ctx.Err())
				return
			}
			c.logger.Errorf("taskx: event[%s][%d] pop error: %v", c.logName(), id, err)
			return
		}
		if raw == "" {
			return
		}
		c.safeProcess(ctx, raw, id)
	}
}

func (c *EventConsumer) safeProcess(ctx context.Context, raw string, id int) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Errorf("taskx: event[%s][%d] panic in process: %v", c.logName(), id, r)
			c.alert(
				core.AlertData{
					Source:     core.AlertSourceEvent,
					AlertType:  core.AlertCorruptMessage,
					RunnerName: c.logName(),
					Remark:     fmt.Sprintf("panic: %v", r),
				},
			)
		}
	}()
	c.process(ctx, raw, id)
}

func (c *EventConsumer) process(ctx context.Context, raw string, id int) {
	env, err := core.DecodeEnvelope(raw)
	if err != nil {
		c.logger.Errorf("taskx: event[%s][%d] decode error: %v, raw: %s", c.logName(), id, err, raw)
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceEvent,
				AlertType:    core.AlertCorruptMessage,
				RunnerName:   c.logName(),
				RunnerResult: core.RunnerFuncResult{IsOk: false, Err: err},
			},
		)
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if ackErr := c.driver.Ack(opCtx, c.keys.Processing, raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack(decode-err) failed: %v", c.logName(), id, ackErr)
		}
		return
	}

	entry, ok := c.runners[env.RunnerName]
	if !ok {
		c.logger.Errorf("taskx: event[%s][%d] unknown runner %q, ack and discard", c.logName(), id, env.RunnerName)
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceEvent,
				AlertType:    core.AlertUnknownRunner,
				RunnerName:   env.RunnerName,
				Envelope:     env,
				RunnerResult: core.RunnerFuncResult{IsOk: false, Err: fmt.Errorf("unknown runner: %s", env.RunnerName)},
			},
		)
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if ackErr := c.driver.Ack(opCtx, c.keys.Processing, raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack(unknown-runner) failed: %v", c.logName(), id, ackErr)
		}
		return
	}

	runCtx := ctx
	if c.traceKey != "" {
		runCtx = context.WithValue(runCtx, c.traceKey, env.ID)
	}
	result := entry.Runner.Run(runCtx, env.Payload)

	if result.IsOk {
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if ackErr := c.driver.Ack(opCtx, c.keys.Processing, raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack failed: %v", c.logName(), id, ackErr)
		}
		return
	}

	c.logger.Warnf("taskx: event[%s][%d] runner=%s run failed: %v", c.logName(), id, env.RunnerName, result.Err)
	if result.NextTime != nil {
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceEvent,
				AlertType:    core.AlertEventNextTimeIgnored,
				RunnerName:   env.RunnerName,
				Envelope:     env,
				RunnerResult: result,
			},
		)
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if ackErr := c.driver.Ack(opCtx, c.keys.Processing, raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack(next-time) failed: %v", c.logName(), id, ackErr)
		}
		return
	}
	env.RetryCount++

	if env.RetryCount > entry.Option.MaxRetry {
		c.logger.Warnf(
			"taskx: event[%s][%d] runner=%s max retry reached (%d), moving to dead letter",
			c.logName(), id, env.RunnerName, entry.Option.MaxRetry,
		)
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceEvent,
				AlertType:    core.AlertMaxRetryExhausted,
				RunnerName:   env.RunnerName,
				Envelope:     env,
				RunnerResult: result,
			},
		)
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if err := c.driver.MoveToDead(opCtx, c.keys.Processing, c.keys.Dead, raw); err != nil {
			c.logger.Errorf("taskx: event[%s][%d] move-to-dead failed: %v", c.logName(), id, err)
		}
		return
	}

	newRaw := env.Encode()
	opCtx, opCancel := c.internalOpContext()
	defer opCancel()
	if err := c.driver.RetryRequeue(opCtx, c.keys.Processing, c.keys.Pending, raw, newRaw); err != nil {
		c.logger.Errorf("taskx: event[%s][%d] retry-requeue failed: %v", c.logName(), id, err)
	}
}
