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

// DelayConsumer 延迟队列消费管理器
// 单一轮询协程取出到期任务，投入 channel 由 N 个 worker 并发执行
type DelayConsumer struct {
	runner core.QueueRunner
	option core.RunnerOption
	driver driver.DelayQueueDriver
	lock   driver.LockDriver
	logger core.Logger

	keys                QueueKeySet
	onAlert             core.AlertFunc
	onHeartbeat         core.ListenerHeartbeatFunc
	traceKey            string
	lockTTL             time.Duration
	pollInterval        time.Duration
	recoveryGracePeriod time.Duration
	recoveryMode        RecoveryMode
	internalOpTimeout   time.Duration
	retryBaseInterval   time.Duration

	taskCh chan string
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewDelayConsumer 创建延迟队列消费器
func NewDelayConsumer(runner core.QueueRunner, opt core.RunnerOption, cfg DelayConsumerConfig) *DelayConsumer {
	return &DelayConsumer{
		runner:              runner,
		option:              opt,
		driver:              cfg.Driver,
		lock:                cfg.Lock,
		keys:                cfg.Keys,
		lockTTL:             cfg.LockTTL,
		pollInterval:        cfg.PollInterval,
		recoveryGracePeriod: cfg.RecoveryGracePeriod,
		recoveryMode:        cfg.RecoveryMode.Normalize(),
		internalOpTimeout:   cfg.InternalOpTimeout,
		retryBaseInterval:   cfg.RetryBaseInterval,
		logger:              cfg.Logger,
		onAlert:             cfg.OnAlert,
		onHeartbeat:         cfg.OnHeartbeat,
		traceKey:            cfg.TraceKey,
	}
}

func (c *DelayConsumer) BuildKey() string {
	return c.keys.Pending
}

func (c *DelayConsumer) alert(data core.AlertData) {
	if c.onAlert != nil {
		if data.Source == "" {
			data.Source = core.AlertSourceDelay
		}
		c.onAlert(data)
	}
}

func (c *DelayConsumer) beat() {
	if c.onHeartbeat == nil {
		return
	}
	c.onHeartbeat(
		core.ListenerHeartbeat{
			Kind: core.ListenerKindDelay,
			Name: c.runner.GetName(),
			At:   time.Now(),
		},
	)
}

func (c *DelayConsumer) internalOpContext() (context.Context, context.CancelFunc) {
	timeout := c.internalOpTimeout
	if timeout <= 0 {
		timeout = defaults.InternalOpTimeout
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (c *DelayConsumer) internalOpContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return c.internalOpContext()
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (c *DelayConsumer) gracePeriod() time.Duration {
	if c.recoveryGracePeriod > 0 {
		return c.recoveryGracePeriod
	}
	return defaults.RecoveryGracePeriod
}

func (c *DelayConsumer) recoverMaxDuration(lockTTL time.Duration) time.Duration {
	gp := c.gracePeriod()
	maxDuration := gp + lockTTL
	minDuration := lockTTL + defaults.RecoveryLockMargin
	if maxDuration < minDuration {
		maxDuration = minDuration
	}
	return maxDuration
}

// Start 启动延迟队列消费器
func (c *DelayConsumer) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)
	c.beat()

	c.taskCh = make(chan string, c.option.ConsumerCount*2)

	// 单一轮询协程
	c.wg.Add(1)
	go c.poll(ctx)

	// N 个 worker 协程
	for i := 0; i < c.option.ConsumerCount; i++ {
		c.wg.Add(1)
		go c.work(ctx, i)
	}

	if c.recoveryMode.WithStartupRecover() {
		// 一次性恢复协程
		c.wg.Add(1)
		go c.startupRecover(ctx)
	}
	if c.recoveryMode.WithPeriodicRecover() {
		// 定期恢复协程（兜底）
		c.wg.Add(1)
		go c.periodicRecover(ctx)
	}

	c.logger.Infof("taskx: delay[%s] started with %d workers", c.runner.GetName(), c.option.ConsumerCount)
	return nil
}

// Stop 停止延迟队列消费器。
// 停止流程：cancel context → 等待所有协程退出 → drain channel 将残留消息 Nack 回 pending。
// drain 操作有 1 秒超时限制，超时后残留消息保留在 processing 中，等待下次启动时的崩溃恢复机制处理。
func (c *DelayConsumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()

	if c.taskCh != nil {
		close(c.taskCh)
		// 1 秒内尽量将 channel 中残留的消息 Nack 回 pending，
		// 超时后剩余消息留在 processing ZSet，由下次启动的崩溃恢复流程兜底。
		drainCtx, drainCancel := c.internalOpContextWithTimeout(defaults.DelayDrainTimeout)
		defer drainCancel()
		var drained int
		for raw := range c.taskCh {
			if drainCtx.Err() != nil {
				c.logger.Warnf(
					"taskx: delay[%s] drain timeout, remaining items stay in processing for crash recovery",
					c.runner.GetName(),
				)
				break
			}
			if err := c.driver.Nack(drainCtx, c.keys.Processing, c.keys.Pending, raw, time.Now().UnixMicro()); err != nil {
				c.logger.Warnf("taskx: delay[%s] drain nack error: %v", c.runner.GetName(), err)
			} else {
				drained++
			}
		}
		if drained > 0 {
			c.logger.Infof("taskx: delay[%s] drained %d items from channel back to pending", c.runner.GetName(), drained)
		}
		c.taskCh = nil
	}
	c.logger.Infof("taskx: delay[%s] stopped", c.runner.GetName())
}

// startupRecover 启动时一次性恢复
func (c *DelayConsumer) startupRecover(ctx context.Context) {
	defer c.wg.Done()
	c.doRecover(ctx, "startup")
}

// periodicRecover 后台定期恢复协程，兜底处理因快速重启或多实例 crash 遗漏的 processing 消息。
func (c *DelayConsumer) periodicRecover(ctx context.Context) {
	defer c.wg.Done()

	if c.lock == nil {
		return
	}

	ticker := time.NewTicker(c.gracePeriod())
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("taskx: delay[%s] periodicRecover exiting, ctx cancelled: %v", c.runner.GetName(), ctx.Err())
			return
		case <-ticker.C:
			c.doRecover(ctx, "periodic")
		}
	}
}

// doRecover 通用恢复逻辑：抢锁 → 按 gracePeriod 过滤恢复 processing 超时消息 → 释放锁。
// 若业务处理耗时超过 gracePeriod，仍可能发生重复投递，业务侧需保持幂等。
func (c *DelayConsumer) doRecover(ctx context.Context, label string) {
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
		c.logger.Warnf("taskx: delay[%s] %s recovery lock error: %v", c.runner.GetName(), label, err)
		return
	}
	if !ok {
		c.logger.Debugf(
			"taskx: delay[%s] %s recovery lock not acquired (held by another), skipping",
			c.runner.GetName(),
			label,
		)
		return
	}
	defer func() {
		unlockCtx, unlockCancel := c.internalOpContext()
		defer unlockCancel()
		_ = c.lock.Unlock(unlockCtx, lockKey)
	}()
	stopRenew, lockLost := c.startRecoverRenewLoop(lockKey, lockTTL)
	defer stopRenew()

	c.logger.Infof("taskx: delay[%s] %s recovery started (gracePeriod=%v)", c.runner.GetName(), label, gp)

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
			err := fmt.Errorf(
				"taskx: delay[%s] %s recovery exceeded max duration %v",
				c.runner.GetName(),
				label,
				maxDuration,
			)
			c.logger.Warnf("%v", err)
			c.alert(
				core.AlertData{
					Source:       core.AlertSourceDelay,
					AlertType:    core.AlertRecoveryExceeded,
					RunnerName:   c.runner.GetName(),
					RunnerResult: core.RunnerFuncResult{IsOk: false, Err: err},
				},
			)
			return
		}
		c.logger.Debugf(
			"taskx: delay[%s] %s recovering, processingKey=%s, pendingKey=%s, gracePeriod=%v",
			c.runner.GetName(), label, c.keys.Processing, c.keys.Pending, gp,
		)
		recovered, err := c.driver.RecoverProcessing(ctx, c.keys.Processing, c.keys.Pending, gp)
		if err != nil {
			c.logger.Warnf("taskx: delay[%s] %s recover processing error: %v", c.runner.GetName(), label, err)
			break
		}
		totalRecovered += recovered
		if recovered == 0 {
			break
		}
		c.logger.Infof("taskx: delay[%s] %s batch recovered %d messages", c.runner.GetName(), label, recovered)
	}
	if totalRecovered > 0 {
		c.logger.Infof(
			"taskx: delay[%s] %s recovery finished, recovered %d orphaned messages from processing",
			c.runner.GetName(),
			label,
			totalRecovered,
		)
	} else {
		c.logger.Infof(
			"taskx: delay[%s] %s recovery finished, recovered %d orphaned messages from processing",
			c.runner.GetName(),
			label,
			totalRecovered,
		)
	}
}

func (c *DelayConsumer) startRecoverRenewLoop(lockKey string, lockTTL time.Duration) (func(), <-chan struct{}) {
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
					c.logger.Warnf("taskx: delay[%s] recovery lock renew error: %v", c.runner.GetName(), err)
					continue
				}
				if !ok {
					err := fmt.Errorf(
						"taskx: delay[%s] recovery lock lost, duplicate recovery may happen",
						c.runner.GetName(),
					)
					c.logger.Warnf("%v", err)
					c.alert(
						core.AlertData{
							Source:       core.AlertSourceDelay,
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

func (c *DelayConsumer) poll(ctx context.Context) {
	defer c.wg.Done()

	// 启动后立即拉取一次，避免首次等待完整 pollInterval
	c.beat()
	c.fetch(ctx)

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("taskx: delay[%s] poll loop exiting, ctx cancelled: %v", c.runner.GetName(), ctx.Err())
			return
		case <-ticker.C:
			c.beat()
			c.fetch(ctx)
		}
	}
}

// fetch 从 pending ZSet 取出到期消息并投入 channel。
//
// 注意：TransferToProcessing 通过 Lua 脚本原子地将消息从 pending ZREM 并 ZADD 到 processing。
// 如果此时 ctx 被取消（Stop 触发），已从 pending 移出但尚未投入 channel 的消息会留在
// processing ZSet 中。这些消息不会丢失，会在下次启动时由崩溃恢复机制（startupRecover）
// 移回 pending 重新消费。
//
// 后续优化方向：可考虑在 ctx 取消时，将 items 中尚未投入 channel 的消息立即 Nack 回
// pending，减少依赖崩溃恢复的等待时间（默认 processingTimeout=5min）。但鉴于 Stop 本身
// 低频且崩溃恢复已兜底，当前设计优先保证停止速度，避免阻塞 pod 销毁。
func (c *DelayConsumer) fetch(ctx context.Context) {
	// 如果 chan 长度已经达到上限，跳过本次 fetch
	if len(c.taskCh) >= cap(c.taskCh) {
		c.logger.Warnf("taskx: delay[%s] channel full, skip fetch", c.runner.GetName())

		c.alert(
			core.AlertData{
				AlertType:  core.AlertQueueBacklog,
				RunnerName: c.runner.GetName(),
				Remark:     "队列积压",
			},
		)

		return
	}

	now := time.Now().UnixMicro()
	batchSize := int64(c.option.ConsumerCount * 2)
	if batchSize < 10 {
		batchSize = 10
	}

	c.logger.Debugf(
		"taskx: delay[%s] fetch pending items, now: %d, batch size: %d, pending key: %s, processing key: %s",
		c.runner.GetName(),
		now,
		batchSize,
		c.keys.Pending,
		c.keys.Processing,
	)
	items, err := c.driver.TransferToProcessing(ctx, c.keys.Pending, c.keys.Processing, now, batchSize)
	if err != nil {
		if ctx.Err() != nil {
			c.logger.Infof("taskx: delay[%s] transfer interrupted, ctx cancelled: %v", c.runner.GetName(), ctx.Err())
			return
		}
		c.logger.Errorf("taskx: delay[%s] transfer error: %v", c.runner.GetName(), err)
		return
	}

	for _, raw := range items {
		select {
		case <-ctx.Done():
			c.logger.Infof(
				"taskx: delay[%s] fetch dispatch interrupted, ctx cancelled: %v, remaining items: %d",
				c.runner.GetName(),
				ctx.Err(),
				len(items),
			)
			return
		case c.taskCh <- raw:
		}
	}
}

func (c *DelayConsumer) work(ctx context.Context, id int) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			c.logger.Infof("taskx: delay[%s][%d] worker exiting, ctx cancelled: %v", c.runner.GetName(), id, ctx.Err())
			return
		case raw, ok := <-c.taskCh:
			if !ok {
				return
			}
			c.safeProcess(ctx, raw, id)
		}
	}
}

func (c *DelayConsumer) safeProcess(ctx context.Context, raw string, id int) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Errorf("taskx: delay[%s][%d] panic in process: %v", c.runner.GetName(), id, r)
			c.alert(
				core.AlertData{
					Source:     core.AlertSourceDelay,
					AlertType:  core.AlertCorruptMessage,
					RunnerName: c.runner.GetName(),
					Remark:     fmt.Sprintf("panic: %v", r),
				},
			)
		}
	}()
	c.process(ctx, raw, id)
}

func (c *DelayConsumer) process(ctx context.Context, raw string, id int) {
	env, err := core.DecodeEnvelope(raw)
	if err != nil {
		// Envelope 已损坏时，重复投递通常仍无法修复，且可能形成 poison message 无限循环。
		// 因此这里告警后直接 Ack 删除；调用方应通过 OnAlert 排查生产端或历史脏数据。
		c.logger.Errorf("taskx: delay[%s][%d] decode error: %v, raw: %s", c.runner.GetName(), id, err, raw)
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceDelay,
				AlertType:    core.AlertCorruptMessage,
				RunnerName:   c.runner.GetName(),
				RunnerResult: core.RunnerFuncResult{IsOk: false, Err: err},
			},
		)
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if ackErr := c.driver.Ack(opCtx, c.keys.Processing, raw); ackErr != nil {
			c.logger.Errorf("taskx: delay[%s][%d] ack(decode-err) failed: %v", c.runner.GetName(), id, ackErr)
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
		if ackErr := c.driver.Ack(opCtx, c.keys.Processing, raw); ackErr != nil {
			c.logger.Errorf("taskx: delay[%s][%d] ack failed: %v", c.runner.GetName(), id, ackErr)
		}
		return
	}

	c.logger.Warnf("taskx: delay[%s][%d] run failed: %v", c.runner.GetName(), id, result.Err)
	env.RetryCount++

	if env.RetryCount > c.option.MaxRetry {
		c.logger.Warnf(
			"taskx: delay[%s][%d] max retry reached (%d), moving to dead letter",
			c.runner.GetName(),
			id,
			c.option.MaxRetry,
		)
		c.alert(
			core.AlertData{
				Source:       core.AlertSourceDelay,
				AlertType:    core.AlertMaxRetryExhausted,
				RunnerName:   c.runner.GetName(),
				Envelope:     env,
				RunnerResult: result,
			},
		)
		opCtx, opCancel := c.internalOpContext()
		defer opCancel()
		if err := c.driver.MoveToDead(opCtx, c.keys.Processing, c.keys.Dead, raw); err != nil {
			c.logger.Errorf("taskx: delay[%s][%d] move-to-dead failed: %v", c.runner.GetName(), id, err)
		}
		return
	}

	newRaw := env.Encode()
	var nextTime int64
	now := time.Now().UnixMicro()
	if result.NextTime != nil && result.NextTime.UnixMicro() > now {
		nextTime = result.NextTime.UnixMicro()
	} else {
		if result.NextTime != nil {
			c.logger.Warnf(
				"taskx: delay[%s][%d] invalid next_time=%v, fallback to default retry schedule",
				c.runner.GetName(), id, result.NextTime,
			)
		}
		retryBase := c.retryBaseInterval
		if retryBase <= 0 {
			retryBase = defaults.DelayRetryBaseInterval
		}
		retryDelay := time.Duration(env.RetryCount) * retryBase
		retryMicros := retryDelay.Microseconds()
		if retryMicros < 1_000_000 {
			retryMicros = 1_000_000
		}
		nextTime = now + retryMicros
	}
	opCtx, opCancel := c.internalOpContext()
	defer opCancel()
	if err := c.driver.RetryRequeue(opCtx, c.keys.Processing, c.keys.Pending, raw, newRaw, nextTime); err != nil {
		c.logger.Errorf("taskx: delay[%s][%d] retry-requeue failed: %v", c.runner.GetName(), id, err)
	}
}
