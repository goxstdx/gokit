package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

// DelayConsumer 延迟队列消费管理器
// 单一轮询协程取出到期任务，投入 channel 由 N 个 worker 并发执行
type DelayConsumer struct {
	runner       core.QueueRunner
	option       core.RunnerOption
	driver       driver.DelayQueueDriver
	lock         driver.LockDriver
	prefix       string
	logger       core.Logger
	lockTTL      time.Duration
	pollInterval time.Duration
	procTimeout  time.Duration

	taskCh chan string
	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewDelayConsumer 创建延迟队列消费器
func NewDelayConsumer(
	runner core.QueueRunner,
	opt core.RunnerOption,
	dq driver.DelayQueueDriver,
	lk driver.LockDriver,
	prefix string,
	lockTTL time.Duration,
	pollInterval time.Duration,
	procTimeout time.Duration,
	logger core.Logger,
) *DelayConsumer {
	return &DelayConsumer{
		runner:       runner,
		option:       opt,
		driver:       dq,
		lock:         lk,
		prefix:       prefix,
		lockTTL:      lockTTL,
		pollInterval: pollInterval,
		procTimeout:  procTimeout,
		logger:       logger,
	}
}

func (c *DelayConsumer) pendingKey() string {
	return fmt.Sprintf("%s:delay:{%s}:pending", c.prefix, c.runner.GetName())
}

func (c *DelayConsumer) processingKey() string {
	return fmt.Sprintf("%s:delay:{%s}:processing", c.prefix, c.runner.GetName())
}

func (c *DelayConsumer) deadKey() string {
	return fmt.Sprintf("%s:delay:{%s}:dead", c.prefix, c.runner.GetName())
}

func (c *DelayConsumer) recoveryLockKey() string {
	return fmt.Sprintf("%s:lock:recover:delay:{%s}", c.prefix, c.runner.GetName())
}

// Start 启动延迟队列消费器
func (c *DelayConsumer) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)

	c.taskCh = make(chan string, c.option.ConsumerCount*2)

	// 单一轮询协程
	c.wg.Add(1)
	go c.poll(ctx)

	// N 个 worker 协程
	for i := 0; i < c.option.ConsumerCount; i++ {
		c.wg.Add(1)
		go c.work(ctx, i)
	}

	// 一次性恢复协程
	c.wg.Add(1)
	go c.startupRecover(ctx)

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
		drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
		defer drainCancel()
		var drained int
		for raw := range c.taskCh {
			if drainCtx.Err() != nil {
				c.logger.Warnf("taskx: delay[%s] drain timeout, remaining items stay in processing for crash recovery", c.runner.GetName())
				break
			}
			if err := c.driver.Nack(drainCtx, c.processingKey(), c.pendingKey(), raw, time.Now().Unix()); err != nil {
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

// startupRecover 启动时一次性恢复：抢锁 → 等待 processingTimeout → 恢复残留消息 → 退出
func (c *DelayConsumer) startupRecover(ctx context.Context) {
	defer c.wg.Done()

	if c.lock == nil {
		return
	}

	lockKey := c.recoveryLockKey()
	lockTTL := c.procTimeout + 30*time.Second
	ok, err := c.lock.Lock(ctx, lockKey, lockTTL)
	if err != nil {
		c.logger.Warnf("taskx: delay[%s] recovery lock error: %v", c.runner.GetName(), err)
		return
	}
	if !ok {
		return
	}
	defer func() { _ = c.lock.Unlock(ctx, lockKey) }()

	c.logger.Infof("taskx: delay[%s] recovery waiting %v for in-flight messages to finish", c.runner.GetName(), c.procTimeout)
	select {
	case <-ctx.Done():
		return
	case <-time.After(c.procTimeout):
	}

	var totalRecovered int64
	for {
		recovered, err := c.driver.RecoverProcessing(ctx, c.processingKey(), c.pendingKey(), c.procTimeout)
		if err != nil {
			c.logger.Warnf("taskx: delay[%s] recover processing error: %v", c.runner.GetName(), err)
			break
		}
		totalRecovered += recovered
		if recovered == 0 {
			break
		}
	}
	if totalRecovered > 0 {
		c.logger.Infof("taskx: delay[%s] recovered %d orphaned messages from processing", c.runner.GetName(), totalRecovered)
	}
}

func (c *DelayConsumer) poll(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.fetch(ctx)
		}
	}
}

func (c *DelayConsumer) fetch(ctx context.Context) {
	now := time.Now().Unix()
	batchSize := int64(c.option.ConsumerCount * 2)
	if batchSize < 10 {
		batchSize = 10
	}

	items, err := c.driver.TransferToProcessing(ctx, c.pendingKey(), c.processingKey(), now, batchSize)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		c.logger.Errorf("taskx: delay[%s] transfer error: %v", c.runner.GetName(), err)
		return
	}

	for _, raw := range items {
		select {
		case <-ctx.Done():
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
			return
		case raw, ok := <-c.taskCh:
			if !ok {
				return
			}
			c.process(ctx, raw, id)
		}
	}
}

func (c *DelayConsumer) process(ctx context.Context, raw string, id int) {
	env, err := core.DecodeEnvelope(raw)
	if err != nil {
		c.logger.Errorf("taskx: delay[%s][%d] decode error: %v, raw: %s", c.runner.GetName(), id, err, raw)
		if ackErr := c.driver.Ack(ctx, c.processingKey(), raw); ackErr != nil {
			c.logger.Errorf("taskx: delay[%s][%d] ack(decode-err) failed: %v", c.runner.GetName(), id, ackErr)
		}
		return
	}

	result := c.runner.Run(ctx, env.Payload)

	if result.IsOk {
		if ackErr := c.driver.Ack(ctx, c.processingKey(), raw); ackErr != nil {
			c.logger.Errorf("taskx: delay[%s][%d] ack failed: %v", c.runner.GetName(), id, ackErr)
		}
		return
	}

	c.logger.Warnf("taskx: delay[%s][%d] run failed: %v", c.runner.GetName(), id, result.Err)
	env.RetryCount++

	if env.RetryCount >= *c.option.MaxRetry {
		c.logger.Warnf("taskx: delay[%s][%d] max retry reached (%d), moving to dead letter", c.runner.GetName(), id, *c.option.MaxRetry)
		if err := c.driver.MoveToDead(ctx, c.processingKey(), c.deadKey(), raw); err != nil {
			c.logger.Errorf("taskx: delay[%s][%d] move-to-dead failed: %v", c.runner.GetName(), id, err)
		}
		return
	}

	newRaw := env.Encode()
	var nextTime int64
	if result.NextTime > 0 {
		nextTime = result.NextTime
	} else {
		nextTime = time.Now().Unix() + int64(env.RetryCount*5)
	}
	if err := c.driver.RetryRequeue(ctx, c.processingKey(), c.pendingKey(), raw, newRaw, nextTime); err != nil {
		c.logger.Errorf("taskx: delay[%s][%d] retry-requeue failed: %v", c.runner.GetName(), id, err)
	}
}
