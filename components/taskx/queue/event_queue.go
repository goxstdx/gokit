package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

// EventConsumer 事件队列消费管理器，为单个 Runner 管理 N 个消费协程
type EventConsumer struct {
	runner      core.QueueRunner
	option      core.RunnerOption
	driver      driver.EventQueueDriver
	lock        driver.LockDriver
	prefix      string
	logger      core.Logger
	onAlert     core.AlertFunc
	lockTTL     time.Duration
	procTimeout time.Duration

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
	logger core.Logger,
	onAlert core.AlertFunc,
) *EventConsumer {
	return &EventConsumer{
		runner:      runner,
		option:      opt,
		driver:      eq,
		lock:        lk,
		prefix:      prefix,
		lockTTL:     lockTTL,
		procTimeout: procTimeout,
		logger:      logger,
		onAlert:     onAlert,
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

func (c *EventConsumer) alert(alertType core.AlertType, msg string) {
	if c.onAlert != nil {
		c.onAlert(
			core.AlertData{
				Source:    core.AlertSourceEvent,
				AlertType: alertType,
				Msg:       msg,
			},
		)
	}
}

// Start 启动消费者
func (c *EventConsumer) Start(ctx context.Context) error {
	ctx, c.cancel = context.WithCancel(ctx)

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
	lockTTL := c.procTimeout + 30*time.Second
	ok, err := c.lock.Lock(ctx, lockKey, lockTTL)
	if err != nil {
		c.logger.Warnf("taskx: event[%s] recovery lock error: %v", c.runner.GetName(), err)
		return
	}
	if !ok {
		// 其他实例已经在执行恢复
		return
	}
	defer func() { _ = c.lock.Unlock(context.Background(), lockKey) }()

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
	for {
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

func (c *EventConsumer) consume(ctx context.Context, id int) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		raw, err := c.driver.PopToProcessing(ctx, c.pendingKey(), c.processingKey(), 3*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Errorf("taskx: event[%s][%d] pop error: %v", c.runner.GetName(), id, err)
			time.Sleep(time.Second)
			continue
		}
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
		c.alert(core.AlertCorruptMessage, fmt.Sprintf("event[%s] decode failed: %v, raw: %s", c.runner.GetName(), err, raw))
		if ackErr := c.driver.Ack(ctx, c.processingKey(), raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack(decode-err) failed: %v", c.runner.GetName(), id, ackErr)
		}
		return
	}

	result := c.runner.Run(ctx, env.Payload)

	if result.IsOk {
		if ackErr := c.driver.Ack(ctx, c.processingKey(), raw); ackErr != nil {
			c.logger.Errorf("taskx: event[%s][%d] ack failed: %v", c.runner.GetName(), id, ackErr)
		}
		return
	}

	c.logger.Warnf("taskx: event[%s][%d] run failed: %v", c.runner.GetName(), id, result.Err)
	if result.NextTime > 0 {
		// NextTime 仅对 DelayQueue 重试生效。EventQueue 当前不自动转投 DelayQueue，
		// 这里保留即时重试语义并触发告警，避免调用方误以为已进入延迟队列。
		c.alert(
			core.AlertEventNextTimeIgnored,
			fmt.Sprintf(
				"event[%s] returned NextTime=%d; EventQueue ignores NextTime and will retry via pending, envelope_id=%s",
				c.runner.GetName(),
				result.NextTime,
				env.ID,
			),
		)
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
			core.AlertMaxRetryExhausted,
			fmt.Sprintf(
				"event[%s] max retry exhausted (%d), envelope_id=%s",
				c.runner.GetName(),
				*c.option.MaxRetry,
				env.ID,
			),
		)
		if err := c.driver.MoveToDead(ctx, c.processingKey(), c.deadKey(), raw); err != nil {
			c.logger.Errorf("taskx: event[%s][%d] move-to-dead failed: %v", c.runner.GetName(), id, err)
		}
		return
	}

	newRaw := env.Encode()
	if err := c.driver.RetryRequeue(ctx, c.processingKey(), c.pendingKey(), raw, newRaw); err != nil {
		c.logger.Errorf("taskx: event[%s][%d] retry-requeue failed: %v", c.runner.GetName(), id, err)
	}
}
