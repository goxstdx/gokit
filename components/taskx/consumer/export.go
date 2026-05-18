package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	redisx "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/provider/redis"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/queue"
)

// NewRedisConsumer 快捷构造：传入 redis.Cmdable 自动创建 Redis 驱动并组装 Consumer。
func NewRedisConsumer(rdb redis.Cmdable, registry *Registry, opts ...Option) *Consumer {
	ep := redisx.NewEventQueueProvider(rdb)
	dp := redisx.NewDelayQueueProvider(rdb)
	lp := redisx.NewLockProvider(rdb)

	allOpts := []Option{
		WithEventQueueDriver(ep),
		WithDelayQueueDriver(dp),
		WithLockDriver(lp),
	}
	allOpts = append(allOpts, opts...)

	c := New(registry, allOpts...)

	ep.SetRecoverBatchSize(c.cfg.RecoverBatchSize)
	dp.SetRecoverBatchSize(c.cfg.RecoverBatchSize)

	return c
}

// RecoverEventDead 从事件队列死信中恢复消息，重置重试计数。
func (c *Consumer) RecoverEventDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	if c.cfg.EventDriver == nil {
		return 0, nil
	}
	groupName := c.resolveEventGroupName(runnerName)
	keys := queue.NewQueueKeySet(c.cfg.KeyPrefix, "event", groupName)
	return recoverEventDeadWithReset(ctx, c.cfg.EventDriver, keys.Dead, keys.Pending, count, c.cfg.Logger, c.cfg.OnAlert)
}

// RecoverDelayDead 从延迟队列死信中恢复消息，重置重试计数。
func (c *Consumer) RecoverDelayDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	if c.cfg.DelayDriver == nil {
		return 0, nil
	}
	keys := queue.NewQueueKeySet(c.cfg.KeyPrefix, "delay", runnerName)
	return recoverDelayDeadWithReset(ctx, c.cfg.DelayDriver, keys.Dead, keys.Pending, count, c.cfg.Logger, c.cfg.OnAlert)
}

func (c *Consumer) resolveEventGroupName(runnerName string) string {
	entries := c.registry.GetEventRunners()
	if entry, ok := entries[runnerName]; ok {
		if entry.Option.QueueGroup != "" {
			return entry.Option.QueueGroup
		}
		return core.DefaultEventQueueGroup
	}
	return runnerName
}

func recoverEventDeadWithReset(
	ctx context.Context,
	drv driver.EventQueueDriver,
	deadKey, pendingKey string,
	count int64,
	logger core.Logger,
	onAlert core.AlertFunc,
) (int64, error) {
	var recovered int64
	for recovered < count {
		raw, err := drv.PopFromDead(ctx, deadKey)
		if err != nil {
			return recovered, err
		}
		if raw == "" {
			break
		}
		env, err := core.DecodeEnvelope(raw)
		if err != nil {
			logger.Warnf("taskx/consumer: recover event dead: skip corrupt message, raw: %s, err: %v", raw, err)
			if onAlert != nil {
				onAlert(core.AlertData{
					Source:    core.AlertSourceEvent,
					AlertType: core.AlertCorruptMessage,
					RunnerResult: core.RunnerFuncResult{
						IsOk: false,
						Err:  fmt.Errorf("recover event dead: corrupt message skipped, raw: %s", raw),
					},
				})
			}
			continue
		}
		env.RetryCount = 0
		env.Source = core.EnvelopeSourceEvent
		if err := drv.Push(ctx, pendingKey, env.Encode()); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}

func recoverDelayDeadWithReset(
	ctx context.Context,
	drv driver.DelayQueueDriver,
	deadKey, pendingKey string,
	count int64,
	logger core.Logger,
	onAlert core.AlertFunc,
) (int64, error) {
	var recovered int64
	for recovered < count {
		raw, err := drv.PopFromDead(ctx, deadKey)
		if err != nil {
			return recovered, err
		}
		if raw == "" {
			break
		}
		env, err := core.DecodeEnvelope(raw)
		if err != nil {
			logger.Warnf("taskx/consumer: recover delay dead: skip corrupt message, raw: %s, err: %v", raw, err)
			if onAlert != nil {
				onAlert(core.AlertData{
					Source:    core.AlertSourceDelay,
					AlertType: core.AlertCorruptMessage,
					RunnerResult: core.RunnerFuncResult{
						IsOk: false,
						Err:  fmt.Errorf("recover delay dead: corrupt message skipped, raw: %s", raw),
					},
				})
			}
			continue
		}
		env.RetryCount = 0
		env.Source = core.EnvelopeSourceDelay
		if err := drv.Add(ctx, pendingKey, env.Encode(), time.Now().UnixMicro()); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}
