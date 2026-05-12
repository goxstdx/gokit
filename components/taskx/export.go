package taskx

import (
	"context"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	redisx "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/provider/redis"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/queue"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/timer"
)

// NewRedisManager 快捷构造：传入 redis.Cmdable 自动创建 Redis 驱动并组装 Manager
func NewRedisManager(rdb redis.Cmdable, registry *Registry, opts ...Option) *Manager {
	ep := redisx.NewEventQueueProvider(rdb)
	dp := redisx.NewDelayQueueProvider(rdb)
	lp := redisx.NewLockProvider(rdb)

	allOpts := []Option{
		WithEventQueueDriver(ep),
		WithDelayQueueDriver(dp),
		WithLockDriver(lp),
	}
	allOpts = append(allOpts, opts...)

	mgr := NewManager(registry, allOpts...)

	mgr.SetEventConsumerFactory(newEventConsumerFactory)
	mgr.SetDelayConsumerFactory(newDelayConsumerFactory)
	mgr.SetTimerSchedulerFactory(newTimerSchedulerFactory)

	return mgr
}

func newEventConsumerFactory(
	runner core.QueueRunner, opt core.RunnerOption,
	eq driver.EventQueueDriver, lk driver.LockDriver,
	cfg *ManagerConfig,
) consumer {
	return queue.NewEventConsumer(
		runner, opt, eq, lk,
		cfg.KeyPrefix, cfg.LockTTL, cfg.ProcessingTimeout, cfg.Logger,
	)
}

func newDelayConsumerFactory(
	runner core.QueueRunner, opt core.RunnerOption,
	dq driver.DelayQueueDriver, lk driver.LockDriver,
	cfg *ManagerConfig,
) consumer {
	return queue.NewDelayConsumer(
		runner, opt, dq, lk,
		cfg.KeyPrefix, cfg.LockTTL, cfg.PollInterval, cfg.ProcessingTimeout, cfg.Logger,
	)
}

func newTimerSchedulerFactory(lk driver.LockDriver, prefix string, cfg *ManagerConfig) timerScheduler {
	return timer.NewScheduler(lk, prefix, cfg.LockTTL, cfg.Logger)
}

// RecoverEventDead 从事件队列死信中恢复消息，重置重试计数
func (m *Manager) RecoverEventDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	cfg := m.Config()
	if cfg.EventDriver == nil {
		return 0, nil
	}
	deadKey := cfg.KeyPrefix + ":event:{" + runnerName + "}:dead"
	pendingKey := cfg.KeyPrefix + ":event:{" + runnerName + "}:pending"
	return recoverEventDeadWithReset(ctx, cfg.EventDriver, deadKey, pendingKey, count)
}

// RecoverDelayDead 从延迟队列死信中恢复消息，重置重试计数
func (m *Manager) RecoverDelayDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	cfg := m.Config()
	if cfg.DelayDriver == nil {
		return 0, nil
	}
	deadKey := cfg.KeyPrefix + ":delay:{" + runnerName + "}:dead"
	pendingKey := cfg.KeyPrefix + ":delay:{" + runnerName + "}:pending"
	return recoverDelayDeadWithReset(ctx, cfg.DelayDriver, deadKey, pendingKey, count)
}

// recoverEventDeadWithReset 逐条弹出死信、重置 RetryCount、推入 pending
func recoverEventDeadWithReset(ctx context.Context, drv driver.EventQueueDriver, deadKey, pendingKey string, count int64) (int64, error) {
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
			if pushErr := drv.Push(ctx, pendingKey, raw); pushErr != nil {
				return recovered, pushErr
			}
			recovered++
			continue
		}
		env.RetryCount = 0
		if err := drv.Push(ctx, pendingKey, env.Encode()); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}

// recoverDelayDeadWithReset 逐条弹出死信、重置 RetryCount、推入 pending
func recoverDelayDeadWithReset(ctx context.Context, drv driver.DelayQueueDriver, deadKey, pendingKey string, count int64) (int64, error) {
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
			if addErr := drv.Add(ctx, pendingKey, raw, 0); addErr != nil {
				return recovered, addErr
			}
			recovered++
			continue
		}
		env.RetryCount = 0
		if err := drv.Add(ctx, pendingKey, env.Encode(), 0); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}
