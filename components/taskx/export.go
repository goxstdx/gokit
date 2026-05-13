package taskx

import (
	"context"
	"fmt"
	"time"

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

	// 将配置中的 RecoverBatchSize 传递给 Redis provider
	ep.SetRecoverBatchSize(mgr.cfg.RecoverBatchSize)
	dp.SetRecoverBatchSize(mgr.cfg.RecoverBatchSize)

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
		cfg.KeyPrefix, cfg.LockTTL, cfg.ProcessingTimeout, cfg.InternalOpTimeout, cfg.Logger, cfg.OnAlert, cfg.OnHeartbeat, cfg.TraceContextKey,
	)
}

func newDelayConsumerFactory(
	runner core.QueueRunner, opt core.RunnerOption,
	dq driver.DelayQueueDriver, lk driver.LockDriver,
	cfg *ManagerConfig,
) consumer {
	return queue.NewDelayConsumer(
		runner, opt, dq, lk,
		cfg.KeyPrefix, cfg.LockTTL, cfg.PollInterval, cfg.ProcessingTimeout, cfg.InternalOpTimeout, cfg.Logger, cfg.OnAlert, cfg.OnHeartbeat, cfg.TraceContextKey,
	)
}

func newTimerSchedulerFactory(lk driver.LockDriver, prefix string, cfg *ManagerConfig) timerScheduler {
	return timer.NewScheduler(lk, prefix, cfg.LockTTL, cfg.InternalOpTimeout, cfg.TimerHeartbeatInterval, cfg.Logger, cfg.OnAlert, cfg.OnHeartbeat)
}

// RecoverEventDead 从事件队列死信中恢复消息，重置重试计数。
// 格式损坏的消息会被跳过（已从 dead 中弹出但不推入 pending），并通过 OnAlert 通知调用方。
// 当前实现为 best-effort：先从 dead 弹出，再重置 RetryCount 后推回 pending。
// 若弹出后进程退出、ctx 超时或 Redis 写 pending 失败，该条消息可能无法自动恢复。
// 后续可按可靠性要求选择：不弹出先复制、Lua 原子恢复但不重写 envelope、或引入 recovering 中间队列。
func (m *Manager) RecoverEventDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	cfg := m.Config()
	if cfg.EventDriver == nil {
		return 0, nil
	}
	deadKey := cfg.KeyPrefix + ":event:{" + runnerName + "}:dead"
	pendingKey := cfg.KeyPrefix + ":event:{" + runnerName + "}:pending"
	return recoverEventDeadWithReset(ctx, cfg.EventDriver, deadKey, pendingKey, count, cfg.Logger, cfg.OnAlert)
}

// RecoverDelayDead 从延迟队列死信中恢复消息，重置重试计数。
// 格式损坏的消息会被跳过（已从 dead 中弹出但不推入 pending），并通过 OnAlert 通知调用方。
// 当前实现为 best-effort：先从 dead 弹出，再重置 RetryCount 后推回 pending。
// 若弹出后进程退出、ctx 超时或 Redis 写 pending 失败，该条消息可能无法自动恢复。
// 后续可按可靠性要求选择：不弹出先复制、Lua 原子恢复但不重写 envelope、或引入 recovering 中间队列。
func (m *Manager) RecoverDelayDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	cfg := m.Config()
	if cfg.DelayDriver == nil {
		return 0, nil
	}
	deadKey := cfg.KeyPrefix + ":delay:{" + runnerName + "}:dead"
	pendingKey := cfg.KeyPrefix + ":delay:{" + runnerName + "}:pending"
	return recoverDelayDeadWithReset(ctx, cfg.DelayDriver, deadKey, pendingKey, count, cfg.Logger, cfg.OnAlert)
}

// recoverEventDeadWithReset 逐条弹出死信、重置 RetryCount、推入 pending
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
			logger.Warnf("taskx: recover event dead: skip corrupt message, raw: %s, err: %v", raw, err)
			if onAlert != nil {
				onAlert(
					core.AlertData{
						Source:       core.AlertSourceEvent,
						AlertType:    core.AlertCorruptMessage,
						RunnerResult: core.RunnerFuncResult{IsOk: false, Err: fmt.Errorf("recover event dead: corrupt message skipped, raw: %s", raw)},
					},
				)
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

// recoverDelayDeadWithReset 逐条弹出死信、重置 RetryCount、推入 pending
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
			logger.Warnf("taskx: recover delay dead: skip corrupt message, raw: %s, err: %v", raw, err)
			if onAlert != nil {
				onAlert(
					core.AlertData{
						Source:       core.AlertSourceDelay,
						AlertType:    core.AlertCorruptMessage,
						RunnerResult: core.RunnerFuncResult{IsOk: false, Err: fmt.Errorf("recover delay dead: corrupt message skipped, raw: %s", raw)},
					},
				)
			}
			continue
		}
		env.RetryCount = 0
		env.Source = core.EnvelopeSourceDelay
		if err := drv.Add(ctx, pendingKey, env.Encode(), time.Now().Unix()); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}
