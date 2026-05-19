package consumer

import (
	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/driver"
	redis_provider "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/provider/redis"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/queue"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/timer"
)

// NewRedisConsumer 快捷构造：传入 redis.Cmdable 自动创建 Redis 驱动并组装 Consumer。
func NewRedisConsumer(rdb redis.Cmdable, registry *Registry, opts ...Option) *Consumer {
	ep := redis_provider.NewEventQueueProvider(rdb)
	dp := redis_provider.NewDelayQueueProvider(rdb)
	lp := redis_provider.NewLockProvider(rdb)

	allOpts := []Option{
		WithEventQueueDriver(ep),
		WithDelayQueueDriver(dp),
		WithLockDriver(lp),
	}
	allOpts = append(allOpts, opts...)

	c := New(registry, allOpts...)

	ep.SetRecoverBatchSize(c.cfg.RecoverBatchSize)
	dp.SetRecoverBatchSize(c.cfg.RecoverBatchSize)

	c.SetDefaultFactories()

	return c
}

// SetDefaultFactories 设置默认的消费器/调度器工厂（Redis 实现）。
// 仅在 Consumer 未运行时调用，NewRedisConsumer 已自动调用。
func (c *Consumer) SetDefaultFactories() {
	_ = c.SetEventConsumerFactory(newEventConsumerFactory)
	_ = c.SetDelayConsumerFactory(newDelayConsumerFactory)
	_ = c.SetTimerSchedulerFactory(newTimerSchedulerFactory)
}

func newEventConsumerFactory(cfg queue.EventConsumerConfig) QueueConsumer {
	return queue.NewEventConsumer(cfg)
}

func newDelayConsumerFactory(
	runner core.QueueRunner, opt core.RunnerOption,
	cfg queue.DelayConsumerConfig,
) QueueConsumer {
	return queue.NewDelayConsumer(runner, opt, cfg)
}

func newTimerSchedulerFactory(lk driver.LockDriver, prefix string, cfg *Config) TimerScheduler {
	return timer.NewScheduler(
		lk,
		prefix,
		cfg.LockTTL,
		cfg.InternalOpTimeout,
		cfg.TimerHeartbeatInterval,
		cfg.Logger,
		cfg.OnAlert,
		cfg.OnHeartbeat,
	)
}
