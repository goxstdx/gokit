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

	c.setDefaultFactories()

	return c
}

// setDefaultFactories 内部构造期直接赋字段，不走公共 Set 方法（无需锁和状态检查）。
func (c *Consumer) setDefaultFactories() {
	c.eventFactory = newEventConsumerFactory
	c.delayFactory = newDelayConsumerFactory
	c.timerFactory = newTimerSchedulerFactory
}

// SetDefaultFactories 设置默认的消费器/调度器工厂（Redis 实现）。
// 仅在 Consumer 空闲时调用，NewRedisConsumer 已自动调用。
func (c *Consumer) SetDefaultFactories() error {
	if err := c.SetEventConsumerFactory(newEventConsumerFactory); err != nil {
		return err
	}
	if err := c.SetDelayConsumerFactory(newDelayConsumerFactory); err != nil {
		return err
	}
	if err := c.SetTimerSchedulerFactory(newTimerSchedulerFactory); err != nil {
		return err
	}
	return nil
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
