package taskx

import (
	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/consumer"
)

// NewRedisManager 快捷构造：传入 redis.Cmdable 自动创建 Redis 驱动并组装 Manager。
func NewRedisManager(rdb redis.Cmdable, registry *Registry, opts ...Option) *Manager {
	c := consumer.NewRedisConsumer(rdb, registry, opts...)
	m := &Manager{Consumer: c}
	m.rebuildProducer()
	return m
}
