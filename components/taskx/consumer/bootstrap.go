package consumer

import (
	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
)

// bootstrap 使用 taskx.NewManager + SetDefaultFactories 创建底层 Manager。
func bootstrap(registry *Registry, opts ...Option) *taskx.Manager {
	mgr := taskx.NewManager(registry, opts...)
	mgr.SetDefaultFactories()
	return mgr
}

// bootstrapRedis 使用 taskx.NewRedisManager 创建底层 Manager。
func bootstrapRedis(rdb redis.Cmdable, registry *Registry, opts ...Option) *taskx.Manager {
	return taskx.NewRedisManager(rdb, registry, opts...)
}
