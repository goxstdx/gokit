package taskx

import (
	"sync"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/consumer"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

// Manager 任务管理器，组合 Consumer（消费生命周期）+ Producer（消息推送）。
// 对于只需消费或只需生产的场景，可分别使用 consumer.Consumer / producer.Producer。
type Manager struct {
	consumer *consumer.Consumer

	pmu      sync.RWMutex
	producer *producer.Producer
}

// NewManager 创建任务管理器
func NewManager(registry *Registry, opts ...Option) *Manager {
	c := consumer.New(registry, opts...)
	m := &Manager{consumer: c}
	m.rebuildProducer()
	return m
}

// NewRedisManager 快捷构造：传入 redis.Cmdable 自动创建 Redis 驱动并组装 Manager。
func NewRedisManager(rdb redis.Cmdable, registry *Registry, opts ...Option) *Manager {
	c := consumer.NewRedisConsumer(rdb, registry, opts...)
	m := &Manager{consumer: c}
	m.rebuildProducer()
	return m
}
