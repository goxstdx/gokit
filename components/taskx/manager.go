package taskx

import (
	"context"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/consumer"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

type QueueListenerHealth = core.QueueListenerHealth
type TimerListenerHealth = core.TimerListenerHealth
type ManagerHealthSnapshot = core.HealthSnapshot

// Manager 任务管理器，组合 Consumer（消费生命周期）+ Producer（消息推送）。
// 对于只需消费或只需生产的场景，可分别使用 consumer.Consumer / producer.Producer。
type Manager struct {
	*consumer.Consumer
	producer *producer.Producer
}

// NewManager 创建任务管理器
func NewManager(registry *Registry, opts ...Option) *Manager {
	c := consumer.New(registry, opts...)
	m := &Manager{Consumer: c}
	m.rebuildProducer()
	return m
}

// Start 启动所有已注册的队列和任务
func (m *Manager) Start(ctx context.Context) error {
	if err := m.Consumer.Start(ctx); err != nil {
		return err
	}
	m.rebuildProducer()
	return nil
}
