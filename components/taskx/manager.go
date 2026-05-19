package taskx

import (
	"context"
	"sync"

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

	pmu      sync.RWMutex
	producer *producer.Producer
}

// NewManager 创建任务管理器
func NewManager(registry *Registry, opts ...Option) *Manager {
	c := consumer.New(registry, opts...)
	m := &Manager{Consumer: c}
	m.rebuildProducer()
	return m
}

// Start 启动所有已注册的队列和任务。
// Start 完成后会重建内部 Producer：
// - 消费态下 Producer 的 OnAlert 需要路由到内部 enqueueAlert（异步队列分发）
// - Producer 是配置快照对象，不会自动感知 Consumer 状态变化
func (m *Manager) Start(ctx context.Context) error {
	if err := m.Consumer.Start(ctx); err != nil {
		return err
	}
	m.rebuildProducer()
	return nil
}

// Stop 优雅停止所有队列和任务。
// Stop 完成后会重建 Producer：
// - 停机后内部 alert dispatcher 已关闭，不应继续把告警投递到内部队列
// - 需要恢复为调用方传入的原始 OnAlert 回调链路
func (m *Manager) Stop(ctx context.Context) error {
	if err := m.Consumer.Stop(ctx); err != nil {
		return err
	}
	m.rebuildProducer()
	return nil
}
