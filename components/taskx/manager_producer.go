package taskx

import (
	"context"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

func (m *Manager) buildProducer() *producer.Producer {
	cfg := m.Consumer.ProducerSnapshot()
	return producer.New(producer.Config{
		EventDriver:       cfg.EventDriver,
		DelayDriver:       cfg.DelayDriver,
		KeyPrefix:         cfg.KeyPrefix,
		Logger:            cfg.Logger,
		OnAlert:           cfg.OnAlert,
		ResolveEventGroup: m.Consumer.EventGroupResolver(),
		IsDelayRegistered: m.Consumer.DelayRegisteredChecker(),
	})
}

func (m *Manager) rebuildProducer() {
	p := m.buildProducer()
	m.pmu.Lock()
	m.producer = p
	m.pmu.Unlock()
}

func (m *Manager) getProducer() *producer.Producer {
	m.pmu.RLock()
	p := m.producer
	m.pmu.RUnlock()
	return p
}

// NewProducer 基于当前 Manager 的配置创建一个独立的 Producer 副本。
// 注意：返回的 Producer 是快照，后续 Start/Stop 不会影响已创建的实例。
// 建议在 Start 之后调用，以获得与消费侧一致的告警路径。
func (m *Manager) NewProducer() *producer.Producer {
	return m.buildProducer()
}

// --- Publish 方法委托给内部 Producer ---

// PublishEvent 发布事件到 EventQueue，并返回创建的 Envelope。
func (m *Manager) PublishEvent(ctx context.Context, runner core.QueueRunner) (*core.Envelope, error) {
	return m.getProducer().PublishEvent(ctx, runner)
}

// PublishEventPayload 直接将 payload 包装为新消息并发布到 EventQueue。
func (m *Manager) PublishEventPayload(ctx context.Context, runnerName string, payload string) (*core.Envelope, error) {
	return m.getProducer().PublishEventPayload(ctx, runnerName, payload)
}

// PublishEventEnvelope 将指定 Envelope 发布到 EventQueue。
// 若 runnerName 未在 Registry 中注册，消息将被推入死信队列并触发告警。
func (m *Manager) PublishEventEnvelope(ctx context.Context, runnerName string, env *core.Envelope) (*core.Envelope, error) {
	return m.getProducer().PublishEventEnvelope(ctx, runnerName, env)
}

// PublishDelay 发布延迟任务到 DelayQueue，并返回创建的 Envelope。
func (m *Manager) PublishDelay(ctx context.Context, runner core.QueueRunner, executeAt time.Time) (*core.Envelope, error) {
	return m.getProducer().PublishDelay(ctx, runner, executeAt)
}

// PublishDelayPayload 直接将 payload 包装为新消息并发布到 DelayQueue。
func (m *Manager) PublishDelayPayload(ctx context.Context, runnerName string, payload string, executeAt time.Time) (*core.Envelope, error) {
	return m.getProducer().PublishDelayPayload(ctx, runnerName, payload, executeAt)
}

// PublishDelayEnvelope 将指定 Envelope 发布到 DelayQueue。
// 若 runnerName 未在 Registry 中注册，消息将被推入死信队列并触发告警。
func (m *Manager) PublishDelayEnvelope(ctx context.Context, runnerName string, env *core.Envelope, executeAt time.Time) (*core.Envelope, error) {
	return m.getProducer().PublishDelayEnvelope(ctx, runnerName, env, executeAt)
}
