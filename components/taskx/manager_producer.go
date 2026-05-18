package taskx

import (
	"context"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

// rebuildProducer 根据当前 cfg/registry 重建内部 Producer。
// 在 NewManager 和 Start（alert dispatcher 就绪后）各调用一次。
func (m *Manager) rebuildProducer() {
	m.producer = producer.New(producer.Config{
		EventDriver:       m.cfg.EventDriver,
		DelayDriver:       m.cfg.DelayDriver,
		KeyPrefix:         m.cfg.KeyPrefix,
		Logger:            m.cfg.Logger,
		OnAlert:           m.enqueueAlert,
		ResolveEventGroup: m.resolveEventGroupNameStrict,
		IsDelayRegistered: m.isDelayRegistered,
	})
}

func (m *Manager) isDelayRegistered(runnerName string) bool {
	_, ok := m.registry.GetDelayRunners()[runnerName]
	return ok
}

// NewProducer 基于当前 Manager 的配置创建一个独立的 Producer 副本。
// 适用于需要将 Producer 传递给其他模块使用的场景。
func (m *Manager) NewProducer() *producer.Producer {
	return producer.New(producer.Config{
		EventDriver:       m.cfg.EventDriver,
		DelayDriver:       m.cfg.DelayDriver,
		KeyPrefix:         m.cfg.KeyPrefix,
		Logger:            m.cfg.Logger,
		OnAlert:           m.enqueueAlert,
		ResolveEventGroup: m.resolveEventGroupNameStrict,
		IsDelayRegistered: m.isDelayRegistered,
	})
}

// --- Publish 方法委托给内部 Producer ---

// PublishEvent 发布事件到 EventQueue，并返回创建的 Envelope。
func (m *Manager) PublishEvent(ctx context.Context, runner core.QueueRunner) (*core.Envelope, error) {
	return m.producer.PublishEvent(ctx, runner)
}

// PublishEventPayload 直接将 payload 包装为新消息并发布到 EventQueue。
func (m *Manager) PublishEventPayload(ctx context.Context, runnerName string, payload string) (*core.Envelope, error) {
	return m.producer.PublishEventPayload(ctx, runnerName, payload)
}

// PublishEventEnvelope 将指定 Envelope 发布到 EventQueue。
// 若 runnerName 未在 Registry 中注册，消息将被推入死信队列并触发告警。
func (m *Manager) PublishEventEnvelope(ctx context.Context, runnerName string, env *core.Envelope) (*core.Envelope, error) {
	return m.producer.PublishEventEnvelope(ctx, runnerName, env)
}

// PublishDelay 发布延迟任务到 DelayQueue，并返回创建的 Envelope。
func (m *Manager) PublishDelay(ctx context.Context, runner core.QueueRunner, executeAt time.Time) (*core.Envelope, error) {
	return m.producer.PublishDelay(ctx, runner, executeAt)
}

// PublishDelayPayload 直接将 payload 包装为新消息并发布到 DelayQueue。
func (m *Manager) PublishDelayPayload(ctx context.Context, runnerName string, payload string, executeAt time.Time) (*core.Envelope, error) {
	return m.producer.PublishDelayPayload(ctx, runnerName, payload, executeAt)
}

// PublishDelayEnvelope 将指定 Envelope 发布到 DelayQueue。
// 若 runnerName 未在 Registry 中注册，消息将被推入死信队列并触发告警。
func (m *Manager) PublishDelayEnvelope(ctx context.Context, runnerName string, env *core.Envelope, executeAt time.Time) (*core.Envelope, error) {
	return m.producer.PublishDelayEnvelope(ctx, runnerName, env, executeAt)
}
