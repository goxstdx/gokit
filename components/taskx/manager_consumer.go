package taskx

import (
	"context"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/consumer"
)

// Start 启动所有已注册的队列和任务。
// Start 完成后会重建内部 Producer：
// - 消费态下 Producer 的 OnAlert 需要路由到内部 enqueueAlert（异步队列分发）
// - Producer 是配置快照对象，不会自动感知 Consumer 状态变化
func (m *Manager) Start(ctx context.Context) error {
	if err := m.consumer.Start(ctx); err != nil {
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
	if err := m.consumer.Stop(ctx); err != nil {
		return err
	}
	m.rebuildProducer()
	return nil
}

// Running 返回是否正在运行。
func (m *Manager) Running() bool {
	return m.consumer.Running()
}

// CheckStartReady 校验当前配置是否满足启动条件。
func (m *Manager) CheckStartReady(ctx context.Context) error {
	return m.consumer.CheckStartReady(ctx)
}

// HealthSnapshot 返回最近一次健康快照。
func (m *Manager) HealthSnapshot() ManagerHealthSnapshot {
	return m.consumer.HealthSnapshot()
}

// HealthOK 返回监听链路是否健康。
func (m *Manager) HealthOK() bool {
	return m.consumer.HealthOK()
}

// RecoverEventDead 按指定 runner 所属的事件组恢复死信消息，并重置重试计数。
func (m *Manager) RecoverEventDead(ctx context.Context, runner QueueRunner, count int64) (int64, error) {
	return m.consumer.RecoverEventDead(ctx, runner, count)
}

// RecoverDelayDead 从延迟队列死信中恢复消息，重置重试计数。
func (m *Manager) RecoverDelayDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	return m.consumer.RecoverDelayDead(ctx, runnerName, count)
}

// GetConsumer 这里注意，获取的是内部使用的 Consumer 实例。如果 consumer stop 或 发生变更，manager 的 consumer 会同步变更。
func (m *Manager) GetConsumer() *consumer.Consumer {
	return m.consumer
}
