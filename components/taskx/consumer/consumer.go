package consumer

import (
	"context"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

// HealthSnapshot / QueueListenerHealth / TimerListenerHealth 与 taskx.Manager 同源。
type HealthSnapshot = core.HealthSnapshot
type QueueListenerHealth = core.QueueListenerHealth
type TimerListenerHealth = core.TimerListenerHealth

// managerFacade 定义 Consumer 需要的 Manager 接口，避免直接暴露包级类型。
type managerFacade interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	HealthSnapshot() HealthSnapshot
	HealthOK() bool
	Config() *Config
	RecoverEventDead(ctx context.Context, runnerName string, count int64) (int64, error)
	RecoverDelayDead(ctx context.Context, runnerName string, count int64) (int64, error)
	ExecuteTimerTaskOnce(ctx context.Context, req core.TimerExecuteRequest) (core.RunnerFuncResult, error)
	NewProducer() *producer.Producer
}

// Consumer 任务消费者，管理 EventQueue / DelayQueue / TimerTask 的消费生命周期。
// 内部委托给已验证的 taskx.Manager 实现，自身仅为薄包装。
type Consumer struct {
	mgr      managerFacade
	registry *Registry
}

// New 创建 Consumer。
// 内部创建 taskx.Manager 并设置默认工厂；opts 与 taskx.NewManager 完全相同。
func New(registry *Registry, opts ...Option) *Consumer {
	mgr := bootstrap(registry, opts...)
	return &Consumer{mgr: mgr, registry: registry}
}

// NewRedisConsumer 快捷构造：传入 redis.Cmdable 自动创建 Redis 驱动并组装 Consumer。
func NewRedisConsumer(rdb redis.Cmdable, registry *Registry, opts ...Option) *Consumer {
	mgr := bootstrapRedis(rdb, registry, opts...)
	return &Consumer{mgr: mgr, registry: registry}
}

// Start 启动所有已注册的队列消费者和定时任务。
func (c *Consumer) Start(ctx context.Context) error { return c.mgr.Start(ctx) }

// Stop 优雅停止所有队列消费者和定时任务。
func (c *Consumer) Stop(ctx context.Context) error { return c.mgr.Stop(ctx) }

// GetHealthSnapshot 返回最近一次健康快照。
func (c *Consumer) GetHealthSnapshot() HealthSnapshot { return c.mgr.HealthSnapshot() }

// HealthOK 返回消费链路是否健康。
func (c *Consumer) HealthOK() bool { return c.mgr.HealthOK() }

// Config 获取消费者配置
func (c *Consumer) Config() *Config { return c.mgr.Config() }

// Registry 获取注册中心
func (c *Consumer) Registry() *Registry { return c.registry }

// RecoverEventDead 从事件队列死信中恢复消息，重置重试计数。
func (c *Consumer) RecoverEventDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	return c.mgr.RecoverEventDead(ctx, runnerName, count)
}

// RecoverDelayDead 从延迟队列死信中恢复消息，重置重试计数。
func (c *Consumer) RecoverDelayDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	return c.mgr.RecoverDelayDead(ctx, runnerName, count)
}

// ExecuteTimerTaskOnce 按任务名手动执行一次定时任务。
func (c *Consumer) ExecuteTimerTaskOnce(ctx context.Context, req core.TimerExecuteRequest) (core.RunnerFuncResult, error) {
	return c.mgr.ExecuteTimerTaskOnce(ctx, req)
}

// NewProducer 基于当前 Consumer 的配置创建一个 Producer 实例。
// 适用于消费端同时需要生产消息的场景。
func (c *Consumer) NewProducer() *producer.Producer {
	return c.mgr.NewProducer()
}
