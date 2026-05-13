package taskx

import (
	"context"
	"fmt"
	"sync"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

// consumer 内部消费器接口
type consumer interface {
	Start(ctx context.Context) error
	Stop()
}

// timerScheduler 内部定时任务调度器接口
type timerScheduler interface {
	Register(task core.TimerTaskRunner, opt core.TimerTaskOption) error
	Start()
	Stop() context.Context
}

// TimerSchedulerFactory 定时任务调度器工厂函数
type TimerSchedulerFactory func(lk driver.LockDriver, prefix string, cfg *ManagerConfig) timerScheduler

// EventConsumerFactory 事件队列消费器工厂函数
type EventConsumerFactory func(
	runner core.QueueRunner, opt core.RunnerOption,
	eq driver.EventQueueDriver, lk driver.LockDriver,
	cfg *ManagerConfig,
) consumer

// DelayConsumerFactory 延迟队列消费器工厂函数
type DelayConsumerFactory func(
	runner core.QueueRunner, opt core.RunnerOption,
	dq driver.DelayQueueDriver, lk driver.LockDriver,
	cfg *ManagerConfig,
) consumer

// Manager 任务管理器，统一管理 EventQueue、DelayQueue、TimerTask 的生命周期
type Manager struct {
	cfg      *ManagerConfig
	registry *Registry

	eventFactory EventConsumerFactory
	delayFactory DelayConsumerFactory
	timerFactory TimerSchedulerFactory

	mu        sync.Mutex
	consumers []consumer
	scheduler timerScheduler
	running   bool
}

// NewManager 创建任务管理器
func NewManager(registry *Registry, opts ...Option) *Manager {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}
	return &Manager{
		cfg:      cfg,
		registry: registry,
	}
}

// SetEventConsumerFactory 设置事件队列消费器工厂
func (m *Manager) SetEventConsumerFactory(f EventConsumerFactory) {
	m.eventFactory = f
}

// SetDelayConsumerFactory 设置延迟队列消费器工厂
func (m *Manager) SetDelayConsumerFactory(f DelayConsumerFactory) {
	m.delayFactory = f
}

// SetTimerSchedulerFactory 设置定时任务调度器工厂
func (m *Manager) SetTimerSchedulerFactory(f TimerSchedulerFactory) {
	m.timerFactory = f
}

// Config 获取管理器配置
func (m *Manager) Config() *ManagerConfig {
	return m.cfg
}

// Start 启动所有已注册的队列和任务
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("taskx: manager already running")
	}

	if m.cfg.Logger == nil {
		return fmt.Errorf("taskx: logger is required, use WithLogger() to set")
	}

	// 检测驱动版本兼容性（如 BLMOVE 要求 Redis >= 6.2）
	type versionChecker interface {
		CheckVersion(ctx context.Context) error
	}
	if v, ok := m.cfg.EventDriver.(versionChecker); ok {
		if err := v.CheckVersion(ctx); err != nil {
			return err
		}
	}

	// 启动 EventQueue 消费者
	if m.cfg.EventDriver != nil && m.eventFactory != nil {
		for _, entry := range m.registry.GetEventRunners() {
			c := m.eventFactory(entry.Runner, entry.Option, m.cfg.EventDriver, m.cfg.LockDriver, m.cfg)
			if err := c.Start(ctx); err != nil {
				m.stopConsumersLocked()
				return fmt.Errorf("taskx: start event[%s]: %w", entry.Runner.GetName(), err)
			}
			m.consumers = append(m.consumers, c)
		}
	}

	// 启动 DelayQueue 消费者
	if m.cfg.DelayDriver != nil && m.delayFactory != nil {
		for _, entry := range m.registry.GetDelayRunners() {
			c := m.delayFactory(entry.Runner, entry.Option, m.cfg.DelayDriver, m.cfg.LockDriver, m.cfg)
			if err := c.Start(ctx); err != nil {
				m.stopConsumersLocked()
				return fmt.Errorf("taskx: start delay[%s]: %w", entry.Runner.GetName(), err)
			}
			m.consumers = append(m.consumers, c)
		}
	}

	// 启动 TimerTask
	if m.cfg.LockDriver != nil && m.timerFactory != nil {
		s := m.timerFactory(m.cfg.LockDriver, m.cfg.KeyPrefix, m.cfg)
		for _, entry := range m.registry.GetTimerTasks() {
			opt := entry.Option.WithDefaults(m.cfg.DefaultTimerTask)
			if err := s.Register(entry.Task, opt); err != nil {
				m.stopConsumersLocked()
				return fmt.Errorf("taskx: register timer[%s]: %w", entry.Task.GetName(), err)
			}
		}
		s.Start()
		m.scheduler = s
	}

	m.running = true
	m.cfg.Logger.Infof("taskx: manager started")
	return nil
}

// Stop 优雅停止所有队列和任务。
//
// 当前实现会无限等待所有消费者和定时任务退出，传入的 ctx 未用于超时控制。
// 在 K8s 等容器环境中，可依赖 terminationGracePeriodSeconds 作为兜底超时。
// TODO: 后续可考虑监听 ctx.Done()，在超时后强制返回（残留消息由崩溃恢复兜底）。
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	if m.scheduler != nil {
		stopCtx := m.scheduler.Stop()
		<-stopCtx.Done()
		m.scheduler = nil
	}

	m.stopConsumersLocked()

	m.running = false
	m.cfg.Logger.Infof("taskx: manager stopped")
	return nil
}

// PublishEvent 发布事件到 EventQueue
func (m *Manager) PublishEvent(ctx context.Context, runner core.QueueRunner) error {
	if m.cfg.EventDriver == nil {
		return fmt.Errorf("taskx: event queue driver not configured")
	}
	payload := runner.Marshal()
	env := core.NewEnvelope(payload)
	key := fmt.Sprintf("%s:event:{%s}:pending", m.cfg.KeyPrefix, runner.GetName())
	return m.cfg.EventDriver.Push(ctx, key, env.Encode())
}

// PublishDelay 发布延迟任务到 DelayQueue
func (m *Manager) PublishDelay(ctx context.Context, runner core.QueueRunner, executeAt int64) error {
	if m.cfg.DelayDriver == nil {
		return fmt.Errorf("taskx: delay queue driver not configured")
	}
	payload := runner.Marshal()
	env := core.NewEnvelope(payload)
	key := fmt.Sprintf("%s:delay:{%s}:pending", m.cfg.KeyPrefix, runner.GetName())
	return m.cfg.DelayDriver.Add(ctx, key, env.Encode(), executeAt)
}

// Registry 获取注册中心
func (m *Manager) Registry() *Registry {
	return m.registry
}

func (m *Manager) stopConsumersLocked() {
	for _, c := range m.consumers {
		c.Stop()
	}
	m.consumers = nil
}
