package taskx

import (
	"context"
	"fmt"
	"sync"
	"time"

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

	monitorCancel context.CancelFunc
	monitorWG     sync.WaitGroup

	healthMu       sync.RWMutex
	eventBeatAt    map[string]time.Time
	delayBeatAt    map[string]time.Time
	timerBeatAt    time.Time
	healthSnapshot ManagerHealthSnapshot
}

// QueueListenerHealth 队列监听健康信息
type QueueListenerHealth struct {
	Alive      bool
	LastBeatAt time.Time
	PendingLen int64
	LenError   string
}

// TimerListenerHealth 定时调度器健康信息
type TimerListenerHealth struct {
	Alive      bool
	LastBeatAt time.Time
}

// ManagerHealthSnapshot 管理器健康快照
type ManagerHealthSnapshot struct {
	Running   bool
	CheckedAt time.Time
	Event     map[string]QueueListenerHealth
	Delay     map[string]QueueListenerHealth
	Timer     TimerListenerHealth
}

// NewManager 创建任务管理器
func NewManager(registry *Registry, opts ...Option) *Manager {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}
	return &Manager{
		cfg:         cfg,
		registry:    registry,
		eventBeatAt: make(map[string]time.Time),
		delayBeatAt: make(map[string]time.Time),
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
	if m.cfg.HealthInterval <= 0 {
		m.cfg.HealthInterval = 5 * time.Second
	}
	if m.cfg.HealthBeatTimeout <= 0 {
		m.cfg.HealthBeatTimeout = 15 * time.Second
	}
	m.cfg.OnHeartbeat = m.recordHeartbeat

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
	m.refreshHealthSnapshot(context.Background(), true)
	m.startMonitorLocked()
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

	if m.monitorCancel != nil {
		m.monitorCancel()
		m.monitorCancel = nil
	}
	m.monitorWG.Wait()

	if m.scheduler != nil {
		stopCtx := m.scheduler.Stop()
		<-stopCtx.Done()
		m.scheduler = nil
	}

	m.stopConsumersLocked()
	m.cfg.OnHeartbeat = nil

	m.running = false
	m.refreshHealthSnapshot(context.Background(), false)
	m.cfg.Logger.Infof("taskx: manager stopped")
	return nil
}

// PublishEvent 发布事件到 EventQueue
func (m *Manager) PublishEvent(ctx context.Context, runner core.QueueRunner) error {
	return m.PublishEventPayload(ctx, runner.GetName(), runner.Marshal())
}

// PublishDelay 发布延迟任务到 DelayQueue
func (m *Manager) PublishDelay(ctx context.Context, runner core.QueueRunner, executeAt int64) error {
	return m.PublishDelayPayload(ctx, runner.GetName(), runner.Marshal(), executeAt)
}

// PublishEventPayload 直接将 payload 包装为新消息并发布到 EventQueue。
func (m *Manager) PublishEventPayload(ctx context.Context, runnerName string, payload string) error {
	if m.cfg.EventDriver == nil {
		return fmt.Errorf("taskx: event queue driver not configured")
	}
	env := core.NewEnvelope(payload)
	key := fmt.Sprintf("%s:event:{%s}:pending", m.cfg.KeyPrefix, runnerName)
	return m.cfg.EventDriver.Push(ctx, key, env.Encode())
}

// PublishDelayPayload 直接将 payload 包装为新消息并发布到 DelayQueue。
func (m *Manager) PublishDelayPayload(ctx context.Context, runnerName string, payload string, executeAt int64) error {
	if m.cfg.DelayDriver == nil {
		return fmt.Errorf("taskx: delay queue driver not configured")
	}
	env := core.NewEnvelope(payload)
	key := fmt.Sprintf("%s:delay:{%s}:pending", m.cfg.KeyPrefix, runnerName)
	return m.cfg.DelayDriver.Add(ctx, key, env.Encode(), executeAt)
}

// Registry 获取注册中心
func (m *Manager) Registry() *Registry {
	return m.registry
}

// HealthSnapshot 返回最近一次健康快照。
func (m *Manager) HealthSnapshot() ManagerHealthSnapshot {
	m.healthMu.RLock()
	defer m.healthMu.RUnlock()

	cp := ManagerHealthSnapshot{
		Running:   m.healthSnapshot.Running,
		CheckedAt: m.healthSnapshot.CheckedAt,
		Event:     make(map[string]QueueListenerHealth, len(m.healthSnapshot.Event)),
		Delay:     make(map[string]QueueListenerHealth, len(m.healthSnapshot.Delay)),
		Timer:     m.healthSnapshot.Timer,
	}
	for k, v := range m.healthSnapshot.Event {
		cp.Event[k] = v
	}
	for k, v := range m.healthSnapshot.Delay {
		cp.Delay[k] = v
	}
	return cp
}

// HealthOK 返回监听链路是否健康，可直接用于健康检查。
func (m *Manager) HealthOK() bool {
	snap := m.HealthSnapshot()
	if !snap.Running {
		return false
	}

	eventEnabled := m.cfg.EventDriver != nil && m.eventFactory != nil
	delayEnabled := m.cfg.DelayDriver != nil && m.delayFactory != nil
	timerEnabled := m.cfg.LockDriver != nil && m.timerFactory != nil && len(m.registry.GetTimerTasks()) > 0

	if eventEnabled {
		for _, st := range snap.Event {
			if !st.Alive || st.LenError != "" {
				return false
			}
		}
	}
	if delayEnabled {
		for _, st := range snap.Delay {
			if !st.Alive || st.LenError != "" {
				return false
			}
		}
	}
	if timerEnabled && !snap.Timer.Alive {
		return false
	}

	return true
}

func (m *Manager) stopConsumersLocked() {
	for _, c := range m.consumers {
		c.Stop()
	}
	m.consumers = nil
}

func (m *Manager) recordHeartbeat(hb core.ListenerHeartbeat) {
	beatAt := hb.At
	if beatAt.IsZero() {
		beatAt = time.Now()
	}

	m.healthMu.Lock()
	defer m.healthMu.Unlock()
	switch hb.Kind {
	case core.ListenerKindEvent:
		m.eventBeatAt[hb.Name] = beatAt
	case core.ListenerKindDelay:
		m.delayBeatAt[hb.Name] = beatAt
	case core.ListenerKindTimer:
		m.timerBeatAt = beatAt
	}
}

func (m *Manager) startMonitorLocked() {
	ctx, cancel := context.WithCancel(context.Background())
	m.monitorCancel = cancel
	m.monitorWG.Add(1)
	go func() {
		defer m.monitorWG.Done()
		ticker := time.NewTicker(m.cfg.HealthInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.refreshHealthSnapshot(ctx, true)
			}
		}
	}()
}

func (m *Manager) refreshHealthSnapshot(ctx context.Context, running bool) {
	now := time.Now()
	eventEntries := m.registry.GetEventRunners()
	delayEntries := m.registry.GetDelayRunners()

	m.healthMu.RLock()
	eventBeat := make(map[string]time.Time, len(m.eventBeatAt))
	delayBeat := make(map[string]time.Time, len(m.delayBeatAt))
	for k, v := range m.eventBeatAt {
		eventBeat[k] = v
	}
	for k, v := range m.delayBeatAt {
		delayBeat[k] = v
	}
	timerBeat := m.timerBeatAt
	m.healthMu.RUnlock()

	snap := ManagerHealthSnapshot{
		Running:   running,
		CheckedAt: now,
		Event:     make(map[string]QueueListenerHealth, len(eventEntries)),
		Delay:     make(map[string]QueueListenerHealth, len(delayEntries)),
	}

	for name := range eventEntries {
		item := QueueListenerHealth{LastBeatAt: eventBeat[name]}
		item.Alive = !item.LastBeatAt.IsZero() && now.Sub(item.LastBeatAt) <= m.cfg.HealthBeatTimeout
		if m.cfg.EventDriver != nil {
			lenCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			n, err := m.cfg.EventDriver.Len(lenCtx, fmt.Sprintf("%s:event:{%s}:pending", m.cfg.KeyPrefix, name))
			cancel()
			if err != nil {
				item.LenError = err.Error()
			} else {
				item.PendingLen = n
			}
		}
		snap.Event[name] = item
	}

	for name := range delayEntries {
		item := QueueListenerHealth{LastBeatAt: delayBeat[name]}
		item.Alive = !item.LastBeatAt.IsZero() && now.Sub(item.LastBeatAt) <= m.cfg.HealthBeatTimeout
		if m.cfg.DelayDriver != nil {
			lenCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			n, err := m.cfg.DelayDriver.Len(lenCtx, fmt.Sprintf("%s:delay:{%s}:pending", m.cfg.KeyPrefix, name))
			cancel()
			if err != nil {
				item.LenError = err.Error()
			} else {
				item.PendingLen = n
			}
		}
		snap.Delay[name] = item
	}

	snap.Timer = TimerListenerHealth{
		LastBeatAt: timerBeat,
		Alive:      !timerBeat.IsZero() && now.Sub(timerBeat) <= m.cfg.HealthBeatTimeout,
	}

	m.healthMu.Lock()
	m.healthSnapshot = snap
	m.healthMu.Unlock()
}
