package taskx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
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
	alertCancel   context.CancelFunc
	alertWG       sync.WaitGroup
	alertQueue    chan core.AlertData
	alertHandler  core.AlertFunc

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
	if registry == nil {
		registry = NewRegistry()
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
		m.cfg.HealthInterval = defaults.HealthInterval
	}
	if m.cfg.HealthBeatTimeout <= 0 {
		m.cfg.HealthBeatTimeout = defaults.HealthBeatTimeout
	}
	if m.cfg.EventPopTimeout <= 0 {
		m.cfg.EventPopTimeout = defaults.EventPopTimeout
	}
	if m.cfg.DelayRetryBaseInterval <= 0 {
		m.cfg.DelayRetryBaseInterval = defaults.DelayRetryBaseInterval
	}
	if m.cfg.TimerHeartbeatInterval <= 0 {
		// 默认让 timer 心跳快于超时阈值，避免低阈值场景误报不健康。
		m.cfg.TimerHeartbeatInterval = m.cfg.HealthInterval
		if hb := m.cfg.HealthBeatTimeout / 2; hb > 0 && (m.cfg.TimerHeartbeatInterval <= 0 || hb < m.cfg.TimerHeartbeatInterval) {
			m.cfg.TimerHeartbeatInterval = hb
		}
		if m.cfg.TimerHeartbeatInterval <= 0 {
			m.cfg.TimerHeartbeatInterval = defaults.TimerHeartbeatFallback
		}
		if m.cfg.TimerHeartbeatInterval < defaults.MinTimerHeartbeat {
			m.cfg.TimerHeartbeatInterval = defaults.MinTimerHeartbeat
		}
	}
	if m.cfg.AlertQueueSize <= 0 {
		m.cfg.AlertQueueSize = 1024
	}
	m.cfg.OnHeartbeat = m.recordHeartbeat

	eventEntries := m.registry.GetEventRunners()
	delayEntries := m.registry.GetDelayRunners()
	timerEntries := m.registry.GetTimerTasks()
	if len(eventEntries)+len(delayEntries) > 0 && m.cfg.LockDriver == nil {
		return fmt.Errorf(
			"taskx: lock driver not configured for %d registered queue runner(s) (event=%d, delay=%d)",
			len(eventEntries)+len(delayEntries), len(eventEntries), len(delayEntries),
		)
	}
	if len(eventEntries) > 0 {
		if m.cfg.EventDriver == nil {
			return fmt.Errorf("taskx: event queue driver not configured for %d registered event runner(s)", len(eventEntries))
		}
		if m.eventFactory == nil {
			return fmt.Errorf("taskx: event consumer factory not configured for %d registered event runner(s)", len(eventEntries))
		}
	}
	if len(delayEntries) > 0 {
		if m.cfg.DelayDriver == nil {
			return fmt.Errorf("taskx: delay queue driver not configured for %d registered delay runner(s)", len(delayEntries))
		}
		if m.delayFactory == nil {
			return fmt.Errorf("taskx: delay consumer factory not configured for %d registered delay runner(s)", len(delayEntries))
		}
	}
	if len(timerEntries) > 0 {
		if m.cfg.LockDriver == nil {
			return fmt.Errorf("taskx: lock driver not configured for %d registered timer task(s)", len(timerEntries))
		}
		if m.timerFactory == nil {
			return fmt.Errorf("taskx: timer scheduler factory not configured for %d registered timer task(s)", len(timerEntries))
		}
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
	m.startAlertDispatcherLocked()
	startSucceeded := false
	defer func() {
		if !startSucceeded {
			m.cleanupStartFailureLocked()
		}
	}()

	// 启动 EventQueue 消费者
	if m.cfg.EventDriver != nil && m.eventFactory != nil {
		for _, entry := range eventEntries {
			c := m.eventFactory(entry.Runner, entry.Option, m.cfg.EventDriver, m.cfg.LockDriver, m.cfg)
			if err := c.Start(ctx); err != nil {
				return fmt.Errorf("taskx: start event[%s]: %w", entry.Runner.GetName(), err)
			}
			m.consumers = append(m.consumers, c)
		}
	}

	// 启动 DelayQueue 消费者
	if m.cfg.DelayDriver != nil && m.delayFactory != nil {
		for _, entry := range delayEntries {
			c := m.delayFactory(entry.Runner, entry.Option, m.cfg.DelayDriver, m.cfg.LockDriver, m.cfg)
			if err := c.Start(ctx); err != nil {
				return fmt.Errorf("taskx: start delay[%s]: %w", entry.Runner.GetName(), err)
			}
			m.consumers = append(m.consumers, c)
		}
	}

	// 启动 TimerTask
	if m.cfg.LockDriver != nil && m.timerFactory != nil {
		s := m.timerFactory(m.cfg.LockDriver, m.cfg.KeyPrefix, m.cfg)
		for _, entry := range timerEntries {
			opt := entry.Option.WithDefaults(m.cfg.DefaultTimerTask)
			if err := s.Register(entry.Task, opt); err != nil {
				return fmt.Errorf("taskx: register timer[%s]: %w", entry.Task.GetName(), err)
			}
		}
		s.Start()
		m.scheduler = s
	}

	m.running = true
	snapCtx, snapCancel := m.internalOpContext(context.Background(), 0)
	m.refreshHealthSnapshot(snapCtx, true)
	snapCancel()
	m.startMonitorLocked()
	m.cfg.Logger.Infof("taskx: manager started")
	startSucceeded = true
	return nil
}

// Stop 优雅停止所有队列和任务，并受 ctx 控制最大等待时长。
// 若在 ctx 截止前未完成，返回 ctx.Err()；此时停止流程可能仍在后台继续。
func (m *Manager) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	if !m.running {
		if m.cfg.Logger != nil {
			m.cfg.Logger.Infof("taskx: manager stop skipped, already stopped")
		}
		return nil
	}
	m.cfg.Logger.Infof("taskx: manager stopping")

	// 1) 停健康监控协程，避免停止过程中继续采样并打扰最终状态。
	if m.monitorCancel != nil {
		m.monitorCancel()
		m.monitorCancel = nil
	}
	if err := waitWithContext(ctx, m.monitorWG.Wait); err != nil {
		m.cfg.Logger.Errorf("taskx: manager stop failed while waiting monitor: %v", err)
		return fmt.Errorf("taskx: wait monitor stop: %w", err)
	}
	m.cfg.Logger.Infof("taskx: monitor stopped")

	// 2) 停 timer 调度器并等待已触发任务退出；如果任务不响应 ctx，可能在此阶段超时。
	if m.scheduler != nil {
		stopCtx := m.scheduler.Stop()
		select {
		case <-stopCtx.Done():
			m.cfg.Logger.Infof("taskx: timer scheduler stopped")
		case <-ctx.Done():
			m.cfg.Logger.Errorf("taskx: manager stop timeout while waiting timer stop: %v", ctx.Err())
			return fmt.Errorf("taskx: wait timer stop: %w", ctx.Err())
		}
		m.scheduler = nil
	}

	// 3) 逐个停止 queue consumer（内部会完成 drain/recover 兜底逻辑）。
	if err := m.stopConsumersWithContextLocked(ctx); err != nil {
		m.cfg.Logger.Errorf("taskx: manager stop failed while stopping consumers: %v", err)
		return err
	}
	m.cfg.Logger.Infof("taskx: all consumers stopped")

	// 4) 停告警分发协程并 drain 剩余告警（写日志后丢弃，不再分发给外部告警回调）。
	if err := m.stopAlertDispatcherWithContextLocked(ctx); err != nil {
		m.cfg.Logger.Errorf("taskx: manager stop failed while stopping alert dispatcher: %v", err)
		return err
	}
	m.cfg.Logger.Infof("taskx: alert dispatcher stopped")
	m.cfg.OnHeartbeat = nil

	m.running = false
	snapCtx, snapCancel := m.internalOpContext(context.Background(), 0)
	m.refreshHealthSnapshot(snapCtx, false)
	snapCancel()
	m.cfg.Logger.Infof("taskx: manager stopped")
	return nil
}

// PublishEvent 发布事件到 EventQueue，并返回创建的 Envelope。
func (m *Manager) PublishEvent(ctx context.Context, runner core.QueueRunner) (*core.Envelope, error) {
	return m.PublishEventPayload(ctx, runner.GetName(), runner.Marshal())
}

// PublishDelay 发布延迟任务到 DelayQueue，并返回创建的 Envelope。
func (m *Manager) PublishDelay(ctx context.Context, runner core.QueueRunner, executeAt int64) (*core.Envelope, error) {
	return m.PublishDelayPayload(ctx, runner.GetName(), runner.Marshal(), executeAt)
}

// PublishEventPayload 直接将 payload 包装为新消息并发布到 EventQueue。
func (m *Manager) PublishEventPayload(ctx context.Context, runnerName string, payload string) (*core.Envelope, error) {
	if m.cfg.EventDriver == nil {
		return nil, fmt.Errorf("taskx: event queue driver not configured")
	}
	env := core.NewEnvelope(payload, core.EnvelopeSourceEvent)
	return m.PublishEventEnvelope(ctx, runnerName, env)
}

// PublishEventEnvelope 将指定 Envelope 发布到 EventQueue。
func (m *Manager) PublishEventEnvelope(ctx context.Context, runnerName string, env *core.Envelope) (*core.Envelope, error) {
	if m.cfg.EventDriver == nil {
		return nil, fmt.Errorf("taskx: event queue driver not configured")
	}
	if env == nil {
		return nil, fmt.Errorf("taskx: envelope is nil")
	}
	env.Source = core.EnvelopeSourceEvent
	key := fmt.Sprintf("%s:event:{%s}:pending", m.cfg.KeyPrefix, runnerName)
	if err := m.cfg.EventDriver.Push(ctx, key, env.Encode()); err != nil {
		return nil, err
	}
	return env, nil
}

// PublishDelayPayload 直接将 payload 包装为新消息并发布到 DelayQueue。
func (m *Manager) PublishDelayPayload(ctx context.Context, runnerName string, payload string, executeAt int64) (*core.Envelope, error) {
	if m.cfg.DelayDriver == nil {
		return nil, fmt.Errorf("taskx: delay queue driver not configured")
	}
	env := core.NewEnvelope(payload, core.EnvelopeSourceDelay)
	return m.PublishDelayEnvelope(ctx, runnerName, env, executeAt)
}

// PublishDelayEnvelope 将指定 Envelope 发布到 DelayQueue。
func (m *Manager) PublishDelayEnvelope(ctx context.Context, runnerName string, env *core.Envelope, executeAt int64) (*core.Envelope, error) {
	if m.cfg.DelayDriver == nil {
		return nil, fmt.Errorf("taskx: delay queue driver not configured")
	}
	if env == nil {
		return nil, fmt.Errorf("taskx: envelope is nil")
	}
	if executeAt <= 0 {
		return nil, fmt.Errorf("taskx: invalid executeAt=%d, must be a positive unix time", executeAt)
	}
	env.Source = core.EnvelopeSourceDelay
	key := fmt.Sprintf("%s:delay:{%s}:pending", m.cfg.KeyPrefix, runnerName)
	if err := m.cfg.DelayDriver.Add(ctx, key, env.Encode(), executeAt); err != nil {
		return nil, err
	}
	return env, nil
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

func (m *Manager) cleanupStartFailureLocked() {
	if m.scheduler != nil {
		stopCtx := m.scheduler.Stop()
		<-stopCtx.Done()
		m.scheduler = nil
	}
	m.stopConsumersLocked()
	m.stopAlertDispatcherLocked()
	m.cfg.OnHeartbeat = nil
	m.running = false
}

func (m *Manager) stopConsumersWithContextLocked(ctx context.Context) error {
	for i, c := range m.consumers {
		if err := waitWithContext(ctx, c.Stop); err != nil {
			return fmt.Errorf("taskx: stop consumer[%d]: %w", i, err)
		}
		m.cfg.Logger.Infof("taskx: consumer[%d] stopped", i)
	}
	m.consumers = nil
	return nil
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
			lenCtx, cancel := m.internalOpContext(ctx, defaults.HealthLenTimeout)
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
			lenCtx, cancel := m.internalOpContext(ctx, defaults.HealthLenTimeout)
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

func (m *Manager) startAlertDispatcherLocked() {
	m.alertHandler = m.cfg.OnAlert
	m.alertQueue = make(chan core.AlertData, m.cfg.AlertQueueSize)
	m.cfg.OnAlert = m.enqueueAlert

	ctx, cancel := context.WithCancel(context.Background())
	m.alertCancel = cancel
	m.alertWG.Add(1)
	go func() {
		defer m.alertWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-m.alertQueue:
				if m.alertHandler != nil {
					m.alertHandler(data)
				}
			}
		}
	}()
}

func (m *Manager) stopAlertDispatcherLocked() {
	if m.alertCancel != nil {
		m.alertCancel()
		m.alertCancel = nil
	}
	m.alertWG.Wait()

	if m.alertQueue != nil {
		drained := 0
		for {
			select {
			case data := <-m.alertQueue:
				drained++
				m.cfg.Logger.Warnf(
					"taskx: alert queue drain on stop, source=%s type=%s runner=%s envelope_id=%s",
					data.Source, data.AlertType, data.RunnerName, alertEnvelopeID(data.Envelope),
				)
			default:
				if drained > 0 {
					m.cfg.Logger.Warnf("taskx: drained %d pending alerts on stop", drained)
				}
				m.alertQueue = nil
				break
			}
			if m.alertQueue == nil {
				break
			}
		}
	}

	m.cfg.OnAlert = m.alertHandler
	m.alertHandler = nil
}

func (m *Manager) stopAlertDispatcherWithContextLocked(ctx context.Context) error {
	if m.alertCancel != nil {
		m.alertCancel()
		m.alertCancel = nil
	}
	if err := waitWithContext(ctx, m.alertWG.Wait); err != nil {
		return fmt.Errorf("taskx: wait alert dispatcher stop: %w", err)
	}

	if m.alertQueue != nil {
		drained := 0
		for {
			select {
			case data := <-m.alertQueue:
				drained++
				m.cfg.Logger.Warnf(
					"taskx: alert queue drain on stop, source=%s type=%s runner=%s envelope_id=%s",
					data.Source, data.AlertType, data.RunnerName, alertEnvelopeID(data.Envelope),
				)
			default:
				if drained > 0 {
					m.cfg.Logger.Warnf("taskx: drained %d pending alerts on stop", drained)
				}
				m.alertQueue = nil
				break
			}
			if m.alertQueue == nil {
				break
			}
		}
	}

	m.cfg.OnAlert = m.alertHandler
	m.alertHandler = nil
	return nil
}

func (m *Manager) enqueueAlert(data core.AlertData) {
	if data.Source == "" && data.Envelope != nil {
		switch data.Envelope.Source {
		case core.EnvelopeSourceEvent:
			data.Source = core.AlertSourceEvent
		case core.EnvelopeSourceDelay:
			data.Source = core.AlertSourceDelay
		case core.EnvelopeSourceTimer:
			data.Source = core.AlertSourceTimer
		}
	}

	if m.alertQueue == nil {
		if m.alertHandler != nil {
			m.alertHandler(data)
		}
		return
	}

	select {
	case m.alertQueue <- data:
	default:
		m.cfg.Logger.Warnf(
			"taskx: alert queue full, dropping alert source=%s type=%s runner=%s envelope_id=%s",
			data.Source, data.AlertType, data.RunnerName, alertEnvelopeID(data.Envelope),
		)
	}
}

func alertEnvelopeID(env *core.Envelope) string {
	if env == nil {
		return ""
	}
	return env.ID
}

func waitWithContext(ctx context.Context, fn func()) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) internalOpContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		timeout = m.cfg.InternalOpTimeout
	}
	if timeout <= 0 {
		timeout = defaults.InternalOpTimeout
	}
	return context.WithTimeout(parent, timeout)
}
