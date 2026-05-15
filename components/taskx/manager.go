package taskx

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/queue"
)

// consumer 内部消费器接口
type consumer interface {
	Start(ctx context.Context) error
	Stop()
	BuildKey() string
}

// timerScheduler 内部定时任务调度器接口
type timerScheduler interface {
	Register(task core.TimerTaskRunner, opt core.TimerTaskOption) error
	Start()
	Stop() context.Context
}

// TimerSchedulerFactory 定时任务调度器工厂函数
type TimerSchedulerFactory func(lk driver.LockDriver, prefix string, cfg *ManagerConfig) timerScheduler

// EventConsumerFactory 事件队列消费器工厂函数（按组创建）
type EventConsumerFactory func(cfg queue.EventConsumerConfig) consumer

// DelayConsumerFactory 延迟队列消费器工厂函数
type DelayConsumerFactory func(
	runner core.QueueRunner, opt core.RunnerOption,
	cfg queue.DelayConsumerConfig,
) consumer

// Manager 任务管理器，统一管理 EventQueue、DelayQueue、TimerTask 的生命周期
type Manager struct {
	cfg      *ManagerConfig
	registry *Registry

	eventFactory EventConsumerFactory
	delayFactory DelayConsumerFactory
	timerFactory TimerSchedulerFactory

	mu              sync.Mutex
	consumers       []consumer
	scheduler       timerScheduler
	running         bool
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	monitorCancel context.CancelFunc
	monitorWG     sync.WaitGroup
	alertCancel   context.CancelFunc
	alertWG       sync.WaitGroup
	alertQueue    chan core.AlertData
	alertHandler  core.AlertFunc

	healthMu         sync.RWMutex
	eventBeatAt      map[string]time.Time
	delayBeatAt      map[string]time.Time
	timerBeatAt      time.Time
	healthSnapshot   ManagerHealthSnapshot
	healthFailCounts map[string]int // key = "event:{group}" / "delay:{name}" / "timer", value = 连续失败次数
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

type startEntries struct {
	event map[string]*EventEntry
	delay map[string]*DelayEntry
	timer map[string]*TimerEntry
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
		cfg:              cfg,
		registry:         registry,
		eventBeatAt:      make(map[string]time.Time),
		delayBeatAt:      make(map[string]time.Time),
		healthFailCounts: make(map[string]int),
	}
}

// Config 获取管理器配置
func (m *Manager) Config() *ManagerConfig {
	return m.cfg
}

// CheckStartReady 校验当前配置是否满足启动条件。
func (m *Manager) CheckStartReady(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.checkStartReadyLocked(ctx)
	return err
}

// Start 启动所有已注册的队列和任务
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureRegistryLocked()

	if m.running {
		return fmt.Errorf("taskx: manager already running")
	}
	entries, err := m.checkStartReadyLocked(ctx)
	if err != nil {
		return err
	}

	// 分离启动 context 和生命周期 context：
	// ctx 仅用于 Start 内的同步初始化操作（版本检测等），带调用方超时控制；
	// lifecycleCtx 由 Manager 内部管理，传给消费协程，仅在 Stop() 时取消。
	m.lifecycleCtx, m.lifecycleCancel = context.WithCancel(context.Background())

	m.startAlertDispatcherLocked()
	startSucceeded := false
	defer func() {
		if !startSucceeded {
			m.cleanupStartFailureLocked()
		}
	}()

	// 启动 EventQueue 消费者（按组聚合）
	if m.cfg.EventDriver != nil && m.eventFactory != nil {
		baseCfg := m.buildConsumerConfig()
		groups := m.groupEventEntries(entries.event)
		for groupName, groupEntries := range groups {
			runners := make(map[string]queue.EventRunnerEntry, len(groupEntries))
			maxConsumer := 1
			for _, entry := range groupEntries {
				runners[entry.Runner.GetName()] = queue.EventRunnerEntry{
					Runner: entry.Runner,
					Option: entry.Option,
				}
				if entry.Option.ConsumerCount > maxConsumer {
					maxConsumer = entry.Option.ConsumerCount
				}
			}
			ecfg := queue.EventConsumerConfig{
				ConsumerConfig: baseCfg,
				Driver:         m.cfg.EventDriver,
				PopTimeout:     m.cfg.EventPollInterval,
				Keys:           queue.NewQueueKeySet(m.cfg.KeyPrefix, "event", groupName),
				Runners:        runners,
				ConsumerCount:  maxConsumer,
			}
			c := m.eventFactory(ecfg)
			if err = c.Start(m.lifecycleCtx); err != nil {
				return fmt.Errorf("taskx: start event group[%s]: %w", groupName, err)
			}
			m.consumers = append(m.consumers, c)
		}
	}

	// 启动 DelayQueue 消费者
	if m.cfg.DelayDriver != nil && m.delayFactory != nil {
		baseCfg := m.buildConsumerConfig()
		for _, entry := range entries.delay {
			dcfg := queue.DelayConsumerConfig{
				ConsumerConfig:    baseCfg,
				Driver:            m.cfg.DelayDriver,
				PollInterval:      m.cfg.PollInterval,
				RetryBaseInterval: m.cfg.DelayRetryBaseInterval,
				Keys:              queue.NewQueueKeySet(m.cfg.KeyPrefix, "delay", entry.Runner.GetName()),
			}
			c := m.delayFactory(entry.Runner, entry.Option, dcfg)
			if err := c.Start(m.lifecycleCtx); err != nil {
				return fmt.Errorf("taskx: start delay[%s]: %w", entry.Runner.GetName(), err)
			}
			m.consumers = append(m.consumers, c)
		}
	}

	// 启动 TimerTask
	if m.cfg.LockDriver != nil && m.timerFactory != nil {
		s := m.timerFactory(m.cfg.LockDriver, m.cfg.KeyPrefix, m.cfg)
		for _, entry := range entries.timer {
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
	m.ensureRegistryLocked()

	if ctx == nil {
		ctx = context.Background()
	}

	if !m.running {
		if m.cfg.Logger != nil {
			m.cfg.Logger.Infof("taskx: manager stop skipped, already stopped")
		}

		return nil
	}
	if m.cfg.Logger == nil {
		return fmt.Errorf("taskx: logger is required, use WithLogger() to set")
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

	// 3) 取消消费协程的生命周期 context，通知所有消费协程退出。
	if m.lifecycleCancel != nil {
		m.lifecycleCancel()
		m.lifecycleCancel = nil
		m.cfg.Logger.Infof("taskx: lifecycle context cancelled, waiting consumers to exit")
	}

	// 4) 逐个停止 queue consumer（内部会完成 drain/recover 兜底逻辑）。
	if err := m.stopConsumersWithContextLocked(ctx); err != nil {
		m.cfg.Logger.Errorf("taskx: manager stop failed while stopping consumers: %v", err)
		return err
	}
	m.cfg.Logger.Infof("taskx: all consumers stopped")

	// 5) 停告警分发协程并 drain 剩余告警（写日志后丢弃，不再分发给外部告警回调）。
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

// Registry 获取注册中心
func (m *Manager) Registry() *Registry {
	m.ensureRegistryLocked()
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
	if m.lifecycleCancel != nil {
		m.lifecycleCancel()
		m.lifecycleCancel = nil
	}
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

// startMonitorLocked starts the health monitoring routine
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

				m.HealthSnapshot()
				m.cfg.Logger.WithFields(
					logger_factory.Fields{
						logger_factory.String("event", fmt.Sprintf("%+v", m.eventBeatAt)),
						logger_factory.String("delay", fmt.Sprintf("%+v", m.delayBeatAt)),
						logger_factory.String("timer", fmt.Sprintf("%+v", m.timerBeatAt)),
					},
				).Infof("taskx: manager health snapshot updated")
			}
		}
	}()
}

// refreshHealthSnapshot updates the health snapshot with current state
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

	eventGroups := m.groupEventEntries(eventEntries)
	for groupName := range eventGroups {
		keys := queue.NewQueueKeySet(m.cfg.KeyPrefix, "event", groupName)
		item := QueueListenerHealth{LastBeatAt: eventBeat[groupName]}
		item.Alive = !item.LastBeatAt.IsZero() && now.Sub(item.LastBeatAt) <= m.cfg.HealthBeatTimeout
		if m.cfg.EventDriver != nil {
			lenCtx, cancel := m.internalOpContext(ctx, defaults.HealthLenTimeout)
			n, err := m.cfg.EventDriver.Len(lenCtx, keys.Pending)
			cancel()
			if err != nil {
				item.LenError = err.Error()
			} else {
				item.PendingLen = n
			}
		}
		snap.Event[groupName] = item
	}

	for name := range delayEntries {
		keys := queue.NewQueueKeySet(m.cfg.KeyPrefix, "delay", name)
		item := QueueListenerHealth{LastBeatAt: delayBeat[name]}
		item.Alive = !item.LastBeatAt.IsZero() && now.Sub(item.LastBeatAt) <= m.cfg.HealthBeatTimeout
		if m.cfg.DelayDriver != nil {
			lenCtx, cancel := m.internalOpContext(ctx, defaults.HealthLenTimeout)
			n, err := m.cfg.DelayDriver.Len(lenCtx, keys.Pending)
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

	if running {
		m.checkHealthAlerts(snap)
	}
}

// checkHealthAlerts 检查健康快照，对连续失败达到阈值的监听器触发告警。
// 恢复后自动清零计数；达到阈值后只在首次触发一次告警，避免重复。
func (m *Manager) checkHealthAlerts(snap ManagerHealthSnapshot) {
	threshold := m.cfg.HealthAlertThreshold
	if threshold <= 0 {
		return
	}

	m.healthMu.Lock()
	defer m.healthMu.Unlock()

	for name, st := range snap.Event {
		key := "event:" + name
		if !st.Alive || st.LenError != "" {
			m.healthFailCounts[key]++
			if m.healthFailCounts[key] == threshold {
				reason := "heartbeat timeout"
				if st.LenError != "" {
					reason = "pending len error: " + st.LenError
				}
				m.cfg.Logger.Errorf("taskx: event[%s] unhealthy for %d consecutive checks: %s", name, threshold, reason)
				m.enqueueAlert(core.AlertData{
					Source:     core.AlertSourceEvent,
					AlertType:  core.AlertListenerUnhealthy,
					RunnerName: name,
					Remark:     fmt.Sprintf("consecutive failures: %d, reason: %s", threshold, reason),
				})
			}
		} else {
			if m.healthFailCounts[key] > 0 {
				m.cfg.Logger.Infof("taskx: event[%s] recovered after %d consecutive failures", name, m.healthFailCounts[key])
			}
			m.healthFailCounts[key] = 0
		}
	}

	for name, st := range snap.Delay {
		key := "delay:" + name
		if !st.Alive || st.LenError != "" {
			m.healthFailCounts[key]++
			if m.healthFailCounts[key] == threshold {
				reason := "heartbeat timeout"
				if st.LenError != "" {
					reason = "pending len error: " + st.LenError
				}
				m.cfg.Logger.Errorf("taskx: delay[%s] unhealthy for %d consecutive checks: %s", name, threshold, reason)
				m.enqueueAlert(core.AlertData{
					Source:     core.AlertSourceDelay,
					AlertType:  core.AlertListenerUnhealthy,
					RunnerName: name,
					Remark:     fmt.Sprintf("consecutive failures: %d, reason: %s", threshold, reason),
				})
			}
		} else {
			if m.healthFailCounts[key] > 0 {
				m.cfg.Logger.Infof("taskx: delay[%s] recovered after %d consecutive failures", name, m.healthFailCounts[key])
			}
			m.healthFailCounts[key] = 0
		}
	}

	timerKey := "timer"
	if !snap.Timer.Alive {
		m.healthFailCounts[timerKey]++
		if m.healthFailCounts[timerKey] == threshold {
			m.cfg.Logger.Errorf("taskx: timer unhealthy for %d consecutive checks: heartbeat timeout", threshold)
			m.enqueueAlert(core.AlertData{
				Source:    core.AlertSourceTimer,
				AlertType: core.AlertListenerUnhealthy,
				Remark:    fmt.Sprintf("consecutive failures: %d, reason: heartbeat timeout", threshold),
			})
		}
	} else {
		if m.healthFailCounts[timerKey] > 0 {
			m.cfg.Logger.Infof("taskx: timer recovered after %d consecutive failures", m.healthFailCounts[timerKey])
		}
		m.healthFailCounts[timerKey] = 0
	}
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

func (m *Manager) ensureRegistryLocked() {
	if m.registry == nil {
		m.registry = NewRegistry()
	}
}

// groupEventEntries 按 QueueGroup 对 event runner 分组
func (m *Manager) groupEventEntries(entries map[string]*EventEntry) map[string][]*EventEntry {
	groups := make(map[string][]*EventEntry)
	for _, entry := range entries {
		group := entry.Option.QueueGroup
		if group == "" {
			group = core.DefaultEventQueueGroup
		}
		groups[group] = append(groups[group], entry)
	}
	return groups
}

// resolveEventGroupName 根据 runner name 解析所属的事件队列组名
func (m *Manager) resolveEventGroupName(runnerName string) string {
	name, _ := m.resolveEventGroupNameStrict(runnerName)
	return name
}

// resolveEventGroupNameStrict 根据 runner name 解析所属的事件队列组名，并返回是否已注册。
func (m *Manager) resolveEventGroupNameStrict(runnerName string) (groupName string, registered bool) {
	entries := m.registry.GetEventRunners()
	if entry, ok := entries[runnerName]; ok {
		if entry.Option.QueueGroup != "" {
			return entry.Option.QueueGroup, true
		}
		return core.DefaultEventQueueGroup, true
	}
	return runnerName, false
}

func (m *Manager) buildConsumerConfig() queue.ConsumerConfig {
	return queue.ConsumerConfig{
		Lock:                m.cfg.LockDriver,
		Prefix:              m.cfg.KeyPrefix,
		LockTTL:             m.cfg.LockTTL,
		RecoveryGracePeriod: m.cfg.RecoveryGracePeriod,
		InternalOpTimeout:   m.cfg.InternalOpTimeout,
		TraceKey:            m.cfg.TraceContextKey,
		Logger:              m.cfg.Logger,
		OnAlert:             m.cfg.OnAlert,
		OnHeartbeat:         m.cfg.OnHeartbeat,
	}
}

func (m *Manager) checkStartReadyLocked(ctx context.Context) (startEntries, error) {
	m.ensureRegistryLocked()
	if m.cfg.Logger == nil {
		return startEntries{}, fmt.Errorf("taskx: logger is required, use WithLogger() to set")
	}

	if m.registry == nil {
		return startEntries{}, fmt.Errorf("taskx: registry is required")
	}
	if !m.registry.IsHasRunner() {
		return startEntries{}, fmt.Errorf("taskx: at least one runner/task must be registered")
	}

	if m.cfg.HealthInterval <= 0 {
		m.cfg.HealthInterval = defaults.HealthInterval
	}
	if m.cfg.HealthBeatTimeout <= 0 {
		m.cfg.HealthBeatTimeout = defaults.HealthBeatTimeout
	}
	if m.cfg.EventPollInterval <= 0 {
		m.cfg.EventPollInterval = defaults.EventPopTimeout
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
	if m.cfg.HealthAlertThreshold < 0 {
		m.cfg.HealthAlertThreshold = defaults.HealthAlertThreshold
	}
	m.cfg.OnHeartbeat = m.recordHeartbeat

	entries := startEntries{
		event: m.registry.GetEventRunners(),
		delay: m.registry.GetDelayRunners(),
		timer: m.registry.GetTimerTasks(),
	}
	if len(entries.event)+len(entries.delay) > 0 && m.cfg.LockDriver == nil {
		return startEntries{}, fmt.Errorf(
			"taskx: lock driver not configured for %d registered queue runner(s) (event=%d, delay=%d)",
			len(entries.event)+len(entries.delay), len(entries.event), len(entries.delay),
		)
	}
	if len(entries.event) > 0 {
		if m.cfg.EventDriver == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: event queue driver not configured for %d registered event runner(s)",
				len(entries.event),
			)
		}
		if m.eventFactory == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: event consumer factory not configured for %d registered event runner(s)",
				len(entries.event),
			)
		}
	}
	if len(entries.delay) > 0 {
		if m.cfg.DelayDriver == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: delay queue driver not configured for %d registered delay runner(s)",
				len(entries.delay),
			)
		}
		if m.delayFactory == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: delay consumer factory not configured for %d registered delay runner(s)",
				len(entries.delay),
			)
		}
	}
	if len(entries.timer) > 0 {
		if m.cfg.LockDriver == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: lock driver not configured for %d registered timer task(s)",
				len(entries.timer),
			)
		}
		if m.timerFactory == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: timer scheduler factory not configured for %d registered timer task(s)",
				len(entries.timer),
			)
		}
	}

	// 检测驱动版本兼容性（如 BLMOVE 要求 Redis >= 6.2）
	type versionChecker interface {
		CheckVersion(ctx context.Context) error
	}
	if v, ok := m.cfg.EventDriver.(versionChecker); ok {
		if err := v.CheckVersion(ctx); err != nil {
			return startEntries{}, err
		}
	}
	return entries, nil
}
