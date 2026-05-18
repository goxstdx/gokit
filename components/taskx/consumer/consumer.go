package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/queue"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/timer"
)

// internalConsumer 内部消费器接口
type internalConsumer interface {
	Start(ctx context.Context) error
	Stop()
	BuildKey() string
}

// Consumer 任务消费者，管理 EventQueue / DelayQueue / TimerTask 的消费生命周期。
// 与 Manager 不同，Consumer 不包含 Publish 方法，适用于"只消费（或消费 + 独立 Producer）"的服务。
type Consumer struct {
	cfg      *Config
	registry *Registry

	mu              sync.Mutex
	consumers       []internalConsumer
	scheduler       *timer.Scheduler
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
	healthSnapshot   HealthSnapshot
	healthFailCounts map[string]int
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

// HealthSnapshot 消费者健康快照
type HealthSnapshot struct {
	Running   bool
	CheckedAt time.Time
	Event     map[string]QueueListenerHealth
	Delay     map[string]QueueListenerHealth
	Timer     TimerListenerHealth
}

// New 创建 Consumer。
func New(registry *Registry, opts ...Option) *Consumer {
	cfg := core.DefaultConfig()
	for _, o := range opts {
		o(cfg)
	}
	if registry == nil {
		registry = NewRegistry()
	}
	return &Consumer{
		cfg:              cfg,
		registry:         registry,
		eventBeatAt:      make(map[string]time.Time),
		delayBeatAt:      make(map[string]time.Time),
		healthFailCounts: make(map[string]int),
	}
}

// Config 获取消费者配置
func (c *Consumer) Config() *Config { return c.cfg }

// Registry 获取注册中心
func (c *Consumer) Registry() *Registry { return c.registry }

// Start 启动所有已注册的队列消费者和定时任务。
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("taskx/consumer: already running")
	}
	if err := c.checkReadyLocked(); err != nil {
		return err
	}

	c.lifecycleCtx, c.lifecycleCancel = context.WithCancel(context.Background())
	c.startAlertDispatcherLocked()

	startSucceeded := false
	defer func() {
		if !startSucceeded {
			c.cleanupStartFailureLocked()
		}
	}()

	eventEntries := c.registry.GetEventRunners()
	delayEntries := c.registry.GetDelayRunners()
	timerEntries := c.registry.GetTimerTasks()

	// 启动 EventQueue 消费者
	if c.cfg.EventDriver != nil && len(eventEntries) > 0 {
		baseCfg := c.buildConsumerConfig()
		groups := groupEventEntries(eventEntries)
		for groupName, groupRunners := range groups {
			runners := make(map[string]queue.EventRunnerEntry, len(groupRunners))
			maxConsumer := 1
			for _, entry := range groupRunners {
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
				Driver:         c.cfg.EventDriver,
				PopTimeout:     c.cfg.EventPollInterval,
				Keys:           queue.NewQueueKeySet(c.cfg.KeyPrefix, "event", groupName),
				Runners:        runners,
				ConsumerCount:  maxConsumer,
			}
			consumer := queue.NewEventConsumer(ecfg)
			if err := consumer.Start(c.lifecycleCtx); err != nil {
				return fmt.Errorf("taskx/consumer: start event group[%s]: %w", groupName, err)
			}
			c.consumers = append(c.consumers, consumer)
		}
	}

	// 启动 DelayQueue 消费者
	if c.cfg.DelayDriver != nil && len(delayEntries) > 0 {
		baseCfg := c.buildConsumerConfig()
		for _, entry := range delayEntries {
			dcfg := queue.DelayConsumerConfig{
				ConsumerConfig:    baseCfg,
				Driver:            c.cfg.DelayDriver,
				PollInterval:      c.cfg.PollInterval,
				RetryBaseInterval: c.cfg.DelayRetryBaseInterval,
				Keys:              queue.NewQueueKeySet(c.cfg.KeyPrefix, "delay", entry.Runner.GetName()),
			}
			consumer := queue.NewDelayConsumer(entry.Runner, entry.Option, dcfg)
			if err := consumer.Start(c.lifecycleCtx); err != nil {
				return fmt.Errorf("taskx/consumer: start delay[%s]: %w", entry.Runner.GetName(), err)
			}
			c.consumers = append(c.consumers, consumer)
		}
	}

	// 启动 TimerTask
	if c.cfg.LockDriver != nil && len(timerEntries) > 0 {
		s := timer.NewScheduler(
			c.cfg.LockDriver, c.cfg.KeyPrefix,
			c.cfg.LockTTL, c.cfg.InternalOpTimeout,
			c.cfg.TimerHeartbeatInterval,
			c.cfg.Logger, c.cfg.OnAlert, c.cfg.OnHeartbeat,
		)
		for _, entry := range timerEntries {
			opt := entry.Option.WithDefaults(c.cfg.DefaultTimerTask)
			if err := s.Register(entry.Task, opt); err != nil {
				return fmt.Errorf("taskx/consumer: register timer[%s]: %w", entry.Task.GetName(), err)
			}
		}
		s.Start()
		c.scheduler = s
	}

	c.running = true
	snapCtx, snapCancel := c.internalOpContext(context.Background(), 0)
	c.refreshHealthSnapshot(snapCtx, true)
	snapCancel()
	c.startMonitorLocked()
	c.cfg.Logger.Infof("taskx/consumer: started")
	startSucceeded = true
	return nil
}

// Stop 优雅停止所有队列消费者和定时任务。
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	if !c.running {
		if c.cfg.Logger != nil {
			c.cfg.Logger.Infof("taskx/consumer: stop skipped, already stopped")
		}
		return nil
	}
	c.cfg.Logger.Infof("taskx/consumer: stopping")

	if c.monitorCancel != nil {
		c.monitorCancel()
		c.monitorCancel = nil
	}
	if err := waitWithContext(ctx, c.monitorWG.Wait); err != nil {
		return fmt.Errorf("taskx/consumer: wait monitor stop: %w", err)
	}

	if c.scheduler != nil {
		stopCtx := c.scheduler.Stop()
		select {
		case <-stopCtx.Done():
		case <-ctx.Done():
			return fmt.Errorf("taskx/consumer: wait timer stop: %w", ctx.Err())
		}
		c.scheduler = nil
	}

	if c.lifecycleCancel != nil {
		c.lifecycleCancel()
		c.lifecycleCancel = nil
	}

	for i, cs := range c.consumers {
		if err := waitWithContext(ctx, cs.Stop); err != nil {
			return fmt.Errorf("taskx/consumer: stop consumer[%d]: %w", i, err)
		}
	}
	c.consumers = nil

	if err := c.stopAlertDispatcherWithContextLocked(ctx); err != nil {
		return err
	}
	c.cfg.OnHeartbeat = nil
	c.running = false

	snapCtx, snapCancel := c.internalOpContext(context.Background(), 0)
	c.refreshHealthSnapshot(snapCtx, false)
	snapCancel()
	c.cfg.Logger.Infof("taskx/consumer: stopped")
	return nil
}

// HealthSnapshot 返回最近一次健康快照。
func (c *Consumer) HealthSnapshot() HealthSnapshot {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()

	cp := HealthSnapshot{
		Running:   c.healthSnapshot.Running,
		CheckedAt: c.healthSnapshot.CheckedAt,
		Event:     make(map[string]QueueListenerHealth, len(c.healthSnapshot.Event)),
		Delay:     make(map[string]QueueListenerHealth, len(c.healthSnapshot.Delay)),
		Timer:     c.healthSnapshot.Timer,
	}
	for k, v := range c.healthSnapshot.Event {
		cp.Event[k] = v
	}
	for k, v := range c.healthSnapshot.Delay {
		cp.Delay[k] = v
	}
	return cp
}

// HealthOK 返回消费链路是否健康。
func (c *Consumer) HealthOK() bool {
	snap := c.HealthSnapshot()
	if !snap.Running {
		return false
	}
	for _, st := range snap.Event {
		if !st.Alive || st.LenError != "" {
			return false
		}
	}
	for _, st := range snap.Delay {
		if !st.Alive || st.LenError != "" {
			return false
		}
	}
	timerEntries := c.registry.GetTimerTasks()
	if len(timerEntries) > 0 && !snap.Timer.Alive {
		return false
	}
	return true
}

func (c *Consumer) checkReadyLocked() error {
	if c.cfg.Logger == nil {
		return fmt.Errorf("taskx/consumer: logger is required, use WithLogger() to set")
	}

	eventEntries := c.registry.GetEventRunners()
	delayEntries := c.registry.GetDelayRunners()
	timerEntries := c.registry.GetTimerTasks()

	if len(eventEntries)+len(delayEntries)+len(timerEntries) == 0 {
		return fmt.Errorf("taskx/consumer: at least one runner/task must be registered")
	}

	c.normalizeConfigLocked()

	if (len(eventEntries)+len(delayEntries)) > 0 &&
		c.cfg.RecoveryMode != queue.RecoveryModeNone &&
		c.cfg.LockDriver == nil {
		return fmt.Errorf(
			"taskx/consumer: lock driver required for recovery mode with %d queue runner(s)",
			len(eventEntries)+len(delayEntries),
		)
	}
	if len(eventEntries) > 0 && c.cfg.EventDriver == nil {
		return fmt.Errorf("taskx/consumer: event queue driver not configured for %d event runner(s)", len(eventEntries))
	}
	if len(delayEntries) > 0 && c.cfg.DelayDriver == nil {
		return fmt.Errorf("taskx/consumer: delay queue driver not configured for %d delay runner(s)", len(delayEntries))
	}
	if len(timerEntries) > 0 && c.cfg.LockDriver == nil {
		return fmt.Errorf("taskx/consumer: lock driver not configured for %d timer task(s)", len(timerEntries))
	}
	return nil
}

func (c *Consumer) normalizeConfigLocked() {
	if c.cfg.HealthInterval <= 0 {
		c.cfg.HealthInterval = defaults.HealthInterval
	}
	if c.cfg.HealthBeatTimeout <= 0 {
		c.cfg.HealthBeatTimeout = defaults.HealthBeatTimeout
	}
	if c.cfg.EventPollInterval <= 0 {
		c.cfg.EventPollInterval = defaults.EventPopTimeout
	}
	if c.cfg.DelayRetryBaseInterval <= 0 {
		c.cfg.DelayRetryBaseInterval = defaults.DelayRetryBaseInterval
	}
	c.cfg.RecoveryMode = c.cfg.RecoveryMode.Normalize()
	if c.cfg.TimerHeartbeatInterval <= 0 {
		c.cfg.TimerHeartbeatInterval = c.cfg.HealthInterval
		if hb := c.cfg.HealthBeatTimeout / 2; hb > 0 && (c.cfg.TimerHeartbeatInterval <= 0 || hb < c.cfg.TimerHeartbeatInterval) {
			c.cfg.TimerHeartbeatInterval = hb
		}
		if c.cfg.TimerHeartbeatInterval <= 0 {
			c.cfg.TimerHeartbeatInterval = defaults.TimerHeartbeatFallback
		}
		if c.cfg.TimerHeartbeatInterval < defaults.MinTimerHeartbeat {
			c.cfg.TimerHeartbeatInterval = defaults.MinTimerHeartbeat
		}
	}
	if c.cfg.AlertQueueSize <= 0 {
		c.cfg.AlertQueueSize = 1024
	}
	if c.cfg.HealthAlertThreshold < 0 {
		c.cfg.HealthAlertThreshold = defaults.HealthAlertThreshold
	}
	c.cfg.OnHeartbeat = c.recordHeartbeat
}

func (c *Consumer) buildConsumerConfig() queue.ConsumerConfig {
	return queue.ConsumerConfig{
		Lock:                c.cfg.LockDriver,
		Prefix:              c.cfg.KeyPrefix,
		LockTTL:             c.cfg.LockTTL,
		RecoveryGracePeriod: c.cfg.RecoveryGracePeriod,
		RecoveryMode:        c.cfg.RecoveryMode,
		InternalOpTimeout:   c.cfg.InternalOpTimeout,
		TraceKey:            c.cfg.TraceContextKey,
		Logger:              c.cfg.Logger,
		OnAlert:             c.cfg.OnAlert,
		OnHeartbeat:         c.cfg.OnHeartbeat,
	}
}

func groupEventEntries(entries map[string]*EventEntry) map[string][]*EventEntry {
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

func (c *Consumer) cleanupStartFailureLocked() {
	if c.lifecycleCancel != nil {
		c.lifecycleCancel()
		c.lifecycleCancel = nil
	}
	if c.scheduler != nil {
		stopCtx := c.scheduler.Stop()
		<-stopCtx.Done()
		c.scheduler = nil
	}
	for _, cs := range c.consumers {
		cs.Stop()
	}
	c.consumers = nil
	c.stopAlertDispatcherLocked()
	c.cfg.OnHeartbeat = nil
	c.running = false
}

// --- alert dispatcher ---

func (c *Consumer) startAlertDispatcherLocked() {
	c.alertHandler = c.cfg.OnAlert
	c.alertQueue = make(chan core.AlertData, c.cfg.AlertQueueSize)
	c.cfg.OnAlert = c.enqueueAlert

	ctx, cancel := context.WithCancel(context.Background())
	c.alertCancel = cancel
	c.alertWG.Add(1)
	go func() {
		defer c.alertWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-c.alertQueue:
				if c.alertHandler != nil {
					c.alertHandler(data)
				}
			}
		}
	}()
}

func (c *Consumer) stopAlertDispatcherLocked() {
	if c.alertCancel != nil {
		c.alertCancel()
		c.alertCancel = nil
	}
	c.alertWG.Wait()
	c.drainAlertQueue()
	c.cfg.OnAlert = c.alertHandler
	c.alertHandler = nil
}

func (c *Consumer) stopAlertDispatcherWithContextLocked(ctx context.Context) error {
	if c.alertCancel != nil {
		c.alertCancel()
		c.alertCancel = nil
	}
	if err := waitWithContext(ctx, c.alertWG.Wait); err != nil {
		return fmt.Errorf("taskx/consumer: wait alert dispatcher stop: %w", err)
	}
	c.drainAlertQueue()
	c.cfg.OnAlert = c.alertHandler
	c.alertHandler = nil
	return nil
}

func (c *Consumer) drainAlertQueue() {
	if c.alertQueue == nil {
		return
	}
	drained := 0
	for {
		select {
		case data := <-c.alertQueue:
			drained++
			c.cfg.Logger.Warnf(
				"taskx/consumer: alert queue drain on stop, source=%s type=%s runner=%s envelope_id=%s",
				data.Source, data.AlertType, data.RunnerName, alertEnvelopeID(data.Envelope),
			)
		default:
			if drained > 0 {
				c.cfg.Logger.Warnf("taskx/consumer: drained %d pending alerts on stop", drained)
			}
			c.alertQueue = nil
			return
		}
	}
}

func (c *Consumer) enqueueAlert(data core.AlertData) {
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
	if c.alertQueue == nil {
		if c.alertHandler != nil {
			c.alertHandler(data)
		}
		return
	}
	select {
	case c.alertQueue <- data:
	default:
		c.cfg.Logger.Warnf(
			"taskx/consumer: alert queue full, dropping alert source=%s type=%s runner=%s envelope_id=%s",
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

// --- health monitor ---

func (c *Consumer) recordHeartbeat(hb core.ListenerHeartbeat) {
	beatAt := hb.At
	if beatAt.IsZero() {
		beatAt = time.Now()
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	switch hb.Kind {
	case core.ListenerKindEvent:
		c.eventBeatAt[hb.Name] = beatAt
	case core.ListenerKindDelay:
		c.delayBeatAt[hb.Name] = beatAt
	case core.ListenerKindTimer:
		c.timerBeatAt = beatAt
	}
}

func (c *Consumer) startMonitorLocked() {
	ctx, cancel := context.WithCancel(context.Background())
	c.monitorCancel = cancel
	c.monitorWG.Add(1)
	go func() {
		defer c.monitorWG.Done()
		ticker := time.NewTicker(c.cfg.HealthInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.refreshHealthSnapshot(ctx, true)
			}
		}
	}()
}

func (c *Consumer) refreshHealthSnapshot(ctx context.Context, running bool) {
	now := time.Now()
	eventEntries := c.registry.GetEventRunners()
	delayEntries := c.registry.GetDelayRunners()

	c.healthMu.RLock()
	eventBeat := make(map[string]time.Time, len(c.eventBeatAt))
	delayBeat := make(map[string]time.Time, len(c.delayBeatAt))
	for k, v := range c.eventBeatAt {
		eventBeat[k] = v
	}
	for k, v := range c.delayBeatAt {
		delayBeat[k] = v
	}
	timerBeat := c.timerBeatAt
	c.healthMu.RUnlock()

	snap := HealthSnapshot{
		Running:   running,
		CheckedAt: now,
		Event:     make(map[string]QueueListenerHealth, len(eventEntries)),
		Delay:     make(map[string]QueueListenerHealth, len(delayEntries)),
	}

	eventGroups := groupEventEntries(eventEntries)
	for groupName := range eventGroups {
		keys := queue.NewQueueKeySet(c.cfg.KeyPrefix, "event", groupName)
		item := QueueListenerHealth{LastBeatAt: eventBeat[groupName]}
		item.Alive = !item.LastBeatAt.IsZero() && now.Sub(item.LastBeatAt) <= c.cfg.HealthBeatTimeout
		if c.cfg.EventDriver != nil {
			lenCtx, cancel := c.internalOpContext(ctx, defaults.HealthLenTimeout)
			n, err := c.cfg.EventDriver.Len(lenCtx, keys.Pending)
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
		keys := queue.NewQueueKeySet(c.cfg.KeyPrefix, "delay", name)
		item := QueueListenerHealth{LastBeatAt: delayBeat[name]}
		item.Alive = !item.LastBeatAt.IsZero() && now.Sub(item.LastBeatAt) <= c.cfg.HealthBeatTimeout
		if c.cfg.DelayDriver != nil {
			lenCtx, cancel := c.internalOpContext(ctx, defaults.HealthLenTimeout)
			n, err := c.cfg.DelayDriver.Len(lenCtx, keys.Pending)
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
		Alive:      !timerBeat.IsZero() && now.Sub(timerBeat) <= c.cfg.HealthBeatTimeout,
	}

	c.healthMu.Lock()
	c.healthSnapshot = snap
	c.healthMu.Unlock()

	if running {
		c.checkHealthAlerts(snap)
	}
}

func (c *Consumer) checkHealthAlerts(snap HealthSnapshot) {
	threshold := c.cfg.HealthAlertThreshold
	if threshold <= 0 {
		return
	}
	c.healthMu.Lock()
	defer c.healthMu.Unlock()

	for name, st := range snap.Event {
		key := "event:" + name
		if !st.Alive || st.LenError != "" {
			c.healthFailCounts[key]++
			if c.healthFailCounts[key] == threshold {
				reason := "heartbeat timeout"
				if st.LenError != "" {
					reason = "pending len error: " + st.LenError
				}
				c.enqueueAlert(
					core.AlertData{
						Source:     core.AlertSourceEvent,
						AlertType:  core.AlertListenerUnhealthy,
						RunnerName: name,
						Remark:     fmt.Sprintf("consecutive failures: %d, reason: %s", threshold, reason),
					},
				)
			}
		} else {
			c.healthFailCounts[key] = 0
		}
	}

	for name, st := range snap.Delay {
		key := "delay:" + name
		if !st.Alive || st.LenError != "" {
			c.healthFailCounts[key]++
			if c.healthFailCounts[key] == threshold {
				reason := "heartbeat timeout"
				if st.LenError != "" {
					reason = "pending len error: " + st.LenError
				}
				c.enqueueAlert(
					core.AlertData{
						Source:     core.AlertSourceDelay,
						AlertType:  core.AlertListenerUnhealthy,
						RunnerName: name,
						Remark:     fmt.Sprintf("consecutive failures: %d, reason: %s", threshold, reason),
					},
				)
			}
		} else {
			c.healthFailCounts[key] = 0
		}
	}

	timerKey := "timer"
	if !snap.Timer.Alive {
		c.healthFailCounts[timerKey]++
		if c.healthFailCounts[timerKey] == threshold {
			c.enqueueAlert(
				core.AlertData{
					Source:    core.AlertSourceTimer,
					AlertType: core.AlertListenerUnhealthy,
					Remark:    fmt.Sprintf("consecutive failures: %d, reason: heartbeat timeout", threshold),
				},
			)
		}
	} else {
		c.healthFailCounts[timerKey] = 0
	}
}

// --- utils ---

func (c *Consumer) internalOpContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	if timeout <= 0 {
		timeout = c.cfg.InternalOpTimeout
	}
	if timeout <= 0 {
		timeout = defaults.InternalOpTimeout
	}
	return context.WithTimeout(parent, timeout)
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
