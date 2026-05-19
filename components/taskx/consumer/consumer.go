package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/queue"
)

// QueueConsumer 内部消费器接口（导出供工厂模式和测试使用）
type QueueConsumer interface {
	Start(ctx context.Context) error
	Stop()
	BuildKey() string
}

// TimerScheduler 内部定时任务调度器接口（导出供工厂模式和测试使用）
type TimerScheduler interface {
	Register(task core.TimerTaskRunner, opt core.TimerTaskOption) error
	Start()
	Stop() context.Context
}

// TimerSchedulerFactory 定时任务调度器工厂函数
type TimerSchedulerFactory func(lk driver.LockDriver, prefix string, cfg *Config) TimerScheduler

// EventConsumerFactory 事件队列消费器工厂函数（按组创建）
type EventConsumerFactory func(cfg queue.EventConsumerConfig) QueueConsumer

// DelayConsumerFactory 延迟队列消费器工厂函数
type DelayConsumerFactory func(
	runner core.QueueRunner, opt core.RunnerOption,
	cfg queue.DelayConsumerConfig,
) QueueConsumer

// ProducerSnapshot 用于构建 Producer 的最小必要配置快照。
type ProducerSnapshot struct {
	EventDriver driver.EventQueueDriver
	DelayDriver driver.DelayQueueDriver
	KeyPrefix   string
	Logger      core.Logger
	OnAlert     core.AlertFunc
}

// Consumer 任务消费者，管理 EventQueue、DelayQueue、TimerTask 的消费生命周期。
type Consumer struct {
	cfg      *Config
	registry *Registry

	eventFactory EventConsumerFactory
	delayFactory DelayConsumerFactory
	timerFactory TimerSchedulerFactory

	mu              sync.Mutex
	consumers       []QueueConsumer
	scheduler       TimerScheduler
	running         bool
	stopping        bool
	stopped         bool
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

type startEntries struct {
	event map[string]*EventEntry
	delay map[string]*DelayEntry
	timer map[string]*TimerEntry
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

// ProducerSnapshot 返回构建 Producer 所需的最小配置快照。
func (c *Consumer) ProducerSnapshot() ProducerSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	onAlert := c.cfg.OnAlert
	if c.running && c.alertQueue != nil {
		onAlert = c.enqueueAlert
	}
	return ProducerSnapshot{
		EventDriver: c.cfg.EventDriver,
		DelayDriver: c.cfg.DelayDriver,
		KeyPrefix:   c.cfg.KeyPrefix,
		Logger:      c.cfg.Logger,
		OnAlert:     onAlert,
	}
}

// Registry 获取注册中心
func (c *Consumer) Registry() *Registry {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureRegistryLocked()
	return c.registry.Snapshot()
}

// SetRegistry 设置注册中心（仅允许在未运行且未停止中的状态下设置）。
func (c *Consumer) SetRegistry(registry *Registry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running || c.stopping {
		return fmt.Errorf("taskx: cannot set registry while consumer is running or stopping")
	}
	if registry == nil {
		registry = NewRegistry()
	}
	c.registry = registry
	return nil
}

// SetEventConsumerFactory 设置事件队列消费器工厂（仅允许在空闲状态下设置）。
func (c *Consumer) SetEventConsumerFactory(f EventConsumerFactory) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running || c.stopping {
		return fmt.Errorf("taskx: cannot set event consumer factory while consumer is running or stopping")
	}
	c.eventFactory = f
	return nil
}

// SetDelayConsumerFactory 设置延迟队列消费器工厂（仅允许在空闲状态下设置）。
func (c *Consumer) SetDelayConsumerFactory(f DelayConsumerFactory) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running || c.stopping {
		return fmt.Errorf("taskx: cannot set delay consumer factory while consumer is running or stopping")
	}
	c.delayFactory = f
	return nil
}

// SetTimerSchedulerFactory 设置定时任务调度器工厂（仅允许在空闲状态下设置）。
func (c *Consumer) SetTimerSchedulerFactory(f TimerSchedulerFactory) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running || c.stopping {
		return fmt.Errorf("taskx: cannot set timer scheduler factory while consumer is running or stopping")
	}
	c.timerFactory = f
	return nil
}

// Running 返回是否正在运行
func (c *Consumer) Running() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.running
}

// CheckStartReady 校验当前配置是否满足启动条件。
func (c *Consumer) CheckStartReady(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err := c.checkStartReadyLocked(ctx)
	return err
}

// Start 启动所有已注册的队列消费者和定时任务
func (c *Consumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureRegistryLocked()

	if c.running {
		return fmt.Errorf("taskx: consumer already running")
	}
	if c.stopping {
		return fmt.Errorf("taskx: consumer is stopping, cannot start until stop completes")
	}
	if c.stopped {
		return fmt.Errorf("taskx: consumer already stopped permanently, restart is not allowed")
	}
	entries, err := c.checkStartReadyLocked(ctx)
	if err != nil {
		return err
	}
	c.registry.Freeze()

	c.resetHealthStateLocked()

	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	c.lifecycleCancel = lifecycleCancel

	c.startAlertDispatcherLocked()
	startSucceeded := false
	defer func() {
		if !startSucceeded {
			c.cleanupStartFailureLocked()
		}
	}()

	if c.cfg.EventDriver != nil && c.eventFactory != nil {
		baseCfg := c.buildConsumerConfig()
		groups := c.groupEventEntries(entries.event)
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
				Driver:         c.cfg.EventDriver,
				PopTimeout:     c.cfg.EventPollInterval,
				Keys:           queue.NewQueueKeySet(c.cfg.KeyPrefix, "event", groupName),
				Runners:        runners,
				ConsumerCount:  maxConsumer,
			}
			qc := c.eventFactory(ecfg)
			if err = qc.Start(lifecycleCtx); err != nil {
				return fmt.Errorf("taskx: start event group[%s]: %w", groupName, err)
			}
			c.consumers = append(c.consumers, qc)
		}
	}

	if c.cfg.DelayDriver != nil && c.delayFactory != nil {
		baseCfg := c.buildConsumerConfig()
		for _, entry := range entries.delay {
			dcfg := queue.DelayConsumerConfig{
				ConsumerConfig:    baseCfg,
				Driver:            c.cfg.DelayDriver,
				PollInterval:      c.cfg.PollInterval,
				RetryBaseInterval: c.cfg.DelayRetryBaseInterval,
				Keys:              queue.NewQueueKeySet(c.cfg.KeyPrefix, "delay", entry.Runner.GetName()),
			}
			qc := c.delayFactory(entry.Runner, entry.Option, dcfg)
			if err := qc.Start(lifecycleCtx); err != nil {
				return fmt.Errorf("taskx: start delay[%s]: %w", entry.Runner.GetName(), err)
			}
			c.consumers = append(c.consumers, qc)
		}
	}

	if c.cfg.LockDriver != nil && c.timerFactory != nil {
		timerCfg := *c.cfg
		timerCfg.OnAlert = c.enqueueAlert
		s := c.timerFactory(c.cfg.LockDriver, c.cfg.KeyPrefix, &timerCfg)
		for _, entry := range entries.timer {
			opt := entry.Option.WithDefaults(c.cfg.DefaultTimerTask)
			if err := s.Register(entry.Task, opt); err != nil {
				return fmt.Errorf("taskx: register timer[%s]: %w", entry.Task.GetName(), err)
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
	c.cfg.Logger.Infof("taskx: consumer started")
	startSucceeded = true
	return nil
}

// Stop 优雅停止所有队列消费者和定时任务。
// 若 ctx 超时，已完成的步骤不可逆，Stop 会尽力推进后续步骤并返回首个超时错误。
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureRegistryLocked()

	if ctx == nil {
		ctx = context.Background()
	}

	if !c.running {
		if c.cfg.Logger != nil {
			c.cfg.Logger.Infof("taskx: consumer stop skipped, already stopped")
		}
		return nil
	}
	if c.cfg.Logger == nil {
		return fmt.Errorf("taskx: logger is required, use WithLogger() to set")
	}
	c.stopping = true
	defer func() { c.stopping = false }()
	c.cfg.Logger.Infof("taskx: consumer stopping")

	var firstErr error
	setErr := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}

	if c.monitorCancel != nil {
		c.monitorCancel()
		c.monitorCancel = nil
	}
	if err := waitWithContext(ctx, c.monitorWG.Wait); err != nil {
		c.cfg.Logger.Errorf("taskx: consumer stop: wait monitor: %v", err)
		setErr(fmt.Errorf("taskx: wait monitor stop: %w", err))
	} else {
		c.cfg.Logger.Infof("taskx: monitor stopped")
	}

	if c.scheduler != nil {
		stopCtx := c.scheduler.Stop()
		select {
		case <-stopCtx.Done():
			c.cfg.Logger.Infof("taskx: timer scheduler stopped")
		case <-ctx.Done():
			c.cfg.Logger.Errorf("taskx: consumer stop: wait timer: %v", ctx.Err())
			setErr(fmt.Errorf("taskx: wait timer stop: %w", ctx.Err()))
		}
		c.scheduler = nil
	}

	if c.lifecycleCancel != nil {
		c.lifecycleCancel()
		c.lifecycleCancel = nil
		c.cfg.Logger.Infof("taskx: lifecycle context cancelled, waiting consumers to exit")
	}

	if err := c.stopConsumersWithContextLocked(ctx); err != nil {
		c.cfg.Logger.Errorf("taskx: consumer stop: stopping consumers: %v", err)
		setErr(err)
	} else {
		c.cfg.Logger.Infof("taskx: all consumers stopped")
	}

	if err := c.stopAlertDispatcherWithContextLocked(ctx); err != nil {
		c.cfg.Logger.Errorf("taskx: consumer stop: stopping alert dispatcher: %v", err)
		setErr(err)
	} else {
		c.cfg.Logger.Infof("taskx: alert dispatcher stopped")
	}
	c.cfg.OnHeartbeat = nil

	c.running = false
	c.registry.Unfreeze()
	if firstErr == nil {
		c.stopped = true
	}
	c.resetHealthStateLocked()
	snapCtx, snapCancel := c.internalOpContext(context.Background(), 0)
	c.refreshHealthSnapshot(snapCtx, false)
	snapCancel()
	c.cfg.Logger.Infof("taskx: consumer stopped")
	return firstErr
}

func (c *Consumer) resetHealthStateLocked() {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	c.eventBeatAt = make(map[string]time.Time)
	c.delayBeatAt = make(map[string]time.Time)
	c.timerBeatAt = time.Time{}
	c.healthFailCounts = make(map[string]int)
}

func (c *Consumer) stopConsumersLocked() {
	for _, qc := range c.consumers {
		qc.Stop()
	}
	c.consumers = nil
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
	c.stopConsumersLocked()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), defaults.InternalOpTimeout)
	_ = c.stopAlertDispatcherWithContextLocked(cleanupCtx)
	cleanupCancel()

	c.cfg.OnHeartbeat = nil
	c.running = false
	c.registry.Unfreeze()
}

func (c *Consumer) stopConsumersWithContextLocked(ctx context.Context) error {
	var firstErr error
	for i, qc := range c.consumers {
		if err := waitWithContext(ctx, qc.Stop); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("taskx: stop consumer[%d]: %w", i, err)
			}
			c.cfg.Logger.Errorf("taskx: consumer[%d] stop timeout: %v", i, err)
			continue
		}
		c.cfg.Logger.Infof("taskx: consumer[%d] stopped", i)
	}
	c.consumers = nil
	return firstErr
}

func (c *Consumer) ensureRegistryLocked() {
	if c.registry == nil {
		c.registry = NewRegistry()
	}
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
		OnAlert:             c.enqueueAlert,
		OnHeartbeat:         c.cfg.OnHeartbeat,
	}
}

func (c *Consumer) checkStartReadyLocked(ctx context.Context) (startEntries, error) {
	c.ensureRegistryLocked()
	if c.cfg.Logger == nil {
		return startEntries{}, fmt.Errorf("taskx: logger is required, use WithLogger() to set")
	}

	if c.registry == nil {
		return startEntries{}, fmt.Errorf("taskx: registry is required")
	}
	if !c.registry.IsHasRunner() {
		return startEntries{}, fmt.Errorf("taskx: at least one runner/task must be registered")
	}

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

	entries := startEntries{
		event: c.registry.GetEventRunners(),
		delay: c.registry.GetDelayRunners(),
		timer: c.registry.GetTimerTasks(),
	}
	if len(entries.event)+len(entries.delay) > 0 &&
		c.cfg.RecoveryMode != queue.RecoveryModeNone &&
		c.cfg.LockDriver == nil {
		return startEntries{}, fmt.Errorf(
			"taskx: lock driver not configured for %d registered queue runner(s) (event=%d, delay=%d)",
			len(entries.event)+len(entries.delay), len(entries.event), len(entries.delay),
		)
	}
	if len(entries.event) > 0 {
		if c.cfg.EventDriver == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: event queue driver not configured for %d registered event runner(s)",
				len(entries.event),
			)
		}
		if c.eventFactory == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: event consumer factory not configured for %d registered event runner(s)",
				len(entries.event),
			)
		}
	}
	if len(entries.delay) > 0 {
		if c.cfg.DelayDriver == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: delay queue driver not configured for %d registered delay runner(s)",
				len(entries.delay),
			)
		}
		if c.delayFactory == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: delay consumer factory not configured for %d registered delay runner(s)",
				len(entries.delay),
			)
		}
	}
	if len(entries.timer) > 0 {
		if c.cfg.LockDriver == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: lock driver not configured for %d registered timer task(s)",
				len(entries.timer),
			)
		}
		if c.timerFactory == nil {
			return startEntries{}, fmt.Errorf(
				"taskx: timer scheduler factory not configured for %d registered timer task(s)",
				len(entries.timer),
			)
		}
	}

	type versionChecker interface {
		CheckVersion(ctx context.Context) error
	}
	if v, ok := c.cfg.EventDriver.(versionChecker); ok {
		if err := v.CheckVersion(ctx); err != nil {
			return startEntries{}, err
		}
	}
	if v, ok := c.cfg.DelayDriver.(versionChecker); ok {
		if err := v.CheckVersion(ctx); err != nil {
			return startEntries{}, err
		}
	}
	return entries, nil
}
