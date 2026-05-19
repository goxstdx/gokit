package consumer

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/queue"
)

// --------------- test helpers ---------------

func testLogger(t *testing.T) core.Logger {
	t.Helper()
	l, err := logger_factory.NewLogger(logger_factory.Config{
		DriverType:  logger_factory.DriverZap,
		Level:       logger_factory.LevelDebug,
		Development: true,
	})
	if err != nil {
		t.Fatalf("create logger: %v", err)
	}
	return l
}

type stubRunner struct{ name string }

func (r stubRunner) GetName() string { return r.name }
func (r stubRunner) Marshal() string { return "payload" }
func (r stubRunner) Run(context.Context, string) core.RunnerFuncResult {
	return core.RunnerFuncResult{IsOk: true}
}

type stubTimerRunner struct{ name string }

func (r stubTimerRunner) GetName() string      { return r.name }
func (r stubTimerRunner) GetCron() string      { return "*/1 * * * * *" }
func (r stubTimerRunner) GetTaskParam() string { return "" }
func (r stubTimerRunner) Run(context.Context, string) core.RunnerFuncResult {
	return core.RunnerFuncResult{IsOk: true}
}

type stubEventDriver struct{}

func (stubEventDriver) Push(context.Context, string, string) error { return nil }
func (stubEventDriver) PopToProcessing(context.Context, string, string, time.Duration) (string, error) {
	return "", nil
}
func (stubEventDriver) Ack(context.Context, string, string) error                { return nil }
func (stubEventDriver) Nack(context.Context, string, string, string) error       { return nil }
func (stubEventDriver) MoveToDead(context.Context, string, string, string) error { return nil }
func (stubEventDriver) PopFromDead(context.Context, string) (string, error)      { return "", nil }
func (stubEventDriver) RecoverDead(context.Context, string, string, int64) (int64, error) {
	return 0, nil
}
func (stubEventDriver) RetryRequeue(context.Context, string, string, string, string) error {
	return nil
}
func (stubEventDriver) RecoverProcessing(context.Context, string, string, time.Duration) (int64, error) {
	return 0, nil
}
func (stubEventDriver) Len(context.Context, string) (int64, error) { return 0, nil }

type stubDelayDriver struct{}

func (stubDelayDriver) Add(context.Context, string, string, int64) error { return nil }
func (stubDelayDriver) TransferToProcessing(context.Context, string, string, int64, int64) ([]string, error) {
	return nil, nil
}
func (stubDelayDriver) Ack(context.Context, string, string) error                 { return nil }
func (stubDelayDriver) Nack(context.Context, string, string, string, int64) error { return nil }
func (stubDelayDriver) MoveToDead(context.Context, string, string, string) error  { return nil }
func (stubDelayDriver) PopFromDead(context.Context, string) (string, error)       { return "", nil }
func (stubDelayDriver) RetryRequeue(context.Context, string, string, string, string, int64) error {
	return nil
}
func (stubDelayDriver) RecoverDead(context.Context, string, string, int64) (int64, error) {
	return 0, nil
}
func (stubDelayDriver) RecoverProcessing(context.Context, string, string, time.Duration) (int64, error) {
	return 0, nil
}
func (stubDelayDriver) Len(context.Context, string) (int64, error) { return 0, nil }

type stubLockDriver struct{}

func (stubLockDriver) Lock(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}
func (stubLockDriver) Unlock(context.Context, string) error                       { return nil }
func (stubLockDriver) Renew(context.Context, string, time.Duration) (bool, error) { return true, nil }

type stubQueueConsumer struct {
	startErr error
	stopped  *atomic.Int64
}

func (c *stubQueueConsumer) Start(context.Context) error { return c.startErr }
func (c *stubQueueConsumer) Stop() {
	if c.stopped != nil {
		c.stopped.Add(1)
	}
}
func (c *stubQueueConsumer) BuildKey() string { return "" }

// --------------- tests ---------------

func TestNewConsumerNilRegistryCreatesEmptyRegistry(t *testing.T) {
	c := New(nil, WithLogger(testLogger(t)))
	if c.Registry() == nil {
		t.Fatal("expected nil registry to be replaced with an empty registry")
	}
}

func TestSetRegistryFailsWhileRunning(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(stubRunner{name: "evt"}); err != nil {
		t.Fatal(err)
	}
	c := New(
		reg,
		WithLogger(testLogger(t)),
		WithLockDriver(stubLockDriver{}),
		WithEventQueueDriver(stubEventDriver{}),
	)
	c.SetEventConsumerFactory(func(queue.EventConsumerConfig) QueueConsumer {
		return &stubQueueConsumer{}
	})
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = c.Stop(context.Background()) }()

	if err := c.SetRegistry(NewRegistry()); err == nil || !strings.Contains(err.Error(), "cannot set registry") {
		t.Fatalf("expected running set registry error, got %v", err)
	}
}

func TestRegistryReturnsReadOnlySnapshotAndFreezesLiveRegistryWhileRunning(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(stubRunner{name: "evt"}); err != nil {
		t.Fatal(err)
	}
	c := New(
		reg,
		WithLogger(testLogger(t)),
		WithLockDriver(stubLockDriver{}),
		WithEventQueueDriver(stubEventDriver{}),
	)
	c.SetEventConsumerFactory(func(queue.EventConsumerConfig) QueueConsumer {
		return &stubQueueConsumer{}
	})
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = c.Stop(context.Background()) }()

	if err := reg.RegisterEventRunner(stubRunner{name: "evt-2"}); err == nil || !strings.Contains(err.Error(), "registry is frozen") {
		t.Fatalf("expected frozen registry error, got %v", err)
	}
	snapshot := c.Registry()
	if snapshot == reg {
		t.Fatal("expected snapshot registry, got live pointer")
	}
	if err := snapshot.RegisterEventRunner(stubRunner{name: "snapshot-only"}); err == nil || !strings.Contains(err.Error(), "registry is frozen") {
		t.Fatalf("expected snapshot to be read-only, got %v", err)
	}
}

func TestEmptyRegistryFailsStartAndCheckReady(t *testing.T) {
	c := New(nil, WithLogger(testLogger(t)))
	if err := c.CheckStartReady(context.Background()); err == nil || !strings.Contains(err.Error(), "at least one runner/task") {
		t.Fatalf("expected empty registry error, got %v", err)
	}
	if err := c.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "at least one runner/task") {
		t.Fatalf("expected start to fail on empty registry, got %v", err)
	}
}

func TestStopWithoutLoggerWhenNotRunning(t *testing.T) {
	c := New(NewRegistry())
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("stop without logger should be no-op, got %v", err)
	}
}

func TestStartFailsWhenMissingDependencies(t *testing.T) {
	log := testLogger(t)

	// event without driver
	eventReg := NewRegistry()
	_ = eventReg.RegisterEventRunner(stubRunner{name: "evt"})
	if err := New(eventReg, WithLogger(log)).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "lock driver not configured") {
		t.Fatalf("expected lock driver error, got %v", err)
	}
	if err := New(eventReg, WithLogger(log), WithLockDriver(stubLockDriver{})).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "event queue driver not configured") {
		t.Fatalf("expected event driver error, got %v", err)
	}
	if err := New(eventReg, WithLogger(log), WithLockDriver(stubLockDriver{}), WithEventQueueDriver(stubEventDriver{})).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "event consumer factory not configured") {
		t.Fatalf("expected event factory error, got %v", err)
	}

	// delay without driver
	delayReg := NewRegistry()
	_ = delayReg.RegisterDelayRunner(stubRunner{name: "dly"})
	if err := New(delayReg, WithLogger(log), WithLockDriver(stubLockDriver{})).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "delay queue driver not configured") {
		t.Fatalf("expected delay driver error, got %v", err)
	}
	if err := New(delayReg, WithLogger(log), WithLockDriver(stubLockDriver{}), WithDelayQueueDriver(stubDelayDriver{})).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "delay consumer factory not configured") {
		t.Fatalf("expected delay factory error, got %v", err)
	}

	// timer without lock
	timerReg := NewRegistry()
	_ = timerReg.RegisterTimerTask(stubTimerRunner{name: "tmr"})
	if err := New(timerReg, WithLogger(log)).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "lock driver not configured") {
		t.Fatalf("expected lock driver error, got %v", err)
	}
	if err := New(timerReg, WithLogger(log), WithLockDriver(stubLockDriver{})).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "timer scheduler factory not configured") {
		t.Fatalf("expected timer factory error, got %v", err)
	}
}

func TestStartFailureCleansAlertDispatcherAndStopsStartedConsumers(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "first"})
	_ = reg.RegisterEventRunner(stubRunner{name: "second"}, core.RunnerOption{QueueGroup: "second-group"})

	var stopped atomic.Int64
	originalAlert := func(core.AlertData) {}
	c := New(
		reg,
		WithLogger(testLogger(t)),
		WithLockDriver(stubLockDriver{}),
		WithEventQueueDriver(stubEventDriver{}),
		WithAlertFunc(originalAlert),
	)
	var created atomic.Int64
	c.SetEventConsumerFactory(func(queue.EventConsumerConfig) QueueConsumer {
		if created.Add(1) == 2 {
			return &stubQueueConsumer{startErr: errors.New("boom"), stopped: &stopped}
		}
		return &stubQueueConsumer{stopped: &stopped}
	})

	if err := c.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected start failure, got %v", err)
	}

	if c.running {
		t.Fatal("consumer should not be running after failed start")
	}
	if c.alertQueue != nil || c.alertCancel != nil || c.alertHandler != nil {
		t.Fatalf("alert dispatcher not cleaned: queue=%v cancel=%v handler=%v", c.alertQueue, c.alertCancel, c.alertHandler)
	}
	if c.cfg.OnAlert == nil {
		t.Fatal("expected original alert handler to be restored")
	}
	if got := stopped.Load(); got != 1 {
		t.Fatalf("expected one successfully started consumer to stop, got %d", got)
	}
}

func TestStopAlertDispatcherDrainDoesNotInvokeExternalHandler(t *testing.T) {
	c := New(nil, WithLogger(testLogger(t)))

	var handled atomic.Int64
	originalAlert := func(core.AlertData) { handled.Add(1) }

	c.cfg.OnAlert = originalAlert
	c.alertHandler = originalAlert
	c.alertQueue = make(chan core.AlertData, 1)
	c.alertQueue <- core.AlertData{
		Source:     core.AlertSourceEvent,
		AlertType:  core.AlertCorruptMessage,
		RunnerName: "evt",
	}

	if err := c.stopAlertDispatcherWithContextLocked(context.Background()); err != nil {
		t.Fatalf("stop alert dispatcher: %v", err)
	}

	if got := handled.Load(); got != 0 {
		t.Fatalf("drained alerts should not invoke external handler, got handled=%d", got)
	}
	if c.alertQueue != nil {
		t.Fatal("expected alert queue to be cleared after stop")
	}
	if c.cfg.OnAlert == nil {
		t.Fatal("expected original alert handler to be restored")
	}
}

func TestCheckHealthAlertsFiresOnThreshold(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "evt"})

	alertCh := make(chan core.AlertData, 10)
	c := New(
		reg,
		WithLogger(testLogger(t)),
		WithLockDriver(stubLockDriver{}),
		WithEventQueueDriver(stubEventDriver{}),
		WithAlertFunc(func(data core.AlertData) { alertCh <- data }),
		WithHealthAlertThreshold(3),
	)
	c.healthFailCounts = make(map[string]int)
	c.alertHandler = c.cfg.OnAlert
	c.alertQueue = make(chan core.AlertData, 10)
	go func() {
		for d := range c.alertQueue {
			if c.alertHandler != nil {
				c.alertHandler(d)
			}
		}
	}()

	unhealthySnap := HealthSnapshot{
		Running: true,
		Event:   map[string]QueueListenerHealth{"evt": {Alive: false}},
		Delay:   map[string]QueueListenerHealth{},
		Timer:   TimerListenerHealth{Alive: true},
	}

	c.checkHealthAlerts(unhealthySnap)
	c.checkHealthAlerts(unhealthySnap)
	select {
	case <-alertCh:
		t.Fatal("should not fire alert before threshold")
	default:
	}

	c.checkHealthAlerts(unhealthySnap)
	select {
	case alert := <-alertCh:
		if alert.AlertType != core.AlertListenerUnhealthy {
			t.Fatalf("expected AlertListenerUnhealthy, got %s", alert.AlertType)
		}
		if alert.RunnerName != "evt" {
			t.Fatalf("expected runner name 'evt', got %s", alert.RunnerName)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for alert on threshold")
	}

	c.checkHealthAlerts(unhealthySnap)
	select {
	case <-alertCh:
		t.Fatal("should not fire alert repeatedly after threshold")
	default:
	}

	healthySnap := HealthSnapshot{
		Running: true,
		Event:   map[string]QueueListenerHealth{"evt": {Alive: true}},
		Delay:   map[string]QueueListenerHealth{},
		Timer:   TimerListenerHealth{Alive: true},
	}
	c.checkHealthAlerts(healthySnap)
	c.healthMu.RLock()
	count := c.healthFailCounts["event:evt"]
	c.healthMu.RUnlock()
	if count != 0 {
		t.Fatalf("expected fail count reset to 0, got %d", count)
	}

	close(c.alertQueue)
}

func TestResolveEventGroupNameStrict(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "r1"}, core.RunnerOption{QueueGroup: "grp"})

	c := New(reg, WithLogger(testLogger(t)))

	group, ok := c.resolveEventGroupNameStrict("r1")
	if !ok || group != "grp" {
		t.Fatalf("expected (grp, true), got (%s, %v)", group, ok)
	}

	_, ok = c.resolveEventGroupNameStrict("not-exist")
	if ok {
		t.Fatal("expected unregistered runner to return false")
	}
}

func TestEventGroupResolverClosure(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "a"}, core.RunnerOption{QueueGroup: "ga"})
	_ = reg.RegisterEventRunner(stubRunner{name: "b"})

	c := New(reg, WithLogger(testLogger(t)))
	resolver := c.EventGroupResolver()

	group, ok := resolver("a")
	if !ok || group != "ga" {
		t.Fatalf("expected (ga, true), got (%s, %v)", group, ok)
	}
	group, ok = resolver("b")
	if !ok || group != core.DefaultEventQueueGroup {
		t.Fatalf("expected (%s, true), got (%s, %v)", core.DefaultEventQueueGroup, group, ok)
	}
	_, ok = resolver("unknown")
	if ok {
		t.Fatal("expected false for unregistered")
	}
}

func TestDelayRegisteredCheckerClosure(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterDelayRunner(stubRunner{name: "dly1"})

	c := New(reg, WithLogger(testLogger(t)))
	checker := c.DelayRegisteredChecker()

	if !checker("dly1") {
		t.Fatal("expected registered delay runner to return true")
	}
	if checker("unknown") {
		t.Fatal("expected unregistered delay runner to return false")
	}
}

func TestGroupEventEntries(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "a"}, core.RunnerOption{QueueGroup: "g1"})
	_ = reg.RegisterEventRunner(stubRunner{name: "b"}, core.RunnerOption{QueueGroup: "g1"})
	_ = reg.RegisterEventRunner(stubRunner{name: "c"})

	c := New(reg, WithLogger(testLogger(t)))
	groups := c.groupEventEntries(reg.GetEventRunners())

	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if len(groups["g1"]) != 2 {
		t.Fatalf("expected 2 entries in g1, got %d", len(groups["g1"]))
	}
	if len(groups[core.DefaultEventQueueGroup]) != 1 {
		t.Fatalf("expected 1 entry in default group, got %d", len(groups[core.DefaultEventQueueGroup]))
	}
}

func TestRunningReflectsStartStopState(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "evt"})

	c := New(
		reg,
		WithLogger(testLogger(t)),
		WithLockDriver(stubLockDriver{}),
		WithEventQueueDriver(stubEventDriver{}),
	)
	c.SetEventConsumerFactory(func(queue.EventConsumerConfig) QueueConsumer {
		return &stubQueueConsumer{}
	})

	if c.Running() {
		t.Fatal("should not be running before Start")
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if !c.Running() {
		t.Fatal("should be running after Start")
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if c.Running() {
		t.Fatal("should not be running after Stop")
	}
}

func TestDoubleStartReturnsError(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "evt"})

	c := New(
		reg,
		WithLogger(testLogger(t)),
		WithLockDriver(stubLockDriver{}),
		WithEventQueueDriver(stubEventDriver{}),
	)
	c.SetEventConsumerFactory(func(queue.EventConsumerConfig) QueueConsumer {
		return &stubQueueConsumer{}
	})

	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Stop(context.Background()) }()

	if err := c.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("expected already running error, got %v", err)
	}
}

func TestRestartAfterStopReturnsError(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "evt"})

	c := New(
		reg,
		WithLogger(testLogger(t)),
		WithLockDriver(stubLockDriver{}),
		WithEventQueueDriver(stubEventDriver{}),
	)
	c.SetEventConsumerFactory(func(queue.EventConsumerConfig) QueueConsumer {
		return &stubQueueConsumer{}
	})

	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := c.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "restart is not allowed") {
		t.Fatalf("expected restart not allowed error, got %v", err)
	}
}

func TestExecuteTimerTaskOnceValidation(t *testing.T) {
	c := New(nil, WithLogger(testLogger(t)))

	// empty name
	_, err := c.ExecuteTimerTaskOnce(context.Background(), core.TimerExecuteRequest{})
	if err == nil || !strings.Contains(err.Error(), "name is required") {
		t.Fatalf("expected name required error, got %v", err)
	}

	// no lock driver
	_, err = c.ExecuteTimerTaskOnce(context.Background(), core.TimerExecuteRequest{Name: "foo"})
	if err == nil || !strings.Contains(err.Error(), "lock driver not configured") {
		t.Fatalf("expected lock driver error, got %v", err)
	}

	// task not registered
	reg := NewRegistry()
	c2 := New(reg, WithLogger(testLogger(t)), WithLockDriver(stubLockDriver{}))
	_, err = c2.ExecuteTimerTaskOnce(context.Background(), core.TimerExecuteRequest{Name: "missing"})
	if err == nil || !strings.Contains(err.Error(), "not registered") {
		t.Fatalf("expected not registered error, got %v", err)
	}
}

func TestExecuteTimerTaskOnceRuns(t *testing.T) {
	var ran atomic.Bool
	runner := &runnableTimerTask{
		name: "exec-me",
		fn: func(_ context.Context, _ string) core.RunnerFuncResult {
			ran.Store(true)
			return core.RunnerFuncResult{IsOk: true}
		},
	}

	reg := NewRegistry()
	_ = reg.RegisterTimerTask(runner)

	c := New(reg, WithLogger(testLogger(t)), WithLockDriver(stubLockDriver{}))

	result, err := c.ExecuteTimerTaskOnce(context.Background(), core.TimerExecuteRequest{Name: "exec-me", Payload: "p"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsOk {
		t.Fatal("expected ok result")
	}
	if !ran.Load() {
		t.Fatal("expected task to have run")
	}
}

type runnableTimerTask struct {
	name string
	fn   func(context.Context, string) core.RunnerFuncResult
}

func (r *runnableTimerTask) GetName() string      { return r.name }
func (r *runnableTimerTask) GetCron() string      { return "*/1 * * * * *" }
func (r *runnableTimerTask) GetTaskParam() string { return "" }
func (r *runnableTimerTask) Run(ctx context.Context, payload string) core.RunnerFuncResult {
	if r.fn != nil {
		return r.fn(ctx, payload)
	}
	return core.RunnerFuncResult{IsOk: true}
}

func TestHealthSnapshotDeepCopied(t *testing.T) {
	reg := NewRegistry()
	_ = reg.RegisterEventRunner(stubRunner{name: "evt"})

	c := New(
		reg,
		WithLogger(testLogger(t)),
		WithEventQueueDriver(stubEventDriver{}),
		WithLockDriver(stubLockDriver{}),
	)
	c.SetEventConsumerFactory(func(queue.EventConsumerConfig) QueueConsumer {
		return &stubQueueConsumer{}
	})
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	snap := c.HealthSnapshot()
	if !snap.Running {
		t.Fatal("expected running=true")
	}

	// mutate returned snapshot
	snap.Event["mutated"] = QueueListenerHealth{Alive: true}

	snap2 := c.HealthSnapshot()
	if _, exists := snap2.Event["mutated"]; exists {
		t.Fatal("HealthSnapshot should return deep copy")
	}

	_ = c.Stop(context.Background())
}
