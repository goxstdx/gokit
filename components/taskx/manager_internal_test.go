package taskx

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

func newInternalTestLogger(t *testing.T) core.Logger {
	t.Helper()
	l, err := logger_factory.NewLogger(
		logger_factory.Config{
			DriverType:  logger_factory.DriverZap,
			Level:       logger_factory.LevelDebug,
			Development: true,
		},
	)
	if err != nil {
		t.Fatalf("create logger: %v", err)
	}
	return l
}

type internalQueueRunner struct{ name string }

func (r internalQueueRunner) GetName() string { return r.name }
func (r internalQueueRunner) Marshal() string { return "payload" }
func (r internalQueueRunner) Run(context.Context, string) core.RunnerFuncResult {
	return core.RunnerFuncResult{IsOk: true}
}

type failingQueueRunner struct{ name string }

func (r failingQueueRunner) GetName() string { return r.name }
func (r failingQueueRunner) Marshal() string { return "payload" }
func (r failingQueueRunner) Run(context.Context, string) core.RunnerFuncResult {
	return core.RunnerFuncResult{IsOk: false, Err: errors.New("retry")}
}

type internalTimerRunner struct{ name string }

func (r internalTimerRunner) GetName() string      { return r.name }
func (r internalTimerRunner) GetCron() string      { return "*/1 * * * * *" }
func (r internalTimerRunner) GetTaskParam() string { return "" }
func (r internalTimerRunner) Run(context.Context, string) core.RunnerFuncResult {
	return core.RunnerFuncResult{IsOk: true}
}

type internalEventDriver struct{}

func (internalEventDriver) Push(context.Context, string, string) error { return nil }
func (internalEventDriver) PopToProcessing(context.Context, string, string, time.Duration) (string, error) {
	return "", nil
}
func (internalEventDriver) Ack(context.Context, string, string) error                { return nil }
func (internalEventDriver) Nack(context.Context, string, string, string) error       { return nil }
func (internalEventDriver) MoveToDead(context.Context, string, string, string) error { return nil }
func (internalEventDriver) PopFromDead(context.Context, string) (string, error)      { return "", nil }
func (internalEventDriver) RecoverDead(context.Context, string, string, int64) (int64, error) {
	return 0, nil
}
func (internalEventDriver) RetryRequeue(context.Context, string, string, string, string) error {
	return nil
}
func (internalEventDriver) RecoverProcessing(context.Context, string, string, time.Duration) (int64, error) {
	return 0, nil
}
func (internalEventDriver) Len(context.Context, string) (int64, error) { return 0, nil }

type eventPollIntervalDriver struct {
	internalEventDriver
	timeoutCh chan time.Duration
}

func (d *eventPollIntervalDriver) PopToProcessing(_ context.Context, _, _ string, timeout time.Duration) (string, error) {
	select {
	case d.timeoutCh <- timeout:
	default:
	}
	return "", nil
}

type internalDelayDriver struct {
	lastQueue     string
	lastData      string
	lastExecuteAt int64
}

func (d *internalDelayDriver) Add(_ context.Context, queue string, data string, executeAt int64) error {
	d.lastQueue = queue
	d.lastData = data
	d.lastExecuteAt = executeAt
	return nil
}
func (*internalDelayDriver) TransferToProcessing(context.Context, string, string, int64, int64) ([]string, error) {
	return nil, nil
}
func (*internalDelayDriver) Ack(context.Context, string, string) error                 { return nil }
func (*internalDelayDriver) Nack(context.Context, string, string, string, int64) error { return nil }
func (*internalDelayDriver) MoveToDead(context.Context, string, string, string) error  { return nil }
func (*internalDelayDriver) PopFromDead(context.Context, string) (string, error)       { return "", nil }
func (*internalDelayDriver) RetryRequeue(context.Context, string, string, string, string, int64) error {
	return nil
}
func (*internalDelayDriver) RecoverDead(context.Context, string, string, int64) (int64, error) {
	return 0, nil
}
func (*internalDelayDriver) RecoverProcessing(context.Context, string, string, time.Duration) (int64, error) {
	return 0, nil
}
func (*internalDelayDriver) Len(context.Context, string) (int64, error) { return 0, nil }

type delayRetryDriver struct {
	internalDelayDriver
	transferred atomic.Bool
	retryAtCh   chan int64
}

func (d *delayRetryDriver) TransferToProcessing(context.Context, string, string, int64, int64) ([]string, error) {
	if !d.transferred.CompareAndSwap(false, true) {
		return nil, nil
	}
	return []string{core.NewEnvelope("retry-payload", core.EnvelopeSourceDelay).Encode()}, nil
}

func (d *delayRetryDriver) RetryRequeue(_ context.Context, _, _ string, _, _ string, executeAt int64) error {
	select {
	case d.retryAtCh <- executeAt:
	default:
	}
	return nil
}

type internalLockDriver struct{}

func (internalLockDriver) Lock(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}
func (internalLockDriver) Unlock(context.Context, string) error { return nil }
func (internalLockDriver) Renew(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

type trackingLockDriver struct {
	lockKey   string
	unlockKey string
}

func (d *trackingLockDriver) Lock(_ context.Context, key string, _ time.Duration) (bool, error) {
	d.lockKey = key
	return true, nil
}

func (d *trackingLockDriver) Unlock(_ context.Context, key string) error {
	d.unlockKey = key
	return nil
}

func (d *trackingLockDriver) Renew(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

type manualExecuteTimerRunner struct {
	name        string
	runCount    atomic.Int64
	lastPayload string
}

func (r *manualExecuteTimerRunner) GetName() string      { return r.name }
func (r *manualExecuteTimerRunner) GetCron() string      { return "*/1 * * * * *" }
func (r *manualExecuteTimerRunner) GetTaskParam() string { return "cron-default" }
func (r *manualExecuteTimerRunner) Run(_ context.Context, payload string) core.RunnerFuncResult {
	r.lastPayload = payload
	r.runCount.Add(1)
	return core.RunnerFuncResult{IsOk: true}
}

type internalConsumer struct {
	startErr error
	stopped  *atomic.Int64
}

func (c *internalConsumer) Start(context.Context) error { return c.startErr }
func (c *internalConsumer) Stop() {
	if c.stopped != nil {
		c.stopped.Add(1)
	}
}
func (c *internalConsumer) BuildKey() string { return "" }

func TestNewManagerWithNilRegistryCreatesEmptyRegistry(t *testing.T) {
	mgr := NewManager(nil, WithLogger(newInternalTestLogger(t)))
	if mgr.Registry() == nil {
		t.Fatal("expected nil registry to be replaced with an empty registry")
	}
}

func TestNewManagerUsesCtorRegistry(t *testing.T) {
	reg := NewRegistry()
	mgr := NewManager(reg)
	if mgr.Registry() == reg {
		t.Fatal("expected Registry to return a read-only snapshot, not the live registry pointer")
	}
	if len(mgr.Registry().GetEventRunners()) != 0 {
		t.Fatal("expected empty registry snapshot")
	}
}

func TestSetRegistrySupportsReplaceAndNilReset(t *testing.T) {
	mgr := NewManager(nil, WithLogger(newInternalTestLogger(t)))
	reg := NewRegistry()
	if err := mgr.SetRegistry(reg); err != nil {
		t.Fatalf("set registry: %v", err)
	}
	if mgr.Registry() == reg {
		t.Fatal("expected Registry to return a snapshot after replacement")
	}
	if err := mgr.SetRegistry(nil); err != nil {
		t.Fatalf("set nil registry: %v", err)
	}
	if mgr.Registry() == nil {
		t.Fatal("expected nil registry input to be replaced by empty registry")
	}
}

func TestRegistrySnapshotIsReadOnlyAndRunningRegistryIsFrozen(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(internalQueueRunner{name: "evt"}); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithLockDriver(internalLockDriver{}),
		WithEventQueueDriver(internalEventDriver{}),
	)
	mgr.SetEventConsumerFactory(func(queue.EventConsumerConfig) QueueConsumer {
		return &internalConsumer{}
	})
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = mgr.Stop(context.Background()) }()

	if err := reg.RegisterEventRunner(internalQueueRunner{name: "evt-2"}); err == nil || !strings.Contains(err.Error(), "registry is frozen") {
		t.Fatalf("expected frozen registry error, got %v", err)
	}
	snapshot := mgr.Registry()
	if err := snapshot.RegisterEventRunner(internalQueueRunner{name: "snapshot-only"}); err == nil || !strings.Contains(err.Error(), "registry is frozen") {
		t.Fatalf("expected snapshot to be read-only, got %v", err)
	}
}

func TestSetRegistryFailsWhileRunning(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(internalQueueRunner{name: "evt"}); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithLockDriver(internalLockDriver{}),
		WithEventQueueDriver(internalEventDriver{}),
	)
	mgr.SetEventConsumerFactory(
		func(queue.EventConsumerConfig) QueueConsumer {
			return &internalConsumer{}
		},
	)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = mgr.Stop(context.Background()) }()

	if err := mgr.SetRegistry(NewRegistry()); err == nil || !strings.Contains(
		err.Error(),
		"cannot set registry while consumer is running",
	) {
		t.Fatalf("expected running set registry error, got %v", err)
	}
}

func TestEmptyRegistryFailsCheckStartReadyAndStart(t *testing.T) {
	mgr := NewManager(nil, WithLogger(newInternalTestLogger(t)))
	if mgr.Registry() == nil {
		t.Fatal("expected nil registry to be replaced with an empty registry")
	}
	if err := mgr.CheckStartReady(context.Background()); err == nil || !strings.Contains(
		err.Error(),
		"at least one runner/task",
	) {
		t.Fatalf("expected empty registry error, got %v", err)
	}
	if err := mgr.Start(context.Background()); err == nil || !strings.Contains(
		err.Error(),
		"at least one runner/task must be registered",
	) {
		t.Fatalf("expected start to fail on empty registry, got %v", err)
	}
}

func TestStopWithoutLoggerWhenNotRunningDoesNotPanic(t *testing.T) {
	mgr := NewManager(NewRegistry())
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("stop without logger should be no-op, got %v", err)
	}
}

func TestRestartAfterStopReturnsError(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(internalQueueRunner{name: "evt"}); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithLockDriver(internalLockDriver{}),
		WithEventQueueDriver(internalEventDriver{}),
	)
	if err := mgr.SetDefaultFactories(); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := mgr.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "restart is not allowed") {
		t.Fatalf("expected restart not allowed error, got %v", err)
	}
}

func TestStartFailsWhenRegisteredComponentsAreMissingDependencies(t *testing.T) {
	log := newInternalTestLogger(t)

	eventReg := NewRegistry()
	if err := eventReg.RegisterEventRunner(internalQueueRunner{name: "evt"}); err != nil {
		t.Fatal(err)
	}
	if err := NewManager(
		eventReg,
		WithLogger(log),
	).Start(context.Background()); err == nil || !strings.Contains(
		err.Error(),
		"lock driver not configured",
	) {
		t.Fatalf("expected missing lock driver error for queue mode, got %v", err)
	}
	if err := NewManager(
		eventReg,
		WithLogger(log),
		WithLockDriver(internalLockDriver{}),
	).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "event queue driver not configured") {
		t.Fatalf("expected missing event driver error after lock is configured, got %v", err)
	}
	if err := NewManager(
		eventReg,
		WithLogger(log),
		WithLockDriver(internalLockDriver{}),
		WithEventQueueDriver(internalEventDriver{}),
	).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "event consumer factory not configured") {
		t.Fatalf("expected missing event factory error, got %v", err)
	}

	delayReg := NewRegistry()
	if err := delayReg.RegisterDelayRunner(internalQueueRunner{name: "delay"}); err != nil {
		t.Fatal(err)
	}
	if err := NewManager(
		delayReg,
		WithLogger(log),
	).Start(context.Background()); err == nil || !strings.Contains(
		err.Error(),
		"lock driver not configured",
	) {
		t.Fatalf("expected missing lock driver error for queue mode, got %v", err)
	}
	if err := NewManager(
		delayReg,
		WithLogger(log),
		WithLockDriver(internalLockDriver{}),
	).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "delay queue driver not configured") {
		t.Fatalf("expected missing delay driver error after lock is configured, got %v", err)
	}
	if err := NewManager(
		delayReg,
		WithLogger(log),
		WithLockDriver(internalLockDriver{}),
		WithDelayQueueDriver(&internalDelayDriver{}),
	).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "delay consumer factory not configured") {
		t.Fatalf("expected missing delay factory error, got %v", err)
	}

	timerReg := NewRegistry()
	if err := timerReg.RegisterTimerTask(internalTimerRunner{name: "timer"}); err != nil {
		t.Fatal(err)
	}
	if err := NewManager(
		timerReg,
		WithLogger(log),
	).Start(context.Background()); err == nil || !strings.Contains(
		err.Error(),
		"lock driver not configured",
	) {
		t.Fatalf("expected missing lock driver error, got %v", err)
	}
	if err := NewManager(
		timerReg,
		WithLogger(log),
		WithLockDriver(internalLockDriver{}),
	).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "timer scheduler factory not configured") {
		t.Fatalf("expected missing timer factory error, got %v", err)
	}
}

func TestStartFailureCleansUpAndReportsNotRunning(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(internalQueueRunner{name: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterEventRunner(
		internalQueueRunner{name: "second"},
		core.RunnerOption{QueueGroup: "second-group"},
	); err != nil {
		t.Fatal(err)
	}

	var stopped atomic.Int64
	var alerts atomic.Int64
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithLockDriver(internalLockDriver{}),
		WithEventQueueDriver(internalEventDriver{}),
		WithAlertFunc(func(core.AlertData) { alerts.Add(1) }),
	)
	var created atomic.Int64
	mgr.SetEventConsumerFactory(
		func(queue.EventConsumerConfig) QueueConsumer {
			if created.Add(1) == 2 {
				return &internalConsumer{startErr: errors.New("boom"), stopped: &stopped}
			}
			return &internalConsumer{stopped: &stopped}
		},
	)

	if err := mgr.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected start failure, got %v", err)
	}
	if mgr.Running() {
		t.Fatal("manager should not be running after failed start")
	}
	if _, err := mgr.NewProducer().PublishEventPayload(context.Background(), "not-registered", "payload"); err == nil {
		t.Fatal("expected publish to unregistered runner to fail")
	}
	if alerts.Load() == 0 {
		t.Fatal("expected original alert handler to be restored and invoked")
	}
	if got := stopped.Load(); got != 1 {
		t.Fatalf("expected one successfully started consumer to stop, got %d", got)
	}
}

func TestPublishDelayAllowsImmediateExecuteAt(t *testing.T) {
	drv := &internalDelayDriver{}
	reg := NewRegistry()
	if err := reg.RegisterDelayRunner(internalQueueRunner{name: "delay-now"}); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(reg, WithLogger(newInternalTestLogger(t)), WithDelayQueueDriver(drv))
	now := time.Now()
	env, err := mgr.PublishDelayPayload(context.Background(), "delay-now", "payload", now)
	if err != nil {
		t.Fatalf("expected current unix second to be accepted: %v", err)
	}
	if env == nil || env.Source != core.EnvelopeSourceDelay {
		t.Fatalf("unexpected envelope: %+v", env)
	}
	if drv.lastExecuteAt != now.UnixMicro() || drv.lastQueue != "taskx:delay:{delay-now}:pending" || drv.lastData == "" {
		t.Fatalf("unexpected driver call: queue=%q executeAt=%d data=%q", drv.lastQueue, drv.lastExecuteAt, drv.lastData)
	}
	if _, err := mgr.PublishDelayPayload(context.Background(), "delay-now", "payload", time.Time{}); err == nil {
		t.Fatal("expected zero executeAt to be rejected")
	}
}

func TestEventPollIntervalConfigIsPassedToConsumer(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(internalQueueRunner{name: "event-pop-timeout"}); err != nil {
		t.Fatal(err)
	}
	drv := &eventPollIntervalDriver{timeoutCh: make(chan time.Duration, 1)}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithLockDriver(internalLockDriver{}),
		WithEventQueueDriver(drv),
		WithEventPollInterval(75*time.Millisecond),
	)
	if err := mgr.SetDefaultFactories(); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Stop(context.Background()) }()

	select {
	case got := <-drv.timeoutCh:
		if got != 75*time.Millisecond {
			t.Fatalf("pop timeout: got %v, want %v", got, 75*time.Millisecond)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for PopToProcessing")
	}
}

func TestDelayRetryBaseIntervalConfigControlsFallbackSchedule(t *testing.T) {
	reg := NewRegistry()
	runner := failingQueueRunner{name: "delay-retry-base"}
	if err := reg.RegisterDelayRunner(runner, core.RunnerOption{MaxRetry: 1, ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}
	drv := &delayRetryDriver{retryAtCh: make(chan int64, 1)}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithLockDriver(internalLockDriver{}),
		WithDelayQueueDriver(drv),
		WithPollInterval(10*time.Millisecond),
		WithDelayRetryBaseInterval(2*time.Second),
	)
	if err := mgr.SetDefaultFactories(); err != nil {
		t.Fatal(err)
	}
	before := time.Now().UnixMicro()
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Stop(context.Background()) }()

	select {
	case got := <-drv.retryAtCh:
		if got < before+2_000_000 || got > time.Now().UnixMicro()+3_000_000 {
			t.Fatalf(
				"retry executeAt not based on configured interval: got=%d before=%d now=%d",
				got,
				before,
				time.Now().UnixMicro(),
			)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for RetryRequeue")
	}
}

func TestResolveEventGroupNameStrictReturnsFalseForUnregistered(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(
		internalQueueRunner{name: "registered-evt"},
		core.RunnerOption{QueueGroup: "my-group"},
	); err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(reg, WithLogger(newInternalTestLogger(t)))
	resolver := mgr.EventGroupResolver()

	group, ok := resolver("registered-evt")
	if !ok {
		t.Fatal("expected registered runner to return true")
	}
	if group != "my-group" {
		t.Fatalf("expected group 'my-group', got %q", group)
	}

	_, ok = resolver("not-exist")
	if ok {
		t.Fatal("expected unregistered runner to return false")
	}
}

func TestExecuteTimerTaskOnceUsesFixedLockAndRequestPayload(t *testing.T) {
	reg := NewRegistry()
	runner := &manualExecuteTimerRunner{name: "manual-exec"}
	if err := reg.RegisterTimerTask(runner); err != nil {
		t.Fatal(err)
	}

	lockDrv := &trackingLockDriver{}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithLockDriver(lockDrv),
		WithKeyPrefix("custom-prefix"),
	)

	result, err := mgr.ExecuteTimerTaskOnce(
		context.Background(),
		core.TimerExecuteRequest{Name: "manual-exec", Payload: "manual-param"},
	)
	if err != nil {
		t.Fatalf("execute timer task once: %v", err)
	}
	if !result.IsOk {
		t.Fatalf("expected manual execute success, got result=%+v", result)
	}
	if got := runner.runCount.Load(); got != 1 {
		t.Fatalf("expected run count=1, got %d", got)
	}
	if runner.lastPayload != "manual-param" {
		t.Fatalf("expected payload from request, got %q", runner.lastPayload)
	}

	wantKey := "custom-prefix:lock:timer:{manual-exec}"
	if lockDrv.lockKey != wantKey {
		t.Fatalf("unexpected lock key: got %q want %q", lockDrv.lockKey, wantKey)
	}
	if lockDrv.unlockKey != wantKey {
		t.Fatalf("unexpected unlock key: got %q want %q", lockDrv.unlockKey, wantKey)
	}
}
