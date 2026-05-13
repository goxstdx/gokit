package taskx

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
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

func (r internalTimerRunner) GetName() string { return r.name }
func (r internalTimerRunner) GetCron() string { return "*/1 * * * * *" }
func (r internalTimerRunner) Run(context.Context) core.RunnerFuncResult {
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

type eventPopTimeoutDriver struct {
	internalEventDriver
	timeoutCh chan time.Duration
}

func (d *eventPopTimeoutDriver) PopToProcessing(_ context.Context, _, _ string, timeout time.Duration) (string, error) {
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

func TestNewManagerWithNilRegistryStarts(t *testing.T) {
	mgr := NewManager(nil, WithLogger(newInternalTestLogger(t)))
	if mgr.Registry() == nil {
		t.Fatal("expected nil registry to be replaced with an empty registry")
	}
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("start with nil registry replacement: %v", err)
	}
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestStartFailsWhenRegisteredComponentsAreMissingDependencies(t *testing.T) {
	log := newInternalTestLogger(t)

	eventReg := NewRegistry()
	if err := eventReg.RegisterEventRunner(internalQueueRunner{name: "evt"}); err != nil {
		t.Fatal(err)
	}
	if err := NewManager(eventReg, WithLogger(log)).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "event queue driver not configured") {
		t.Fatalf("expected missing event driver error, got %v", err)
	}
	if err := NewManager(eventReg, WithLogger(log), WithEventQueueDriver(internalEventDriver{})).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "event consumer factory not configured") {
		t.Fatalf("expected missing event factory error, got %v", err)
	}

	delayReg := NewRegistry()
	if err := delayReg.RegisterDelayRunner(internalQueueRunner{name: "delay"}); err != nil {
		t.Fatal(err)
	}
	if err := NewManager(delayReg, WithLogger(log)).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "delay queue driver not configured") {
		t.Fatalf("expected missing delay driver error, got %v", err)
	}
	if err := NewManager(delayReg, WithLogger(log), WithDelayQueueDriver(&internalDelayDriver{})).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "delay consumer factory not configured") {
		t.Fatalf("expected missing delay factory error, got %v", err)
	}

	timerReg := NewRegistry()
	if err := timerReg.RegisterTimerTask(internalTimerRunner{name: "timer"}); err != nil {
		t.Fatal(err)
	}
	if err := NewManager(timerReg, WithLogger(log)).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "lock driver not configured") {
		t.Fatalf("expected missing lock driver error, got %v", err)
	}
	if err := NewManager(timerReg, WithLogger(log), WithLockDriver(internalLockDriver{})).Start(context.Background()); err == nil || !strings.Contains(err.Error(), "timer scheduler factory not configured") {
		t.Fatalf("expected missing timer factory error, got %v", err)
	}
}

func TestStartFailureCleansAlertDispatcherAndStartedConsumers(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(internalQueueRunner{name: "first"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterEventRunner(internalQueueRunner{name: "second"}); err != nil {
		t.Fatal(err)
	}

	var stopped atomic.Int64
	originalAlert := func(core.AlertData) {}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithEventQueueDriver(internalEventDriver{}),
		WithAlertFunc(originalAlert),
	)
	var created atomic.Int64
	mgr.SetEventConsumerFactory(func(core.QueueRunner, core.RunnerOption, driver.EventQueueDriver, driver.LockDriver, *ManagerConfig) consumer {
		if created.Add(1) == 2 {
			return &internalConsumer{startErr: errors.New("boom"), stopped: &stopped}
		}
		return &internalConsumer{stopped: &stopped}
	})

	if err := mgr.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected start failure, got %v", err)
	}
	if mgr.running {
		t.Fatal("manager should not be running after failed start")
	}
	if mgr.alertQueue != nil || mgr.alertCancel != nil || mgr.alertHandler != nil {
		t.Fatalf("alert dispatcher not cleaned: queue=%v cancel=%v handler=%v", mgr.alertQueue, mgr.alertCancel, mgr.alertHandler)
	}
	if mgr.cfg.OnAlert == nil {
		t.Fatal("expected original alert handler to be restored")
	}
	if got := stopped.Load(); got != 1 {
		t.Fatalf("expected one successfully started consumer to stop, got %d", got)
	}
}

func TestPublishDelayAllowsImmediateExecuteAt(t *testing.T) {
	drv := &internalDelayDriver{}
	mgr := NewManager(NewRegistry(), WithLogger(newInternalTestLogger(t)), WithDelayQueueDriver(drv))
	now := time.Now().Unix()
	env, err := mgr.PublishDelayPayload(context.Background(), "delay-now", "payload", now)
	if err != nil {
		t.Fatalf("expected current unix second to be accepted: %v", err)
	}
	if env == nil || env.Source != core.EnvelopeSourceDelay {
		t.Fatalf("unexpected envelope: %+v", env)
	}
	if drv.lastExecuteAt != now || drv.lastQueue != "taskx:delay:{delay-now}:pending" || drv.lastData == "" {
		t.Fatalf("unexpected driver call: queue=%q executeAt=%d data=%q", drv.lastQueue, drv.lastExecuteAt, drv.lastData)
	}
	if _, err := mgr.PublishDelayPayload(context.Background(), "delay-now", "payload", 0); err == nil {
		t.Fatal("expected non-positive executeAt to be rejected")
	}
}

func TestEventPopTimeoutConfigIsPassedToConsumer(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterEventRunner(internalQueueRunner{name: "event-pop-timeout"}); err != nil {
		t.Fatal(err)
	}
	drv := &eventPopTimeoutDriver{timeoutCh: make(chan time.Duration, 1)}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithEventQueueDriver(drv),
		WithEventPopTimeout(75*time.Millisecond),
	)
	mgr.SetEventConsumerFactory(newEventConsumerFactory)
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
	if err := reg.RegisterDelayRunner(runner, core.RunnerOption{MaxRetry: core.IntPtr(1), ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}
	drv := &delayRetryDriver{retryAtCh: make(chan int64, 1)}
	mgr := NewManager(
		reg,
		WithLogger(newInternalTestLogger(t)),
		WithDelayQueueDriver(drv),
		WithPollInterval(10*time.Millisecond),
		WithDelayRetryBaseInterval(2*time.Second),
	)
	mgr.SetDelayConsumerFactory(newDelayConsumerFactory)
	before := time.Now().Unix()
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Stop(context.Background()) }()

	select {
	case got := <-drv.retryAtCh:
		if got < before+2 || got > time.Now().Unix()+3 {
			t.Fatalf("retry executeAt not based on configured interval: got=%d before=%d now=%d", got, before, time.Now().Unix())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for RetryRequeue")
	}
}
