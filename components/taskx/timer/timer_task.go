package timer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
)

// Scheduler 定时任务调度器
type Scheduler struct {
	cron              *cron.Cron
	parser            cron.Parser
	lock              driver.LockDriver
	prefix            string
	lockTTL           time.Duration
	internalOpTimeout time.Duration
	heartbeatInterval time.Duration
	logger            core.Logger
	onAlert           core.AlertFunc
	onBeat            core.ListenerHeartbeatFunc

	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	entries []cron.EntryID
}

// NewScheduler 创建定时任务调度器
func NewScheduler(
	lk driver.LockDriver,
	prefix string,
	lockTTL time.Duration,
	internalOpTimeout time.Duration,
	heartbeatInterval time.Duration,
	logger core.Logger,
	onAlert core.AlertFunc,
	onBeat core.ListenerHeartbeatFunc,
) *Scheduler {
	return &Scheduler{
		cron:              cron.New(cron.WithSeconds(), cron.WithChain(cron.Recover(cronLogger{logger: logger}))),
		parser:            cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor),
		lock:              lk,
		prefix:            prefix,
		lockTTL:           lockTTL,
		internalOpTimeout: internalOpTimeout,
		heartbeatInterval: heartbeatInterval,
		logger:            logger,
		onAlert:           onAlert,
		onBeat:            onBeat,
	}
}

func (s *Scheduler) alert(data core.AlertData) {
	if s.onAlert != nil {
		if data.Source == "" {
			data.Source = core.AlertSourceTimer
		}
		s.onAlert(data)
	}
}

func (s *Scheduler) beat() {
	if s.onBeat == nil {
		return
	}
	s.onBeat(core.ListenerHeartbeat{
		Kind: core.ListenerKindTimer,
		Name: "cron",
		At:   time.Now(),
	})
}

func (s *Scheduler) internalOpContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := s.internalOpTimeout
	if timeout <= 0 {
		timeout = defaults.InternalOpTimeout
	}
	return context.WithTimeout(parent, timeout)
}

// Register 注册一个定时任务。
//
// 多机语义说明：
//  1. forbid_overlap：使用固定锁 key，并在执行期间自动续租，降低长任务因 TTL 到期而被其他实例重入的风险。
//  2. single_per_tick：使用“计划触发时间”生成锁 key，仅对同一次 cron tick 做去重，不阻止不同 tick 重叠执行。
//  3. 两类策略都仍依赖各机器时钟大体一致；如发生长时间 STW、网络抖动、Redis 不可用或时钟明显漂移，仍可能出现重复执行，业务应保持幂等。
func (s *Scheduler) Register(task core.TimerTaskRunner, opt core.TimerTaskOption) error {
	name := task.GetName()
	schedule, err := s.parser.Parse(task.GetCron())
	if err != nil {
		return fmt.Errorf("taskx: parse cron %q: %w", name, err)
	}
	job := &timerJob{
		scheduler: s,
		name:      name,
		task:      task,
		opt:       opt,
		schedule:  schedule,
		nextTick:  schedule.Next(time.Now().UTC()),
	}

	entryID := s.cron.Schedule(schedule, job)

	s.mu.Lock()
	s.entries = append(s.entries, entryID)
	s.mu.Unlock()

	s.logger.Infof("taskx: timer[%s] registered with cron=%q policy=%s", name, task.GetCron(), *opt.ConcurrencyPolicy)
	return nil
}

func (s *Scheduler) runTask(name string, task core.TimerTaskRunner, opt core.TimerTaskOption, scheduledAt time.Time) {
	s.beat()
	s.mu.Lock()
	ctx := s.ctx
	s.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}

	lockKey := s.lockKey(name, *opt.ConcurrencyPolicy, scheduledAt)

	lockCtx, lockCancel := s.internalOpContext(ctx)
	ok, err := s.lock.Lock(lockCtx, lockKey, s.lockTTL)
	lockCancel()
	if err != nil {
		s.logger.Errorf("taskx: timer[%s] lock error: %v", name, err)
		return
	}
	if !ok {
		return
	}
	stopRenew := func() {}
	if *opt.ConcurrencyPolicy == core.TimerConcurrencyForbidOverlap {
		stopRenew = s.startRenewLoop(lockKey)
	}
	defer func() {
		stopRenew()
		unlockCtx, unlockCancel := s.internalOpContext(context.Background())
		defer unlockCancel()
		_ = s.lock.Unlock(unlockCtx, lockKey)
	}()

	maxAttempts := 1 + *opt.MaxRetry
	payload := task.GetTaskParam()
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			s.logger.Infof("taskx: timer[%s] cancelled, stopping retries", name)
			return
		}
		result := task.Run(ctx, payload)
		if result.IsOk {
			return
		}
		s.logger.Warnf("taskx: timer[%s] attempt %d/%d failed: %v", name, attempt+1, maxAttempts, result.Err)
	}
	s.logger.Errorf("taskx: timer[%s] all %d attempts failed", name, maxAttempts)
	s.alert(
		core.AlertData{
			Source:       core.AlertSourceTimer,
			AlertType:    core.AlertTimerAllAttemptsFailed,
			RunnerName:   name,
			RunnerResult: core.RunnerFuncResult{IsOk: false, Err: fmt.Errorf("timer[%s] all %d attempts failed", name, maxAttempts)},
		},
	)
}

func (s *Scheduler) startRenewLoop(lockKey string) func() {
	interval := s.lockTTL / defaults.LockRenewIntervalDivisor
	if interval <= 0 {
		interval = defaults.DefaultLockRenewInterval
	}
	if interval < defaults.MinLockRenewInterval {
		interval = defaults.MinLockRenewInterval
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := s.internalOpContext(ctx)
				ok, err := s.lock.Renew(renewCtx, lockKey, s.lockTTL)
				renewCancel()
				if err != nil {
					s.logger.Warnf("taskx: timer renew lock error, key=%s err=%v", lockKey, err)
					continue
				}
				if !ok {
					s.logger.Warnf(
						"taskx: timer lock lost during execution, key=%s; duplicate execution may occur in multi-instance deployment",
						lockKey,
					)
					return
				}
			}
		}
	}()

	return func() {
		cancel()
		wg.Wait()
	}
}

func (s *Scheduler) lockKey(name string, policy core.TimerConcurrencyPolicy, scheduledAt time.Time) string {
	switch policy {
	case core.TimerConcurrencySinglePerTick:
		return fmt.Sprintf("%s:lock:timer:{%s}:%d", s.prefix, name, scheduledAt.UTC().Unix())
	default:
		return fmt.Sprintf("%s:lock:timer:{%s}", s.prefix, name)
	}
}

type timerJob struct {
	scheduler *Scheduler
	name      string
	task      core.TimerTaskRunner
	opt       core.TimerTaskOption
	schedule  cron.Schedule

	mu       sync.Mutex
	nextTick time.Time
}

func (j *timerJob) Run() {
	scheduledAt := j.consumeScheduledTick(time.Now().UTC())
	j.scheduler.runTask(j.name, j.task, j.opt, scheduledAt)
}

func (j *timerJob) consumeScheduledTick(now time.Time) time.Time {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.nextTick.IsZero() {
		j.nextTick = j.schedule.Next(now)
	}
	scheduledAt := j.nextTick
	for !j.nextTick.After(now) {
		scheduledAt = j.nextTick
		j.nextTick = j.schedule.Next(j.nextTick)
	}
	if j.nextTick.Equal(scheduledAt) {
		j.nextTick = j.schedule.Next(j.nextTick)
	}
	return scheduledAt
}

// Start 启动定时任务调度器
func (s *Scheduler) Start() {
	s.mu.Lock()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	heartbeatInterval := s.heartbeatInterval
	s.mu.Unlock()

	if heartbeatInterval <= 0 {
		heartbeatInterval = defaults.TimerHeartbeatFallback
	}
	if _, err := s.cron.AddFunc("@every "+heartbeatInterval.String(), func() { s.beat() }); err != nil {
		s.logger.Warnf("taskx: timer heartbeat registration failed: %v", err)
	}
	s.cron.Start()
	s.beat()
	s.logger.Infof("taskx: timer scheduler started")
}

// Stop 停止定时任务调度器，取消运行中任务的 context 并等待完成。
// 注意：Stop 会先取消 ctx 通知运行中的任务退出，然后等待 cron 内所有 job 完成。
// 如果 TimerTaskRunner.Run 实现中未检查 ctx.Done()，Stop 仍会阻塞直到任务自然结束。
// 调用方应确保 TimerTaskRunner.Run 能响应 context 取消信号以实现快速退出。
func (s *Scheduler) Stop() context.Context {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	s.mu.Unlock()

	cronCtx := s.cron.Stop()
	s.logger.Infof("taskx: timer scheduler stopped")
	return cronCtx
}

type cronLogger struct {
	logger core.Logger
}

func (l cronLogger) Info(msg string, keysAndValues ...interface{}) {
	l.logger.Infof("taskx: cron: %s %v", msg, keysAndValues)
}

func (l cronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	l.logger.Errorf("taskx: cron: %s: %v %v", msg, err, keysAndValues)
}
