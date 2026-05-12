package timer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

// Scheduler 定时任务调度器
type Scheduler struct {
	cron    *cron.Cron
	lock    driver.LockDriver
	prefix  string
	lockTTL time.Duration
	logger  core.Logger

	mu      sync.Mutex
	entries []cron.EntryID
}

// NewScheduler 创建定时任务调度器
func NewScheduler(lk driver.LockDriver, prefix string, lockTTL time.Duration, logger core.Logger) *Scheduler {
	return &Scheduler{
		cron:    cron.New(cron.WithSeconds(), cron.WithChain(cron.Recover(cronLogger{logger: logger}))),
		lock:    lk,
		prefix:  prefix,
		lockTTL: lockTTL,
		logger:  logger,
	}
}

// Register 注册一个定时任务
func (s *Scheduler) Register(task core.TimerTaskRunner, opt core.TimerTaskOption) error {
	lockKey := fmt.Sprintf("%s:lock:timer:{%s}", s.prefix, task.GetName())
	name := task.GetName()

	entryID, err := s.cron.AddFunc(task.GetCron(), func() {
		s.runTask(name, task, opt, lockKey)
	})
	if err != nil {
		return fmt.Errorf("taskx: add cron %q: %w", name, err)
	}

	s.mu.Lock()
	s.entries = append(s.entries, entryID)
	s.mu.Unlock()

	s.logger.Infof("taskx: timer[%s] registered with cron=%q", name, task.GetCron())
	return nil
}

func (s *Scheduler) runTask(name string, task core.TimerTaskRunner, opt core.TimerTaskOption, lockKey string) {
	ctx := context.Background()

	ok, err := s.lock.Lock(ctx, lockKey, s.lockTTL)
	if err != nil {
		s.logger.Errorf("taskx: timer[%s] lock error: %v", name, err)
		return
	}
	if !ok {
		return
	}
	defer func() {
		_ = s.lock.Unlock(ctx, lockKey)
	}()

	maxAttempts := 1 + opt.MaxRetry
	for attempt := 0; attempt < maxAttempts; attempt++ {
		result := task.Run(ctx)
		if result.IsOk {
			return
		}
		s.logger.Warnf("taskx: timer[%s] attempt %d/%d failed: %v", name, attempt+1, maxAttempts, result.Err)
	}
	s.logger.Errorf("taskx: timer[%s] all %d attempts failed", name, maxAttempts)
}

// Start 启动定时任务调度器
func (s *Scheduler) Start() {
	s.cron.Start()
	s.logger.Infof("taskx: timer scheduler started")
}

// Stop 停止定时任务调度器，等待运行中的任务完成
func (s *Scheduler) Stop() context.Context {
	ctx := s.cron.Stop()
	s.logger.Infof("taskx: timer scheduler stopped")
	return ctx
}

type cronLogger struct {
	logger core.Logger
}

func (l cronLogger) Info(msg string, keysAndValues ...interface{}) {
	l.logger.Infof("taskx: cron: "+msg, keysAndValues...)
}

func (l cronLogger) Error(err error, msg string, keysAndValues ...interface{}) {
	l.logger.Errorf("taskx: cron: %s: %v %v", msg, err, keysAndValues)
}
