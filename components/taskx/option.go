package taskx

import (
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

// Option Manager 配置选项
type Option func(*ManagerConfig)

// ManagerConfig 管理器配置
type ManagerConfig struct {
	EventDriver       driver.EventQueueDriver
	DelayDriver       driver.DelayQueueDriver
	LockDriver        driver.LockDriver
	KeyPrefix         string
	PollInterval      time.Duration
	LockTTL           time.Duration
	ProcessingTimeout time.Duration
	RecoverBatchSize  int64 // 崩溃恢复每批次移动的消息数量
	DefaultTimerTask  core.TimerTaskOption
	Logger            core.Logger
}

func defaultConfig() *ManagerConfig {
	return &ManagerConfig{
		KeyPrefix:         "taskx",
		PollInterval:      time.Second,
		LockTTL:           30 * time.Second,
		ProcessingTimeout: 5 * time.Minute,
		RecoverBatchSize:  1000,
		DefaultTimerTask: core.TimerTaskOption{
			MaxRetry:          core.IntPtr(0),
			ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencyForbidOverlap),
		},
		Logger: nil, // 调用方必须提供 Logger
	}
}

func WithEventQueueDriver(d driver.EventQueueDriver) Option {
	return func(c *ManagerConfig) { c.EventDriver = d }
}

func WithDelayQueueDriver(d driver.DelayQueueDriver) Option {
	return func(c *ManagerConfig) { c.DelayDriver = d }
}

func WithLockDriver(d driver.LockDriver) Option {
	return func(c *ManagerConfig) { c.LockDriver = d }
}

func WithKeyPrefix(prefix string) Option {
	return func(c *ManagerConfig) { c.KeyPrefix = prefix }
}

func WithPollInterval(d time.Duration) Option {
	return func(c *ManagerConfig) { c.PollInterval = d }
}

func WithLockTTL(ttl time.Duration) Option {
	return func(c *ManagerConfig) { c.LockTTL = ttl }
}

func WithProcessingTimeout(d time.Duration) Option {
	return func(c *ManagerConfig) { c.ProcessingTimeout = d }
}

func WithRecoverBatchSize(n int64) Option {
	return func(c *ManagerConfig) { c.RecoverBatchSize = n }
}

func WithDefaultTimerTaskOption(opt core.TimerTaskOption) Option {
	return func(c *ManagerConfig) { c.DefaultTimerTask = opt.Normalize() }
}

func WithLogger(l core.Logger) Option {
	return func(c *ManagerConfig) { c.Logger = l }
}
