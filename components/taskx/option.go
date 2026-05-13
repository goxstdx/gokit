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
	OnAlert           core.AlertFunc // 异常告警回调，nil 时仅记录日志
	OnHeartbeat       core.ListenerHeartbeatFunc
	HealthInterval    time.Duration
	HealthBeatTimeout time.Duration
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
		Logger:            nil, // 调用方必须提供 Logger
		HealthInterval:    5 * time.Second,
		HealthBeatTimeout: 15 * time.Second,
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

func WithAlertFunc(f core.AlertFunc) Option {
	return func(c *ManagerConfig) { c.OnAlert = f }
}

// WithHealthInterval 设置健康监控采样间隔
func WithHealthInterval(d time.Duration) Option {
	return func(c *ManagerConfig) { c.HealthInterval = d }
}

// WithHealthBeatTimeout 设置监听器心跳超时时间
func WithHealthBeatTimeout(d time.Duration) Option {
	return func(c *ManagerConfig) { c.HealthBeatTimeout = d }
}
