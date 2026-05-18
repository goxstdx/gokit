package consumer

import (
	"strings"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/queue"
)

// Config Consumer 配置
type Config struct {
	EventDriver driver.EventQueueDriver
	DelayDriver driver.DelayQueueDriver
	LockDriver  driver.LockDriver
	Logger      core.Logger

	KeyPrefix              string
	PollInterval           time.Duration
	EventPollInterval      time.Duration
	DelayRetryBaseInterval time.Duration
	LockTTL                time.Duration
	InternalOpTimeout      time.Duration
	TimerHeartbeatInterval time.Duration
	RecoveryGracePeriod    time.Duration
	RecoveryMode           queue.RecoveryMode
	RecoverBatchSize       int64
	DefaultTimerTask       core.TimerTaskOption
	OnAlert                core.AlertFunc
	AlertQueueSize         int
	TraceContextKey        string
	OnHeartbeat            core.ListenerHeartbeatFunc
	HealthInterval         time.Duration
	HealthBeatTimeout      time.Duration
	HealthAlertThreshold   int
}

func defaultConfig() *Config {
	return &Config{
		KeyPrefix:              "taskx",
		PollInterval:           defaults.PollInterval,
		EventPollInterval:      defaults.EventPopTimeout,
		DelayRetryBaseInterval: defaults.DelayRetryBaseInterval,
		LockTTL:                defaults.LockTTL,
		InternalOpTimeout:      defaults.InternalOpTimeout,
		RecoveryGracePeriod:    defaults.RecoveryGracePeriod,
		RecoveryMode:           queue.RecoveryModeStartupOnly,
		RecoverBatchSize:       defaults.RecoverBatchSize,
		DefaultTimerTask: core.TimerTaskOption{
			MaxRetry:          core.IntPtr(0),
			ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencyForbidOverlap),
		},
		AlertQueueSize:    1024,
		TraceContextKey:   "taskx_trace_id",
		HealthInterval:    defaults.HealthInterval,
		HealthBeatTimeout: defaults.HealthBeatTimeout,
	}
}

// Option Consumer 配置选项
type Option func(*Config)

func WithEventQueueDriver(d driver.EventQueueDriver) Option {
	return func(c *Config) { c.EventDriver = d }
}

func WithDelayQueueDriver(d driver.DelayQueueDriver) Option {
	return func(c *Config) { c.DelayDriver = d }
}

func WithLockDriver(d driver.LockDriver) Option {
	return func(c *Config) { c.LockDriver = d }
}

func WithKeyPrefix(prefix string) Option {
	return func(c *Config) { c.KeyPrefix = strings.TrimRight(prefix, ":") }
}

func WithPollInterval(d time.Duration) Option {
	return func(c *Config) { c.PollInterval = d }
}

func WithEventPollInterval(d time.Duration) Option {
	return func(c *Config) { c.EventPollInterval = d }
}

func WithDelayRetryBaseInterval(d time.Duration) Option {
	return func(c *Config) { c.DelayRetryBaseInterval = d }
}

func WithLockTTL(ttl time.Duration) Option {
	return func(c *Config) { c.LockTTL = ttl }
}

func WithInternalOpTimeout(d time.Duration) Option {
	return func(c *Config) { c.InternalOpTimeout = d }
}

func WithTimerHeartbeatInterval(d time.Duration) Option {
	return func(c *Config) { c.TimerHeartbeatInterval = d }
}

func WithRecoverBatchSize(n int64) Option {
	return func(c *Config) { c.RecoverBatchSize = n }
}

func WithRecoveryGracePeriod(d time.Duration) Option {
	return func(c *Config) { c.RecoveryGracePeriod = d }
}

func WithRecoveryMode(mode queue.RecoveryMode) Option {
	return func(c *Config) { c.RecoveryMode = mode.Normalize() }
}

func WithDefaultTimerTaskOption(opt core.TimerTaskOption) Option {
	return func(c *Config) { c.DefaultTimerTask = opt.Normalize() }
}

func WithLogger(l core.Logger) Option {
	return func(c *Config) { c.Logger = l }
}

func WithAlertFunc(f core.AlertFunc) Option {
	return func(c *Config) { c.OnAlert = f }
}

func WithAlertQueueSize(n int) Option {
	return func(c *Config) { c.AlertQueueSize = n }
}

func WithTraceContextKey(key string) Option {
	return func(c *Config) { c.TraceContextKey = key }
}

func WithHealthInterval(d time.Duration) Option {
	return func(c *Config) { c.HealthInterval = d }
}

func WithHealthBeatTimeout(d time.Duration) Option {
	return func(c *Config) { c.HealthBeatTimeout = d }
}

func WithHealthAlertThreshold(n int) Option {
	return func(c *Config) { c.HealthAlertThreshold = n }
}
