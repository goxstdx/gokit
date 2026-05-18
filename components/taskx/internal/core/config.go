package core

import (
	"strings"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/driver"
)

// Config 任务管理器 / 消费者共享配置。
// Manager（taskx 根包）和 Consumer 均使用此结构，避免重复定义。
type Config struct {
	EventDriver driver.EventQueueDriver
	DelayDriver driver.DelayQueueDriver
	LockDriver  driver.LockDriver
	Logger      Logger

	KeyPrefix              string
	PollInterval           time.Duration // DelayQueue 轮询间隔
	EventPollInterval      time.Duration // EventQueue 消费者轮询间隔
	DelayRetryBaseInterval time.Duration
	LockTTL                time.Duration
	InternalOpTimeout      time.Duration
	TimerHeartbeatInterval time.Duration
	RecoveryGracePeriod    time.Duration // processing 中停留超过该时间的消息视为孤儿并恢复（默认 30s）
	RecoveryMode           RecoveryMode
	RecoverBatchSize       int64 // 崩溃恢复每批次移动的消息数量
	DefaultTimerTask       TimerTaskOption
	OnAlert                AlertFunc // 异常告警回调，nil 时仅记录日志
	AlertQueueSize         int       // 异常告警内部通道容量，满时丢弃并记录日志
	TraceContextKey        string    // 执行 Run 时注入 Envelope.ID 的 context key
	OnHeartbeat            ListenerHeartbeatFunc
	HealthInterval         time.Duration
	HealthBeatTimeout      time.Duration
	HealthAlertThreshold   int // 连续多少次健康检测失败后触发告警，默认 3；0 表示不告警
}

// DefaultConfig 返回具有合理默认值的 Config。
func DefaultConfig() *Config {
	return &Config{
		KeyPrefix:              "taskx",
		PollInterval:           defaults.PollInterval,
		EventPollInterval:      defaults.EventPopTimeout,
		DelayRetryBaseInterval: defaults.DelayRetryBaseInterval,
		LockTTL:                defaults.LockTTL,
		InternalOpTimeout:      defaults.InternalOpTimeout,
		RecoveryGracePeriod:    defaults.RecoveryGracePeriod,
		RecoveryMode:           RecoveryModeStartupOnly,
		RecoverBatchSize:       defaults.RecoverBatchSize,
		DefaultTimerTask: TimerTaskOption{
			MaxRetry:          IntPtr(0),
			ConcurrencyPolicy: TimerConcurrencyPolicyPtr(TimerConcurrencyForbidOverlap),
		},
		AlertQueueSize:    1024,
		TraceContextKey:   "taskx_trace_id",
		HealthInterval:    defaults.HealthInterval,
		HealthBeatTimeout: defaults.HealthBeatTimeout,
	}
}

// Option 配置选项函数。Manager 和 Consumer 共用。
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

// WithEventPollInterval 设置 EventQueue 消费者轮询间隔。
func WithEventPollInterval(d time.Duration) Option {
	return func(c *Config) { c.EventPollInterval = d }
}

// WithDelayRetryBaseInterval 设置 DelayQueue 未显式返回 NextTime 时的线性重试基准间隔。
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

// WithRecoveryGracePeriod 设置恢复容错时间。processing 中停留超过该时间的消息才会被恢复到 pending。
func WithRecoveryGracePeriod(d time.Duration) Option {
	return func(c *Config) { c.RecoveryGracePeriod = d }
}

// WithRecoveryMode 设置队列恢复模式：不恢复 / 仅启动恢复（默认）/ 启动+轮询恢复。
func WithRecoveryMode(mode RecoveryMode) Option {
	return func(c *Config) { c.RecoveryMode = mode.Normalize() }
}

func WithDefaultTimerTaskOption(opt TimerTaskOption) Option {
	return func(c *Config) { c.DefaultTimerTask = opt.Normalize() }
}

func WithLogger(l Logger) Option {
	return func(c *Config) { c.Logger = l }
}

func WithAlertFunc(f AlertFunc) Option {
	return func(c *Config) { c.OnAlert = f }
}

// WithAlertQueueSize 设置内部告警通知通道容量
func WithAlertQueueSize(n int) Option {
	return func(c *Config) { c.AlertQueueSize = n }
}

// WithTraceContextKey 设置注入到 Runner ctx 中的 trace id key（值为 Envelope.ID）
func WithTraceContextKey(key string) Option {
	return func(c *Config) { c.TraceContextKey = key }
}

// WithHealthInterval 设置健康监控采样间隔
func WithHealthInterval(d time.Duration) Option {
	return func(c *Config) { c.HealthInterval = d }
}

// WithHealthBeatTimeout 设置监听器心跳超时时间
func WithHealthBeatTimeout(d time.Duration) Option {
	return func(c *Config) { c.HealthBeatTimeout = d }
}

// WithHealthAlertThreshold 设置连续健康检测失败多少次后触发告警，0 表示不告警
func WithHealthAlertThreshold(n int) Option {
	return func(c *Config) { c.HealthAlertThreshold = n }
}
