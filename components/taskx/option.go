package taskx

import (
	"strings"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
)

// Option Manager 配置选项
type Option func(*ManagerConfig)

// ManagerConfig 管理器配置
type ManagerConfig struct {
	EventDriver driver.EventQueueDriver
	DelayDriver driver.DelayQueueDriver
	LockDriver  driver.LockDriver
	Logger      core.Logger

	KeyPrefix              string
	PollInterval           time.Duration // DelayQueue 轮询间隔
	EventPollInterval      time.Duration // EventQueue 消费者轮询间隔
	DelayRetryBaseInterval time.Duration
	LockTTL                time.Duration
	InternalOpTimeout      time.Duration
	TimerHeartbeatInterval time.Duration
	RecoveryGracePeriod    time.Duration // processing 中停留超过该时间的消息视为孤儿并恢复（默认 30s）
	RecoverBatchSize       int64         // 崩溃恢复每批次移动的消息数量
	DefaultTimerTask       core.TimerTaskOption
	OnAlert                core.AlertFunc // 异常告警回调，nil 时仅记录日志
	AlertQueueSize         int            // 异常告警内部通道容量，满时丢弃并记录日志
	TraceContextKey        string         // 执行 Run 时注入 Envelope.ID 的 context key
	OnHeartbeat            core.ListenerHeartbeatFunc
	HealthInterval         time.Duration
	HealthBeatTimeout      time.Duration
	HealthAlertThreshold   int // 连续多少次健康检测失败后触发告警，默认 3；0 表示不告警
}

func defaultConfig() *ManagerConfig {
	return &ManagerConfig{
		KeyPrefix:              "taskx",
		PollInterval:           defaults.PollInterval,
		EventPollInterval:      defaults.EventPopTimeout,
		DelayRetryBaseInterval: defaults.DelayRetryBaseInterval,
		LockTTL:                defaults.LockTTL,
		InternalOpTimeout:      defaults.InternalOpTimeout,
		TimerHeartbeatInterval: 0,
		RecoveryGracePeriod:    defaults.RecoveryGracePeriod,
		RecoverBatchSize:       defaults.RecoverBatchSize,
		DefaultTimerTask: core.TimerTaskOption{
			MaxRetry:          core.IntPtr(0),
			ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencyForbidOverlap),
		},
		Logger:            nil, // 调用方必须提供 Logger
		AlertQueueSize:    1024,
		TraceContextKey:   "taskx_trace_id",
		HealthInterval:    defaults.HealthInterval,
		HealthBeatTimeout: defaults.HealthBeatTimeout,
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
	return func(c *ManagerConfig) { c.KeyPrefix = strings.TrimRight(prefix, ":") }
}

func WithPollInterval(d time.Duration) Option {
	return func(c *ManagerConfig) { c.PollInterval = d }
}

// WithEventPollInterval 设置 EventQueue 消费者轮询间隔。
func WithEventPollInterval(d time.Duration) Option {
	return func(c *ManagerConfig) { c.EventPollInterval = d }
}

// WithDelayRetryBaseInterval 设置 DelayQueue 未显式返回 NextTime 时的线性重试基准间隔。
func WithDelayRetryBaseInterval(d time.Duration) Option {
	return func(c *ManagerConfig) { c.DelayRetryBaseInterval = d }
}

func WithLockTTL(ttl time.Duration) Option {
	return func(c *ManagerConfig) { c.LockTTL = ttl }
}

func WithInternalOpTimeout(d time.Duration) Option {
	return func(c *ManagerConfig) { c.InternalOpTimeout = d }
}

func WithTimerHeartbeatInterval(d time.Duration) Option {
	return func(c *ManagerConfig) { c.TimerHeartbeatInterval = d }
}

func WithRecoverBatchSize(n int64) Option {
	return func(c *ManagerConfig) { c.RecoverBatchSize = n }
}

// WithRecoveryGracePeriod 设置恢复容错时间。processing 中停留超过该时间的消息才会被恢复到 pending。
func WithRecoveryGracePeriod(d time.Duration) Option {
	return func(c *ManagerConfig) { c.RecoveryGracePeriod = d }
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

// WithAlertQueueSize 设置内部告警通知通道容量
func WithAlertQueueSize(n int) Option {
	return func(c *ManagerConfig) { c.AlertQueueSize = n }
}

// WithTraceContextKey 设置注入到 Runner ctx 中的 trace id key（值为 Envelope.ID）
func WithTraceContextKey(key string) Option {
	return func(c *ManagerConfig) { c.TraceContextKey = key }
}

// WithHealthInterval 设置健康监控采样间隔
func WithHealthInterval(d time.Duration) Option {
	return func(c *ManagerConfig) { c.HealthInterval = d }
}

// WithHealthBeatTimeout 设置监听器心跳超时时间
func WithHealthBeatTimeout(d time.Duration) Option {
	return func(c *ManagerConfig) { c.HealthBeatTimeout = d }
}

// WithHealthAlertThreshold 设置连续健康检测失败多少次后触发告警，0 表示不告警
func WithHealthAlertThreshold(n int) Option {
	return func(c *ManagerConfig) { c.HealthAlertThreshold = n }
}
