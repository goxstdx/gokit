package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
)

// QueueRunner 队列任务接口，EventQueue 和 DelayQueue 共用
type QueueRunner interface {
	GetName() string
	Marshal() string
	Run(ctx context.Context, payload string) RunnerFuncResult
}

// RunnerFunc 便捷函数类型
type RunnerFunc func(context.Context, string) RunnerFuncResult

// RunnerFuncResult 执行结果
type RunnerFuncResult struct {
	IsOk bool
	Err  error

	NextTime int64 // DelayQueue 失败重试的下次执行时间；EventQueue 返回该值时会告警并 Ack（不回 event 重试）
}

// TimerTaskRunner 定时任务接口
type TimerTaskRunner interface {
	GetName() string
	GetCron() string
	GetTaskParam() string
	Run(ctx context.Context, payload string) RunnerFuncResult
}

// TimerExecuteRequest 手动执行定时任务请求参数
type TimerExecuteRequest struct {
	Name    string // 要执行的任务名，对应 TimerTaskRunner.GetName()
	Payload string // 透传给 TimerTaskRunner.Run
}

// RunnerOption 队列 Runner 注册选项
type RunnerOption struct {
	MaxRetry      *int // 最大重试次数，超过后进入死信队列。nil=默认 3，IntPtr(0)=不重试
	ConsumerCount int  // 并发消费者数量，默认 1
}

// TimerTaskOption 定时任务注册选项
type TimerTaskOption struct {
	MaxRetry          *int                    // 执行失败重试次数，nil=继承全局默认
	ConcurrencyPolicy *TimerConcurrencyPolicy // nil=继承全局默认
}

// TimerConcurrencyPolicy 定时任务并发策略
type TimerConcurrencyPolicy string

const (
	// TimerConcurrencyForbidOverlap 全局串行：同一 task 上一轮未结束时，下一轮直接跳过
	// 多机下通过固定锁 key + 自动续租降低长任务重入风险，但极端情况下仍建议业务保持幂等。
	TimerConcurrencyForbidOverlap TimerConcurrencyPolicy = "forbid_overlap"
	// TimerConcurrencySinglePerTick 每个 cron tick 全局至多执行一次，不阻止不同 tick 重叠执行
	// 适用于“同一轮只执行一次”的诉求；仍依赖各节点时钟大体一致，业务侧建议保持幂等。
	TimerConcurrencySinglePerTick TimerConcurrencyPolicy = "single_per_tick"
)

// IntPtr 返回 int 值的指针，用于设置 MaxRetry 等可选字段
func IntPtr(v int) *int { return new(v) }

// TimerConcurrencyPolicyPtr 返回并发策略指针，用于设置可选字段
func TimerConcurrencyPolicyPtr(v TimerConcurrencyPolicy) *TimerConcurrencyPolicy { return &v }

// Logger 使用项目内 logger_factory.Logger
type Logger = logger_factory.Logger

type AlertSource string

const (
	AlertSourceEvent AlertSource = "event_queue"
	AlertSourceTimer AlertSource = "timer_queue"
	AlertSourceDelay AlertSource = "delay_queue"
)

type AlertData struct {
	Source    AlertSource
	AlertType AlertType

	RunnerName   string
	Envelope     *Envelope
	RunnerResult RunnerFuncResult

	Remark string
}

// AlertFunc 异常告警回调。当框架遇到无法自动处理的异常（如消息格式损坏、重试全部失败等）时调用，
// 使调用方可以感知并接入自己的告警通道（钉钉、飞书、监控系统等）。
// 参数 data 包含告警来源、告警类型、Runner 名称、相关 Envelope 和执行结果等上下文信息。
type AlertFunc func(AlertData)

// ListenerKind 监听器类型
type ListenerKind string

const (
	ListenerKindEvent ListenerKind = "event"
	ListenerKindDelay ListenerKind = "delay"
	ListenerKindTimer ListenerKind = "timer"
)

// ListenerHeartbeat 监听器心跳数据
type ListenerHeartbeat struct {
	Kind ListenerKind
	Name string
	At   time.Time
}

// ListenerHeartbeatFunc 监听器心跳回调
type ListenerHeartbeatFunc func(ListenerHeartbeat)

// AlertType 告警类型
type AlertType string

const (
	// AlertCorruptMessage 消息格式损坏，无法解码
	AlertCorruptMessage AlertType = "corrupt_message"
	// AlertMaxRetryExhausted 消息重试次数耗尽，进入死信队列
	AlertMaxRetryExhausted AlertType = "max_retry_exhausted"
	// AlertEventNextTimeIgnored EventQueue 中返回 NextTime；仅告警，后续是否转入 DelayQueue 由业务决定
	AlertEventNextTimeIgnored AlertType = "event_next_time_ignored"
	// AlertTimerAllAttemptsFailed 定时任务所有重试均失败
	AlertTimerAllAttemptsFailed AlertType = "timer_all_attempts_failed"
	// AlertRecoveryLockLost 启动恢复过程中丢失分布式锁，可能导致多实例并发恢复
	AlertRecoveryLockLost AlertType = "recovery_lock_lost"
	// AlertRecoveryExceeded 启动恢复耗时超过上限，提前终止避免无限恢复
	AlertRecoveryExceeded AlertType = "recovery_exceeded"
	// AlertQueuePressure 队列积压
	AlertQueueBacklog AlertType = "queue_backlog"
)

func (o RunnerOption) Normalize() RunnerOption {
	// 默认不重试
	if o.MaxRetry == nil {
		o.MaxRetry = IntPtr(defaults.DefaultMaxRetry)
	} else if *o.MaxRetry < 0 {
		o.MaxRetry = IntPtr(defaults.DefaultMaxRetry)
	}

	// 默认单消费者
	if o.ConsumerCount <= 0 {
		o.ConsumerCount = 1
	}

	return o
}

func (o TimerTaskOption) Normalize() TimerTaskOption {
	if o.MaxRetry != nil && *o.MaxRetry < 0 {
		o.MaxRetry = IntPtr(0)
	}
	if o.ConcurrencyPolicy != nil {
		switch *o.ConcurrencyPolicy {
		case TimerConcurrencyForbidOverlap, TimerConcurrencySinglePerTick:
		default:
			o.ConcurrencyPolicy = nil
		}
	}
	return o
}

func (o TimerTaskOption) WithDefaults(defaults TimerTaskOption) TimerTaskOption {
	o = o.Normalize()
	defaults = defaults.Normalize()
	if o.MaxRetry == nil {
		o.MaxRetry = defaults.MaxRetry
	}
	if o.ConcurrencyPolicy == nil {
		o.ConcurrencyPolicy = defaults.ConcurrencyPolicy
	}
	if o.MaxRetry == nil {
		o.MaxRetry = IntPtr(0)
	}
	if o.ConcurrencyPolicy == nil {
		o.ConcurrencyPolicy = TimerConcurrencyPolicyPtr(TimerConcurrencyForbidOverlap)
	}
	return o
}

type EnvelopeSource string

const (
	EnvelopeSourceEvent EnvelopeSource = "event_queue"
	EnvelopeSourceTimer EnvelopeSource = "timer_queue"
	EnvelopeSourceDelay EnvelopeSource = "delay_queue"
)

// Envelope 消息信封，包装 payload 并附带元数据（重试次数等）。
// ID 保证每条消息的唯一性，避免 DelayQueue ZSet member 去重导致消息丢失。
type Envelope struct {
	ID         string         `json:"id"`
	Payload    string         `json:"payload"`
	RetryCount int            `json:"retry_count"`
	CreatedAt  int64          `json:"created_at"`
	Source     EnvelopeSource `json:"source"`
}

func NewEnvelope(payload string, source EnvelopeSource) *Envelope {
	return &Envelope{
		ID:         uuid.NewString(),
		Payload:    payload,
		RetryCount: 0,
		CreatedAt:  time.Now().Unix(),
		Source:     source,
	}
}

func (e *Envelope) Encode() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func DecodeEnvelope(raw string) (*Envelope, error) {
	var env Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return nil, fmt.Errorf("taskx: decode envelope: %w", err)
	}
	return &env, nil
}
