package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
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

	NextTime int64 // >0 时转入 DelayQueue 延迟重试
}

// TimerTaskRunner 定时任务接口
type TimerTaskRunner interface {
	GetName() string
	GetCron() string
	Run(ctx context.Context) RunnerFuncResult
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
	// TimerConcurrencyAllowOverlap 仅对同一次 cron tick 做分布式去重，不阻止不同触发时刻重叠执行
	// 多机下依赖各节点时钟大体一致；如存在明显时钟漂移，同一 tick 仍可能出现重复执行。
	TimerConcurrencyAllowOverlap TimerConcurrencyPolicy = "allow_overlap"
)

// IntPtr 返回 int 值的指针，用于设置 MaxRetry 等可选字段
func IntPtr(v int) *int { return &v }

// TimerConcurrencyPolicyPtr 返回并发策略指针，用于设置可选字段
func TimerConcurrencyPolicyPtr(v TimerConcurrencyPolicy) *TimerConcurrencyPolicy { return &v }

// Logger 使用项目内 logger_factory.Logger
type Logger = logger_factory.Logger

func (o RunnerOption) Normalize() RunnerOption {
	if o.MaxRetry == nil {
		o.MaxRetry = IntPtr(3)
	} else if *o.MaxRetry < 0 {
		o.MaxRetry = IntPtr(0)
	}
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
		case TimerConcurrencyForbidOverlap, TimerConcurrencyAllowOverlap:
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

// Envelope 消息信封，包装 payload 并附带元数据（重试次数等）。
// ID 保证每条消息的唯一性，避免 DelayQueue ZSet member 去重导致消息丢失。
type Envelope struct {
	ID         string `json:"id"`
	Payload    string `json:"payload"`
	RetryCount int    `json:"retry_count"`
	CreatedAt  int64  `json:"created_at"`
}

func NewEnvelope(payload string) *Envelope {
	return &Envelope{
		ID:         uuid.NewString(),
		Payload:    payload,
		RetryCount: 0,
		CreatedAt:  time.Now().Unix(),
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
