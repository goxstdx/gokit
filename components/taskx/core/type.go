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
	Marshal(QueueRunner) string
	Unmarshal(val string, obj QueueRunner) error
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
	MaxRetry *int // 执行失败重试次数，nil=默认 0（不重试）
}

// IntPtr 返回 int 值的指针，用于设置 MaxRetry 等可选字段
func IntPtr(v int) *int { return &v }

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
	if o.MaxRetry == nil {
		o.MaxRetry = IntPtr(0)
	} else if *o.MaxRetry < 0 {
		o.MaxRetry = IntPtr(0)
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
