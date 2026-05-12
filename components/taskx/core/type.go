package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
	MaxRetry      int // 最大重试次数，超过后进入死信队列。0=不重试，默认 3
	ConsumerCount int // 并发消费者数量，默认 1
}

// TimerTaskOption 定时任务注册选项
type TimerTaskOption struct {
	MaxRetry int // 执行失败重试次数，默认 0（不重试）
}

// Logger 使用项目内 logger_factory.Logger
type Logger = logger_factory.Logger

func (o RunnerOption) Normalize() RunnerOption {
	if o.MaxRetry < 0 {
		o.MaxRetry = 0
	}
	if o.MaxRetry == 0 {
		o.MaxRetry = 3
	}
	if o.ConsumerCount <= 0 {
		o.ConsumerCount = 1
	}
	return o
}

func (o TimerTaskOption) Normalize() TimerTaskOption {
	if o.MaxRetry < 0 {
		o.MaxRetry = 0
	}
	return o
}

// Envelope 消息信封，包装 payload 并附带元数据（重试次数等）
type Envelope struct {
	Payload    string `json:"payload"`
	RetryCount int    `json:"retry_count"`
	CreatedAt  int64  `json:"created_at"`
}

func NewEnvelope(payload string) *Envelope {
	return &Envelope{
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
