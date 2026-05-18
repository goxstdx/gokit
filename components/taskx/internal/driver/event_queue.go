package driver

import (
	"context"
	"time"
)

// EventQueueDriver 事件队列驱动接口
type EventQueueDriver interface {
	// Push 推入 pending 队列
	Push(ctx context.Context, queue string, data string) error

	// PopToProcessing 从 pending 原子弹出并移入 processing，阻塞等待 timeout
	PopToProcessing(ctx context.Context, pendingQueue, processingQueue string, timeout time.Duration) (string, error)

	// Ack 执行成功，从 processing 移除
	Ack(ctx context.Context, processingQueue string, data string) error

	// Nack 执行失败，从 processing 移回 pending
	Nack(ctx context.Context, processingQueue, pendingQueue string, data string) error

	// MoveToDead 从 processing 移入死信队列
	MoveToDead(ctx context.Context, processingQueue, deadQueue string, data string) error

	// PopFromDead 从死信队列弹出一条消息（用于 envelope 感知恢复）
	PopFromDead(ctx context.Context, deadQueue string) (string, error)

	// RecoverDead 从死信队列批量恢复到 pending（不重置 envelope）
	RecoverDead(ctx context.Context, deadQueue, pendingQueue string, count int64) (int64, error)

	// RetryRequeue 重试时原子地从 processing 删除旧消息并将新消息推入 pending
	RetryRequeue(ctx context.Context, processingQueue, pendingQueue string, oldData, newData string) error

	// RecoverProcessing 恢复超时的 processing 消息到 pending（进程崩溃恢复）
	RecoverProcessing(ctx context.Context, processingQueue, pendingQueue string, timeout time.Duration) (int64, error)

	// Len 获取队列长度
	Len(ctx context.Context, queue string) (int64, error)
}
