package driver

import (
	"context"
	"time"
)

// DelayQueueDriver 延迟队列驱动接口。
// 所有时间相关的 int64 参数（executeAt / maxScore / score）均为 Unix 微秒（UnixMicro）。
type DelayQueueDriver interface {
	// Add 添加到 pending（ZSet，score=executeAt，Unix 微秒）
	Add(ctx context.Context, queue string, data string, executeAt int64) error

	// TransferToProcessing 将到期任务从 pending 原子转移到 processing（maxScore 为 Unix 微秒）
	TransferToProcessing(ctx context.Context, pendingQueue, processingQueue string, maxScore int64, count int64) ([]string, error)

	// Ack 执行成功，从 processing 移除
	Ack(ctx context.Context, processingQueue string, data string) error

	// Nack 执行失败，从 processing 移回 pending（executeAt 为 Unix 微秒）
	Nack(ctx context.Context, processingQueue, pendingQueue string, data string, executeAt int64) error

	// MoveToDead 从 processing 移入死信队列
	MoveToDead(ctx context.Context, processingQueue, deadQueue string, data string) error

	// PopFromDead 从死信队列弹出一条消息（用于 envelope 感知恢复）
	PopFromDead(ctx context.Context, deadQueue string) (string, error)

	// RetryRequeue 重试时原子地从 processing 删除旧消息并将新消息加入 pending（executeAt 为 Unix 微秒）
	RetryRequeue(ctx context.Context, processingQueue, pendingQueue string, oldData, newData string, executeAt int64) error

	// RecoverDead 从死信队列批量恢复到 pending（不重置 envelope）
	RecoverDead(ctx context.Context, deadQueue, pendingQueue string, count int64) (int64, error)

	// RecoverProcessing 恢复超时的 processing 消息到 pending（进程崩溃恢复）
	RecoverProcessing(ctx context.Context, processingQueue, pendingQueue string, timeout time.Duration) (int64, error)

	// Len 获取队列长度
	Len(ctx context.Context, queue string) (int64, error)
}
