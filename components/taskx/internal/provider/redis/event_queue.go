package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/driver"
)

var _ driver.EventQueueDriver = (*EventQueueProvider)(nil)

// EventQueueProvider 基于 Redis 的事件队列驱动。
// pending 使用 List，processing 使用 ZSet（score = 进入时间戳 UnixMicro）。
type EventQueueProvider struct {
	rdb          redis.Cmdable
	recoverBatch int64
}

func NewEventQueueProvider(rdb redis.Cmdable) *EventQueueProvider {
	return &EventQueueProvider{rdb: rdb, recoverBatch: 1000}
}

// SetRecoverBatchSize 设置崩溃恢复时每次 Lua 调用移动的消息数量
func (p *EventQueueProvider) SetRecoverBatchSize(n int64) {
	if n > 0 {
		p.recoverBatch = n
	}
}

func (p *EventQueueProvider) Push(ctx context.Context, queue string, data string) error {
	return p.rdb.LPush(ctx, queue, data).Err()
}

// PopToProcessing 通过 Lua 脚本原子地从 pending List RPOP 并 ZADD 到 processing ZSet。
// 非阻塞操作，调用方需自行轮询。timeout 参数保留以兼容接口签名，内部不使用。
func (p *EventQueueProvider) PopToProcessing(
	ctx context.Context,
	pendingQueue, processingQueue string,
	_ time.Duration,
) (string, error) {
	score := float64(time.Now().UnixMicro())
	val, err := scriptEventPopToProcessing.Run(
		ctx, p.rdb,
		[]string{pendingQueue, processingQueue},
		score,
	).Text()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	return val, nil
}

func (p *EventQueueProvider) Ack(ctx context.Context, processingQueue string, data string) error {
	return p.rdb.ZRem(ctx, processingQueue, data).Err()
}

func (p *EventQueueProvider) RetryRequeue(
	ctx context.Context,
	processingQueue, pendingQueue string,
	oldData, newData string,
) error {
	_, err := scriptEventRetryRequeue.Run(ctx, p.rdb, []string{processingQueue, pendingQueue}, oldData, newData).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (p *EventQueueProvider) Nack(ctx context.Context, processingQueue, pendingQueue string, data string) error {
	_, err := scriptEventNack.Run(ctx, p.rdb, []string{processingQueue, pendingQueue}, data).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (p *EventQueueProvider) MoveToDead(ctx context.Context, processingQueue, deadQueue string, data string) error {
	_, err := scriptEventMoveToDead.Run(ctx, p.rdb, []string{processingQueue, deadQueue}, data).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (p *EventQueueProvider) RecoverDead(ctx context.Context, deadQueue, pendingQueue string, count int64) (int64, error) {
	result, err := scriptRecoverDead.Run(ctx, p.rdb, []string{deadQueue, pendingQueue}, count).Int64()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return result, nil
}

func (p *EventQueueProvider) PopFromDead(ctx context.Context, deadQueue string) (string, error) {
	val, err := p.rdb.RPop(ctx, deadQueue).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	return val, nil
}

// RecoverProcessing 恢复 processing ZSet 中停留超过 timeout 的消息到 pending List。
func (p *EventQueueProvider) RecoverProcessing(
	ctx context.Context,
	processingQueue, pendingQueue string,
	timeout time.Duration,
) (int64, error) {
	timeoutScore := time.Now().Add(-timeout).UnixMicro()
	result, err := scriptEventRecoverProcessing.Run(
		ctx, p.rdb,
		[]string{processingQueue, pendingQueue},
		fmt.Sprintf("%d", timeoutScore), p.recoverBatch,
	).Int64()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return result, nil
}

func (p *EventQueueProvider) Len(ctx context.Context, queue string) (int64, error) {
	return p.rdb.LLen(ctx, queue).Result()
}
