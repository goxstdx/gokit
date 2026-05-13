package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

var _ driver.DelayQueueDriver = (*DelayQueueProvider)(nil)

// DelayQueueProvider 基于 Redis ZSet 的延迟队列驱动
type DelayQueueProvider struct {
	rdb          redis.Cmdable
	recoverBatch int64
}

func NewDelayQueueProvider(rdb redis.Cmdable) *DelayQueueProvider {
	return &DelayQueueProvider{rdb: rdb, recoverBatch: 1000}
}

// SetRecoverBatchSize 设置崩溃恢复时每次 Lua 调用移动的消息数量
func (p *DelayQueueProvider) SetRecoverBatchSize(n int64) {
	if n > 0 {
		p.recoverBatch = n
	}
}

func (p *DelayQueueProvider) Add(ctx context.Context, queue string, data string, executeAt int64) error {
	return p.rdb.ZAdd(ctx, queue, redis.Z{Score: float64(executeAt), Member: data}).Err()
}

func (p *DelayQueueProvider) TransferToProcessing(ctx context.Context, pendingQueue, processingQueue string, maxScore int64, count int64) ([]string, error) {
	processingScore := time.Now().Unix()
	result, err := scriptDelayTransfer.Run(ctx, p.rdb,
		[]string{pendingQueue, processingQueue},
		maxScore, count, processingScore,
	).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	items, ok := result.([]interface{})
	if !ok || len(items) == 0 {
		return nil, nil
	}

	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

func (p *DelayQueueProvider) Ack(ctx context.Context, processingQueue string, data string) error {
	return p.rdb.ZRem(ctx, processingQueue, data).Err()
}

func (p *DelayQueueProvider) RetryRequeue(ctx context.Context, processingQueue, pendingQueue string, oldData, newData string, executeAt int64) error {
	_, err := scriptDelayRetryRequeue.Run(ctx, p.rdb, []string{processingQueue, pendingQueue}, oldData, newData, executeAt).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (p *DelayQueueProvider) Nack(ctx context.Context, processingQueue, pendingQueue string, data string, executeAt int64) error {
	_, err := scriptDelayNack.Run(ctx, p.rdb,
		[]string{processingQueue, pendingQueue},
		data, executeAt,
	).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (p *DelayQueueProvider) MoveToDead(ctx context.Context, processingQueue, deadQueue string, data string) error {
	deadAt := time.Now().Unix()
	_, err := scriptDelayMoveToDead.Run(ctx, p.rdb,
		[]string{processingQueue, deadQueue},
		data, deadAt,
	).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (p *DelayQueueProvider) RecoverDead(ctx context.Context, deadQueue, pendingQueue string, count int64) (int64, error) {
	newScore := time.Now().Unix()
	result, err := scriptDelayRecoverDead.Run(ctx, p.rdb,
		[]string{deadQueue, pendingQueue},
		count, newScore,
	).Int64()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return result, nil
}

func (p *DelayQueueProvider) PopFromDead(ctx context.Context, deadQueue string) (string, error) {
	result, err := scriptDelayPopFromDead.Run(ctx, p.rdb, []string{deadQueue}).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	if result == nil {
		return "", nil
	}
	if s, ok := result.(string); ok {
		return s, nil
	}
	return "", nil
}

func (p *DelayQueueProvider) RecoverProcessing(ctx context.Context, processingQueue, pendingQueue string, timeout time.Duration) (int64, error) {
	timeoutScore := time.Now().Add(-timeout).Unix()
	newScore := time.Now().Unix()
	result, err := scriptDelayRecoverProcessing.Run(ctx, p.rdb,
		[]string{processingQueue, pendingQueue},
		fmt.Sprintf("%d", timeoutScore), newScore, p.recoverBatch,
	).Int64()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return result, nil
}

func (p *DelayQueueProvider) Len(ctx context.Context, queue string) (int64, error) {
	return p.rdb.ZCard(ctx, queue).Result()
}
