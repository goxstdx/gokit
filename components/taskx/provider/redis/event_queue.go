package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

var _ driver.EventQueueDriver = (*EventQueueProvider)(nil)

// EventQueueProvider 基于 Redis List 的事件队列驱动
type EventQueueProvider struct {
	rdb redis.Cmdable
}

func NewEventQueueProvider(rdb redis.Cmdable) *EventQueueProvider {
	return &EventQueueProvider{rdb: rdb}
}

func (p *EventQueueProvider) Push(ctx context.Context, queue string, data string) error {
	return p.rdb.LPush(ctx, queue, data).Err()
}

func (p *EventQueueProvider) PopToProcessing(ctx context.Context, pendingQueue, processingQueue string, timeout time.Duration) (string, error) {
	// 先尝试非阻塞 Lua 弹出
	result, err := scriptPopToProcessing.Run(ctx, p.rdb, []string{pendingQueue, processingQueue}).Result()
	if err != nil && err != redis.Nil {
		return "", err
	}
	if err == nil && result != nil {
		if s, ok := result.(string); ok && s != "" {
			return s, nil
		}
	}

	// 队列为空时阻塞等待，使用 BRPOP 拿到后再 LPUSH 到 processing
	// 这里无法用 Lua 做阻塞，所以分两步，极端情况下可能丢失（进程在两步之间崩溃），
	// 但 RecoverProcessing 会兜底。
	vals, err := p.rdb.BRPop(ctx, timeout, pendingQueue).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	if len(vals) < 2 {
		return "", nil
	}
	data := vals[1]
	if err := p.rdb.LPush(ctx, processingQueue, data).Err(); err != nil {
		// 推入 processing 失败，重新放回 pending 尽量不丢
		_ = p.rdb.LPush(ctx, pendingQueue, data)
		return "", err
	}
	return data, nil
}

func (p *EventQueueProvider) Ack(ctx context.Context, processingQueue string, data string) error {
	return p.rdb.LRem(ctx, processingQueue, 1, data).Err()
}

func (p *EventQueueProvider) Nack(ctx context.Context, processingQueue, pendingQueue string, data string) error {
	_, err := scriptNack.Run(ctx, p.rdb, []string{processingQueue, pendingQueue}, data).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (p *EventQueueProvider) MoveToDead(ctx context.Context, processingQueue, deadQueue string, data string) error {
	_, err := scriptMoveToDead.Run(ctx, p.rdb, []string{processingQueue, deadQueue}, data).Result()
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

func (p *EventQueueProvider) RecoverProcessing(ctx context.Context, processingQueue, pendingQueue string, timeout time.Duration) (int64, error) {
	// EventQueue processing 用 List，无法按时间过滤。
	// 策略：启动时将 processing 中所有消息移回 pending。
	result, err := scriptRecoverDead.Run(ctx, p.rdb, []string{processingQueue, pendingQueue}, 1000).Int64()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return result, nil
}

func (p *EventQueueProvider) Len(ctx context.Context, queue string) (int64, error) {
	return p.rdb.LLen(ctx, queue).Result()
}
