package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

var _ driver.EventQueueDriver = (*EventQueueProvider)(nil)

// EventQueueProvider 基于 Redis List 的事件队列驱动
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

func (p *EventQueueProvider) PopToProcessing(ctx context.Context, pendingQueue, processingQueue string, timeout time.Duration) (string, error) {
	// BLMOVE (Redis 6.2+)：原子地从 pending 尾部弹出并推入 processing 头部。
	// 队列非空时立即返回，为空时阻塞等待 timeout。
	val, err := p.rdb.BLMove(ctx, pendingQueue, processingQueue, "RIGHT", "LEFT", timeout).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", err
	}
	return val, nil
}

func (p *EventQueueProvider) Ack(ctx context.Context, processingQueue string, data string) error {
	return p.rdb.LRem(ctx, processingQueue, 1, data).Err()
}

func (p *EventQueueProvider) RetryRequeue(ctx context.Context, processingQueue, pendingQueue string, oldData, newData string) error {
	_, err := scriptEventRetryRequeue.Run(ctx, p.rdb, []string{processingQueue, pendingQueue}, oldData, newData).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
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
	// 策略：启动时将 processing 中所有消息移回 pending，分批执行避免阻塞 Redis。
	result, err := scriptRecoverDead.Run(ctx, p.rdb, []string{processingQueue, pendingQueue}, p.recoverBatch).Int64()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	return result, nil
}

func (p *EventQueueProvider) Len(ctx context.Context, queue string) (int64, error) {
	return p.rdb.LLen(ctx, queue).Result()
}

// CheckVersion 检测 Redis/Valkey 版本，BLMOVE 要求 >= 6.2
func (p *EventQueueProvider) CheckVersion(ctx context.Context) error {
	info, err := p.rdb.Info(ctx, "server").Result()
	if err != nil {
		return fmt.Errorf("taskx: failed to get server info: %w", err)
	}
	major, minor, ver, err := parseRedisVersion(info)
	if err != nil {
		return fmt.Errorf("taskx: %w", err)
	}
	if major < 6 || (major == 6 && minor < 2) {
		return fmt.Errorf("taskx: Redis/Valkey >= 6.2 required (BLMOVE), current version: %s", ver)
	}
	return nil
}

func parseRedisVersion(info string) (major, minor int, raw string, err error) {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		for _, prefix := range []string{"redis_version:", "valkey_version:"} {
			if strings.HasPrefix(line, prefix) {
				raw = strings.TrimPrefix(line, prefix)
				raw = strings.TrimSpace(raw)
				raw = strings.Fields(raw)[0]
				parts := strings.SplitN(raw, ".", 3)
				if len(parts) < 2 {
					continue
				}
				major, _ = strconv.Atoi(parts[0])
				minor, _ = strconv.Atoi(parts[1])
				return major, minor, raw, nil
			}
		}
	}
	return 0, 0, "", fmt.Errorf("unable to determine Redis/Valkey version from INFO output")
}
