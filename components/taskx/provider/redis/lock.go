package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

var _ driver.LockDriver = (*LockProvider)(nil)

// LockProvider 基于 Redis 的分布式锁实现
type LockProvider struct {
	rdb redis.Cmdable

	mu     sync.Mutex
	values map[string]string // key -> lock value，用于安全释放
}

func NewLockProvider(rdb redis.Cmdable) *LockProvider {
	return &LockProvider{
		rdb:    rdb,
		values: make(map[string]string),
	}
}

func (p *LockProvider) Lock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, held := p.values[key]; held {
		return false, nil
	}

	val := randomValue()
	ok, err := p.rdb.SetNX(ctx, key, val, ttl).Result()
	if err != nil {
		return false, err
	}
	if ok {
		p.values[key] = val
	}
	return ok, nil
}

func (p *LockProvider) Unlock(ctx context.Context, key string) error {
	p.mu.Lock()
	val, exists := p.values[key]
	if exists {
		delete(p.values, key)
	}
	p.mu.Unlock()

	if !exists {
		return nil
	}

	_, err := scriptUnlock.Run(ctx, p.rdb, []string{key}, val).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func (p *LockProvider) Renew(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	p.mu.Lock()
	val, exists := p.values[key]
	p.mu.Unlock()

	if !exists {
		return false, nil
	}

	result, err := scriptRenew.Run(ctx, p.rdb, []string{key}, val, ttl.Milliseconds()).Int64()
	if err != nil && err != redis.Nil {
		return false, err
	}
	return result == 1, nil
}

func randomValue() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
