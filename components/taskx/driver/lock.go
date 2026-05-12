package driver

import (
	"context"
	"time"
)

// LockDriver 分布式锁驱动接口
type LockDriver interface {
	// Lock 尝试获取锁，返回是否成功
	Lock(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// Unlock 释放锁
	Unlock(ctx context.Context, key string) error

	// Renew 续期锁
	Renew(ctx context.Context, key string, ttl time.Duration) (bool, error)
}
