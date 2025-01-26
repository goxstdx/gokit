package components_redis

import (
	"sync"

	"github.com/redis/go-redis/v9"
)

var initLock sync.Mutex

func initClient() *redis.Client {
	// 加锁
	initLock.Lock()
	defer initLock.Unlock()

	return redis.NewClient(cfg.Options)
}
