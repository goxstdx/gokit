package components_redis_cluster

import (
	"sync"

	"github.com/redis/go-redis/v9"
)

var initLock sync.Mutex

func initClient() *redis.ClusterClient {
	// 加锁
	initLock.Lock()
	defer initLock.Unlock()

	return redis.NewClusterClient(cfg.ClusterOptions)
}
