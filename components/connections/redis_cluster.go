package connections

import (
	"context"

	"github.com/redis/go-redis/v9"

	components_redis_cluster "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/redis_cluster"
)

var (
	redisClusterClient *redis.ClusterClient
)

// GenDefaultRedisClusterClient 注意，这里每次执行都会覆盖默认的链接
func GenDefaultRedisClusterClient(ctx context.Context, c *components_redis_cluster.RedisClusterClientConf) (err error) {
	redisClusterClient, err = components_redis_cluster.InitRedisCluster(ctx, c)
	if err != nil {
		return err
	}

	return nil
}

// GetRedisClusterClient 获取默认的 redis cluster 链接
func GetRedisClusterClient() *redis.ClusterClient {
	return redisClusterClient
}
