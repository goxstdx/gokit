package connections

import (
	"context"

	"github.com/redis/go-redis/v9"

	components_redis "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/redis"
)

var (
	redisClient *redis.Client
)

// GenDefaultRedisClient 注意，这里每次执行都会覆盖默认的链接
func GenDefaultRedisClient(ctx context.Context, c *components_redis.RedisClientConf) (err error) {
	redisClient, err = components_redis.InitRedisClient(ctx, c)
	if err != nil {
		return err
	}

	return nil
}

// GetRedisClient 获取默认的redis链接
func GetRedisClient() *redis.Client {
	return redisClient
}
