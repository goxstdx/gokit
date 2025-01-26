package components_redis

import (
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const REDIS_DEFAULT_ADDR = "127.0.0.1:6379"

type RedisClientConf struct {
	*redis.Options

	Logger *zap.SugaredLogger
}

var cfg *RedisClientConf
