package components_redis

import (
	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
)

const REDIS_DEFAULT_ADDR = "127.0.0.1:6379"

type RedisClientConf struct {
	*redis.Options

	Logger logger_factory.Logger
}

var cfg *RedisClientConf
