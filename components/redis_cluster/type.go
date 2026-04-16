package components_redis_cluster

import (
	"errors"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
)

const (
	ErrNil = redis.Nil
)

var RedisClusterClientNilErr = errors.New("RedisClusterClient is nill, Connection unavailable")
var RedisClusterClientConfEmptyErr = errors.New("RedisCluster not init")

type RedisClusterClientConf struct {
	*redis.ClusterOptions

	Logger logger_factory.Logger
}

var cfg *RedisClusterClientConf
