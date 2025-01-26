package components_redis_cluster

import (
	"errors"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	ErrNil = redis.Nil
)

var RedisClusterClientNilErr = errors.New("RedisClusterClient is nill, Connection unavailable")
var RedisClusterClientConfEmptyErr = errors.New("RedisCluster not init")

type RedisClusterClientConf struct {
	*redis.ClusterOptions

	Logger *zap.SugaredLogger
}

var cfg *RedisClusterClientConf
