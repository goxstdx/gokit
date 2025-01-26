package components_redis_cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func InitRedisCluster(ctx context.Context, c *RedisClusterClientConf) (*redis.ClusterClient, error) {
	cfg = c
	if cfg == nil || cfg.ClusterOptions == nil || len(cfg.Addrs) < 1 {
		return nil, RedisClusterClientConfEmptyErr
	}

	cfg.Logger.Infof("InitRedisCluster cfg: %+v", cfg)
	// 加锁的初始化
	client := initClient()

	if err := CheckRedisConnectStatus(ctx, client, true); err != nil {
		return nil, errors.New(fmt.Sprintf("InitRedisCluster ping fail, err: %v", err))
	}
	cfg.Logger.Infof("InitRedisCluster init succ")

	return client, nil
}

// CheckRedisConnectStatus 检测节点是否可用
func CheckRedisConnectStatus(ctx context.Context, client *redis.ClusterClient, isCheckAll bool) (err error) {
	if client == nil {
		return RedisClusterClientNilErr
	}
	if isCheckAll {
		err = client.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
			return shard.Ping(ctx).Err()
		})
	} else {
		err = client.Ping(ctx).Err()
	}

	return err
}

func NewRedisClient() *redis.ClusterClient {
	return redis.NewClusterClient(cfg.ClusterOptions)
}
