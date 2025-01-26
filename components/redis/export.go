package components_redis

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/redis/go-redis/v9"
)

// InitRedisClient
/*
 *  初始化 redis1 单点连接
 *
 *  @param ctx context.Context
 *  @param c RedisClientConf
 *  @return error
 *
 *  @Author: zhouxing@sailone.team
 *  @Date: 2023-07-18 15:16:32
 */
func InitRedisClient(ctx context.Context, c *RedisClientConf) (*redis.Client, error) {
	cfg = c
	if cfg == nil || cfg.Options == nil {
		return nil, errors.New("invalid configuration")
	}

	if cfg.Addr == "" {
		cfg.Addr = REDIS_DEFAULT_ADDR
	}

	cfg.Logger.Infof("InitRedisClient cfg: %+v", cfg)

	// 加锁的初始化
	client := initClient()

	cfg.Logger.Info("InitRedisClient init succ")

	return client, client.Ping(ctx).Err()
}

// CheckRedisConnectStatus
/*
 *  验证当前连接有效性，当连接不可用后，此方法会主动关闭连接，但不会主动初始化新的连接
 *
 *  @param ctx context.Context
 *  @return status bool
 *  @return err error
 *
 *  @Author: zhouxing@sailone.team
 *  @Date: 2023-07-18 15:16:58
 */
func CheckRedisConnectStatus(ctx context.Context, client *redis.Client) (status bool, err error) {
	err = client.Ping(ctx).Err()
	if err == nil {
		return true, err
	}

	if err == io.EOF {
		status = false
	}
	if strings.Contains(err.Error(), "use of closed network connection") {
		status = false
	}
	if strings.Contains(err.Error(), "connect: connection refused") {
		status = false
	}

	return status, err
}

func NewRedisClient() *redis.Client {
	return redis.NewClient(cfg.Options)
}
