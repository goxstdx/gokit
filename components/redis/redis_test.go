package components_redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestRedis(t *testing.T) {
	ctx := context.Background()
	rdb, _ := InitRedisClient(ctx, &RedisClientConf{
		Options: &redis.Options{Addr: "127.0.0.1:6379"},
		Logger:  zap.NewExample().Sugar(),
	})

	fmt.Println(rdb.Ping(ctx).Result())

	rdb.Set(ctx, "test", "test", 0)
	fmt.Println(rdb.Get(ctx, "test").Result())
	fmt.Println(rdb.TTL(ctx, "test").Result())
	rdb.Set(ctx, "test", "test", time.Second*5)
	fmt.Println(rdb.Get(ctx, "test").Result())
	fmt.Println(rdb.TTL(ctx, "test").Result())
	for i := 0; i <= 5; i++ {
		fmt.Println(rdb.Get(ctx, "test").Result())
		fmt.Println(rdb.TTL(ctx, "test").Result())
		time.Sleep(1 * time.Second)
	}
}
