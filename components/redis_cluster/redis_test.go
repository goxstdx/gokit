package components_redis_cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func TestCluster(t *testing.T) {
	ctx := context.Background()
	rdb, err := InitRedisCluster(ctx, &RedisClusterClientConf{
		ClusterOptions: &redis.ClusterOptions{
			Addrs: []string{
				"127.0.0.1:7000",
				"127.0.0.1:7001",
				"127.0.0.1:7002",
				"127.0.0.1:7003",
				"127.0.0.1:7004",
				"127.0.0.1:7005",
			},
		},
		Logger: zap.NewExample().Sugar(),
	})
	if err != nil {
		panic(err)
	}
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
