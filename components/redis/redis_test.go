package components_redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
)

func TestRedis(t *testing.T) {
	ctx := context.Background()
	logger, err := logger_factory.NewExample()
	if err != nil {
		panic(err)
	}

	rdb, _ := InitRedisClient(
		ctx, &RedisClientConf{
			Options: &redis.Options{Addr: "127.0.0.1:6379"},
			Logger:  logger,
		},
	)

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
