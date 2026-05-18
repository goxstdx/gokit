package example

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/consumer"
)

// ConsumerOnlyExample 展示"纯消费"场景：服务只消费队列任务，不主动推送。
// 适用于独立部署的 worker 服务。
func ConsumerOnlyExample() {
	log, _ := logger_factory.NewLogger(
		logger_factory.Config{
			DriverType:  logger_factory.DriverZap,
			Level:       logger_factory.LevelInfo,
			Development: true,
		},
	)

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	reg := consumer.NewRegistry()
	_ = reg.RegisterEventRunner(&OrderNotifyRunner{}, consumer.RunnerOption{MaxRetry: 5, ConsumerCount: 3})
	_ = reg.RegisterDelayRunner(&OrderNotifyRunner{}, consumer.RunnerOption{MaxRetry: 3, ConsumerCount: 2})
	_ = reg.RegisterTimerTask(&ReportTimerTask{})

	c := consumer.NewRedisConsumer(rdb, reg,
		consumer.WithKeyPrefix("myapp"),
		consumer.WithLogger(log),
		consumer.WithPollInterval(time.Second),
		consumer.WithRecoveryMode(consumer.RecoveryModeStartupAndPeriodic),
	)

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		panic(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	_ = c.Stop(ctx)
}
