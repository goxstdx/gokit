package example

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
)

// BootstrapExample 展示注册、启动、发布和优雅退出。
//
// 多机部署推荐：
//  1. 关键任务默认使用 forbid_overlap。
//  2. WithLockTTL 建议大于常规任务执行时长；即使框架会自动续租，也应避免 TTL 过小。
//  3. 如果业务诉求是“每个 tick 全局仅执行一次”，可单独对该任务显式配置 single_per_tick。
//  4. 无论哪种策略，任务逻辑本身都建议保持幂等。
func BootstrapExample() {
	log, _ := logger_factory.NewLogger(
		logger_factory.Config{
			DriverType:  logger_factory.DriverZap,
			Level:       logger_factory.LevelInfo,
			Development: true,
		},
	)

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	registry := taskx.NewRegistry()

	_ = registry.RegisterEventRunner(
		&OrderNotifyRunner{},
		core.RunnerOption{MaxRetry: core.IntPtr(5), ConsumerCount: 3},
	)
	_ = registry.RegisterDelayRunner(
		&OrderNotifyRunner{},
		core.RunnerOption{MaxRetry: core.IntPtr(3), ConsumerCount: 2},
	)
	_ = registry.RegisterTimerTask(
		&ReportTimerTask{}, core.TimerTaskOption{
			ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencySinglePerTick),
		},
	)

	mgr := taskx.NewRedisManager(
		rdb, registry,
		taskx.WithKeyPrefix("myapp"),
		taskx.WithLogger(log),
		taskx.WithPollInterval(time.Second),
		taskx.WithLockTTL(30*time.Second),
		taskx.WithProcessingTimeout(5*time.Minute),
		taskx.WithDefaultTimerTaskOption(
			core.TimerTaskOption{
				MaxRetry:          core.IntPtr(0),
				ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencyForbidOverlap),
			},
		),
	)

	ctx := context.Background()
	if err := mgr.Start(ctx); err != nil {
		panic(err)
	}

	_, _ = mgr.PublishEvent(ctx, &OrderNotifyRunner{OrderID: "ORD-001", UserID: "USR-123"})
	_, _ = mgr.PublishDelay(
		ctx,
		&OrderNotifyRunner{OrderID: "ORD-002", UserID: "USR-456"},
		time.Now().Add(10*time.Minute).Unix(),
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	_ = mgr.Stop(ctx)
}
