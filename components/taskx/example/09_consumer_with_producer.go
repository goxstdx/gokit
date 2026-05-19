package example

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
)

// OrderPaymentRunner 订单支付 Runner。
// 该 Runner 只负责消费支付事件本身，不在消费逻辑内部继续推送新任务。
type OrderPaymentRunner struct {
	OrderID string `json:"order_id"`
}

func (r *OrderPaymentRunner) GetName() string { return "order-payment" }

func (r *OrderPaymentRunner) Marshal() string {
	b, _ := json.Marshal(r)
	return string(b)
}

func (r *OrderPaymentRunner) Run(ctx context.Context, payload string) taskx.RunnerFuncResult {
	var data OrderPaymentRunner
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		return taskx.RunnerFuncResult{IsOk: false, Err: err}
	}
	fmt.Printf("processing payment: order=%s\n", data.OrderID)
	return taskx.RunnerFuncResult{IsOk: true}
}

// ConsumerWithProducerExample 展示 Manager 同时具备消费与推送能力的场景。
// 与纯 Producer / 纯 Consumer 不同，这里同一个 Manager 既启动消费链路，也直接通过自身的 Publish API 发布任务。
func ConsumerWithProducerExample() {
	log, _ := logger_factory.NewLogger(
		logger_factory.Config{
			DriverType:  logger_factory.DriverZap,
			Level:       logger_factory.LevelInfo,
			Development: true,
		},
	)

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	// 1. 创建 Manager：同一实例既负责消费，也负责推送。
	reg := taskx.NewRegistry()
	_ = reg.RegisterEventRunner(&OrderPaymentRunner{})
	_ = reg.RegisterDelayRunner(&OrderNotifyRunner{}, taskx.RunnerOption{MaxRetry: 3})

	m := taskx.NewRedisManager(rdb, reg,
		taskx.WithKeyPrefix("myapp"),
		taskx.WithLogger(log),
	)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		panic(err)
	}

	// 2. 使用同一个 Manager 直接发布事件任务。
	_, _ = m.PublishEvent(ctx, &OrderPaymentRunner{OrderID: "ORD-200"})

	// 3. 同一个 Manager 也可以直接发布延迟任务。
	_, _ = m.PublishDelay(ctx,
		&OrderNotifyRunner{OrderID: "ORD-201", UserID: "USR-200"},
		time.Now().Add(10*time.Second),
	)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	_ = m.Stop(ctx)
}
