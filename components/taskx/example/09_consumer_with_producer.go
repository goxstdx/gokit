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
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/consumer"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

// OrderPaymentRunner 订单支付 Runner，消费后需要延迟发送通知（消费端推送场景）。
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

	// 消费完成后，通过注入的 producer 发布一条延迟通知任务
	if p := ProducerFromCtx(ctx); p != nil {
		_, _ = p.PublishDelay(ctx,
			&OrderNotifyRunner{OrderID: data.OrderID, UserID: "auto"},
			time.Now().Add(10*time.Second),
		)
	}
	return taskx.RunnerFuncResult{IsOk: true}
}

type ctxKey struct{}

func ProducerFromCtx(ctx context.Context) *producer.Producer {
	if v, ok := ctx.Value(ctxKey{}).(*producer.Producer); ok {
		return v
	}
	return nil
}

// ConsumerWithProducerExample 展示"消费端也推送"场景：
// Consumer 和 Producer 独立创建，共享同一套 Redis + KeyPrefix。
// Consumer 消费任务后，通过 Producer 发布新的延迟任务。
func ConsumerWithProducerExample() {
	log, _ := logger_factory.NewLogger(
		logger_factory.Config{
			DriverType:  logger_factory.DriverZap,
			Level:       logger_factory.LevelInfo,
			Development: true,
		},
	)

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	// 1. 创建 Consumer（注册并启动消费）
	reg := consumer.NewRegistry()
	_ = reg.RegisterEventRunner(&OrderPaymentRunner{})
	_ = reg.RegisterDelayRunner(&OrderNotifyRunner{}, consumer.RunnerOption{MaxRetry: 3})

	c := consumer.NewRedisConsumer(rdb, reg,
		consumer.WithKeyPrefix("myapp"),
		consumer.WithLogger(log),
	)

	// 2. 创建 Producer（复用注册中心的分组/校验信息）
	p := producer.NewRedisProducer(rdb,
		producer.WithKeyPrefix("myapp"),
		producer.WithLogger(log),
		producer.WithEventGroupResolver(reg.EventGroupResolver()),
		producer.WithDelayRegisteredChecker(reg.DelayRegisteredChecker()),
	)

	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		panic(err)
	}

	// 发布一条支付事件，Runner 消费后会自动创建延迟通知
	_, _ = p.PublishEvent(ctx, &OrderPaymentRunner{OrderID: "ORD-200"})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	_ = c.Stop(ctx)
}
