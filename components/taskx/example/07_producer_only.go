package example

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

// ProducerOnlyExample 展示"纯推送"场景：服务只发布任务，不消费。
// 适用于 API 网关、BFF 等只需将任务投递到队列的服务。
func ProducerOnlyExample() {
	log, _ := logger_factory.NewLogger(
		logger_factory.Config{
			DriverType:  logger_factory.DriverZap,
			Level:       logger_factory.LevelInfo,
			Development: true,
		},
	)

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	p := producer.NewRedisProducer(rdb,
		producer.WithKeyPrefix("myapp"),
		producer.WithLogger(log),
	)

	ctx := context.Background()

	// 发布事件（即时队列）
	_, _ = p.PublishEvent(ctx, &OrderNotifyRunner{OrderID: "ORD-100", UserID: "USR-001"})

	// 发布延迟任务
	_, _ = p.PublishDelay(
		ctx,
		&OrderNotifyRunner{OrderID: "ORD-101", UserID: "USR-002"},
		time.Now().Add(5*time.Minute),
	)

	// 也可以直接用 payload 字符串
	_, _ = p.PublishEventPayload(ctx, "order-notify", `{"order_id":"ORD-102","user_id":"USR-003"}`)
}
