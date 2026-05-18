package producer

import (
	"github.com/redis/go-redis/v9"

	redisx "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/provider/redis"
)

// NewRedisProducer 快捷构造：传入 redis.Cmdable 自动创建 Redis 驱动并组装 Producer。
func NewRedisProducer(rdb redis.Cmdable, opts ...Option) *Producer {
	ep := redisx.NewEventQueueProvider(rdb)
	dp := redisx.NewDelayQueueProvider(rdb)

	cfg := NewConfig(opts...)
	cfg.EventDriver = ep
	cfg.DelayDriver = dp

	return New(cfg)
}
