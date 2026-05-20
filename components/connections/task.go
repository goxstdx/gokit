package connections

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/consumer"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

var (
	taskManager  *taskx.Manager
	taskProducer *producer.Producer
	taskConsumer *consumer.Consumer
)

// GenDefaultTaskManager 注意，这里每次执行都会覆盖默认的任务管理器
// 使用 redis 作为底层驱动，registry 用于注册任务，opts 为可选配置
func GenDefaultTaskManager(rdb redis.Cmdable, registry *taskx.Registry, opts ...taskx.Option) *taskx.Manager {
	taskManager = taskx.NewRedisManager(rdb, registry, opts...)
	return taskManager
}

// GetTaskManager 获取默认的任务管理器
func GetTaskManager() *taskx.Manager {
	return taskManager
}

// GenDefaultTaskProducer 注意，这里每次执行都会覆盖默认的任务生产者
// 使用 redis 作为底层驱动，opts 为可选配置
func GenDefaultTaskProducer(rdb redis.Cmdable, opts ...producer.Option) *producer.Producer {
	taskProducer = producer.NewRedisProducer(rdb, opts...)
	return taskProducer
}

// GetTaskProducer 获取默认的任务生产者
func GetTaskProducer() *producer.Producer {
	return taskProducer
}

// PublishEventQueue 推送 event 队列，只给 producer 推，如果想给 manager 推送，请获取 manager 后再推送
func PublishEventQueue(ctx context.Context, runner taskx.QueueRunner) (*taskx.Envelope, error) {
	if taskProducer == nil {
		return nil, fmt.Errorf("taskx: task producer is not initialized")
	}

	return taskProducer.PublishEvent(ctx, runner)
}

// PublishDelayQueue 推送 delay 队列，只给 producer 推，如果想给 manager 推送，请获取 manager 后再推送
func PublishDelayQueue(ctx context.Context, runner taskx.QueueRunner, executeAt time.Time) (*taskx.Envelope, error) {
	if taskProducer == nil {
		return nil, fmt.Errorf("taskx: task producer is not initialized")
	}

	return taskProducer.PublishDelay(ctx, runner, executeAt)
}

// GenDefaultTaskConsumer 注意，这里每次执行都会覆盖默认的任务消费者
// 使用 redis 作为底层驱动，registry 用于注册任务，opts 为可选配置
func GenDefaultTaskConsumer(rdb redis.Cmdable, registry *consumer.Registry, opts ...consumer.Option) *consumer.Consumer {
	taskConsumer = consumer.NewRedisConsumer(rdb, registry, opts...)
	return taskConsumer
}

// GetTaskConsumer 获取默认的任务消费者
func GetTaskConsumer() *consumer.Consumer {
	return taskConsumer
}

// ManualTimerTask 只能使用 consumer 进行执行，manager 请获取后执行
func ManualTimerTask(ctx context.Context, taskName string, payload string) (taskx.RunnerFuncResult, error) {
	if taskConsumer == nil || !taskConsumer.HealthOK() {
		return taskx.RunnerFuncResult{
			IsOk:     false,
			Err:      fmt.Errorf("taskx: timer task not run"),
			NextTime: nil,
		}, fmt.Errorf("taskx: task consumer is not running")
	}

	return taskConsumer.ExecuteTimerTaskOnce(
		ctx, taskx.TimerExecuteRequest{
			Name:    taskName,
			Payload: payload,
		},
	)
}
