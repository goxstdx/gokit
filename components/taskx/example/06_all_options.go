package example

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	redisx "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/provider/redis"
)

// AllOptionsExample 展示 taskx 的“大而全”配置用法：
// - 覆盖所有 WithXxx 选项
// - 包含 Event / Delay / Timer 三类任务注册
// - 演示启动、投递、健康快照和停止的完整流程
func AllOptionsExample(ctx context.Context, rdb redis.Cmdable) error {
	log, _ := logger_factory.NewLogger(
		logger_factory.Config{
			DriverType:  logger_factory.DriverZap,
			Level:       logger_factory.LevelInfo,
			Development: true,
		},
	)

	registry := taskx.NewRegistry()

	// 注册 EventRunner：示例消费并发=2，最大重试次数=5。
	_ = registry.RegisterEventRunner(
		&OrderNotifyRunner{},
		core.RunnerOption{
			MaxRetry:      core.IntPtr(5),
			ConsumerCount: 2,
		},
	)

	// 注册 DelayRunner：示例消费并发=2，最大重试次数=3。
	_ = registry.RegisterDelayRunner(
		&OrderNotifyRunner{},
		core.RunnerOption{
			MaxRetry:      core.IntPtr(3),
			ConsumerCount: 2,
		},
	)

	// 注册 TimerTask：显式使用 single_per_tick 作为单任务并发策略。
	_ = registry.RegisterTimerTask(
		&ReportTimerTask{},
		core.TimerTaskOption{
			MaxRetry:          core.IntPtr(1),
			ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencySinglePerTick),
		},
	)

	// 显式创建 driver，并通过 WithXxxDriver 注入，演示所有可配置项。
	eventDriver := redisx.NewEventQueueProvider(rdb)
	delayDriver := redisx.NewDelayQueueProvider(rdb)
	lockDriver := redisx.NewLockProvider(rdb)

	mgr := taskx.NewRedisManager(
		rdb,
		registry,
		// WithEventQueueDriver：设置 EventQueueDriver（覆盖 NewRedisManager 默认驱动）。
		taskx.WithEventQueueDriver(eventDriver),
		// WithDelayQueueDriver：设置 DelayQueueDriver（覆盖 NewRedisManager 默认驱动）。
		taskx.WithDelayQueueDriver(delayDriver),
		// WithLockDriver：设置 LockDriver（queue 启动恢复与 timer 分布式锁都依赖它）。
		taskx.WithLockDriver(lockDriver),
		// WithKeyPrefix：统一 Redis key 前缀。
		taskx.WithKeyPrefix("myapp-taskx-all-options"),
		// WithPollInterval：DelayQueue 拉取到期任务的轮询间隔。
		taskx.WithPollInterval(500*time.Millisecond),
		// WithEventPopTimeout：EventQueue 每次 PopToProcessing 的阻塞等待时长。
		taskx.WithEventPopTimeout(2*time.Second),
		// WithDelayRetryBaseInterval：DelayQueue 未显式返回 NextTime 时的线性重试基准间隔。
		taskx.WithDelayRetryBaseInterval(3*time.Second),
		// WithLockTTL：分布式锁 TTL（timer 运行锁、启动恢复锁都使用该基线）。
		taskx.WithLockTTL(45*time.Second),
		// WithInternalOpTimeout：内部关键操作（Ack/Retry/MoveToDead/恢复锁续租等）超时。
		taskx.WithInternalOpTimeout(5*time.Second),
		// WithTimerHeartbeatInterval：Timer 监听器心跳上报周期。
		taskx.WithTimerHeartbeatInterval(2*time.Second),
		// WithProcessingTimeout：processing 中消息的超时阈值（用于启动恢复判断）。
		taskx.WithProcessingTimeout(4*time.Minute),
		// WithRecoverBatchSize：启动恢复时每批次处理消息数量（传递给 Redis provider）。
		taskx.WithRecoverBatchSize(500),
		// WithRecoveryGracePeriod：processing 中停留超过该时间的消息视为孤儿并恢复到 pending（默认 30s）。
		taskx.WithRecoveryGracePeriod(30*time.Second),
		// WithDefaultTimerTaskOption：全局 TimerTask 默认配置（单任务可覆盖）。
		taskx.WithDefaultTimerTaskOption(
			core.TimerTaskOption{
				MaxRetry:          core.IntPtr(0),
				ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencyForbidOverlap),
			},
		),
		// WithLogger：必填日志器，Manager.Start() 会校验。
		taskx.WithLogger(log),
		// WithAlertFunc：异常告警回调（内部异步调用，不阻塞消费主流程）。
		taskx.WithAlertFunc(
			func(data core.AlertData) {
				log.Warnf(
					"taskx alert: source=%s type=%s runner=%s err=%v",
					data.Source, data.AlertType, data.RunnerName, data.RunnerResult.Err,
				)
			},
		),
		// WithAlertQueueSize：内部告警通道容量，满时丢弃并写日志。
		taskx.WithAlertQueueSize(2048),
		// WithTraceContextKey：将 Envelope.ID 注入 Runner ctx 的 key 名。
		taskx.WithTraceContextKey("trace_id"),
		// WithHealthInterval：健康快照采样周期。
		taskx.WithHealthInterval(3*time.Second),
		// WithHealthBeatTimeout：监听器心跳超时阈值。
		taskx.WithHealthBeatTimeout(12*time.Second),
	)

	if err := mgr.Start(ctx); err != nil {
		return err
	}
	defer mgr.Stop(context.Background())

	// 发布一条 event 消息。
	if _, err := mgr.PublishEvent(
		ctx,
		&OrderNotifyRunner{OrderID: "ORD-1001", UserID: "USR-2001"},
	); err != nil {
		return err
	}

	// 发布一条 delay 消息，2 分钟后执行。
	if _, err := mgr.PublishDelay(
		ctx,
		&OrderNotifyRunner{OrderID: "ORD-1002", UserID: "USR-2002"},
		time.Now().Add(2*time.Minute),
	); err != nil {
		return err
	}

	// 可选：读取健康快照，用于上报监控或健康检查。
	_ = mgr.HealthSnapshot()
	return nil
}
