package example

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
)

// AlertNotifyExample 展示如何在告警回调中接收 NextTime 通知，并由业务决定是否转投 DelayQueue。
func AlertNotifyExample() {
	log, _ := logger_factory.NewLogger(logger_factory.Config{
		DriverType:  logger_factory.DriverZap,
		Level:       logger_factory.LevelInfo,
		Development: true,
	})

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	registry := taskx.NewRegistry()
	runner := &rescheduleRunner{name: "notify-reschedule"}
	_ = registry.RegisterEventRunner(runner, core.RunnerOption{MaxRetry: core.IntPtr(3), ConsumerCount: 1})
	_ = registry.RegisterDelayRunner(runner, core.RunnerOption{MaxRetry: core.IntPtr(3), ConsumerCount: 1})

	var mgr *taskx.Manager
	onAlert := func(data core.AlertData) {
		if data.AlertType != core.AlertEventNextTimeIgnored || data.Envelope == nil || mgr == nil {
			return
		}

		executeAt := data.RunnerResult.NextTime
		if executeAt <= time.Now().Unix() {
			executeAt = time.Now().Add(time.Second).Unix()
		}

		env, err := mgr.PublishDelayEnvelope(context.Background(), data.RunnerName, data.Envelope, executeAt)
		if err != nil {
			log.Warnf("taskx: alert notify republish failed, runner=%s err=%v", data.RunnerName, err)
			return
		}
		log.Infof("taskx: alert notify republished to delay, runner=%s envelope_id=%s executeAt=%d", data.RunnerName, env.ID, executeAt)
	}

	mgr = taskx.NewRedisManager(
		rdb, registry,
		taskx.WithKeyPrefix("myapp-alert"),
		taskx.WithLogger(log),
		taskx.WithPollInterval(500*time.Millisecond),
		taskx.WithTraceContextKey("trace_id"),
		taskx.WithAlertFunc(onAlert),
	)

	ctx := context.Background()
	if err := mgr.Start(ctx); err != nil {
		panic(err)
	}

	_, _ = mgr.PublishEvent(ctx, runner)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	_ = mgr.Stop(ctx)
}

type rescheduleRunner struct {
	name string
	step atomic.Int64
}

func (r *rescheduleRunner) GetName() string { return r.name }
func (r *rescheduleRunner) Marshal() string { return `{"kind":"demo"}` }

func (r *rescheduleRunner) Run(ctx context.Context, payload string) core.RunnerFuncResult {
	traceID, _ := ctx.Value("trace_id").(string)
	fmt.Printf("runner=%s payload=%s trace_id=%s\n", r.name, payload, traceID)

	// 第一次在 EventQueue 中触发 NextTime 通知，后续在 DelayQueue 中按正常成功路径结束。
	if r.step.Add(1) == 1 {
		return core.RunnerFuncResult{
			IsOk:     false,
			Err:      fmt.Errorf("need reschedule"),
			NextTime: time.Now().Add(3 * time.Second).Unix(),
		}
	}
	return core.RunnerFuncResult{IsOk: true}
}
