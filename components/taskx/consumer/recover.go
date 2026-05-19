package consumer

import (
	"context"
	"fmt"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/queue"
)

// RecoverEventDead 从事件队列死信中恢复消息，重置重试计数。
func (c *Consumer) RecoverEventDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	cfg := c.Config()
	if cfg.EventDriver == nil {
		return 0, nil
	}
	groupName := c.resolveEventGroupName(runnerName)
	keys := queue.NewQueueKeySet(cfg.KeyPrefix, "event", groupName)
	return recoverEventDeadWithReset(ctx, cfg.EventDriver, keys.Dead, keys.Pending, count, cfg.Logger, cfg.OnAlert)
}

// RecoverDelayDead 从延迟队列死信中恢复消息，重置重试计数。
func (c *Consumer) RecoverDelayDead(ctx context.Context, runnerName string, count int64) (int64, error) {
	cfg := c.Config()
	if cfg.DelayDriver == nil {
		return 0, nil
	}
	keys := queue.NewQueueKeySet(cfg.KeyPrefix, "delay", runnerName)
	return recoverDelayDeadWithReset(ctx, cfg.DelayDriver, keys.Dead, keys.Pending, count, cfg.Logger, cfg.OnAlert)
}

func recoverEventDeadWithReset(
	ctx context.Context,
	drv driver.EventQueueDriver,
	deadKey, pendingKey string,
	count int64,
	logger core.Logger,
	onAlert core.AlertFunc,
) (int64, error) {
	var recovered int64
	for recovered < count {
		raw, err := drv.PopFromDead(ctx, deadKey)
		if err != nil {
			return recovered, err
		}
		if raw == "" {
			break
		}
		env, err := core.DecodeEnvelope(raw)
		if err != nil {
			logger.Warnf("taskx: recover event dead: skip corrupt message, raw: %s, err: %v", raw, err)
			if onAlert != nil {
				onAlert(
					core.AlertData{
						Source:    core.AlertSourceEvent,
						AlertType: core.AlertCorruptMessage,
						RunnerResult: core.RunnerFuncResult{
							IsOk: false,
							Err:  fmt.Errorf("recover event dead: corrupt message skipped, raw: %s", raw),
						},
					},
				)
			}
			continue
		}
		env.RetryCount = 0
		env.Source = core.EnvelopeSourceEvent
		if err := drv.Push(ctx, pendingKey, env.Encode()); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}

func recoverDelayDeadWithReset(
	ctx context.Context,
	drv driver.DelayQueueDriver,
	deadKey, pendingKey string,
	count int64,
	logger core.Logger,
	onAlert core.AlertFunc,
) (int64, error) {
	var recovered int64
	for recovered < count {
		raw, err := drv.PopFromDead(ctx, deadKey)
		if err != nil {
			return recovered, err
		}
		if raw == "" {
			break
		}
		env, err := core.DecodeEnvelope(raw)
		if err != nil {
			logger.Warnf("taskx: recover delay dead: skip corrupt message, raw: %s, err: %v", raw, err)
			if onAlert != nil {
				onAlert(
					core.AlertData{
						Source:    core.AlertSourceDelay,
						AlertType: core.AlertCorruptMessage,
						RunnerResult: core.RunnerFuncResult{
							IsOk: false,
							Err:  fmt.Errorf("recover delay dead: corrupt message skipped, raw: %s", raw),
						},
					},
				)
			}
			continue
		}
		env.RetryCount = 0
		env.Source = core.EnvelopeSourceDelay
		if err := drv.Add(ctx, pendingKey, env.Encode(), time.Now().UnixMicro()); err != nil {
			return recovered, err
		}
		recovered++
	}
	return recovered, nil
}
