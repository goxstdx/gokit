package taskx

import (
	"context"
	"fmt"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/queue"
)

// PublishDelay 发布延迟任务到 DelayQueue，并返回创建的 Envelope。
func (m *Manager) PublishDelay(ctx context.Context, runner core.QueueRunner, executeAt time.Time) (*core.Envelope, error) {
	return m.PublishDelayPayload(ctx, runner.GetName(), runner.Marshal(), executeAt)
}

// PublishDelayPayload 直接将 payload 包装为新消息并发布到 DelayQueue。
func (m *Manager) PublishDelayPayload(
	ctx context.Context,
	runnerName string,
	payload string,
	executeAt time.Time,
) (*core.Envelope, error) {
	if m.cfg.DelayDriver == nil {
		return nil, fmt.Errorf("taskx: delay queue driver not configured")
	}
	env := core.NewEnvelope(payload, core.EnvelopeSourceDelay)
	return m.PublishDelayEnvelope(ctx, runnerName, env, executeAt)
}

// PublishDelayEnvelope 将指定 Envelope 发布到 DelayQueue。
// 若 runnerName 未在 Registry 中注册，消息将被推入死信队列并触发告警。
func (m *Manager) PublishDelayEnvelope(
	ctx context.Context,
	runnerName string,
	env *core.Envelope,
	executeAt time.Time,
) (*core.Envelope, error) {
	if m.cfg.DelayDriver == nil {
		return nil, fmt.Errorf("taskx: delay queue driver not configured")
	}
	if env == nil {
		return nil, fmt.Errorf("taskx: envelope is nil")
	}
	now := time.Now()
	if executeAt.IsZero() || executeAt.Unix() < now.Unix() {
		return nil, fmt.Errorf("taskx: executeAt must not be zero or in the past")
	}

	env.Source = core.EnvelopeSourceDelay
	env.RunnerName = runnerName
	keys := queue.NewQueueKeySet(m.cfg.KeyPrefix, "delay", runnerName)

	if _, ok := m.registry.GetDelayRunners()[runnerName]; !ok {
		errMsg := fmt.Errorf("taskx: delay runner %q not registered, message pushed to dead letter queue", runnerName)
		m.cfg.Logger.Errorf("%v", errMsg)
		m.enqueueAlert(core.AlertData{
			Source:     core.AlertSourceDelay,
			AlertType:  core.AlertPublishUnregistered,
			RunnerName: runnerName,
			Envelope:   env,
			Remark:     errMsg.Error(),
		})
		deadAt := time.Now().UnixMicro()
		if addErr := m.cfg.DelayDriver.Add(ctx, keys.Dead, env.Encode(), deadAt); addErr != nil {
			m.cfg.Logger.Errorf("taskx: delay[%s] push to dead letter failed: %v", runnerName, addErr)
			return nil, fmt.Errorf("%w; additionally failed to push to dead letter: %v", errMsg, addErr)
		}
		return nil, errMsg
	}

	if err := m.cfg.DelayDriver.Add(ctx, keys.Pending, env.Encode(), executeAt.UnixMicro()); err != nil {
		return nil, err
	}
	return env, nil
}
