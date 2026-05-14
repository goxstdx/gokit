package taskx

import (
	"context"
	"fmt"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
)

// PublishDelay 发布延迟任务到 DelayQueue，并返回创建的 Envelope。
func (m *Manager) PublishDelay(ctx context.Context, runner core.QueueRunner, executeAt int64) (*core.Envelope, error) {
	return m.PublishDelayPayload(ctx, runner.GetName(), runner.Marshal(), executeAt)
}

// PublishDelayPayload 直接将 payload 包装为新消息并发布到 DelayQueue。
func (m *Manager) PublishDelayPayload(
	ctx context.Context,
	runnerName string,
	payload string,
	executeAt int64,
) (*core.Envelope, error) {
	if m.cfg.DelayDriver == nil {
		return nil, fmt.Errorf("taskx: delay queue driver not configured")
	}
	env := core.NewEnvelope(payload, core.EnvelopeSourceDelay)
	return m.PublishDelayEnvelope(ctx, runnerName, env, executeAt)
}

// PublishDelayEnvelope 将指定 Envelope 发布到 DelayQueue。
func (m *Manager) PublishDelayEnvelope(
	ctx context.Context,
	runnerName string,
	env *core.Envelope,
	executeAt int64,
) (*core.Envelope, error) {
	if m.cfg.DelayDriver == nil {
		return nil, fmt.Errorf("taskx: delay queue driver not configured")
	}
	if env == nil {
		return nil, fmt.Errorf("taskx: envelope is nil")
	}
	if executeAt <= 0 {
		return nil, fmt.Errorf("taskx: invalid executeAt=%d, must be a positive unix time", executeAt)
	}
	env.Source = core.EnvelopeSourceDelay
	key := fmt.Sprintf("%s:delay:{%s}:pending", m.cfg.KeyPrefix, runnerName)
	if err := m.cfg.DelayDriver.Add(ctx, key, env.Encode(), executeAt); err != nil {
		return nil, err
	}
	return env, nil
}
