package taskx

import (
	"context"
	"fmt"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/queue"
)

// PublishEvent 发布事件到 EventQueue，并返回创建的 Envelope。
func (m *Manager) PublishEvent(ctx context.Context, runner core.QueueRunner) (*core.Envelope, error) {
	return m.PublishEventPayload(ctx, runner.GetName(), runner.Marshal())
}

// PublishEventPayload 直接将 payload 包装为新消息并发布到 EventQueue。
func (m *Manager) PublishEventPayload(ctx context.Context, runnerName string, payload string) (*core.Envelope, error) {
	if m.cfg.EventDriver == nil {
		return nil, fmt.Errorf("taskx: event queue driver not configured")
	}
	env := core.NewEnvelope(payload, core.EnvelopeSourceEvent)
	return m.PublishEventEnvelope(ctx, runnerName, env)
}

// PublishEventEnvelope 将指定 Envelope 发布到 EventQueue。
func (m *Manager) PublishEventEnvelope(ctx context.Context, runnerName string, env *core.Envelope) (*core.Envelope, error) {
	if m.cfg.EventDriver == nil {
		return nil, fmt.Errorf("taskx: event queue driver not configured")
	}
	if env == nil {
		return nil, fmt.Errorf("taskx: envelope is nil")
	}
	env.Source = core.EnvelopeSourceEvent
	env.RunnerName = runnerName
	groupName := m.resolveEventGroupName(runnerName)
	keys := queue.NewQueueKeySet(m.cfg.KeyPrefix, "event", groupName)

	if err := m.cfg.EventDriver.Push(ctx, keys.Pending, env.Encode()); err != nil {
		return nil, err
	}
	return env, nil
}
