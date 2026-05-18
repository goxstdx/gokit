package producer

import (
	"context"
	"fmt"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/queue"
)

// EventGroupResolver 根据 runnerName 解析所属事件队列组名。
// 返回 (groupName, registered)。若 registered 为 false，消息将推入以 runnerName 命名的死信队列。
type EventGroupResolver func(runnerName string) (groupName string, registered bool)

// DelayRegisteredChecker 检查 delay runner 是否已注册。
type DelayRegisteredChecker func(runnerName string) bool

// Producer 任务生产者，负责将消息推入 EventQueue / DelayQueue。
// 与 Manager 不同，Producer 不启动消费协程，适用于"只推送不消费"的服务。
type Producer struct {
	cfg *Config
}

// New 创建 Producer。
func New(cfg Config) *Producer {
	cfg.normalize()
	return &Producer{cfg: &cfg}
}

// PublishEvent 发布事件到 EventQueue，并返回创建的 Envelope。
func (p *Producer) PublishEvent(ctx context.Context, runner core.QueueRunner) (*core.Envelope, error) {
	return p.PublishEventPayload(ctx, runner.GetName(), runner.Marshal())
}

// PublishEventPayload 直接将 payload 包装为新消息并发布到 EventQueue。
func (p *Producer) PublishEventPayload(ctx context.Context, runnerName string, payload string) (*core.Envelope, error) {
	if p.cfg.EventDriver == nil {
		return nil, fmt.Errorf("taskx/producer: event queue driver not configured")
	}
	env := core.NewEnvelope(payload, core.EnvelopeSourceEvent)
	return p.PublishEventEnvelope(ctx, runnerName, env)
}

// PublishEventEnvelope 将指定 Envelope 发布到 EventQueue。
// 若 runnerName 未注册（通过 EventGroupResolver 判断），消息将被推入死信队列并触发告警。
func (p *Producer) PublishEventEnvelope(ctx context.Context, runnerName string, env *core.Envelope) (*core.Envelope, error) {
	if p.cfg.EventDriver == nil {
		return nil, fmt.Errorf("taskx/producer: event queue driver not configured")
	}
	if env == nil {
		return nil, fmt.Errorf("taskx/producer: envelope is nil")
	}
	env.Source = core.EnvelopeSourceEvent
	env.RunnerName = runnerName

	groupName, registered := p.resolveEventGroup(runnerName)
	keys := queue.NewQueueKeySet(p.cfg.KeyPrefix, "event", groupName)

	if !registered {
		errMsg := fmt.Errorf("taskx/producer: event runner %q not registered, message pushed to dead letter queue", runnerName)
		p.logErrorf("%v", errMsg)
		p.alert(core.AlertData{
			Source:     core.AlertSourceEvent,
			AlertType:  core.AlertPublishUnregistered,
			RunnerName: runnerName,
			Envelope:   env,
			Remark:     errMsg.Error(),
		})
		if pushErr := p.cfg.EventDriver.Push(ctx, keys.Dead, env.Encode()); pushErr != nil {
			p.logErrorf("taskx/producer: event[%s] push to dead letter failed: %v", runnerName, pushErr)
			return nil, fmt.Errorf("%w; additionally failed to push to dead letter: %v", errMsg, pushErr)
		}
		return nil, errMsg
	}

	if err := p.cfg.EventDriver.Push(ctx, keys.Pending, env.Encode()); err != nil {
		return nil, err
	}
	return env, nil
}

// PublishDelay 发布延迟任务到 DelayQueue，并返回创建的 Envelope。
func (p *Producer) PublishDelay(ctx context.Context, runner core.QueueRunner, executeAt time.Time) (*core.Envelope, error) {
	return p.PublishDelayPayload(ctx, runner.GetName(), runner.Marshal(), executeAt)
}

// PublishDelayPayload 直接将 payload 包装为新消息并发布到 DelayQueue。
func (p *Producer) PublishDelayPayload(
	ctx context.Context,
	runnerName string,
	payload string,
	executeAt time.Time,
) (*core.Envelope, error) {
	if p.cfg.DelayDriver == nil {
		return nil, fmt.Errorf("taskx/producer: delay queue driver not configured")
	}
	env := core.NewEnvelope(payload, core.EnvelopeSourceDelay)
	return p.PublishDelayEnvelope(ctx, runnerName, env, executeAt)
}

// PublishDelayEnvelope 将指定 Envelope 发布到 DelayQueue。
// 若 runnerName 未注册（通过 DelayRegisteredChecker 判断），消息将被推入死信队列并触发告警。
func (p *Producer) PublishDelayEnvelope(
	ctx context.Context,
	runnerName string,
	env *core.Envelope,
	executeAt time.Time,
) (*core.Envelope, error) {
	if p.cfg.DelayDriver == nil {
		return nil, fmt.Errorf("taskx/producer: delay queue driver not configured")
	}
	if env == nil {
		return nil, fmt.Errorf("taskx/producer: envelope is nil")
	}
	now := time.Now()
	if executeAt.IsZero() || executeAt.Unix() < now.Unix() {
		return nil, fmt.Errorf("taskx/producer: executeAt must not be zero or in the past")
	}

	env.Source = core.EnvelopeSourceDelay
	env.RunnerName = runnerName
	keys := queue.NewQueueKeySet(p.cfg.KeyPrefix, "delay", runnerName)

	if !p.isDelayRegistered(runnerName) {
		errMsg := fmt.Errorf("taskx/producer: delay runner %q not registered, message pushed to dead letter queue", runnerName)
		p.logErrorf("%v", errMsg)
		p.alert(core.AlertData{
			Source:     core.AlertSourceDelay,
			AlertType:  core.AlertPublishUnregistered,
			RunnerName: runnerName,
			Envelope:   env,
			Remark:     errMsg.Error(),
		})
		deadAt := time.Now().UnixMicro()
		if addErr := p.cfg.DelayDriver.Add(ctx, keys.Dead, env.Encode(), deadAt); addErr != nil {
			p.logErrorf("taskx/producer: delay[%s] push to dead letter failed: %v", runnerName, addErr)
			return nil, fmt.Errorf("%w; additionally failed to push to dead letter: %v", errMsg, addErr)
		}
		return nil, errMsg
	}

	if err := p.cfg.DelayDriver.Add(ctx, keys.Pending, env.Encode(), executeAt.UnixMicro()); err != nil {
		return nil, err
	}
	return env, nil
}

func (p *Producer) resolveEventGroup(runnerName string) (groupName string, registered bool) {
	if p.cfg.ResolveEventGroup != nil {
		return p.cfg.ResolveEventGroup(runnerName)
	}
	return core.DefaultEventQueueGroup, true
}

func (p *Producer) isDelayRegistered(runnerName string) bool {
	if p.cfg.IsDelayRegistered != nil {
		return p.cfg.IsDelayRegistered(runnerName)
	}
	return true
}

func (p *Producer) logErrorf(format string, args ...interface{}) {
	if p.cfg.Logger != nil {
		p.cfg.Logger.Errorf(format, args...)
	}
}

func (p *Producer) alert(data core.AlertData) {
	if p.cfg.OnAlert != nil {
		p.cfg.OnAlert(data)
	}
}

// EventDriver 返回配置的事件队列驱动（可能为 nil）。
func (p *Producer) EventDriver() driver.EventQueueDriver { return p.cfg.EventDriver }

// DelayDriver 返回配置的延迟队列驱动（可能为 nil）。
func (p *Producer) DelayDriver() driver.DelayQueueDriver { return p.cfg.DelayDriver }
