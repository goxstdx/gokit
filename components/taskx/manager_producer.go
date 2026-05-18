package taskx

import (
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/producer"
)

// NewProducer 基于当前 Manager 的配置创建一个独立的 Producer。
// 适用于需要在消费端同时推送消息的场景，或将 Producer 传递给其他模块使用。
// 返回的 Producer 与 Manager 共享相同的 driver、key prefix 和注册中心校验逻辑。
func (m *Manager) NewProducer() *producer.Producer {
	return producer.New(producer.Config{
		EventDriver:       m.cfg.EventDriver,
		DelayDriver:       m.cfg.DelayDriver,
		KeyPrefix:         m.cfg.KeyPrefix,
		Logger:            m.cfg.Logger,
		OnAlert:           m.enqueueAlert,
		ResolveEventGroup: m.resolveEventGroupNameStrict,
		IsDelayRegistered: m.isDelayRegistered,
	})
}

func (m *Manager) isDelayRegistered(runnerName string) bool {
	m.ensureRegistryLocked()
	_, ok := m.registry.GetDelayRunners()[runnerName]
	return ok
}
