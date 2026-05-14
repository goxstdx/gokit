package taskx

import (
	"fmt"
)

// SetRegistry 设置注册中心（仅允许在未运行状态下设置）。
// 传入 nil 会自动替换为空注册中心。
func (m *Manager) SetRegistry(registry *Registry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return fmt.Errorf("taskx: cannot set registry while manager is running")
	}
	if registry == nil {
		registry = NewRegistry()
	}
	m.registry = registry
	return nil
}

// SetEventConsumerFactory 设置事件队列消费器工厂
func (m *Manager) SetEventConsumerFactory(f EventConsumerFactory) {
	m.eventFactory = f
}

// SetDelayConsumerFactory 设置延迟队列消费器工厂
func (m *Manager) SetDelayConsumerFactory(f DelayConsumerFactory) {
	m.delayFactory = f
}

// SetTimerSchedulerFactory 设置定时任务调度器工厂
func (m *Manager) SetTimerSchedulerFactory(f TimerSchedulerFactory) {
	m.timerFactory = f
}
