package core

import "time"

// QueueListenerHealth 队列监听健康信息
type QueueListenerHealth struct {
	Alive      bool
	LastBeatAt time.Time
	PendingLen int64
	LenError   string
}

// TimerListenerHealth 定时调度器健康信息
type TimerListenerHealth struct {
	Alive      bool
	LastBeatAt time.Time
}

// HealthSnapshot 管理器 / 消费者健康快照
type HealthSnapshot struct {
	Running   bool
	CheckedAt time.Time
	Event     map[string]QueueListenerHealth
	Delay     map[string]QueueListenerHealth
	Timer     TimerListenerHealth
}
