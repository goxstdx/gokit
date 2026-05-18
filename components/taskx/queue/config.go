package queue

import (
	"fmt"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
)

// RecoveryMode 队列恢复模式
type RecoveryMode uint8

const (
	// RecoveryModeNone 不进行任何恢复
	RecoveryModeNone RecoveryMode = iota
	// RecoveryModeStartupOnly 仅在启动时恢复一次（默认）
	RecoveryModeStartupOnly
	// RecoveryModeStartupAndPeriodic 启动恢复 + 周期性兜底恢复
	RecoveryModeStartupAndPeriodic
)

func (m RecoveryMode) Normalize() RecoveryMode {
	switch m {
	case RecoveryModeNone, RecoveryModeStartupOnly, RecoveryModeStartupAndPeriodic:
		return m
	default:
		return RecoveryModeStartupOnly
	}
}

func (m RecoveryMode) WithStartupRecover() bool {
	return m.Normalize() != RecoveryModeNone
}

func (m RecoveryMode) WithPeriodicRecover() bool {
	return m.Normalize() == RecoveryModeStartupAndPeriodic
}

// QueueKeySet 队列 key 集合，创建时一次性计算好，运行时直接引用，杜绝拼接错误。
type QueueKeySet struct {
	Pending      string
	Processing   string
	Dead         string
	RecoveryLock string
}

// NewQueueKeySet 根据 prefix、队列类型（event/delay）和名称构造完整 key 集合。
func NewQueueKeySet(prefix, queueType, name string) QueueKeySet {
	base := fmt.Sprintf("%s:%s:{%s}", prefix, queueType, name)
	return QueueKeySet{
		Pending:      base + ":pending",
		Processing:   base + ":processing",
		Dead:         base + ":dead",
		RecoveryLock: fmt.Sprintf("%s:lock:recover:%s:{%s}", prefix, queueType, name),
	}
}

// ConsumerConfig 队列消费者公共配置。
// LockDriver 放入此处是因为 Event 和 Delay 消费者都需要它做崩溃恢复锁，属于公共基础设施。
// 队列类型特定的 Driver（EventQueueDriver / DelayQueueDriver）保留在各自的子配置中，
// 避免共享 struct 携带不相关的依赖。
type ConsumerConfig struct {
	Lock                driver.LockDriver
	Prefix              string
	LockTTL             time.Duration
	RecoveryGracePeriod time.Duration
	RecoveryMode        RecoveryMode
	InternalOpTimeout   time.Duration
	TraceKey            string
	Logger              core.Logger
	OnAlert             core.AlertFunc
	OnHeartbeat         core.ListenerHeartbeatFunc
}

// EventRunnerEntry 事件队列消费器内部的 runner 路由条目
type EventRunnerEntry struct {
	Runner core.QueueRunner
	Option core.RunnerOption
}

// EventConsumerConfig 事件队列消费者配置
type EventConsumerConfig struct {
	ConsumerConfig
	Driver        driver.EventQueueDriver
	PopTimeout    time.Duration
	Keys          QueueKeySet
	Runners       map[string]EventRunnerEntry // runner name -> entry，聚合队列路由表
	ConsumerCount int                         // 该组的消费者协程数量
}

// DelayConsumerConfig 延迟队列消费者配置
type DelayConsumerConfig struct {
	ConsumerConfig
	Driver            driver.DelayQueueDriver
	PollInterval      time.Duration
	RetryBaseInterval time.Duration
	Keys              QueueKeySet
}
