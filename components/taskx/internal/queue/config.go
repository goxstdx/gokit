package queue

import (
	"fmt"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/driver"
)

// RecoveryMode 保留为 core.RecoveryMode 的 alias，便于 queue 内部引用。
type RecoveryMode = core.RecoveryMode

const (
	RecoveryModeNone               = core.RecoveryModeNone
	RecoveryModeStartupOnly        = core.RecoveryModeStartupOnly
	RecoveryModeStartupAndPeriodic = core.RecoveryModeStartupAndPeriodic
)

// QueueKeyMeta 保存构建 QueueKeySet 时的原始语义信息。
type QueueKeyMeta struct {
	Prefix    string
	QueueType string
	Name      string
	Base      string
}

// QueueKeySet 队列 key 集合，创建时一次性计算好，运行时直接引用，杜绝拼接错误。
type QueueKeySet struct {
	Meta         QueueKeyMeta
	Pending      string
	Processing   string
	Dead         string
	RecoveryLock string
}

// NewQueueKeySet 根据 prefix、队列类型（event/delay）和名称构造完整 key 集合。
func NewQueueKeySet(prefix, queueType, name string) QueueKeySet {
	base := fmt.Sprintf("%s:%s:{%s}", prefix, queueType, name)
	return QueueKeySet{
		Meta: QueueKeyMeta{
			Prefix:    prefix,
			QueueType: queueType,
			Name:      name,
			Base:      base,
		},
		Pending:      base + ":pending",
		Processing:   base + ":processing",
		Dead:         base + ":dead",
		RecoveryLock: fmt.Sprintf("%s:lock:recover:%s:{%s}", prefix, queueType, name),
	}
}

// ConsumerConfig 队列消费者公共配置。
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
	Runners       map[string]EventRunnerEntry
	ConsumerCount int
}

// DelayConsumerConfig 延迟队列消费者配置
type DelayConsumerConfig struct {
	ConsumerConfig
	Driver            driver.DelayQueueDriver
	PollInterval      time.Duration
	RetryBaseInterval time.Duration
	Keys              QueueKeySet
}
