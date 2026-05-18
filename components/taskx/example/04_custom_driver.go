package example

import (
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
)

type MyEventQueueDriver struct{ taskx.EventQueueDriver }
type MyDelayQueueDriver struct{ taskx.DelayQueueDriver }
type MyLockDriver struct{ taskx.LockDriver }

var _ taskx.EventQueueDriver = (*MyEventQueueDriver)(nil)
var _ taskx.DelayQueueDriver = (*MyDelayQueueDriver)(nil)
var _ taskx.LockDriver = (*MyLockDriver)(nil)

// NewCustomDriverManager 展示如何注入自定义驱动。
// 注意：这里只展示接线方式，驱动方法需要由业务自行完整实现。
func NewCustomDriverManager(registry *taskx.Registry, log logger_factory.Logger) *taskx.Manager {
	return taskx.NewManager(
		registry,
		taskx.WithEventQueueDriver(&MyEventQueueDriver{}),
		taskx.WithDelayQueueDriver(&MyDelayQueueDriver{}),
		taskx.WithLockDriver(&MyLockDriver{}),
		taskx.WithRecoveryMode(taskx.RecoveryModeStartupOnly),
		taskx.WithLogger(log),
	)
}
