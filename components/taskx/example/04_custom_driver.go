package example

import (
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"
)

type MyEventQueueDriver struct{ driver.EventQueueDriver }
type MyDelayQueueDriver struct{ driver.DelayQueueDriver }
type MyLockDriver struct{ driver.LockDriver }

var _ driver.EventQueueDriver = (*MyEventQueueDriver)(nil)
var _ driver.DelayQueueDriver = (*MyDelayQueueDriver)(nil)
var _ driver.LockDriver = (*MyLockDriver)(nil)

// NewCustomDriverManager 展示如何注入自定义驱动。
// 注意：这里只展示接线方式，驱动方法需要由业务自行完整实现。
func NewCustomDriverManager(registry *taskx.Registry, log logger_factory.Logger) *taskx.Manager {
	return taskx.NewManager(registry,
		taskx.WithEventQueueDriver(&MyEventQueueDriver{}),
		taskx.WithDelayQueueDriver(&MyDelayQueueDriver{}),
		taskx.WithLockDriver(&MyLockDriver{}),
		taskx.WithLogger(log),
	)
}
