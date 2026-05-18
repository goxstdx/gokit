package consumer

import "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"

// Registry 及 Entry 类型从 core 重新导出，与 taskx.Registry 同源。

type EventEntry = core.EventEntry
type DelayEntry = core.DelayEntry
type TimerEntry = core.TimerEntry
type Registry = core.Registry

var NewRegistry = core.NewRegistry
