package consumer

import "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"

// 重新导出常用类型，使 consumer 包用户无需额外引入 taskx 根包。

type RunnerOption = core.RunnerOption
type TimerTaskOption = core.TimerTaskOption
type RecoveryMode = core.RecoveryMode
type TimerExecuteRequest = core.TimerExecuteRequest
type RunnerFuncResult = core.RunnerFuncResult

const (
	RecoveryModeNone               = core.RecoveryModeNone
	RecoveryModeStartupOnly        = core.RecoveryModeStartupOnly
	RecoveryModeStartupAndPeriodic = core.RecoveryModeStartupAndPeriodic
)
