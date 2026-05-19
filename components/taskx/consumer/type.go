package consumer

import "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"

type RunnerOption = core.RunnerOption
type TimerTaskOption = core.TimerTaskOption
type RecoveryMode = core.RecoveryMode
type TimerExecuteRequest = core.TimerExecuteRequest
type RunnerFuncResult = core.RunnerFuncResult
type HealthSnapshot = core.HealthSnapshot
type QueueListenerHealth = core.QueueListenerHealth
type TimerListenerHealth = core.TimerListenerHealth

const (
	RecoveryModeNone               = core.RecoveryModeNone
	RecoveryModeStartupOnly        = core.RecoveryModeStartupOnly
	RecoveryModeStartupAndPeriodic = core.RecoveryModeStartupAndPeriodic
)
