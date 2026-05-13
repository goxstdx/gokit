package taskx

import "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"

// 重新导出 core 包中的类型，使外部使用方可以直接通过 taskx 包访问

type QueueRunner = core.QueueRunner
type RunnerFunc = core.RunnerFunc
type RunnerFuncResult = core.RunnerFuncResult
type TimerTaskRunner = core.TimerTaskRunner
type RunnerOption = core.RunnerOption
type TimerTaskOption = core.TimerTaskOption
type TimerConcurrencyPolicy = core.TimerConcurrencyPolicy
type Envelope = core.Envelope
type Logger = core.Logger

const (
	TimerConcurrencyForbidOverlap = core.TimerConcurrencyForbidOverlap
	TimerConcurrencyAllowOverlap  = core.TimerConcurrencyAllowOverlap
)

var NewEnvelope = core.NewEnvelope
var DecodeEnvelope = core.DecodeEnvelope
var IntPtr = core.IntPtr
var TimerConcurrencyPolicyPtr = core.TimerConcurrencyPolicyPtr
