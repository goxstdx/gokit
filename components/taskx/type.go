package taskx

import (
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/consumer"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/driver"
)

// 重新导出 core 包中的类型，使外部使用方可以直接通过 taskx 包访问

type QueueRunner = core.QueueRunner
type RunnerFunc = core.RunnerFunc
type RunnerFuncResult = core.RunnerFuncResult
type TimerTaskRunner = core.TimerTaskRunner
type TimerExecuteRequest = core.TimerExecuteRequest
type RunnerOption = core.RunnerOption
type TimerTaskOption = core.TimerTaskOption
type TimerConcurrencyPolicy = core.TimerConcurrencyPolicy
type Envelope = core.Envelope
type EnvelopeSource = core.EnvelopeSource
type Logger = core.Logger
type AlertFunc = core.AlertFunc
type AlertType = core.AlertType
type AlertSource = core.AlertSource
type AlertData = core.AlertData
type RecoveryMode = core.RecoveryMode

// 重新导出 driver 接口，使外部使用方可自定义后端实现

type EventQueueDriver = driver.EventQueueDriver
type DelayQueueDriver = driver.DelayQueueDriver
type LockDriver = driver.LockDriver

const (
	TimerConcurrencyForbidOverlap = core.TimerConcurrencyForbidOverlap
	TimerConcurrencySinglePerTick = core.TimerConcurrencySinglePerTick
)

const DefaultEventQueueGroup = core.DefaultEventQueueGroup

const (
	RecoveryModeNone               = core.RecoveryModeNone
	RecoveryModeStartupOnly        = core.RecoveryModeStartupOnly
	RecoveryModeStartupAndPeriodic = core.RecoveryModeStartupAndPeriodic
)

const (
	AlertCorruptMessage         = core.AlertCorruptMessage
	AlertMaxRetryExhausted      = core.AlertMaxRetryExhausted
	AlertEventNextTimeIgnored   = core.AlertEventNextTimeIgnored
	AlertTimerAllAttemptsFailed = core.AlertTimerAllAttemptsFailed
	AlertUnknownRunner          = core.AlertUnknownRunner
	AlertRecoveryLockLost       = core.AlertRecoveryLockLost
	AlertRecoveryExceeded       = core.AlertRecoveryExceeded
	AlertQueueBacklog           = core.AlertQueueBacklog
	AlertPublishUnregistered    = core.AlertPublishUnregistered
	AlertListenerUnhealthy      = core.AlertListenerUnhealthy
)

// 工厂类型从 consumer 包重新导出
type QueueConsumer = consumer.QueueConsumer
type EventConsumerFactory = consumer.EventConsumerFactory
type DelayConsumerFactory = consumer.DelayConsumerFactory
type TimerSchedulerFactory = consumer.TimerSchedulerFactory
type ManagerHealthSnapshot = core.HealthSnapshot

var NewEnvelope = core.NewEnvelope
var DecodeEnvelope = core.DecodeEnvelope
var IntPtr = core.IntPtr
var TimePtr = core.TimePtr
var TimerConcurrencyPolicyPtr = core.TimerConcurrencyPolicyPtr
