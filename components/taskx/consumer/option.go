package consumer

import "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"

// Config 与 Option 从 core 重新导出，与 taskx.ManagerConfig / taskx.Option 同源。

type Config = core.Config
type Option = core.Option

var (
	WithEventQueueDriver       = core.WithEventQueueDriver
	WithDelayQueueDriver       = core.WithDelayQueueDriver
	WithLockDriver             = core.WithLockDriver
	WithKeyPrefix              = core.WithKeyPrefix
	WithPollInterval           = core.WithPollInterval
	WithEventPollInterval      = core.WithEventPollInterval
	WithDelayRetryBaseInterval = core.WithDelayRetryBaseInterval
	WithLockTTL                = core.WithLockTTL
	WithInternalOpTimeout      = core.WithInternalOpTimeout
	WithTimerHeartbeatInterval = core.WithTimerHeartbeatInterval
	WithRecoverBatchSize       = core.WithRecoverBatchSize
	WithRecoveryGracePeriod    = core.WithRecoveryGracePeriod
	WithRecoveryMode           = core.WithRecoveryMode
	WithDefaultTimerTaskOption = core.WithDefaultTimerTaskOption
	WithLogger                 = core.WithLogger
	WithAlertFunc              = core.WithAlertFunc
	WithAlertQueueSize         = core.WithAlertQueueSize
	WithTraceContextKey        = core.WithTraceContextKey
	WithHealthInterval         = core.WithHealthInterval
	WithHealthBeatTimeout      = core.WithHealthBeatTimeout
	WithHealthAlertThreshold   = core.WithHealthAlertThreshold
)
