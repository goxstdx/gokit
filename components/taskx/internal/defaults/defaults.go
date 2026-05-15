package defaults

import "time"

const (
	PollInterval           = time.Second
	LockTTL                = 30 * time.Second
	InternalOpTimeout      = 3 * time.Second
	HealthInterval         = 5 * time.Second
	HealthBeatTimeout      = 15 * time.Second
	TimerHeartbeatFallback = 5 * time.Second
	MinTimerHeartbeat      = time.Second

	EventPopTimeout        = 3 * time.Second
	EventPopErrorBackoff   = time.Second
	DelayRetryBaseInterval = 5 * time.Second
	DelayDrainTimeout      = time.Second
	HealthLenTimeout       = 2 * time.Second

	HealthAlertThreshold     = 3
	RecoveryGracePeriod      = 30 * time.Second
	RecoveryLockMargin       = 30 * time.Second
	DefaultLockRenewInterval = time.Second
	MinLockRenewInterval     = 200 * time.Millisecond
	LockRenewIntervalDivisor = 3
)

const RecoverBatchSize int64 = 1000

const (
	DefaultMaxRetry = 0

	EventMaxRetry      = 0
	EventConsumerCount = 1

	DelayMaxRetry      = 1
	DelayConsumerCount = 1
)
