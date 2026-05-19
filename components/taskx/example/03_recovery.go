package example

import (
	"context"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
)

// RecoveryExample 展示死信恢复调用。
func RecoveryExample(ctx context.Context, mgr *taskx.Manager) {
	_, _ = mgr.RecoverEventDead(ctx, &OrderNotifyRunner{}, 100)
	_, _ = mgr.RecoverDelayDead(ctx, "order-notify", 100)
}
