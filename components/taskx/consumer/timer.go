package consumer

import (
	"context"
	"fmt"
	"strings"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
)

// ExecuteTimerTaskOnce 按任务名手动执行一次定时任务。
func (c *Consumer) ExecuteTimerTaskOnce(ctx context.Context, req core.TimerExecuteRequest) (core.RunnerFuncResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		return core.RunnerFuncResult{
			IsOk: false,
			Err:  fmt.Errorf("taskx: timer task name is required"),
		}, fmt.Errorf("taskx: timer task name is required")
	}
	if c.cfg.LockDriver == nil {
		return core.RunnerFuncResult{
			IsOk: false,
			Err:  fmt.Errorf("taskx: lock driver not configured"),
		}, fmt.Errorf("taskx: lock driver not configured")
	}

	entry, ok := c.registry.GetTimerTasks()[name]
	if !ok || entry == nil || entry.Task == nil {
		return core.RunnerFuncResult{
			IsOk: false,
			Err:  fmt.Errorf("taskx: timer task %q not registered", name),
		}, fmt.Errorf("taskx: timer task %q not registered", name)
	}

	lockKey := fmt.Sprintf("%s:lock:timer:{%s}", c.cfg.KeyPrefix, name)
	lockCtx, lockCancel := c.internalOpContext(ctx, 0)
	locked, err := c.cfg.LockDriver.Lock(lockCtx, lockKey, c.cfg.LockTTL)
	lockCancel()
	if err != nil {
		return core.RunnerFuncResult{
			IsOk: false,
			Err:  fmt.Errorf("taskx: acquire timer task lock %q: %w", name, err),
		}, fmt.Errorf("taskx: acquire timer task lock %q: %w", name, err)
	}
	if !locked {
		return core.RunnerFuncResult{
			IsOk: false,
			Err:  fmt.Errorf("taskx: timer task %q is already running", name),
		}, fmt.Errorf("taskx: timer task %q is already running", name)
	}
	defer func() {
		unlockCtx, unlockCancel := c.internalOpContext(context.Background(), 0)
		defer unlockCancel()
		_ = c.cfg.LockDriver.Unlock(unlockCtx, lockKey)
	}()

	return entry.Task.Run(ctx, req.Payload), nil
}
