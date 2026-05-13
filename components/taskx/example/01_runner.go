package example

import (
	"context"
	"encoding/json"
	"fmt"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
)

// OrderNotifyRunner 订单通知 Runner（EventQueue / DelayQueue 用）
type OrderNotifyRunner struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
}

func (r *OrderNotifyRunner) GetName() string { return "order-notify" }

func (r *OrderNotifyRunner) Marshal() string {
	b, _ := json.Marshal(r)
	return string(b)
}

func (r *OrderNotifyRunner) Run(ctx context.Context, payload string) core.RunnerFuncResult {
	var data OrderNotifyRunner
	if err := json.Unmarshal([]byte(payload), &data); err != nil {
		return core.RunnerFuncResult{IsOk: false, Err: err}
	}
	fmt.Printf("sending notification: order=%s, user=%s\n", data.OrderID, data.UserID)
	return core.RunnerFuncResult{IsOk: true}
}

// ReportTimerTask 定时报表任务（TimerTask 用）
type ReportTimerTask struct{}

func (t *ReportTimerTask) GetName() string      { return "daily-report" }
func (t *ReportTimerTask) GetCron() string      { return "0 0 2 * * *" }
func (t *ReportTimerTask) GetTaskParam() string { return "" }

func (t *ReportTimerTask) Run(ctx context.Context, payload string) core.RunnerFuncResult {
	_ = payload
	fmt.Println("generating daily report...")
	return core.RunnerFuncResult{IsOk: true}
}
