package debug

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestDebugInfo_AddStep(t *testing.T) {
	debugInfo := NewDebugInfo("x + y")

	step := DebugStep{
		NodeType:  "BinaryOpNode",
		Operation: "Add",
		Operands:  []string{"10", "20"},
		Result:    "30",
	}

	debugInfo.AddStep(step)

	if len(debugInfo.Steps) != 1 {
		t.Errorf("DebugInfo.AddStep() did not add step, got %d steps", len(debugInfo.Steps))
	}

	if debugInfo.Steps[0].NodeType != "BinaryOpNode" {
		t.Errorf("DebugInfo.AddStep() step.NodeType = %v, want %v", debugInfo.Steps[0].NodeType, "BinaryOpNode")
	}
}

func TestDebugInfo_SetResult(t *testing.T) {
	debugInfo := NewDebugInfo("x + y")

	result := decimal.NewFromInt(30)
	debugInfo.SetResult(result)

	if debugInfo.Result != "30" {
		t.Errorf("DebugInfo.SetResult() result = %v, want %v", debugInfo.Result, "30")
	}
}

func TestDebugInfo_SetError(t *testing.T) {
	debugInfo := NewDebugInfo("x + y")

	err := &internal.ParseError{
		Pos:     0,
		Message: "测试错误",
		Cause:   internal.ErrInvalidExpression,
	}

	debugInfo.SetError(err)

	if debugInfo.Error == nil {
		t.Errorf("DebugInfo.SetError() error = nil, want non-nil")
	}
}

func TestDebugInfo_SetVariables(t *testing.T) {
	debugInfo := NewDebugInfo("x + y")

	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromInt(10),
		"y": decimal.NewFromInt(20),
	}

	debugInfo.SetVariables(vars)

	if len(debugInfo.Variables) != 2 {
		t.Errorf("DebugInfo.SetVariables() did not set variables, got %d variables", len(debugInfo.Variables))
	}

	if debugInfo.Variables["x"] != "10" {
		t.Errorf("DebugInfo.SetVariables() variables[\"x\"] = %v, want %v", debugInfo.Variables["x"], "10")
	}

	if debugInfo.Variables["y"] != "20" {
		t.Errorf("DebugInfo.SetVariables() variables[\"y\"] = %v, want %v", debugInfo.Variables["y"], "20")
	}
}

func TestDebugInfo_String(t *testing.T) {
	debugInfo := NewDebugInfo("x + y")

	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromInt(10),
		"y": decimal.NewFromInt(20),
	}

	debugInfo.SetVariables(vars)

	step := DebugStep{
		NodeType:  "BinaryOpNode",
		Operation: "Add",
		Operands:  []string{"10", "20"},
		Result:    "30",
	}

	debugInfo.AddStep(step)

	result := decimal.NewFromInt(30)
	debugInfo.SetResult(result)

	str := debugInfo.String()

	// 检查字符串包含关键信息
	if !strings.Contains(str, "表达式: x + y") {
		t.Errorf("DebugInfo.String() does not contain expression")
	}

	if !strings.Contains(str, "变量:") {
		t.Errorf("DebugInfo.String() does not contain variables section")
	}

	if !strings.Contains(str, "x = 10") {
		t.Errorf("DebugInfo.String() does not contain variable x")
	}

	if !strings.Contains(str, "y = 20") {
		t.Errorf("DebugInfo.String() does not contain variable y")
	}

	if !strings.Contains(str, "执行步骤:") {
		t.Errorf("DebugInfo.String() does not contain steps section")
	}

	if !strings.Contains(str, "类型: BinaryOpNode") {
		t.Errorf("DebugInfo.String() does not contain step node type")
	}

	if !strings.Contains(str, "结果: 30") {
		t.Errorf("DebugInfo.String() does not contain result")
	}
}

func TestDebugCalculate(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		vars       map[string]decimal.Decimal
		debugMode  math_config.DebugMode
		want       decimal.Decimal
		wantErr    bool
	}{
		{
			name:       "简单表达式",
			expression: "x + y",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)},
			debugMode:  math_config.DebugBasic,
			want:       decimal.NewFromInt(30),
			wantErr:    false,
		},
		{
			name:       "空表达式",
			expression: "",
			vars:       nil,
			debugMode:  math_config.DebugBasic,
			want:       decimal.Zero,
			wantErr:    true,
		},
		{
			name:       "复杂表达式",
			expression: "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3)",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromFloat(5.0)},
			debugMode:  math_config.DebugDetailed,
			want:       decimal.NewFromFloat(94.0),
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := math_config.NewDefaultCalcConfig()
			conf.DebugMode = tt.debugMode
			got, debugInfo, err := DebugCalculate(tt.expression, tt.vars, conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("DebugCalculate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("DebugCalculate() = %v, want %v", got, tt.want)
			}

			// 检查调试信息
			if !tt.wantErr {
				if debugInfo == nil {
					t.Errorf("DebugCalculate() debugInfo = nil, want non-nil")
				}

				if debugInfo.Expression != tt.expression {
					t.Errorf("debugInfo.Expression = %v, want %v", debugInfo.Expression, tt.expression)
				}

				if debugInfo.Result != tt.want.String() {
					t.Errorf("debugInfo.Result = %v, want %v", debugInfo.Result, tt.want.String())
				}

				// 检查变量
				for k, v := range tt.vars {
					if debugInfo.Variables[k] != v.String() {
						t.Errorf("debugInfo.Variables[%s] = %v, want %v", k, debugInfo.Variables[k], v.String())
					}
				}

				// 检查步骤
				if len(debugInfo.Steps) == 0 {
					t.Errorf("debugInfo.Steps is empty")
				}
			}
		})
	}
}
