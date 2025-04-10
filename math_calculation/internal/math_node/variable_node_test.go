package math_node

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestVariableNode_Eval(t *testing.T) {
	// 创建测试用的上下文
	ctx := context.Background()

	// 创建测试用的变量映射
	vars := map[string]decimal.Decimal{
		"x":    decimal.NewFromInt(10),
		"y":    decimal.NewFromInt(5),
		"z":    decimal.NewFromFloat(123.456),
		"zero": decimal.Zero,
		"neg":  decimal.NewFromInt(-789),
	}

	// 创建测试用的配置
	config := math_config.NewDefaultCalcConfig()
	config.ApplyPrecisionEachStep = true
	config.Precision = 2

	tests := []struct {
		name     string
		node     *VariableNode
		vars     map[string]decimal.Decimal
		config   *math_config.CalcConfig
		want     decimal.Decimal
		wantErr  bool
		errorMsg string
	}{
		{
			name:    "整数变量-应用精度控制",
			node:    &VariableNode{VarName: "x", Pos: 0},
			vars:    vars,
			config:  config,
			want:    decimal.NewFromInt(10),
			wantErr: false,
		},
		{
			name:    "小数变量-应用精度控制",
			node:    &VariableNode{VarName: "z", Pos: 0},
			vars:    vars,
			config:  config,
			want:    decimal.NewFromFloat(123.46),
			wantErr: false,
		},
		{
			name:    "整数变量-不应用精度控制",
			node:    &VariableNode{VarName: "x", Pos: 0},
			vars:    vars,
			config:  &math_config.CalcConfig{ApplyPrecisionEachStep: false, Precision: 2},
			want:    decimal.NewFromInt(10),
			wantErr: false,
		},
		{
			name:    "小数变量-不应用精度控制",
			node:    &VariableNode{VarName: "z", Pos: 0},
			vars:    vars,
			config:  &math_config.CalcConfig{ApplyPrecisionEachStep: false, Precision: 2},
			want:    decimal.NewFromFloat(123.456),
			wantErr: false,
		},
		{
			name:    "零变量",
			node:    &VariableNode{VarName: "zero", Pos: 0},
			vars:    vars,
			config:  config,
			want:    decimal.Zero,
			wantErr: false,
		},
		{
			name:    "负数变量",
			node:    &VariableNode{VarName: "neg", Pos: 0},
			vars:    vars,
			config:  config,
			want:    decimal.NewFromInt(-789),
			wantErr: false,
		},
		{
			name:     "变量不存在",
			node:     &VariableNode{VarName: "undefined", Pos: 5},
			vars:     vars,
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "未定义的变量: undefined",
		},
		{
			name:     "变量映射为nil",
			node:     &VariableNode{VarName: "x", Pos: 0},
			vars:     nil,
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "未定义的变量: x",
		},
		{
			name:    "配置为nil",
			node:    &VariableNode{VarName: "x", Pos: 0},
			vars:    vars,
			config:  nil,
			want:    decimal.NewFromInt(10),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.node.Eval(ctx, tt.vars, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("VariableNode.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Errorf("VariableNode.Eval() expected error but got nil")
					return
				}
				if parseErr, ok := err.(*internal.ParseError); ok {
					if parseErr.Message != tt.errorMsg {
						t.Errorf("VariableNode.Eval() error message = %v, want %v", parseErr.Message, tt.errorMsg)
					}
				} else {
					t.Errorf("VariableNode.Eval() error type = %T, want *internal.ParseError", err)
				}
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("VariableNode.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}
