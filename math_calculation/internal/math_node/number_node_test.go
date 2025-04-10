package math_node

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestNumberNode_Eval(t *testing.T) {
	// 创建测试用的上下文
	ctx := context.Background()

	// 创建测试用的变量映射
	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromInt(10),
		"y": decimal.NewFromInt(5),
	}

	tests := []struct {
		name   string
		node   *NumberNode
		config *math_config.CalcConfig
		want   decimal.Decimal
	}{
		{
			name:   "整数值-应用精度控制",
			node:   &NumberNode{Value: decimal.NewFromInt(123)},
			config: &math_config.CalcConfig{ApplyPrecisionEachStep: true, Precision: 2},
			want:   decimal.NewFromInt(123),
		},
		{
			name:   "小数值-应用精度控制",
			node:   &NumberNode{Value: decimal.NewFromFloat(123.456)},
			config: &math_config.CalcConfig{ApplyPrecisionEachStep: true, Precision: 2},
			want:   decimal.NewFromFloat(123.46),
		},
		{
			name:   "整数值-不应用精度控制",
			node:   &NumberNode{Value: decimal.NewFromInt(123)},
			config: &math_config.CalcConfig{ApplyPrecisionEachStep: false, Precision: 2},
			want:   decimal.NewFromInt(123),
		},
		{
			name:   "小数值-不应用精度控制",
			node:   &NumberNode{Value: decimal.NewFromFloat(123.456)},
			config: &math_config.CalcConfig{ApplyPrecisionEachStep: false, Precision: 2},
			want:   decimal.NewFromFloat(123.456),
		},
		{
			name:   "配置为nil",
			node:   &NumberNode{Value: decimal.NewFromFloat(123.456)},
			config: nil,
			want:   decimal.NewFromFloat(123.456),
		},
		{
			name:   "零值",
			node:   &NumberNode{Value: decimal.Zero},
			config: &math_config.CalcConfig{ApplyPrecisionEachStep: true, Precision: 2},
			want:   decimal.Zero,
		},
		{
			name:   "负数值",
			node:   &NumberNode{Value: decimal.NewFromFloat(-123.456)},
			config: &math_config.CalcConfig{ApplyPrecisionEachStep: true, Precision: 2},
			want:   decimal.NewFromFloat(-123.46),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.node.Eval(ctx, vars, tt.config)
			if err != nil {
				t.Errorf("NumberNode.Eval() error = %v", err)
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("NumberNode.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}
