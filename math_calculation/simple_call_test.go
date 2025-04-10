package math_calculation

import (
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestCalculate(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		vars       map[string]decimal.Decimal
		want       decimal.Decimal
		wantErr    bool
	}{
		{
			name:       "空表达式",
			expression: "",
			vars:       nil,
			want:       decimal.Zero,
			wantErr:    true,
		},
		{
			name:       "单个数字",
			expression: "123",
			vars:       nil,
			want:       decimal.NewFromInt(123),
			wantErr:    false,
		},
		{
			name:       "简单加法",
			expression: "1 + 2",
			vars:       nil,
			want:       decimal.NewFromInt(3),
			wantErr:    false,
		},
		{
			name:       "简单减法",
			expression: "5 - 3",
			vars:       nil,
			want:       decimal.NewFromInt(2),
			wantErr:    false,
		},
		{
			name:       "简单乘法",
			expression: "2 * 3",
			vars:       nil,
			want:       decimal.NewFromInt(6),
			wantErr:    false,
		},
		{
			name:       "简单除法",
			expression: "6 / 2",
			vars:       nil,
			want:       decimal.NewFromInt(3),
			wantErr:    false,
		},
		{
			name:       "幂运算",
			expression: "2 ^ 3",
			vars:       nil,
			want:       decimal.NewFromInt(8),
			wantErr:    false,
		},
		{
			name:       "带括号的表达式",
			expression: "(1 + 2) * 3",
			vars:       nil,
			want:       decimal.NewFromInt(9),
			wantErr:    false,
		},
		{
			name:       "带变量的表达式",
			expression: "x + y",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)},
			want:       decimal.NewFromInt(30),
			wantErr:    false,
		},
		{
			name:       "未定义变量",
			expression: "x + y",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10)},
			want:       decimal.Zero,
			wantErr:    true,
		},
		{
			name:       "函数调用",
			expression: "sqrt(25)",
			vars:       nil,
			want:       decimal.NewFromInt(5),
			wantErr:    false,
		},
		{
			name:       "多参数函数调用",
			expression: "min(10, 5, 8)",
			vars:       nil,
			want:       decimal.NewFromInt(5),
			wantErr:    false,
		},
		{
			name:       "复杂表达式",
			expression: "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3)",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromFloat(5.0)},
			want:       decimal.NewFromFloat(94.0),
			wantErr:    false,
		},
		{
			name:       "除以零",
			expression: "1 / 0",
			vars:       nil,
			want:       decimal.Zero,
			wantErr:    true,
		},
		{
			name:       "语法错误",
			expression: "1 + * 2",
			vars:       nil,
			want:       decimal.Zero,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Calculate(tt.expression, tt.vars, math_config.NewDefaultCalcConfig())
			if (err != nil) != tt.wantErr {
				t.Errorf("Calculate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("Calculate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculateParallel(t *testing.T) {
	expressions := []string{
		"1 + 2",
		"3 * 4",
		"sqrt(25)",
		"min(10, 5, 8)",
		"max(3, 7, 2)",
	}

	expected := []decimal.Decimal{
		decimal.NewFromInt(3),
		decimal.NewFromInt(12),
		decimal.NewFromInt(5),
		decimal.NewFromInt(5),
		decimal.NewFromInt(7),
	}

	results, errs := CalculateParallel(expressions, nil, math_config.NewDefaultCalcConfig())

	for i, result := range results {
		if errs[i] != nil {
			t.Errorf("CalculateParallel() error at index %d: %v", i, errs[i])
			continue
		}

		if !result.Equal(expected[i]) {
			t.Errorf("CalculateParallel() at index %d = %v, want %v", i, result, expected[i])
		}
	}
}

func TestCalculateWithNilConfig(t *testing.T) {
	// 测试空配置
	expression := "1 + 2"
	result, err := Calculate(expression, nil, nil)

	if err != nil {
		t.Errorf("Calculate() with nil config error = %v", err)
		return
	}

	expected := decimal.NewFromInt(3)
	if !result.Equal(expected) {
		t.Errorf("Calculate() with nil config = %v, want %v", result, expected)
	}
}

func TestCalculateWithCustomConfig(t *testing.T) {
	// 测试自定义配置
	expression := "1/3 + 1/3 + 1/3"

	// 每步控制精度
	config1 := math_config.NewDefaultCalcConfig()
	config1.Precision = 2
	config1.ApplyPrecisionEachStep = true

	result1, err := Calculate(expression, nil, config1)
	if err != nil {
		t.Errorf("Calculate() with custom config error = %v", err)
		return
	}

	// 只在最终结果控制精度
	config2 := math_config.NewDefaultCalcConfig()
	config2.Precision = 2
	config2.ApplyPrecisionEachStep = false

	result2, err := Calculate(expression, nil, config2)
	if err != nil {
		t.Errorf("Calculate() with custom config error = %v", err)
		return
	}

	// 两种精度控制策略应该产生不同的结果
	if result1.Equal(result2) {
		t.Errorf("Different precision strategies produced same result: %v", result1)
	}
}

func TestCalculateWithTimeout(t *testing.T) {
	// 测试超时
	// 注意：这个测试可能不稳定，因为超时依赖于系统性能

	// 跳过这个测试，因为它可能不稳定
	// t.Skip("Skipping timeout test as it may be unstable")

	// 创建一个复杂的表达式，可能需要较长时间计算
	var expression string
	for i := 0; i < 1000; i++ {
		expression += "sqrt(2) * "
	}
	expression += "2"

	// 设置非常短的超时时间
	config := math_config.NewDefaultCalcConfig()
	config.Timeout = 1 // 1纳秒，几乎肯定会超时

	_, err := Calculate(expression, nil, config)

	// 应该返回超时错误
	if err == nil {
		t.Errorf("Calculate() with short timeout did not return error")
	}
}
