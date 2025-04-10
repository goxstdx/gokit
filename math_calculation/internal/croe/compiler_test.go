package croe

import (
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestCompile(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		wantErr    bool
	}{
		{
			name:       "空表达式",
			expression: "",
			wantErr:    true,
		},
		{
			name:       "有效表达式",
			expression: "x + y * z",
			wantErr:    false,
		},
		{
			name:       "无效表达式",
			expression: "x + * y",
			wantErr:    true,
		},
		{
			name:       "复杂表达式",
			expression: "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3)",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Compile(tt.expression, math_config.NewDefaultCalcConfig())
			if (err != nil) != tt.wantErr {
				t.Errorf("Compile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCompiledExpression_Evaluate(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		vars       map[string]decimal.Decimal
		want       decimal.Decimal
		wantErr    bool
	}{
		{
			name:       "简单表达式",
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
			name:       "复杂表达式",
			expression: "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3)",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromFloat(5.0)},
			want:       decimal.NewFromFloat(94.0),
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compiled, err := Compile(tt.expression, math_config.NewDefaultCalcConfig())
			if err != nil {
				t.Errorf("Compile() error = %v", err)
				return
			}

			got, err := compiled.Evaluate(tt.vars)
			if (err != nil) != tt.wantErr {
				t.Errorf("CompiledExpression.Evaluate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("CompiledExpression.Evaluate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCompiledExpression_WithConfig(t *testing.T) {
	expression := "1/3 + 1/3 + 1/3"

	compiled, err := Compile(expression, math_config.NewDefaultCalcConfig())
	if err != nil {
		t.Errorf("Compile() error = %v", err)
		return
	}

	// 使用每步控制精度
	config1 := math_config.NewDefaultCalcConfig()
	config1.Precision = 2
	config1.ApplyPrecisionEachStep = true

	compiled.WithConfig(config1)

	result1, err := compiled.Evaluate(nil)
	if err != nil {
		t.Errorf("CompiledExpression.Evaluate() error = %v", err)
		return
	}

	// 使用只在最终结果控制精度
	config2 := math_config.NewDefaultCalcConfig()
	config2.Precision = 2
	config2.ApplyPrecisionEachStep = false

	compiled.WithConfig(config2)

	result2, err := compiled.Evaluate(nil)
	if err != nil {
		t.Errorf("CompiledExpression.Evaluate() error = %v", err)
		return
	}

	// 两种精度控制策略应该产生不同的结果
	if result1.Equal(result2) {
		t.Errorf("Different precision strategies produced same result: %v", result1)
	}
}

func TestCompiledExpression_WithTimeout(t *testing.T) {
	// 跳过这个测试，因为它可能不稳定
	// t.Skip("Skipping timeout test as it may be unstable")

	expression := "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3)"
	vars := map[string]decimal.Decimal{"x": decimal.NewFromFloat(5.0)}

	compiled, err := Compile(expression, math_config.NewDefaultCalcConfig())
	if err != nil {
		t.Errorf("Compile() error = %v", err)
		return
	}

	// 设置超时
	compiled.WithTimeout(1000000000) // 1秒

	// 正常计算应该成功
	_, err = compiled.Evaluate(vars)
	if err != nil {
		t.Errorf("CompiledExpression.Evaluate() error = %v", err)
	}
}

func TestCompiledExpression_WithPrecision(t *testing.T) {
	expression := "3.14159265359"

	compiled, err := Compile(expression, math_config.NewDefaultCalcConfig())
	if err != nil {
		t.Errorf("Compile() error = %v", err)
		return
	}

	// 设置精度为2
	compiled.WithPrecision(2)

	result, err := compiled.Evaluate(nil)
	if err != nil {
		t.Errorf("CompiledExpression.Evaluate() error = %v", err)
		return
	}

	expected := decimal.NewFromFloat(3.14)
	if !result.Equal(expected) {
		t.Errorf("CompiledExpression.Evaluate() = %v, want %v", result, expected)
	}
}

func TestCompiledExpression_WithPrecisionEachStep(t *testing.T) {
	expression := "1/3 + 1/3 + 1/3"

	compiled, err := Compile(expression, math_config.NewDefaultCalcConfig())
	if err != nil {
		t.Errorf("Compile() error = %v", err)
		return
	}

	// 设置每步控制精度
	compiled.WithPrecisionEachStep()

	result, err := compiled.Evaluate(nil)
	if err != nil {
		t.Errorf("CompiledExpression.Evaluate() error = %v", err)
		return
	}

	// 结果应该接近1，但不等于1
	if result.Equal(decimal.NewFromInt(1)) {
		t.Errorf("WithPrecisionEachStep() should not produce exactly 1, got %v", result)
	}
}

func TestCompiledExpression_WithPrecisionFinalResult(t *testing.T) {
	expression := "1/3 + 1/3 + 1/3"

	compiled, err := Compile(expression, math_config.NewDefaultCalcConfig())
	if err != nil {
		t.Errorf("Compile() error = %v", err)
		return
	}

	// 设置只在最终结果控制精度
	compiled.WithPrecisionFinalResult()

	result, err := compiled.Evaluate(nil)
	if err != nil {
		t.Errorf("CompiledExpression.Evaluate() error = %v", err)
		return
	}

	// 结果应该等于1
	if !result.Equal(decimal.NewFromInt(1)) {
		t.Errorf("WithPrecisionFinalResult() should produce exactly 1, got %v", result)
	}
}

func TestCompiledExpression_GetLastError(t *testing.T) {
	expression := "x + y"

	compiled, err := Compile(expression, math_config.NewDefaultCalcConfig())
	if err != nil {
		t.Errorf("Compile() error = %v", err)
		return
	}

	// 使用不完整的变量映射，应该产生错误
	vars := map[string]decimal.Decimal{"x": decimal.NewFromInt(10)}

	_, err = compiled.Evaluate(vars)
	if err == nil {
		t.Errorf("CompiledExpression.Evaluate() should return error")
		return
	}

	// 获取最后一次错误
	lastErr := compiled.GetLastError()
	if lastErr == nil {
		t.Errorf("CompiledExpression.GetLastError() = nil, want error")
	}
}
