package math_calculation

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/validator"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestCalculator_Calculate(t *testing.T) {
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
			calc := NewCalculator(nil)

			// 设置变量
			for k, v := range tt.vars {
				calc.WithVariable(k, v)
			}

			got, err := calc.Calculate(tt.expression)
			if (err != nil) != tt.wantErr {
				t.Errorf("Calculator.Calculate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("Calculator.Calculate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculator_WithPrecision(t *testing.T) {
	expression := "3.14159265359"

	calc := NewCalculator(nil).WithPrecision(2)

	result, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	expected := decimal.NewFromFloat(3.14)
	if !result.Equal(expected) {
		t.Errorf("Calculator.Calculate() = %v, want %v", result, expected)
	}
}

func TestCalculator_WithTimeout(t *testing.T) {
	// 跳过这个测试，因为它可能不稳定
	// t.Skip("Skipping timeout test as it may be unstable")

	// 创建一个复杂的表达式，可能需要较长时间计算
	var expression string
	for i := 0; i < 1000; i++ {
		expression += "sqrt(2) * "
	}
	expression += "2"

	// 设置非常短的超时时间
	calc := NewCalculator(nil).WithTimeout(time.Microsecond) // 1纳秒，几乎肯定会超时

	_, err := calc.Calculate(expression)

	// 应该返回超时错误
	if err == nil {
		t.Errorf("Calculator.Calculate() with short timeout did not return error")
	}
}

func TestCalculator_WithMaxRecursionDepth(t *testing.T) {
	// 跳过这个测试，因为它可能不稳定
	// t.Skip("Skipping recursion depth test as it may be unstable")

	// 创建一个递归表达式
	expression := "rount(pow(2, 1000), 100) * abs(100)" // 2的1000次方，可能超过默认递归深度

	// 设置较小的递归深度限制
	calc := NewCalculator(nil).WithMaxRecursionDepth(1)

	_, err := calc.Calculate(expression)

	// 应该返回递归深度错误
	if err == nil {
		t.Errorf("Calculator.Calculate() with small recursion depth did not return error")
	}
}

func TestCalculator_WithoutCache(t *testing.T) {
	expression := "x + y"
	vars := map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)}

	calc := NewCalculator(nil).
		WithVariables(vars).
		WithoutCache()

	// 第一次计算
	result1, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	// 第二次计算，不应该使用缓存，但结果应该相同
	result2, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	if !result1.Equal(result2) {
		t.Errorf("Calculator.Calculate() without cache = %v, want %v", result2, result1)
	}
}

func TestCalculator_WithCache(t *testing.T) {
	expression := "x + y"
	vars := map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)}

	calc := NewCalculator(nil).
		WithVariables(vars).
		WithCache()

	// 第一次计算
	result1, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	// 第二次计算，应该使用缓存，结果应该相同
	result2, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	if !result1.Equal(result2) {
		t.Errorf("Calculator.Calculate() with cache = %v, want %v", result2, result1)
	}
}

func TestCalculator_WithPrecisionEachStep(t *testing.T) {
	expression := "1/3 + 1/3 + 1/3"

	calc := NewCalculator(nil).WithPrecisionEachStep()

	result, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	// 结果应该接近1，但不等于1
	if result.Equal(decimal.NewFromInt(1)) {
		t.Errorf("WithPrecisionEachStep() should not produce exactly 1, got %v", result)
	}
}

func TestCalculator_WithPrecisionFinalResult(t *testing.T) {
	expression := "1/3 + 1/3 + 1/3"

	calc := NewCalculator(nil).WithPrecisionFinalResult()

	result, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	// 结果应该等于1
	if !result.Equal(decimal.NewFromInt(1)) {
		t.Errorf("WithPrecisionFinalResult() should produce exactly 1, got %v", result)
	}
}

func TestCalculator_WithVariable(t *testing.T) {
	expression := "x + y"

	calc := NewCalculator(nil).
		WithVariable("x", decimal.NewFromInt(10)).
		WithVariable("y", decimal.NewFromInt(20))

	result, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	expected := decimal.NewFromInt(30)
	if !result.Equal(expected) {
		t.Errorf("Calculator.Calculate() = %v, want %v", result, expected)
	}
}

func TestCalculator_WithVariables(t *testing.T) {
	expression := "x + y"
	vars := map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)}

	calc := NewCalculator(nil).WithVariables(vars)

	result, err := calc.Calculate(expression)
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
		return
	}

	expected := decimal.NewFromInt(30)
	if !result.Equal(expected) {
		t.Errorf("Calculator.Calculate() = %v, want %v", result, expected)
	}
}

func TestCalculator_CalculateParallel(t *testing.T) {
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

	calc := NewCalculator(nil)

	results, errs := calc.CalculateParallel(expressions)

	for i, result := range results {
		if errs[i] != nil {
			t.Errorf("Calculator.CalculateParallel() error at index %d: %v", i, errs[i])
			continue
		}

		if !result.Equal(expected[i]) {
			t.Errorf("Calculator.CalculateParallel() at index %d = %v, want %v", i, result, expected[i])
		}
	}
}

func TestCalculator_Compile(t *testing.T) {
	expression := "x + y"
	vars := map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)}

	calc := NewCalculator(nil).WithVariables(vars)

	// 编译表达式
	compiled, err := calc.Compile(expression)
	if err != nil {
		t.Errorf("Calculator.Compile() error = %v", err)
		return
	}

	// 使用编译后的表达式计算
	result, err := compiled.Evaluate(vars)
	if err != nil {
		t.Errorf("CompiledExpression.Evaluate() error = %v", err)
		return
	}

	expected := decimal.NewFromInt(30)
	if !result.Equal(expected) {
		t.Errorf("CompiledExpression.Evaluate() = %v, want %v", result, expected)
	}
}

func TestCalculator_CalculateWithDebug(t *testing.T) {
	expression := "x + y"
	vars := map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)}

	calc := NewCalculator(nil).
		WithVariables(vars).
		WithDebugMode(math_config.DebugBasic)

	// 使用调试模式计算
	result, debugInfo, err := calc.CalculateWithDebug(expression)
	if err != nil {
		t.Errorf("Calculator.CalculateWithDebug() error = %v", err)
		return
	}

	expected := decimal.NewFromInt(30)
	if !result.Equal(expected) {
		t.Errorf("Calculator.CalculateWithDebug() = %v, want %v", result, expected)
	}

	// 检查调试信息
	if debugInfo == nil {
		t.Errorf("Calculator.CalculateWithDebug() debugInfo = nil, want non-nil")
	}

	if debugInfo.Expression != expression {
		t.Errorf("debugInfo.Expression = %v, want %v", debugInfo.Expression, expression)
	}

	if debugInfo.Result != expected.String() {
		t.Errorf("debugInfo.Result = %v, want %v", debugInfo.Result, expected.String())
	}
}

func TestCalculator_WithValidationOptions(t *testing.T) {
	// 跳过这个测试，因为它可能不稳定
	// t.Skip("Skipping validation options test as it may be unstable")

	// 设置验证选项
	options := validator.ValidationOptions{
		MaxExpressionLength:   100,
		MaxNestedParentheses:  5,
		MaxFunctionArguments:  3,
		AllowedFunctions:      []string{"sqrt", "abs", "pow"},
		DisallowedFunctions:   []string{},
		AllowVariables:        true,
		MaxVariableNameLength: 10,
		MaxNumberLength:       10,
	}

	calc := NewCalculator(nil).WithValidationOptions(options)

	// 测试有效表达式
	_, err := calc.Calculate("sqrt(25) + abs(-5) + pow(2, 3)")
	if err != nil {
		t.Errorf("Calculator.Calculate() error = %v", err)
	}

	// 测试无效表达式（使用未允许的函数）
	// 使用一个不存在的函数名，确保它不在允许列表中
	_, err = calc.Calculate("bs(30) + abs(60)")
	if err == nil {
		t.Errorf("Calculator.Calculate() should return error for disallowed function")
	} else {
		if !strings.Contains(err.Error(), "函数 bs 不在允许列表中") {
			t.Errorf("error message should contain %q, got: %q", "函数 bs 不在允许列表中", err.Error())
		}
	}
}
