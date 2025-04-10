package test

import (
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/croe"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// 集成测试，测试所有组件协同工作
func TestIntegration(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		vars       map[string]decimal.Decimal
		want       decimal.Decimal
		wantErr    bool
	}{
		{
			name:       "简单算术",
			expression: "1 + 2 * 3 - 4 / 2",
			vars:       nil,
			want:       decimal.NewFromInt(5),
			wantErr:    false,
		},
		{
			name:       "带括号的表达式",
			expression: "(1 + 2) * (3 - 4 / 2)",
			vars:       nil,
			want:       decimal.NewFromInt(3),
			wantErr:    false,
		},
		{
			name:       "带变量的表达式",
			expression: "x + y * z",
			vars: map[string]decimal.Decimal{
				"x": decimal.NewFromInt(1), "y": decimal.NewFromInt(2), "z": decimal.NewFromInt(3),
			},
			want:    decimal.NewFromInt(7),
			wantErr: false,
		},
		{
			name:       "带函数的表达式",
			expression: "sqrt(25) + abs(-5) + pow(2, 3)",
			vars:       nil,
			want:       decimal.NewFromInt(18),
			wantErr:    false,
		},
		{
			name:       "带多参数函数的表达式",
			expression: "min(10, 5, 8) + max(3, 7, 2)",
			vars:       nil,
			want:       decimal.NewFromInt(12),
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
			name:       "精度控制表达式",
			expression: "1/3 + 1/3 + 1/3",
			vars:       nil,
			// 注意：由于默认配置中的ApplyPrecisionEachStep为true，
			// 所以结果不会精确等于1，而是接近于1的值
			want:    decimal.NewFromFloat(0.9999999999),
			wantErr: false,
		},
		{
			name:       "舍入函数",
			expression: "round(3.14159, 2)",
			vars:       nil,
			want:       decimal.NewFromFloat(3.14),
			wantErr:    false,
		},
		{
			name:       "向上舍入函数",
			expression: "ceil(3.14159, 2)",
			vars:       nil,
			want:       decimal.NewFromFloat(3.15),
			wantErr:    false,
		},
		{
			name:       "向下舍入函数",
			expression: "floor(3.14159, 2)",
			vars:       nil,
			want:       decimal.NewFromFloat(3.14),
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 测试普通计算
			got, err := math_calculation.Calculate(tt.expression, tt.vars, math_config.NewDefaultCalcConfig())
			if (err != nil) != tt.wantErr {
				t.Errorf("Calculate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("Calculate() = %v, want %v", got, tt.want)
			}

			// 测试链式API
			calc := math_calculation.NewCalculator(nil)
			for k, v := range tt.vars {
				calc.WithVariable(k, v)
			}

			got, err = calc.Calculate(tt.expression)
			if (err != nil) != tt.wantErr {
				t.Errorf("Calculator.Calculate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("Calculator.Calculate() = %v, want %v", got, tt.want)
			}

			// 测试预编译
			if !tt.wantErr {
				compiled, err := croe.Compile(tt.expression, math_config.NewDefaultCalcConfig())
				if err != nil {
					t.Errorf("Compile() error = %v", err)
					return
				}

				got, err = compiled.Evaluate(tt.vars)
				if err != nil {
					t.Errorf("CompiledExpression.Evaluate() error = %v", err)
					return
				}

				if !got.Equal(tt.want) {
					t.Errorf("CompiledExpression.Evaluate() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// 测试不同精度控制策略
func TestPrecisionStrategies(t *testing.T) {
	expression := "1/3 + 1/3 + 1/3"

	// 每步控制精度
	config1 := math_config.NewDefaultCalcConfig()
	config1.Precision = 10
	config1.ApplyPrecisionEachStep = true

	result1, err := math_calculation.Calculate(expression, nil, config1)
	if err != nil {
		t.Errorf("Calculate() with each step precision error = %v", err)
		return
	}
	if !result1.Equal(decimal.NewFromFloat(0.9999999999)) {
		t.Errorf("Final result precision strategy should produce exactly 0.9999999999, got %v", result1)
	}

	// 只在最终结果控制精度
	config2 := math_config.NewDefaultCalcConfig()
	config2.Precision = 10
	config2.ApplyPrecisionEachStep = false

	result2, err := math_calculation.Calculate(expression, nil, config2)
	if err != nil {
		t.Errorf("Calculate() with final result precision error = %v", err)
		return
	}

	// 只在最终结果控制精度应该产生精确的1
	if !result2.Equal(decimal.NewFromInt(1)) {
		t.Errorf("Final result precision strategy should produce exactly 1, got %v", result2)
	}
}

// 测试缓存功能
func TestCaching(t *testing.T) {
	expression := "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3)"
	vars := map[string]decimal.Decimal{"x": decimal.NewFromFloat(5.0)}

	// 启用缓存
	config1 := math_config.NewDefaultCalcConfig()
	config1.UseExprCache = true
	config1.UseLexerCache = true

	// 第一次计算
	result1, err := math_calculation.Calculate(expression, vars, config1)
	if err != nil {
		t.Errorf("Calculate() with cache error = %v", err)
		return
	}

	// 第二次计算，应该使用缓存
	result2, err := math_calculation.Calculate(expression, vars, config1)
	if err != nil {
		t.Errorf("Calculate() with cache error = %v", err)
		return
	}

	// 两次结果应该相同
	if !result1.Equal(result2) {
		t.Errorf("Calculate() with cache = %v, want %v", result2, result1)
	}

	// 禁用缓存
	config2 := math_config.NewDefaultCalcConfig()
	config2.UseExprCache = false
	config2.UseLexerCache = false

	// 第一次计算
	result1, err = math_calculation.Calculate(expression, vars, config2)
	if err != nil {
		t.Errorf("Calculate() without cache error = %v", err)
		return
	}

	// 第二次计算，不应该使用缓存
	result2, err = math_calculation.Calculate(expression, vars, config2)
	if err != nil {
		t.Errorf("Calculate() without cache error = %v", err)
		return
	}

	// 两次结果应该相同
	if !result1.Equal(result2) {
		t.Errorf("Calculate() without cache = %v, want %v", result2, result1)
	}
}

// 测试并行计算
func TestParallelCalculation(t *testing.T) {
	expressions := []string{
		"1 + 2",
		"3 * 4",
		"sqrt(25)",
		"min(10, 5, 8)",
		"max(3, 7, 2)",
		"round(3.14159, 2)",
		"ceil(3.14159, 2)",
		"floor(3.14159, 2)",
		"abs(-5)",
		"pow(2, 3)",
	}

	expected := []decimal.Decimal{
		decimal.NewFromInt(3),
		decimal.NewFromInt(12),
		decimal.NewFromInt(5),
		decimal.NewFromInt(5),
		decimal.NewFromInt(7),
		decimal.NewFromFloat(3.14),
		decimal.NewFromFloat(3.15),
		decimal.NewFromFloat(3.14),
		decimal.NewFromInt(5),
		decimal.NewFromInt(8),
	}

	// 使用普通API
	results1, errs1 := math_calculation.CalculateParallel(expressions, nil, math_config.NewDefaultCalcConfig())

	for i, result := range results1 {
		if errs1[i] != nil {
			t.Errorf("CalculateParallel() error at index %d: %v", i, errs1[i])
			continue
		}

		if !result.Equal(expected[i]) {
			t.Errorf("CalculateParallel() at index %d = %v, want %v", i, result, expected[i])
		}
	}

	// 使用链式API
	calc := math_calculation.NewCalculator(nil)
	results2, errs2 := calc.CalculateParallel(expressions)

	for i, result := range results2 {
		if errs2[i] != nil {
			t.Errorf("Calculator.CalculateParallel() error at index %d: %v", i, errs2[i])
			continue
		}

		if !result.Equal(expected[i]) {
			t.Errorf("Calculator.CalculateParallel() at index %d = %v, want %v", i, result, expected[i])
		}
	}
}

func TestPrecisionControl(t *testing.T) {
	tests := []struct {
		name                   string
		expression             string
		applyPrecisionEachStep bool
		precision              int32
		want                   decimal.Decimal
	}{
		{
			name:                   "每步控制精度",
			expression:             "1/3 + 1/3 + 1/3",
			applyPrecisionEachStep: true,
			precision:              10,
			want:                   decimal.NewFromFloat(0.9999999999),
		},
		{
			name:                   "只在最终结果控制精度",
			expression:             "1/3 + 1/3 + 1/3",
			applyPrecisionEachStep: false,
			precision:              10,
			want:                   decimal.NewFromInt(1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := math_config.NewDefaultCalcConfig()
			config.Precision = tt.precision
			config.ApplyPrecisionEachStep = tt.applyPrecisionEachStep

			result, err := math_calculation.Calculate(tt.expression, nil, config)
			if err != nil {
				t.Errorf("Calculate() error = %v", err)
				return
			}

			if !result.Equal(tt.want) {
				t.Errorf("Calculate() = %v, want %v", result, tt.want)
			}
		})
	}
}
