package test

import (
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestPrecisionMode(t *testing.T) {
	// 测试数据
	testValue := decimal.NewFromFloat(123.456)
	precision := int32(2)

	tests := []struct {
		name          string
		precisionMode math_config.PrecisionMode
		want          decimal.Decimal
	}{
		{
			name:          "四舍五入模式",
			precisionMode: math_config.RoundPrecision,
			want:          decimal.NewFromFloat(123.46),
		},
		{
			name:          "向上取整模式",
			precisionMode: math_config.CeilPrecision,
			want:          decimal.NewFromFloat(123.46),
		},
		{
			name:          "向下取整模式",
			precisionMode: math_config.FloorPrecision,
			want:          decimal.NewFromFloat(123.45),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建计算器并设置精度模式
			calc := math_calculation.NewCalculator(nil).
				WithPrecision(precision).
				WithPrecisionMode(tt.precisionMode).
				WithPrecisionEachStep()

			// 计算简单表达式
			result, err := calc.Calculate(testValue.String())
			if err != nil {
				t.Errorf("Calculate() error = %v", err)
				return
			}

			// 验证结果
			if !result.Equal(tt.want) {
				t.Errorf("Calculate() = %v, want %v", result, tt.want)
			}
		})
	}

	// 测试链式API
	t.Run("链式API - 四舍五入", func(t *testing.T) {
		calc := math_calculation.NewCalculator(nil).
			WithPrecision(precision).
			WithRoundPrecision().
			WithPrecisionEachStep()

		result, _ := calc.Calculate(testValue.String())
		if !result.Equal(decimal.NewFromFloat(123.46)) {
			t.Errorf("WithRoundPrecision() = %v, want %v", result, decimal.NewFromFloat(123.46))
		}
	})

	t.Run("链式API - 向上取整", func(t *testing.T) {
		calc := math_calculation.NewCalculator(nil).
			WithPrecision(precision).
			WithCeilPrecision().
			WithPrecisionEachStep()

		result, _ := calc.Calculate(testValue.String())
		if !result.Equal(decimal.NewFromFloat(123.46)) {
			t.Errorf("WithCeilPrecision() = %v, want %v", result, decimal.NewFromFloat(123.46))
		}
	})

	t.Run("链式API - 向下取整", func(t *testing.T) {
		calc := math_calculation.NewCalculator(nil).
			WithPrecision(precision).
			WithFloorPrecision().
			WithPrecisionEachStep()

		result, _ := calc.Calculate(testValue.String())
		if !result.Equal(decimal.NewFromFloat(123.45)) {
			t.Errorf("WithFloorPrecision() = %v, want %v", result, decimal.NewFromFloat(123.45))
		}
	})

	// 测试复杂表达式
	t.Run("复杂表达式", func(t *testing.T) {
		expression := "123.456 + 0.001" // 123.457

		// 四舍五入
		calcRound := math_calculation.NewCalculator(nil).
			WithPrecision(precision).
			WithRoundPrecision().
			WithPrecisionEachStep()
		resultRound, _ := calcRound.Calculate(expression)

		// 向上取整
		calcCeil := math_calculation.NewCalculator(nil).
			WithPrecision(precision).
			WithCeilPrecision().
			WithPrecisionEachStep()
		resultCeil, _ := calcCeil.Calculate(expression)

		// 向上取整
		calcCeil2 := math_calculation.NewCalculator(nil).
			WithPrecision(precision).
			WithCeilPrecision().
			WithPrecisionFinalResult()
		resultCeil2, _ := calcCeil2.Calculate(expression)

		// 向下取整
		calcFloor := math_calculation.NewCalculator(nil).
			WithPrecision(precision).
			WithFloorPrecision().
			WithPrecisionEachStep()
		resultFloor, _ := calcFloor.Calculate(expression)

		// 验证结果
		if !resultRound.Equal(decimal.NewFromFloat(123.46)) {
			t.Errorf("Round complex = %v, want %v", resultRound, decimal.NewFromFloat(123.46))
		}

		if !resultCeil.Equal(decimal.NewFromFloat(123.47)) {
			t.Errorf("Ceil complex = %v, want %v", resultCeil, decimal.NewFromFloat(123.47))
		}

		if !resultCeil2.Equal(decimal.NewFromFloat(123.46)) {
			t.Errorf("Ceil complex = %v, want %v", resultCeil, decimal.NewFromFloat(123.46))
		}

		if !resultFloor.Equal(decimal.NewFromFloat(123.45)) {
			t.Errorf("Floor complex = %v, want %v", resultFloor, decimal.NewFromFloat(123.45))
		}
	})
}
