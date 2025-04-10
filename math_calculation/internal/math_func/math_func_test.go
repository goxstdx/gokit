package math_func

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestFastPow(t *testing.T) {
	tests := []struct {
		name     string
		base     decimal.Decimal
		exponent int64
		want     decimal.Decimal
	}{
		{
			name:     "零次幂",
			base:     decimal.NewFromInt(5),
			exponent: 0,
			want:     decimal.NewFromInt(1),
		},
		{
			name:     "正整数次幂",
			base:     decimal.NewFromInt(2),
			exponent: 3,
			want:     decimal.NewFromInt(8),
		},
		{
			name:     "负整数次幂",
			base:     decimal.NewFromInt(2),
			exponent: -3,
			want:     decimal.NewFromFloat(0.125),
		},
		{
			name:     "小数底数",
			base:     decimal.NewFromFloat(2.5),
			exponent: 2,
			want:     decimal.NewFromFloat(6.25),
		},
		{
			name:     "大指数",
			base:     decimal.NewFromInt(2),
			exponent: 10,
			want:     decimal.NewFromInt(1024),
		},
		{
			name:     "底数为1",
			base:     decimal.NewFromInt(1),
			exponent: 100,
			want:     decimal.NewFromInt(1),
		},
		{
			name:     "底数为0",
			base:     decimal.Zero,
			exponent: 5,
			want:     decimal.Zero,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FastPow(tt.base, tt.exponent)
			if !got.Equal(tt.want) {
				t.Errorf("FastPow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFastSqrt(t *testing.T) {
	tests := []struct {
		name  string
		value decimal.Decimal
		want  decimal.Decimal
	}{
		{
			name:  "零",
			value: decimal.Zero,
			want:  decimal.Zero,
		},
		{
			name:  "负数",
			value: decimal.NewFromInt(-25),
			want:  decimal.Zero,
		},
		{
			name:  "完全平方数",
			value: decimal.NewFromInt(25),
			want:  decimal.NewFromInt(5),
		},
		{
			name:  "非完全平方数",
			value: decimal.NewFromInt(2),
			want:  decimal.NewFromFloat(1.414213562373095), // 近似值
		},
		{
			name:  "小数",
			value: decimal.NewFromFloat(0.25),
			want:  decimal.NewFromFloat(0.5),
		},
		{
			name:  "大数",
			value: decimal.NewFromInt(1000000),
			want:  decimal.NewFromInt(1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fastSqrt(tt.value)

			// 对于非完全平方数，使用近似比较
			if tt.name == "非完全平方数" {
				diff := got.Sub(tt.want).Abs()
				if diff.GreaterThan(decimal.NewFromFloat(0.0000001)) {
					t.Errorf("fastSqrt() = %v, want approximately %v", got, tt.want)
				}
			} else {
				if !got.Equal(tt.want) {
					t.Errorf("fastSqrt() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestOptimizedDecimalSqrt(t *testing.T) {
	tests := []struct {
		name  string
		value decimal.Decimal
		want  decimal.Decimal
	}{
		{
			name:  "零",
			value: decimal.Zero,
			want:  decimal.Zero,
		},
		{
			name:  "负数",
			value: decimal.NewFromInt(-25),
			want:  decimal.Zero,
		},
		{
			name:  "完全平方数",
			value: decimal.NewFromInt(25),
			want:  decimal.NewFromInt(5),
		},
		{
			name:  "非完全平方数",
			value: decimal.NewFromInt(2),
			want:  decimal.NewFromFloat(1.414213562373095), // 近似值
		},
		{
			name:  "小数",
			value: decimal.NewFromFloat(0.25),
			want:  decimal.NewFromFloat(0.5),
		},
		{
			name:  "大数",
			value: decimal.NewFromInt(1000000),
			want:  decimal.NewFromInt(1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OptimizedDecimalSqrt(tt.value)

			// 对于非完全平方数，使用近似比较
			if tt.name == "非完全平方数" {
				diff := got.Sub(tt.want).Abs()
				if diff.GreaterThan(decimal.NewFromFloat(0.0000001)) {
					t.Errorf("OptimizedDecimalSqrt() = %v, want approximately %v", got, tt.want)
				}
			} else {
				if !got.Equal(tt.want) {
					t.Errorf("OptimizedDecimalSqrt() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestDecimalSqrt(t *testing.T) {
	tests := []struct {
		name  string
		value decimal.Decimal
		want  decimal.Decimal
	}{
		{
			name:  "零",
			value: decimal.Zero,
			want:  decimal.Zero,
		},
		{
			name:  "负数",
			value: decimal.NewFromInt(-25),
			want:  decimal.Zero,
		},
		{
			name:  "完全平方数",
			value: decimal.NewFromInt(25),
			want:  decimal.NewFromInt(5),
		},
		{
			name:  "非完全平方数",
			value: decimal.NewFromInt(2),
			want:  decimal.NewFromFloat(1.414213562373095), // 近似值
		},
		{
			name:  "小数",
			value: decimal.NewFromFloat(0.25),
			want:  decimal.NewFromFloat(0.5),
		},
		{
			name:  "大数",
			value: decimal.NewFromInt(1000000),
			want:  decimal.NewFromInt(1000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecimalSqrt(tt.value)

			// 对于非完全平方数，使用近似比较
			if tt.name == "非完全平方数" {
				diff := got.Sub(tt.want).Abs()
				if diff.GreaterThan(decimal.NewFromFloat(0.0000001)) {
					t.Errorf("DecimalSqrt() = %v, want approximately %v", got, tt.want)
				}
			} else {
				if !got.Equal(tt.want) {
					t.Errorf("DecimalSqrt() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestRoundToPlaces(t *testing.T) {
	tests := []struct {
		name   string
		value  decimal.Decimal
		places int32
		want   decimal.Decimal
	}{
		{
			name:   "整数",
			value:  decimal.NewFromInt(123),
			places: 0,
			want:   decimal.NewFromInt(123),
		},
		{
			name:   "四舍",
			value:  decimal.NewFromFloat(123.454),
			places: 2,
			want:   decimal.NewFromFloat(123.45),
		},
		{
			name:   "五入",
			value:  decimal.NewFromFloat(123.455),
			places: 2,
			want:   decimal.NewFromFloat(123.46),
		},
		{
			name:   "负数",
			value:  decimal.NewFromFloat(-123.456),
			places: 2,
			want:   decimal.NewFromFloat(-123.46),
		},
		{
			name:   "零小数位",
			value:  decimal.NewFromFloat(123.456),
			places: 0,
			want:   decimal.NewFromInt(123),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RoundToPlaces(tt.value, tt.places)
			if !got.Equal(tt.want) {
				t.Errorf("RoundToPlaces() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCeilToPlaces(t *testing.T) {
	tests := []struct {
		name   string
		value  decimal.Decimal
		places int32
		want   decimal.Decimal
	}{
		{
			name:   "整数",
			value:  decimal.NewFromInt(123),
			places: 0,
			want:   decimal.NewFromInt(123),
		},
		{
			name:   "有小数",
			value:  decimal.NewFromFloat(123.456),
			places: 2,
			want:   decimal.NewFromFloat(123.46),
		},
		{
			name:   "刚好两位小数",
			value:  decimal.NewFromFloat(123.45),
			places: 2,
			want:   decimal.NewFromFloat(123.45),
		},
		{
			name:   "负数",
			value:  decimal.NewFromFloat(-123.456),
			places: 2,
			want:   decimal.NewFromFloat(-123.45),
		},
		{
			name:   "零小数位",
			value:  decimal.NewFromFloat(123.456),
			places: 0,
			want:   decimal.NewFromInt(124),
		},
		{
			name:   "负数小数位",
			value:  decimal.NewFromFloat(123.456),
			places: -1,
			want:   decimal.NewFromFloat(123.456),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CeilToPlaces(tt.value, tt.places)
			if !got.Equal(tt.want) {
				t.Errorf("CeilToPlaces() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFloorToPlaces(t *testing.T) {
	tests := []struct {
		name   string
		value  decimal.Decimal
		places int32
		want   decimal.Decimal
	}{
		{
			name:   "整数",
			value:  decimal.NewFromInt(123),
			places: 0,
			want:   decimal.NewFromInt(123),
		},
		{
			name:   "有小数",
			value:  decimal.NewFromFloat(123.456),
			places: 2,
			want:   decimal.NewFromFloat(123.45),
		},
		{
			name:   "刚好两位小数",
			value:  decimal.NewFromFloat(123.45),
			places: 2,
			want:   decimal.NewFromFloat(123.45),
		},
		{
			name:   "负数",
			value:  decimal.NewFromFloat(-123.456),
			places: 2,
			want:   decimal.NewFromFloat(-123.46),
		},
		{
			name:   "零小数位",
			value:  decimal.NewFromFloat(123.456),
			places: 0,
			want:   decimal.NewFromInt(123),
		},
		{
			name:   "负数小数位",
			value:  decimal.NewFromFloat(123.456),
			places: -1,
			want:   decimal.NewFromFloat(123.456),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FloorToPlaces(tt.value, tt.places)
			if !got.Equal(tt.want) {
				t.Errorf("FloorToPlaces() = %v, want %v", got, tt.want)
			}
		})
	}
}
