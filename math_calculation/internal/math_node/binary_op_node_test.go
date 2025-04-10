package math_node

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestBinaryOpNode_Eval(t *testing.T) {
	// 创建测试用的上下文
	ctx := context.Background()

	// 创建测试用的变量映射
	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromInt(10),
		"y": decimal.NewFromInt(5),
	}

	// 创建测试用的配置
	config := math_config.NewDefaultCalcConfig()
	config.ApplyPrecisionEachStep = true
	config.Precision = 2
	config.MaxRecursionDepth = 100

	// 创建测试用的数字节点
	num1 := &NumberNode{Value: decimal.NewFromInt(10)}
	num2 := &NumberNode{Value: decimal.NewFromInt(5)}
	num3 := &NumberNode{Value: decimal.NewFromInt(0)}
	num4 := &NumberNode{Value: decimal.NewFromFloat(2.5)}
	num5 := &NumberNode{Value: decimal.NewFromInt(-2)}

	tests := []struct {
		name     string
		node     *BinaryOpNode
		config   *math_config.CalcConfig
		want     decimal.Decimal
		wantErr  bool
		errorMsg string
	}{
		{
			name:    "加法运算",
			node:    &BinaryOpNode{Left: num1, Operator: "+", Right: num2, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(15),
			wantErr: false,
		},
		{
			name:    "减法运算",
			node:    &BinaryOpNode{Left: num1, Operator: "-", Right: num2, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(5),
			wantErr: false,
		},
		{
			name:    "乘法运算",
			node:    &BinaryOpNode{Left: num1, Operator: "*", Right: num2, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(50),
			wantErr: false,
		},
		{
			name:    "除法运算",
			node:    &BinaryOpNode{Left: num1, Operator: "/", Right: num2, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(2),
			wantErr: false,
		},
		{
			name:     "除以零错误",
			node:     &BinaryOpNode{Left: num1, Operator: "/", Right: num3, Pos: 5},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "除数不能为零",
		},
		{
			name:    "幂运算-正整数指数",
			node:    &BinaryOpNode{Left: num2, Operator: "^", Right: num2, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(3125), // 5^5 = 3125
			wantErr: false,
		},
		{
			name:    "幂运算-负整数指数",
			node:    &BinaryOpNode{Left: num2, Operator: "^", Right: num5, Pos: 0},
			config:  config,
			want:    decimal.NewFromFloat(0.04), // 5^(-2) = 1/25 = 0.04
			wantErr: false,
		},
		{
			name:     "不支持的运算符",
			node:     &BinaryOpNode{Left: num1, Operator: "%", Right: num2, Pos: 3},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "不支持的运算符: %",
		},
		{
			name:    "配置为nil",
			node:    &BinaryOpNode{Left: num1, Operator: "+", Right: num2, Pos: 0},
			config:  nil,
			want:    decimal.NewFromInt(15),
			wantErr: false,
		},
		{
			name:    "不应用精度控制",
			node:    &BinaryOpNode{Left: num1, Operator: "/", Right: num4, Pos: 0},
			config:  &math_config.CalcConfig{ApplyPrecisionEachStep: false, MaxRecursionDepth: 100},
			want:    decimal.NewFromFloat(4), // 10/2.5 = 4
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.node.Eval(ctx, vars, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("BinaryOpNode.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Errorf("BinaryOpNode.Eval() expected error but got nil")
					return
				}
				if parseErr, ok := err.(*internal.ParseError); ok {
					if parseErr.Message != tt.errorMsg {
						t.Errorf("BinaryOpNode.Eval() error message = %v, want %v", parseErr.Message, tt.errorMsg)
					}
				} else {
					t.Errorf("BinaryOpNode.Eval() error type = %T, want *internal.ParseError", err)
				}
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("BinaryOpNode.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

// 测试递归深度超限和上下文取消的情况
func TestBinaryOpNode_EvalWithContextAndDepth(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 创建测试用的数字节点
	num1 := &NumberNode{Value: decimal.NewFromInt(10)}
	num2 := &NumberNode{Value: decimal.NewFromInt(5)}

	// 创建测试用的配置，设置最大递归深度为0，这样第一次调用就会超过限制
	config := math_config.NewDefaultCalcConfig()
	config.MaxRecursionDepth = 0

	// 测试递归深度超限
	t.Run("递归深度超限", func(t *testing.T) {
		// 使用一个已经超过最大递归深度的配置
		config := math_config.NewDefaultCalcConfig()
		config.MaxRecursionDepth = -1

		node := &BinaryOpNode{Left: num1, Operator: "+", Right: num2, Pos: 0}
		_, err := node.Eval(ctx, nil, config)
		if err == nil {
			t.Errorf("BinaryOpNode.Eval() expected error for max recursion depth")
			return
		}
		if err != internal.ErrMaxRecursionDepth {
			t.Errorf("BinaryOpNode.Eval() error = %v, want %v", err, internal.ErrMaxRecursionDepth)
		}
	})

	// 测试上下文取消
	t.Run("上下文取消", func(t *testing.T) {
		// 取消上下文
		cancel()

		node := &BinaryOpNode{Left: num1, Operator: "+", Right: num2, Pos: 0}
		_, err := node.Eval(ctx, nil, math_config.NewDefaultCalcConfig())
		if err == nil {
			t.Errorf("BinaryOpNode.Eval() expected error for canceled context")
			return
		}
		if err != internal.ErrExecutionTimeout {
			t.Errorf("BinaryOpNode.Eval() error = %v, want %v", err, internal.ErrExecutionTimeout)
		}
	})
}

// 测试操作数计算错误的情况
func TestBinaryOpNode_EvalWithOperandErrors(t *testing.T) {
	// 创建测试用的上下文
	ctx := context.Background()

	// 创建一个会返回错误的节点
	errorNode := &VariableNode{VarName: "undefined", Pos: 0}

	// 创建测试用的数字节点
	num1 := &NumberNode{Value: decimal.NewFromInt(10)}

	// 测试左操作数错误
	t.Run("左操作数错误", func(t *testing.T) {
		node := &BinaryOpNode{Left: errorNode, Operator: "+", Right: num1, Pos: 0}
		_, err := node.Eval(ctx, nil, math_config.NewDefaultCalcConfig())
		if err == nil {
			t.Errorf("BinaryOpNode.Eval() expected error from left operand")
		}
	})

	// 测试右操作数错误
	t.Run("右操作数错误", func(t *testing.T) {
		node := &BinaryOpNode{Left: num1, Operator: "+", Right: errorNode, Pos: 0}
		_, err := node.Eval(ctx, nil, math_config.NewDefaultCalcConfig())
		if err == nil {
			t.Errorf("BinaryOpNode.Eval() expected error from right operand")
		}
	})
}
