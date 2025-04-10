package math_node

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestUnaryOpNode_Eval(t *testing.T) {
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
	num1 := &NumberNode{Value: decimal.NewFromInt(123)}
	num2 := &NumberNode{Value: decimal.NewFromInt(-456)}
	num3 := &NumberNode{Value: decimal.Zero}

	tests := []struct {
		name     string
		node     *UnaryOpNode
		config   *math_config.CalcConfig
		want     decimal.Decimal
		wantErr  bool
		errorMsg string
	}{
		{
			name:    "正号运算符",
			node:    &UnaryOpNode{Operator: "+", Operand: num1, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(123),
			wantErr: false,
		},
		{
			name:    "负号运算符-正数",
			node:    &UnaryOpNode{Operator: "-", Operand: num1, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(-123),
			wantErr: false,
		},
		{
			name:    "负号运算符-负数",
			node:    &UnaryOpNode{Operator: "-", Operand: num2, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(456),
			wantErr: false,
		},
		{
			name:    "负号运算符-零",
			node:    &UnaryOpNode{Operator: "-", Operand: num3, Pos: 0},
			config:  config,
			want:    decimal.Zero,
			wantErr: false,
		},
		{
			name:     "不支持的运算符",
			node:     &UnaryOpNode{Operator: "*", Operand: num1, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "不支持的一元运算符: *",
		},
		{
			name:    "配置为nil",
			node:    &UnaryOpNode{Operator: "+", Operand: num1, Pos: 0},
			config:  nil,
			want:    decimal.NewFromInt(123),
			wantErr: false,
		},
		{
			name:    "不应用精度控制",
			node:    &UnaryOpNode{Operator: "-", Operand: &NumberNode{Value: decimal.NewFromFloat(123.456)}, Pos: 0},
			config:  &math_config.CalcConfig{ApplyPrecisionEachStep: false, MaxRecursionDepth: 100},
			want:    decimal.NewFromFloat(-123.456),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.node.Eval(ctx, vars, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnaryOpNode.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Errorf("UnaryOpNode.Eval() expected error but got nil")
					return
				}
				if parseErr, ok := err.(*internal.ParseError); ok {
					if parseErr.Message != tt.errorMsg {
						t.Errorf("UnaryOpNode.Eval() error message = %v, want %v", parseErr.Message, tt.errorMsg)
					}
				} else {
					t.Errorf("UnaryOpNode.Eval() error type = %T, want *internal.ParseError", err)
				}
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("UnaryOpNode.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

// 测试递归深度超限和上下文取消的情况
func TestUnaryOpNode_EvalWithContextAndDepth(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 创建测试用的数字节点
	num1 := &NumberNode{Value: decimal.NewFromInt(123)}

	// 测试递归深度超限
	t.Run("递归深度超限", func(t *testing.T) {
		// 使用一个已经超过最大递归深度的配置
		config := math_config.NewDefaultCalcConfig()
		config.MaxRecursionDepth = -1

		node := &UnaryOpNode{Operator: "-", Operand: num1, Pos: 0}
		_, err := node.Eval(ctx, nil, config)
		if err == nil {
			t.Errorf("UnaryOpNode.Eval() expected error for max recursion depth")
			return
		}
		if err != internal.ErrMaxRecursionDepth {
			t.Errorf("UnaryOpNode.Eval() error = %v, want %v", err, internal.ErrMaxRecursionDepth)
		}
	})

	// 测试上下文取消
	t.Run("上下文取消", func(t *testing.T) {
		// 取消上下文
		cancel()

		node := &UnaryOpNode{Operator: "-", Operand: num1, Pos: 0}
		_, err := node.Eval(ctx, nil, math_config.NewDefaultCalcConfig())
		if err == nil {
			t.Errorf("UnaryOpNode.Eval() expected error for canceled context")
			return
		}
		if err != internal.ErrExecutionTimeout {
			t.Errorf("UnaryOpNode.Eval() error = %v, want %v", err, internal.ErrExecutionTimeout)
		}
	})
}

// 测试操作数计算错误的情况
func TestUnaryOpNode_EvalWithOperandError(t *testing.T) {
	// 创建测试用的上下文
	ctx := context.Background()

	// 创建一个会返回错误的节点
	errorNode := &VariableNode{VarName: "undefined", Pos: 0}

	// 测试操作数计算错误
	t.Run("操作数计算错误", func(t *testing.T) {
		node := &UnaryOpNode{Operator: "-", Operand: errorNode, Pos: 0}
		_, err := node.Eval(ctx, nil, math_config.NewDefaultCalcConfig())
		if err == nil {
			t.Errorf("UnaryOpNode.Eval() expected error from operand evaluation")
		}
	})
}
