package math_node

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestFunctionNode_Eval(t *testing.T) {
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
	num1 := &NumberNode{Value: decimal.NewFromInt(25)}
	num2 := &NumberNode{Value: decimal.NewFromInt(-5)}
	num3 := &NumberNode{Value: decimal.NewFromInt(2)}
	num4 := &NumberNode{Value: decimal.NewFromInt(3)}
	num5 := &NumberNode{Value: decimal.NewFromFloat(123.456)}
	num6 := &NumberNode{Value: decimal.NewFromInt(-2)}
	num7 := &NumberNode{Value: decimal.NewFromFloat(2.5)}

	tests := []struct {
		name     string
		node     *FunctionNode
		config   *math_config.CalcConfig
		want     decimal.Decimal
		wantErr  bool
		errorMsg string
	}{
		{
			name:    "sqrt函数",
			node:    &FunctionNode{FuncName: "sqrt", Args: []Node{num1}, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(5),
			wantErr: false,
		},
		{
			name:     "sqrt函数-参数数量错误",
			node:     &FunctionNode{FuncName: "sqrt", Args: []Node{num1, num2}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "sqrt 函数需要正好 1 个参数，实际收到 2 个",
		},
		{
			name:     "sqrt函数-负数参数",
			node:     &FunctionNode{FuncName: "sqrt", Args: []Node{num2}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "不能计算负数的平方根: -5",
		},
		{
			name:    "abs函数",
			node:    &FunctionNode{FuncName: "abs", Args: []Node{num2}, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(5),
			wantErr: false,
		},
		{
			name:     "abs函数-参数数量错误",
			node:     &FunctionNode{FuncName: "abs", Args: []Node{num1, num2}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "abs 函数需要正好 1 个参数，实际收到 2 个",
		},
		{
			name:    "round函数-一个参数",
			node:    &FunctionNode{FuncName: "round", Args: []Node{num5}, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(123),
			wantErr: false,
		},
		{
			name:    "round函数-两个参数",
			node:    &FunctionNode{FuncName: "round", Args: []Node{num5, num3}, Pos: 0},
			config:  config,
			want:    decimal.NewFromFloat(123.46),
			wantErr: false,
		},
		{
			name:     "round函数-参数数量错误",
			node:     &FunctionNode{FuncName: "round", Args: []Node{num1, num2, num3}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "round 函数需要 1 或 2 个参数，实际收到 3 个",
		},
		{
			name:     "round函数-小数位数非整数",
			node:     &FunctionNode{FuncName: "round", Args: []Node{num5, num7}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "小数位数必须是非负整数",
		},
		{
			name:     "round函数-小数位数为负数",
			node:     &FunctionNode{FuncName: "round", Args: []Node{num5, num6}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "小数位数必须是非负整数",
		},
		{
			name:    "ceil函数-一个参数",
			node:    &FunctionNode{FuncName: "ceil", Args: []Node{num5}, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(124),
			wantErr: false,
		},
		{
			name:    "ceil函数-两个参数",
			node:    &FunctionNode{FuncName: "ceil", Args: []Node{num5, num3}, Pos: 0},
			config:  config,
			want:    decimal.NewFromFloat(123.46),
			wantErr: false,
		},
		{
			name:     "ceil函数-参数数量错误",
			node:     &FunctionNode{FuncName: "ceil", Args: []Node{num1, num2, num3}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "ceil 函数需要 1 或 2 个参数，实际收到 3 个",
		},
		{
			name:     "ceil函数-小数位数非整数",
			node:     &FunctionNode{FuncName: "ceil", Args: []Node{num5, num7}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "小数位数必须是非负整数",
		},
		{
			name:    "floor函数-一个参数",
			node:    &FunctionNode{FuncName: "floor", Args: []Node{num5}, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(123),
			wantErr: false,
		},
		{
			name:    "floor函数-两个参数",
			node:    &FunctionNode{FuncName: "floor", Args: []Node{num5, num3}, Pos: 0},
			config:  config,
			want:    decimal.NewFromFloat(123.46),
			wantErr: false,
		},
		{
			name:     "floor函数-参数数量错误",
			node:     &FunctionNode{FuncName: "floor", Args: []Node{num1, num2, num3}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "floor 函数需要 1 或 2 个参数，实际收到 3 个",
		},
		{
			name:    "pow函数-正整数指数",
			node:    &FunctionNode{FuncName: "pow", Args: []Node{num3, num4}, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(8),
			wantErr: false,
		},
		{
			name:    "pow函数-负整数指数",
			node:    &FunctionNode{FuncName: "pow", Args: []Node{num3, num6}, Pos: 0},
			config:  config,
			want:    decimal.NewFromFloat(0.25),
			wantErr: false,
		},
		{
			name:     "pow函数-非整数指数",
			node:     &FunctionNode{FuncName: "pow", Args: []Node{num3, num7}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "目前不支持非整数指数",
		},
		{
			name:     "pow函数-参数数量错误",
			node:     &FunctionNode{FuncName: "pow", Args: []Node{num1}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "pow 函数需要正好 2 个参数，实际收到 1 个",
		},
		{
			name:    "min函数",
			node:    &FunctionNode{FuncName: "min", Args: []Node{num1, num2, num3}, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(-5),
			wantErr: false,
		},
		{
			name:     "min函数-无参数",
			node:     &FunctionNode{FuncName: "min", Args: []Node{}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "min 函数需要至少 1 个参数",
		},
		{
			name:    "max函数",
			node:    &FunctionNode{FuncName: "max", Args: []Node{num1, num2, num3}, Pos: 0},
			config:  config,
			want:    decimal.NewFromInt(25),
			wantErr: false,
		},
		{
			name:     "max函数-无参数",
			node:     &FunctionNode{FuncName: "max", Args: []Node{}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "max 函数需要至少 1 个参数",
		},
		{
			name:     "不支持的函数",
			node:     &FunctionNode{FuncName: "unknown", Args: []Node{num1}, Pos: 0},
			config:   config,
			want:     decimal.Zero,
			wantErr:  true,
			errorMsg: "不支持的函数: unknown",
		},
		{
			name:    "配置为nil",
			node:    &FunctionNode{FuncName: "abs", Args: []Node{num2}, Pos: 0},
			config:  nil,
			want:    decimal.NewFromInt(5),
			wantErr: false,
		},
		{
			name:    "不应用精度控制",
			node:    &FunctionNode{FuncName: "sqrt", Args: []Node{num1}, Pos: 0},
			config:  &math_config.CalcConfig{ApplyPrecisionEachStep: false, MaxRecursionDepth: 100},
			want:    decimal.NewFromInt(5),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.node.Eval(ctx, vars, tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("FunctionNode.Eval() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				if err == nil {
					t.Errorf("FunctionNode.Eval() expected error but got nil")
					return
				}
				if parseErr, ok := err.(*internal.ParseError); ok {
					if parseErr.Message != tt.errorMsg {
						t.Errorf("FunctionNode.Eval() error message = %v, want %v", parseErr.Message, tt.errorMsg)
					}
				} else {
					t.Errorf("FunctionNode.Eval() error type = %T, want *internal.ParseError", err)
				}
				return
			}
			if !got.Equal(tt.want) {
				t.Errorf("FunctionNode.Eval() = %v, want %v", got, tt.want)
			}
		})
	}
}

// 测试递归深度超限和上下文取消的情况
func TestFunctionNode_EvalWithContextAndDepth(t *testing.T) {
	// 创建一个可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())

	// 创建测试用的数字节点
	num1 := &NumberNode{Value: decimal.NewFromInt(25)}

	// 测试递归深度超限
	t.Run("递归深度超限", func(t *testing.T) {
		// 使用一个已经超过最大递归深度的配置
		config := math_config.NewDefaultCalcConfig()
		config.MaxRecursionDepth = -1

		node := &FunctionNode{FuncName: "sqrt", Args: []Node{num1}, Pos: 0}
		_, err := node.Eval(ctx, nil, config)
		if err == nil {
			t.Errorf("FunctionNode.Eval() expected error for max recursion depth")
			return
		}
		if err != internal.ErrMaxRecursionDepth {
			t.Errorf("FunctionNode.Eval() error = %v, want %v", err, internal.ErrMaxRecursionDepth)
		}
	})

	// 测试上下文取消
	t.Run("上下文取消", func(t *testing.T) {
		// 取消上下文
		cancel()

		node := &FunctionNode{FuncName: "sqrt", Args: []Node{num1}, Pos: 0}
		_, err := node.Eval(ctx, nil, math_config.NewDefaultCalcConfig())
		if err == nil {
			t.Errorf("FunctionNode.Eval() expected error for canceled context")
			return
		}
		if err != internal.ErrExecutionTimeout {
			t.Errorf("FunctionNode.Eval() error = %v, want %v", err, internal.ErrExecutionTimeout)
		}
	})
}

// 测试参数计算错误的情况
func TestFunctionNode_EvalWithArgErrors(t *testing.T) {
	// 创建测试用的上下文
	ctx := context.Background()

	// 创建一个会返回错误的节点
	errorNode := &VariableNode{VarName: "undefined", Pos: 0}

	// 测试参数计算错误
	t.Run("参数计算错误", func(t *testing.T) {
		node := &FunctionNode{FuncName: "sqrt", Args: []Node{errorNode}, Pos: 0}
		_, err := node.Eval(ctx, nil, math_config.NewDefaultCalcConfig())
		if err == nil {
			t.Errorf("FunctionNode.Eval() expected error from argument evaluation")
		}
	})
}
