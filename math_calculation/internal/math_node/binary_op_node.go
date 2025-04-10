package math_node

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// BinaryOpNode 二元运算符节点
type BinaryOpNode struct {
	Left     Node
	Operator string
	Right    Node
	Pos      int // 运算符在表达式中的位置，用于错误报告
}

// Eval 实现 BinaryOpNode 的 Eval 方法
func (n *BinaryOpNode) Eval(ctx context.Context, vars map[string]decimal.Decimal, config *math_config.CalcConfig) (decimal.Decimal, error) {
	// 空指针检查
	if config == nil {
		config = math_config.NewDefaultCalcConfig()
	}
	// 检查递归深度和超时
	if err := math_utils.CheckContext(ctx, config.MaxRecursionDepth); err != nil {
		return decimal.Zero, err
	}

	// 计算左操作数
	leftVal, err := n.Left.Eval(ctx, vars, config)
	if err != nil {
		return decimal.Zero, err
	}

	// 计算右操作数
	rightVal, err := n.Right.Eval(ctx, vars, config)
	if err != nil {
		return decimal.Zero, err
	}

	// 根据运算符执行相应的运算
	var result decimal.Decimal
	switch n.Operator {
	case "+":
		result = leftVal.Add(rightVal)
	case "-":
		result = leftVal.Sub(rightVal)
	case "*":
		result = leftVal.Mul(rightVal)
	case "/":
		if rightVal.IsZero() {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: "除数不能为零",
				Cause:   internal.ErrDivisionByZero,
			}
		}
		result = leftVal.Div(rightVal)
	case "^":
		// 处理负指数
		if rightVal.LessThan(decimal.Zero) {
			// 对于负指数，计算 1/(base^|exponent|)
			invertedBase := decimal.New(1, 0).Div(leftVal)
			exponent := rightVal.Abs().IntPart()
			result = decimal.New(1, 0)
			base := invertedBase
			for exponent > 0 {
				if exponent&1 == 1 {
					result = result.Mul(base)
				}
				base = base.Mul(base)
				exponent >>= 1
			}
		} else {
			// 处理正指数
			exponent := rightVal.IntPart()
			result = decimal.New(1, 0)
			base := leftVal
			for exponent > 0 {
				if exponent&1 == 1 {
					result = result.Mul(base)
				}
				base = base.Mul(base)
				exponent >>= 1
			}
		}
	default:
		return decimal.Zero, &internal.ParseError{
			Pos:     n.Pos,
			Message: fmt.Sprintf("不支持的运算符: %s", n.Operator),
			Cause:   internal.ErrUnsupportedOperator,
		}
	}

	// 根据精度控制策略决定是否应用精度控制
	if config.ApplyPrecisionEachStep {
		return math_utils.SetPrecision(result, config.Precision, config.PrecisionMode), nil
	}
	return result, nil
}
