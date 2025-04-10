package math_node

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// UnaryOpNode 一元运算符节点
type UnaryOpNode struct {
	Operator string
	Operand  Node
	Pos      int // 运算符在表达式中的位置，用于错误报告
}

// Eval 实现 UnaryOpNode 的 Eval 方法
func (n *UnaryOpNode) Eval(ctx context.Context, vars map[string]decimal.Decimal, config *math_config.CalcConfig) (decimal.Decimal, error) {
	// 空指针检查
	if config == nil {
		config = math_config.NewDefaultCalcConfig()
	}
	// 检查递归深度和超时
	if err := math_utils.CheckContext(ctx, config.MaxRecursionDepth); err != nil {
		return decimal.Zero, err
	}

	// 计算操作数
	val, err := n.Operand.Eval(ctx, vars, config)
	if err != nil {
		return decimal.Zero, err
	}

	// 根据运算符执行相应的运算
	var result decimal.Decimal
	switch n.Operator {
	case "-":
		result = val.Neg()
	case "+":
		result = val
	default:
		return decimal.Zero, &internal.ParseError{
			Pos:     n.Pos,
			Message: fmt.Sprintf("不支持的一元运算符: %s", n.Operator),
			Cause:   internal.ErrUnsupportedOperator,
		}
	}

	// 根据精度控制策略决定是否应用精度控制
	if config.ApplyPrecisionEachStep {
		return math_utils.SetPrecision(result, config.Precision, config.PrecisionMode), nil
	}
	return result, nil
}
