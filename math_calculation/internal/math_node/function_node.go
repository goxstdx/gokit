package math_node

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_func"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// FunctionNode 函数节点
type FunctionNode struct {
	FuncName string
	Args     []Node
	Pos      int // 函数在表达式中的位置，用于错误报告
}

// Eval 实现 FunctionNode 的 Eval 方法
func (n *FunctionNode) Eval(ctx context.Context, vars map[string]decimal.Decimal, config *math_config.CalcConfig) (decimal.Decimal, error) {
	// 空指针检查
	if config == nil {
		config = math_config.NewDefaultCalcConfig()
	}
	// 检查递归深度和超时
	if err := math_utils.CheckContext(ctx, config.MaxRecursionDepth); err != nil {
		return decimal.Zero, err
	}

	// 计算所有参数
	var args []decimal.Decimal
	for _, arg := range n.Args {
		result, err := arg.Eval(ctx, vars, config)
		if err != nil {
			return decimal.Zero, err
		}
		args = append(args, result)
	}

	// 根据函数名执行相应的函数
	var result decimal.Decimal

	switch n.FuncName {
	case "sqrt":
		if len(args) != 1 {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: fmt.Sprintf("sqrt 函数需要正好 1 个参数，实际收到 %d 个", len(args)),
				Cause:   internal.ErrInvalidArgument,
			}
		}
		val := args[0]
		if val.LessThan(decimal.Zero) {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: fmt.Sprintf("不能计算负数的平方根: %s", val),
				Cause:   internal.ErrInvalidArgument,
			}
		}
		// 使用优化的平方根计算
		result = math_func.OptimizedDecimalSqrt(val)
	case "abs":
		if len(args) != 1 {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: fmt.Sprintf("abs 函数需要正好 1 个参数，实际收到 %d 个", len(args)),
				Cause:   internal.ErrInvalidArgument,
			}
		}
		result = args[0].Abs()
	case "round": // 四舍五入
		if len(args) < 1 || len(args) > 2 {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: fmt.Sprintf("round 函数需要 1 或 2 个参数，实际收到 %d 个", len(args)),
				Cause:   internal.ErrInvalidArgument,
			}
		}

		// 如果只有一个参数，四舍五入到整数
		if len(args) == 1 {
			result = math_func.RoundToPlaces(args[0], 0)
		} else {
			// 如果有两个参数，第二个参数指定小数位数
			places := args[1]
			if !places.Equal(places.Floor()) || places.LessThan(decimal.Zero) {
				return decimal.Zero, &internal.ParseError{
					Pos:     n.Pos,
					Message: "小数位数必须是非负整数",
					Cause:   internal.ErrInvalidArgument,
				}
			}
			result = math_func.RoundToPlaces(args[0], int32(places.IntPart()))
		}
	case "ceil": // 向上取整
		if len(args) < 1 || len(args) > 2 {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: fmt.Sprintf("ceil 函数需要 1 或 2 个参数，实际收到 %d 个", len(args)),
				Cause:   internal.ErrInvalidArgument,
			}
		}

		// 如果只有一个参数，向上取整到整数
		if len(args) == 1 {
			result = math_func.CeilToPlaces(args[0], 0)
		} else {
			// 如果有两个参数，第二个参数指定小数位数
			places := args[1]
			if !places.Equal(places.Floor()) || places.LessThan(decimal.Zero) {
				return decimal.Zero, &internal.ParseError{
					Pos:     n.Pos,
					Message: "小数位数必须是非负整数",
					Cause:   internal.ErrInvalidArgument,
				}
			}

			result = math_func.CeilToPlaces(args[0], int32(places.IntPart()))
		}
	case "floor": // 向下取整
		if len(args) < 1 || len(args) > 2 {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: fmt.Sprintf("floor 函数需要 1 或 2 个参数，实际收到 %d 个", len(args)),
				Cause:   internal.ErrInvalidArgument,
			}
		}

		// 如果只有一个参数，向下取整到整数
		if len(args) == 1 {
			result = math_func.FloorToPlaces(args[0], 0)
		} else {
			// 如果有两个参数，第二个参数指定小数位数
			places := args[1]
			if !places.Equal(places.Floor()) || places.LessThan(decimal.Zero) {
				return decimal.Zero, &internal.ParseError{
					Pos:     n.Pos,
					Message: "小数位数必须是非负整数",
					Cause:   internal.ErrInvalidArgument,
				}
			}

			result = math_func.FloorToPlaces(args[0], int32(places.IntPart()))
		}
	case "pow":
		if len(args) != 2 {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: fmt.Sprintf("pow 函数需要正好 2 个参数，实际收到 %d 个", len(args)),
				Cause:   internal.ErrInvalidArgument,
			}
		}
		base := args[0]
		exponent := args[1]

		// 处理负指数
		if exponent.LessThan(decimal.Zero) {
			// 对于负指数，计算 1/(base^|exponent|)
			exponent = exponent.Abs()
			base = decimal.New(1, 0).Div(base)
		}

		// 处理整数指数
		if exponent.Equal(exponent.Floor()) {
			exp := exponent.IntPart()
			// 使用优化的幂运算
			result = math_func.FastPow(base, exp)
		} else {
			// 对于非整数指数，返回错误
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: "目前不支持非整数指数",
				Cause:   internal.ErrInvalidArgument,
			}
		}
	case "min":
		if len(args) < 1 {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: "min 函数需要至少 1 个参数",
				Cause:   internal.ErrInvalidArgument,
			}
		}
		result = args[0]
		for _, arg := range args[1:] {
			if arg.LessThan(result) {
				result = arg
			}
		}
	case "max":
		if len(args) < 1 {
			return decimal.Zero, &internal.ParseError{
				Pos:     n.Pos,
				Message: "max 函数需要至少 1 个参数",
				Cause:   internal.ErrInvalidArgument,
			}
		}
		result = args[0]
		for _, arg := range args[1:] {
			if arg.GreaterThan(result) {
				result = arg
			}
		}
	default:
		return decimal.Zero, &internal.ParseError{
			Pos:     n.Pos,
			Message: fmt.Sprintf("不支持的函数: %s", n.FuncName),
			Cause:   internal.ErrUnsupportedOperator,
		}
	}

	// 根据精度控制策略决定是否应用精度控制
	if config.ApplyPrecisionEachStep {
		return math_utils.SetPrecision(result, config.Precision, config.PrecisionMode), nil
	}
	return result, nil
}
