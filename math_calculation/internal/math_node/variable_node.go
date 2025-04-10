package math_node

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// VariableNode 变量节点
type VariableNode struct {
	VarName string
	Pos     int // 变量在表达式中的位置，用于错误报告
}

// Eval 实现 VariableNode 的 Eval 方法
func (n *VariableNode) Eval(ctx context.Context, vars map[string]decimal.Decimal, config *math_config.CalcConfig) (decimal.Decimal, error) {
	// 空指针检查
	if config == nil {
		config = math_config.NewDefaultCalcConfig()
	}
	// 检查变量是否存在
	if val, ok := vars[n.VarName]; ok {
		// 根据精度控制策略决定是否应用精度控制
		if config.ApplyPrecisionEachStep {
			return math_utils.SetPrecision(val, config.Precision, config.PrecisionMode), nil
		}
		return val, nil
	}
	// 返回变量未定义错误，并包含位置信息
	return decimal.Zero, &internal.ParseError{
		Pos:     n.Pos,
		Message: fmt.Sprintf("未定义的变量: %s", n.VarName),
		Cause:   internal.ErrUndefinedVariable,
	}
}
