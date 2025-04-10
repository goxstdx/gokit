package debug

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/croe"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// DebugInfo 调试信息结构体
type DebugInfo struct {
	Expression string            // 表达式
	Steps      []DebugStep       // 调试步骤
	Variables  map[string]string // 变量值
	Result     string            // 结果
	Error      error             // 错误
	mutex      sync.Mutex        // 用于并发安全
}

// DebugStep 调试步骤结构体
type DebugStep struct {
	NodeType    string      // 节点类型
	Operation   string      // 操作
	Operands    []string    // 操作数
	Result      string      // 结果
	Error       error       // 错误
	SubSteps    []DebugStep // 子步骤
	ElapsedTime int64       // 耗时（纳秒）
}

// NewDebugInfo 创建新的调试信息
func NewDebugInfo(expression string) *DebugInfo {
	return &DebugInfo{
		Expression: expression,
		Steps:      make([]DebugStep, 0),
		Variables:  make(map[string]string),
	}
}

// AddStep 添加调试步骤
func (di *DebugInfo) AddStep(step DebugStep) {
	di.mutex.Lock()
	defer di.mutex.Unlock()
	di.Steps = append(di.Steps, step)
}

// SetResult 设置结果
func (di *DebugInfo) SetResult(result decimal.Decimal) {
	di.mutex.Lock()
	defer di.mutex.Unlock()
	di.Result = result.String()
}

// SetError 设置错误
func (di *DebugInfo) SetError(err error) {
	di.mutex.Lock()
	defer di.mutex.Unlock()
	di.Error = err
}

// SetVariables 设置变量
func (di *DebugInfo) SetVariables(vars map[string]decimal.Decimal) {
	di.mutex.Lock()
	defer di.mutex.Unlock()
	for k, v := range vars {
		di.Variables[k] = v.String()
	}
}

// String 返回调试信息的字符串表示
func (di *DebugInfo) String() string {
	di.mutex.Lock()
	defer di.mutex.Unlock()

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("表达式: %s\n", di.Expression))

	sb.WriteString("变量:\n")
	for k, v := range di.Variables {
		sb.WriteString(fmt.Sprintf("  %s = %s\n", k, v))
	}

	sb.WriteString("执行步骤:\n")
	for i, step := range di.Steps {
		sb.WriteString(fmt.Sprintf("步骤 %d: %s\n", i+1, formatStep(step, 1)))
	}

	if di.Error != nil {
		sb.WriteString(fmt.Sprintf("错误: %v\n", di.Error))
	} else {
		sb.WriteString(fmt.Sprintf("结果: %s\n", di.Result))
	}

	return sb.String()
}

// formatStep 格式化调试步骤
func formatStep(step DebugStep, indent int) string {
	indentStr := strings.Repeat("  ", indent)
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("%s类型: %s, 操作: %s\n", indentStr, step.NodeType, step.Operation))

	if len(step.Operands) > 0 {
		sb.WriteString(fmt.Sprintf("%s操作数:\n", indentStr))
		for i, operand := range step.Operands {
			sb.WriteString(fmt.Sprintf("%s  %d: %s\n", indentStr, i+1, operand))
		}
	}

	if step.Error != nil {
		sb.WriteString(fmt.Sprintf("%s错误: %v\n", indentStr, step.Error))
	} else {
		sb.WriteString(fmt.Sprintf("%s结果: %s\n", indentStr, step.Result))
	}

	if len(step.SubSteps) > 0 {
		sb.WriteString(fmt.Sprintf("%s子步骤:\n", indentStr))
		for i, subStep := range step.SubSteps {
			sb.WriteString(fmt.Sprintf("%s子步骤 %d: %s\n", indentStr, i+1, formatStep(subStep, indent+1)))
		}
	}

	if step.ElapsedTime > 0 {
		sb.WriteString(fmt.Sprintf("%s耗时: %d ns\n", indentStr, step.ElapsedTime))
	}

	return sb.String()
}

// DebugCalculate 带调试信息的计算
func DebugCalculate(expression string, vars map[string]decimal.Decimal, config *math_config.CalcConfig) (decimal.Decimal, *DebugInfo, error) {
	// 创建调试信息
	debugInfo := NewDebugInfo(expression)
	debugInfo.SetVariables(vars)

	// 验证表达式
	if len(expression) == 0 {
		err := &internal.ParseError{
			Pos:     0,
			Message: "空表达式",
			Cause:   internal.ErrInvalidExpression,
		}
		debugInfo.SetError(err)
		return decimal.Zero, debugInfo, err
	}

	// 验证配置
	if config == nil {
		config = math_config.NewDefaultCalcConfig()
	}

	// 创建上下文并设置超时
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()

	// 创建解析器
	parser := croe.NewParser(vars, config)

	// 解析表达式
	ast, err := parser.Parse(expression)
	if err != nil {
		debugInfo.SetError(err)
		return decimal.Zero, debugInfo, err
	}

	// 创建变量副本，避免并发问题
	varsCopy := make(map[string]decimal.Decimal, len(vars))
	for k, v := range vars {
		varsCopy[k] = v
	}

	// 添加一些基本调试信息
	if config.DebugMode >= math_config.DebugBasic {
		// 添加表达式类型的调试步骤
		debugInfo.AddStep(DebugStep{
			NodeType:  "Expression",
			Operation: "Parse",
			Operands:  []string{expression},
		})
	}

	// 计算表达式
	result, err := ast.Eval(ctx, varsCopy, config)
	if err != nil {
		debugInfo.SetError(err)
		return decimal.Zero, debugInfo, err
	}

	// 如果策略是只在最终结果控制精度，则在这里应用精度控制
	if !config.ApplyPrecisionEachStep {
		result = math_utils.SetPrecision(result, config.Precision, config.PrecisionMode)
	}

	// 添加结果调试信息
	if config.DebugMode >= math_config.DebugBasic {
		debugInfo.AddStep(DebugStep{
			NodeType:  "Result",
			Operation: "Evaluate",
			Result:    result.String(),
		})
	}

	debugInfo.SetResult(result)
	return result, debugInfo, nil
}
