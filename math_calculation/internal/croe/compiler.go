package croe

import (
	"context"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// CompiledExpression 预编译表达式结构体
type CompiledExpression struct {
	ast       math_node.Node          // 抽象语法树
	config    *math_config.CalcConfig // 计算配置
	mutex     sync.RWMutex            // 用于并发安全
	lastError error                   // 最后一次错误
}

// Compile 预编译表达式
func Compile(expression string, config *math_config.CalcConfig) (*CompiledExpression, error) {
	// 验证表达式
	if len(expression) == 0 {
		return nil, &internal.ParseError{
			Pos:     0,
			Message: "空表达式",
			Cause:   internal.ErrInvalidExpression,
		}
	}

	// 验证配置
	if config == nil {
		config = math_config.NewDefaultCalcConfig()
	}

	// 创建解析器
	parser := NewParser(nil, config)

	// 解析表达式
	ast, err := parser.Parse(expression)
	if err != nil {
		return nil, err
	}

	// 创建预编译表达式
	return &CompiledExpression{
		ast:    ast,
		config: config,
	}, nil
}

// Evaluate 使用预编译表达式计算结果
func (ce *CompiledExpression) Evaluate(vars map[string]decimal.Decimal) (decimal.Decimal, error) {
	// 创建上下文并设置超时
	ctx, cancel := context.WithTimeout(context.Background(), ce.config.Timeout)
	defer cancel()

	// 创建变量副本，避免并发问题
	varsCopy := make(map[string]decimal.Decimal, len(vars))
	for k, v := range vars {
		varsCopy[k] = v
	}

	// 计算表达式
	result, err := ce.ast.Eval(ctx, varsCopy, ce.config)
	if err != nil {
		ce.mutex.Lock()
		ce.lastError = err
		ce.mutex.Unlock()
		return decimal.Zero, err
	}

	// 如果策略是只在最终结果控制精度，则在这里应用精度控制
	if !ce.config.ApplyPrecisionEachStep {
		return math_utils.SetPrecision(result, ce.config.Precision, ce.config.PrecisionMode), nil
	}

	return result, nil
}

// GetLastError 获取最后一次错误
func (ce *CompiledExpression) GetLastError() error {
	ce.mutex.RLock()
	defer ce.mutex.RUnlock()
	return ce.lastError
}

// WithConfig 设置新的配置
func (ce *CompiledExpression) WithConfig(config *math_config.CalcConfig) *CompiledExpression {
	if config == nil {
		return ce
	}
	ce.mutex.Lock()
	ce.config = config
	ce.mutex.Unlock()
	return ce
}

// WithTimeout 设置超时时间
func (ce *CompiledExpression) WithTimeout(timeout time.Duration) *CompiledExpression {
	ce.mutex.Lock()
	ce.config.Timeout = timeout
	ce.mutex.Unlock()
	return ce
}

// WithPrecision 设置精度
func (ce *CompiledExpression) WithPrecision(precision int32) *CompiledExpression {
	ce.mutex.Lock()
	ce.config.Precision = precision
	ce.mutex.Unlock()
	return ce
}

// WithPrecisionMode 设置精度模式
func (ce *CompiledExpression) WithPrecisionMode(mode math_config.PrecisionMode) *CompiledExpression {
	ce.mutex.Lock()
	ce.config.PrecisionMode = mode
	ce.mutex.Unlock()
	return ce
}

// WithPrecisionEachStep 在每一步应用精度控制
func (ce *CompiledExpression) WithPrecisionEachStep() *CompiledExpression {
	ce.mutex.Lock()
	ce.config.ApplyPrecisionEachStep = true
	ce.mutex.Unlock()
	return ce
}

// WithPrecisionFinalResult 只在最终结果应用精度控制
func (ce *CompiledExpression) WithPrecisionFinalResult() *CompiledExpression {
	ce.mutex.Lock()
	ce.config.ApplyPrecisionEachStep = false
	ce.mutex.Unlock()
	return ce
}
