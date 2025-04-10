package math_calculation

import (
	"time"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/croe"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/debug"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/validator"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// Calculator 计算器结构体，支持链式API
type Calculator struct {
	config            *math_config.CalcConfig
	vars              map[string]decimal.Decimal
	validationOptions validator.ValidationOptions
	compiled          *croe.CompiledExpression
	lastDebugInfo     *debug.DebugInfo
}

// NewCalculator 创建新的计算器实例
func NewCalculator(cfg *math_config.CalcConfig) *Calculator {
	if cfg == nil {
		cfg = math_config.NewDefaultCalcConfig()
	}

	return &Calculator{
		config:            math_config.NewDefaultCalcConfig(),
		vars:              make(map[string]decimal.Decimal),
		validationOptions: validator.DefaultValidationOptions,
	}
}

// WithPrecision 设置精度
func (c *Calculator) WithPrecision(precision int32) *Calculator {
	c.config.Precision = precision
	return c
}

// WithPrecisionMode 设置精度模式
func (c *Calculator) WithPrecisionMode(mode math_config.PrecisionMode) *Calculator {
	c.config.PrecisionMode = mode
	return c
}

// WithRoundPrecision 设置精度模式为四舍五入
func (c *Calculator) WithRoundPrecision() *Calculator {
	c.config.PrecisionMode = math_config.RoundPrecision
	return c
}

// WithCeilPrecision 设置精度模式为向上取整
func (c *Calculator) WithCeilPrecision() *Calculator {
	c.config.PrecisionMode = math_config.CeilPrecision
	return c
}

// WithFloorPrecision 设置精度模式为向下取整
func (c *Calculator) WithFloorPrecision() *Calculator {
	c.config.PrecisionMode = math_config.FloorPrecision
	return c
}

// WithTimeout 设置超时时间
func (c *Calculator) WithTimeout(timeout time.Duration) *Calculator {
	c.config.Timeout = timeout
	return c
}

// WithMaxRecursionDepth 设置最大递归深度
func (c *Calculator) WithMaxRecursionDepth(depth int) *Calculator {
	c.config.MaxRecursionDepth = depth
	return c
}

// WithoutCache 禁用缓存
func (c *Calculator) WithoutCache() *Calculator {
	c.config.UseExprCache = false
	c.config.UseLexerCache = false
	return c
}

// WithCache 启用缓存
func (c *Calculator) WithCache() *Calculator {
	c.config.UseExprCache = true
	c.config.UseLexerCache = true
	return c
}

// WithPrecisionEachStep 在每一步应用精度控制
func (c *Calculator) WithPrecisionEachStep() *Calculator {
	c.config.ApplyPrecisionEachStep = true
	return c
}

// WithPrecisionFinalResult 只在最终结果应用精度控制
func (c *Calculator) WithPrecisionFinalResult() *Calculator {
	c.config.ApplyPrecisionEachStep = false
	return c
}

// WithVariable 添加变量
func (c *Calculator) WithVariable(name string, value decimal.Decimal) *Calculator {
	c.vars[name] = value
	return c
}

// WithVariables 添加多个变量
func (c *Calculator) WithVariables(vars map[string]decimal.Decimal) *Calculator {
	for k, v := range vars {
		c.vars[k] = v
	}
	return c
}

// CalculateParallel 并行计算多个表达式
func (c *Calculator) CalculateParallel(expressions []string) ([]decimal.Decimal, []error) {
	return CalculateParallel(expressions, c.vars, c.config)
}

// WithDebugMode 设置调试模式
func (c *Calculator) WithDebugMode(mode math_config.DebugMode) *Calculator {
	c.config.DebugMode = mode
	return c
}

// WithValidationOptions 设置验证选项
func (c *Calculator) WithValidationOptions(options validator.ValidationOptions) *Calculator {
	c.validationOptions = options
	return c
}

// Compile 预编译表达式
func (c *Calculator) Compile(expression string) (*croe.CompiledExpression, error) {
	// 验证表达式
	sanitized, err := validator.ValidateAndSanitizeExpression(expression, c.validationOptions)
	if err != nil {
		return nil, err
	}

	// 预编译表达式
	compiled, err := croe.Compile(sanitized, c.config)
	if err != nil {
		return nil, err
	}

	// 保存预编译表达式
	c.compiled = compiled
	return compiled, nil
}

// CalculateWithDebug 带调试信息的计算
func (c *Calculator) CalculateWithDebug(expression string) (decimal.Decimal, *debug.DebugInfo, error) {
	// 验证表达式
	sanitized, err := validator.ValidateAndSanitizeExpression(expression, c.validationOptions)
	if err != nil {
		return decimal.Zero, nil, err
	}

	// 计算表达式
	result, debugInfo, err := debug.DebugCalculate(sanitized, c.vars, c.config)
	if err != nil {
		return decimal.Zero, debugInfo, err
	}

	// 保存调试信息
	c.lastDebugInfo = debugInfo
	return result, debugInfo, nil
}

// GetLastDebugInfo 获取最后一次调试信息
func (c *Calculator) GetLastDebugInfo() *debug.DebugInfo {
	return c.lastDebugInfo
}

// Calculate 计算表达式
func (c *Calculator) Calculate(expression string) (decimal.Decimal, error) {
	// 验证表达式
	sanitized, err := validator.ValidateAndSanitizeExpression(expression, c.validationOptions)
	if err != nil {
		return decimal.Zero, err
	}

	// 如果开启了调试模式，使用调试计算
	if c.config.DebugMode != math_config.DebugNone {
		result, _, err := c.CalculateWithDebug(sanitized)
		return result, err
	}

	// 如果有预编译表达式，使用预编译表达式计算
	if c.compiled != nil {
		return c.compiled.Evaluate(c.vars)
	}

	// 使用普通计算
	return Calculate(sanitized, c.vars, c.config)
}
