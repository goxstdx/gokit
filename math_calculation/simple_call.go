package math_calculation

import (
	"context"
	"fmt"
	"sync"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/croe"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// Calculate 计算表达式的便捷函数
func Calculate(expression string, vars map[string]decimal.Decimal, cfg *math_config.CalcConfig) (decimal.Decimal, error) {
	// 验证表达式
	if len(expression) == 0 {
		return decimal.Zero, &internal.ParseError{
			Pos:     0,
			Message: "空表达式",
			Cause:   internal.ErrInvalidExpression,
		}
	}
	if cfg == nil {
		cfg = math_config.NewDefaultCalcConfig()
	}

	// 验证配置
	if cfg.Timeout <= 0 {
		cfg.Timeout = math_config.DefaultConfig.Timeout
	}
	if cfg.MaxRecursionDepth <= 0 {
		cfg.MaxRecursionDepth = math_config.DefaultConfig.MaxRecursionDepth
	}

	// 创建上下文并设置超时
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	// 创建解析器，使用完整的配置
	parser := croe.NewParser(vars, cfg)

	// 解析表达式
	ast, err := parser.Parse(expression)
	if err != nil {
		return decimal.Zero, err
	}

	// 创建变量副本，避免并发问题
	varsCopy := make(map[string]decimal.Decimal, len(vars))
	for k, v := range vars {
		varsCopy[k] = v
	}

	// 计算表达式
	result, err := ast.Eval(ctx, varsCopy, cfg)
	if err != nil {
		return decimal.Zero, err
	}

	// 如果策略是只在最终结果控制精度，则在这里应用精度控制
	if !cfg.ApplyPrecisionEachStep {
		return math_utils.SetPrecision(result, cfg.Precision, cfg.PrecisionMode), nil
	}

	return result, nil
}

// CalculateParallel 并行计算多个表达式
func CalculateParallel(expressions []string, vars map[string]decimal.Decimal, cfg *math_config.CalcConfig) ([]decimal.Decimal, []error) {
	// 验证表达式列表
	if len(expressions) == 0 {
		return []decimal.Decimal{}, []error{}
	}
	if cfg == nil {
		cfg = math_config.NewDefaultCalcConfig()
	}

	// 验证配置
	if cfg.Timeout <= 0 {
		cfg.Timeout = math_config.DefaultConfig.Timeout
	}
	if cfg.MaxRecursionDepth <= 0 {
		cfg.MaxRecursionDepth = math_config.DefaultConfig.MaxRecursionDepth
	}

	// 预分配结果切片
	results := make([]decimal.Decimal, len(expressions))
	errs := make([]error, len(expressions))

	// 限制并发数量，避免资源耗尽
	maxConcurrency := 10
	if len(expressions) < maxConcurrency {
		maxConcurrency = len(expressions)
	}

	// 使用信号量控制并发
	sem := make(chan struct{}, maxConcurrency)
	wg := sync.WaitGroup{}

	// 并行计算每个表达式
	for i, expr := range expressions {
		wg.Add(1)
		sem <- struct{}{} // 获取信号量，如果满了就会阻塞
		go func(index int, expression string) {
			defer func() {
				<-sem // 释放信号量
				wg.Done()

				// 捕获panic
				if r := recover(); r != nil {
					errs[index] = fmt.Errorf("计算表达式时发生异常: %v", r)
					results[index] = decimal.Zero
				}
			}()

			// 创建变量副本，避免并发问题
			varsCopy := make(map[string]decimal.Decimal, len(vars))
			for k, v := range vars {
				varsCopy[k] = v
			}

			result, err := Calculate(expression, varsCopy, cfg)
			results[index] = result
			errs[index] = err
		}(i, expr)
	}

	// 等待所有计算完成
	wg.Wait()
	return results, errs
}
