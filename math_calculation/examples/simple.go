//go:build simple

package main

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func main() {
	// 测试表达式
	expr := "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3) + min(10, 5, 8) + max(3, 7, 2)"

	// 更复杂的表达式，用于测试精度控制策略的差异
	complexExpr := "1/3 + 1/3 + 1/3" // 理论上等于1，但浮点数计算可能会有误差

	// 变量映射
	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromFloat(5.0),
	}

	fmt.Println("=== 使用原始API计算 ===")
	// 使用默认配置计算表达式
	result, err := math_calculation.Calculate(expr, vars, math_config.NewDefaultCalcConfig())
	if err != nil {
		fmt.Printf("解析表达式失败: %v\n", err)
		return
	}

	fmt.Printf("结果: %s\n", result)

	fmt.Println("\n=== 使用链式API计算 ===")
	// 使用链式API计算表达式
	calc := math_calculation.NewCalculator(nil)
	chainResult, err := calc.WithVariable("x", decimal.NewFromFloat(5.0)).Calculate(expr)
	if err != nil {
		fmt.Printf("使用链式API解析表达式失败: %v\n", err)
		return
	}

	fmt.Printf("链式API结果: %s\n", chainResult)

	// 使用链式API禁用缓存
	noCacheResult, err := math_calculation.NewCalculator(nil).
		WithVariable("x", decimal.NewFromFloat(5.0)).
		WithoutCache().
		WithPrecisionEachStep().
		Calculate(expr)
	if err != nil {
		fmt.Printf("禁用缓存的链式API解析失败: %v\n", err)
		return
	}

	fmt.Printf("禁用缓存的链式API结果: %s\n", noCacheResult)

	// 使用链式API只在最终结果控制精度
	finalPrecisionResult, err := math_calculation.NewCalculator(nil).
		WithVariables(vars).
		WithPrecisionFinalResult().
		Calculate(expr)
	if err != nil {
		fmt.Printf("只在最终结果控制精度的链式API解析失败: %v\n", err)
		return
	}

	fmt.Printf("只在最终结果控制精度的链式API结果: %s\n", finalPrecisionResult)

	// 测试复杂表达式的精度控制差异
	fmt.Println("\n=== 测试精度控制策略的差异 ===")

	// 测试设置全局缓存容量
	math_calculation.SetLexerCacheCapacity(2000)
	math_calculation.SetExprCacheCapacity(3000)
	fmt.Println("已设置词法分析器缓存容量为2000，表达式缓存容量为3000")

	// 每步控制精度
	eachStepResult, _ := math_calculation.NewCalculator(nil).
		WithPrecisionEachStep().
		Calculate(complexExpr)
	fmt.Printf("每步控制精度 (1/3 + 1/3 + 1/3): %s\n", eachStepResult)

	// 只在最终结果控制精度
	finalResult, _ := math_calculation.NewCalculator(nil).
		WithPrecisionFinalResult().
		Calculate(complexExpr)
	fmt.Printf("只在最终结果控制精度 (1/3 + 1/3 + 1/3): %s\n", finalResult)

	// 测试并行计算
	fmt.Println("\n=== 测试并行计算 ===")
	expressions := []string{
		"sqrt(16) + 10",
		"pow(2, 8) / 4",
		"min(abs(-10), 5, 8)",
		"max(3 * 2, 7, 2 + 3)",
		"round(3.14159)",
		"round(3.14159, 2)",
		"ceil(3.14159)",
		"ceil(3.14159, 2)",
		"floor(3.14159)",
		"floor(3.14159, 2)",
	}

	// 使用链式API并行计算
	results, errs := math_calculation.NewCalculator(nil).
		WithVariables(vars).
		WithTimeout(time.Second * 10). // 设置更长的超时时间
		CalculateParallel(expressions)
	for i, result := range results {
		if errs[i] != nil {
			fmt.Printf("表达式 %s 计算失败: %v\n", expressions[i], errs[i])
		} else {
			fmt.Printf("表达式 %s = %s\n", expressions[i], result)
		}
	}
}
