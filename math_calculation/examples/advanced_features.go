//go:build advanced_features

package main

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/validator"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func main() {
	fmt.Println("=== 高级功能示例 ===")

	// 创建计算器
	calc := math_calculation.NewCalculator(nil)

	// 设置变量
	calc.WithVariable("x", decimal.NewFromFloat(5.0))
	calc.WithVariable("y", decimal.NewFromFloat(3.0))

	// 1. 表达式预编译
	fmt.Println("\n1. 表达式预编译")
	expr := "sqrt(x) * (y + 2.5) - abs(-5) + pow(2, 3)"

	// 预编译表达式
	compiled, err := calc.Compile(expr)
	if err != nil {
		fmt.Printf("预编译表达式失败: %v\n", err)
		return
	}

	// 使用预编译表达式计算
	result, err := compiled.Evaluate(map[string]decimal.Decimal{
		"x": decimal.NewFromFloat(5.0),
		"y": decimal.NewFromFloat(3.0),
	})
	if err != nil {
		fmt.Printf("计算失败: %v\n", err)
		return
	}

	fmt.Printf("预编译表达式结果: %s\n", result)

	// 修改配置并重新计算
	compiled.WithPrecisionFinalResult()
	result, err = compiled.Evaluate(map[string]decimal.Decimal{
		"x": decimal.NewFromFloat(5.0),
		"y": decimal.NewFromFloat(3.0),
	})
	if err != nil {
		fmt.Printf("计算失败: %v\n", err)
		return
	}

	fmt.Printf("修改配置后的结果: %s\n", result)

	// 2. 调试模式
	fmt.Println("\n2. 调试模式")
	calc.WithDebugMode(math_config.DebugDetailed)

	// 使用调试模式计算
	result, debugInfo, err := calc.CalculateWithDebug("1/3 + 1/3 + 1/3")
	if err != nil {
		fmt.Printf("计算失败: %v\n", err)
		return
	}

	fmt.Printf("调试模式结果: %s\n", result)
	fmt.Println("调试信息:")
	fmt.Println(debugInfo.String())

	// 3. 输入验证和限制
	fmt.Println("\n3. 输入验证和限制")

	// 设置验证选项
	validationOptions := validator.ValidationOptions{
		MaxExpressionLength:   100,
		MaxNestedParentheses:  5,
		MaxFunctionArguments:  3,
		AllowedFunctions:      []string{"sqrt", "abs", "pow", "min", "max"},
		DisallowedFunctions:   []string{"sin", "cos", "tan"},
		AllowVariables:        true,
		MaxVariableNameLength: 10,
		MaxNumberLength:       10,
	}

	calc.WithValidationOptions(validationOptions)
	calc.WithDebugMode(math_config.DebugDetailed)

	// 验证有效表达式
	result, err = calc.Calculate("sqrt(x) + abs(-5) + pow(2, 3)")
	if err != nil {
		fmt.Printf("计算失败: %v\n", err)
	} else {
		fmt.Printf("有效表达式结果: %s\n", result)
	}

	// 验证无效表达式
	result, err = calc.Calculate("sin(x) + cos(y)")
	if err != nil {
		fmt.Printf("无效表达式错误: %v\n", err)
	} else {
		fmt.Printf("无效表达式结果: %s\n", result)
	}

	// 验证嵌套括号过多的表达式
	result, err = calc.Calculate("((((((x + y)))))) + 5")
	if err != nil {
		fmt.Printf("嵌套括号过多错误: %v\n", err)
	} else {
		fmt.Printf("嵌套括号过多结果: %s\n", result)
	}

	// 4. 性能测试
	fmt.Println("\n4. 性能测试")

	// 创建新的计算器
	perfCalc := math_calculation.NewCalculator(nil).
		WithVariable("x", decimal.NewFromFloat(5.0)).
		WithVariable("y", decimal.NewFromFloat(3.0))

	// 普通计算
	start := time.Now()
	for i := 0; i < 1000; i++ {
		perfCalc.Calculate("sqrt(x) * (y + 2.5) - abs(-5) + pow(2, 3)")
	}
	normalDuration := time.Since(start)
	fmt.Printf("普通计算1000次耗时: %v\n", normalDuration)

	// 预编译计算
	compiled, _ = perfCalc.Compile("sqrt(x) * (y + 2.5) - abs(-5) + pow(2, 3)")
	start = time.Now()
	for i := 0; i < 1000; i++ {
		compiled.Evaluate(map[string]decimal.Decimal{
			"x": decimal.NewFromFloat(5.0),
			"y": decimal.NewFromFloat(3.0),
		})
	}
	compiledDuration := time.Since(start)
	fmt.Printf("预编译计算1000次耗时: %v\n", compiledDuration)
	fmt.Printf("性能提升: %.2f%%\n", float64(normalDuration-compiledDuration)/float64(normalDuration)*100)
}
