package test

import (
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/croe"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_func"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// BenchmarkLex 测试词法分析性能
func BenchmarkLex(b *testing.B) {
	expr := "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3) + min(10, 5, 8) + max(3, 7, 2)"
	lexer := croe.NewLexer(math_config.NewDefaultCalcConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lexer.Lex(expr)
	}
}

// BenchmarkParse 测试解析性能
func BenchmarkParse(b *testing.B) {
	expr := "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3) + min(10, 5, 8) + max(3, 7, 2)"
	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromFloat(5.0),
	}
	parser := croe.NewParser(vars, math_config.NewDefaultCalcConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		parser.Parse(expr)
	}
}

// BenchmarkCalculate 测试计算性能
func BenchmarkCalculate(b *testing.B) {
	expr := "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3) + min(10, 5, 8) + max(3, 7, 2)"
	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromFloat(5.0),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		math_calculation.Calculate(expr, vars, math_config.NewDefaultCalcConfig())
	}
}

// BenchmarkCompile 测试预编译性能
func BenchmarkCompile(b *testing.B) {
	expr := "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3) + min(10, 5, 8) + max(3, 7, 2)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		croe.Compile(expr, math_config.NewDefaultCalcConfig())
	}
}

// BenchmarkCompiledEvaluate 测试预编译表达式计算性能
func BenchmarkCompiledEvaluate(b *testing.B) {
	expr := "sqrt(25) * (3.14 * x + 2.5) - abs(-5) + pow(2, 3) + min(10, 5, 8) + max(3, 7, 2)"
	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromFloat(5.0),
	}

	compiled, _ := croe.Compile(expr, math_config.NewDefaultCalcConfig())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		compiled.Evaluate(vars)
	}
}

// BenchmarkFastPow 测试优化的幂运算性能
func BenchmarkFastPow(b *testing.B) {
	base := decimal.NewFromFloat(2.5)
	exponent := int64(8)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		math_func.FastPow(base, exponent)
	}
}

// BenchmarkOptimizedSqrt 测试优化的平方根计算性能
func BenchmarkOptimizedSqrt(b *testing.B) {
	value := decimal.NewFromFloat(25.0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		math_func.OptimizedDecimalSqrt(value)
	}
}

// BenchmarkShardedCache 测试分片缓存性能
func BenchmarkShardedCache(b *testing.B) {
	cache := croe.NewShardedLRUCache(1000)
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	nodes := []math_node.Node{
		&math_node.NumberNode{Value: decimal.NewFromFloat(1.0)},
		&math_node.NumberNode{Value: decimal.NewFromFloat(2.0)},
		&math_node.NumberNode{Value: decimal.NewFromFloat(3.0)},
		&math_node.NumberNode{Value: decimal.NewFromFloat(4.0)},
		&math_node.NumberNode{Value: decimal.NewFromFloat(5.0)},
	}

	// 预填充缓存
	for i := 0; i < len(keys); i++ {
		cache.Set(keys[i], nodes[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(keys[i%len(keys)])
	}
}

// BenchmarkObjectPool 测试对象池性能
func BenchmarkObjectPool(b *testing.B) {
	b.Run("WithPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			token := croe.GetToken()
			token.Type = croe.TokenNumber
			token.Value = "123.45"
			token.Pos = 0
			croe.PutToken(token)
		}
	})

	b.Run("WithoutPool", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			token := &croe.Token{
				Type:  croe.TokenNumber,
				Value: "123.45",
				Pos:   0,
			}
			_ = token
		}
	})
}
