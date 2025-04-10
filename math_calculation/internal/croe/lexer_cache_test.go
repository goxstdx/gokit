package croe

import (
	"reflect"
	"testing"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestLexer_LexWithCache(t *testing.T) {
	// 测试缓存功能
	input := "x + y * z"
	lexer := NewLexer(math_config.NewDefaultCalcConfig())

	// 第一次调用，应该没有缓存
	tokens1 := lexer.Lex(input)

	// 第二次调用，应该使用缓存
	tokens2 := lexer.Lex(input)

	// 两次结果应该相同
	if !reflect.DeepEqual(tokens1, tokens2) {
		t.Errorf("Lex() with cache = %v, want %v", tokens2, tokens1)
	}

	// 测试禁用缓存
	config := math_config.NewDefaultCalcConfig()
	config.UseLexerCache = false
	lexer = NewLexer(config)

	// 第一次调用
	tokens1 = lexer.Lex(input)

	// 第二次调用，不应该使用缓存，但结果应该相同
	tokens2 = lexer.Lex(input)

	// 两次结果应该相同
	if !reflect.DeepEqual(tokens1, tokens2) {
		t.Errorf("Lex() without cache = %v, want %v", tokens2, tokens1)
	}
}

func TestResetLexerCache(t *testing.T) {
	// 先使用缓存
	input := "x + y * z"
	lexer := NewLexer(math_config.NewDefaultCalcConfig())
	tokens1 := lexer.Lex(input)

	// 重置缓存
	ResetLexerCache()

	// 再次使用缓存，应该重新计算
	tokens2 := lexer.Lex(input)

	// 两次结果应该相同
	if !reflect.DeepEqual(tokens1, tokens2) {
		t.Errorf("Lex() after reset cache = %v, want %v", tokens2, tokens1)
	}
}
