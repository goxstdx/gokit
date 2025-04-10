package croe

import (
	"reflect"
	"testing"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestLexer_Lex(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []Token
	}{
		{
			name:     "空表达式",
			input:    "",
			expected: []Token{},
		},
		{
			name:  "单个数字",
			input: "123",
			expected: []Token{
				{Type: TokenNumber, Value: "123", Pos: 0},
			},
		},
		{
			name:  "负数",
			input: "-123",
			expected: []Token{
				{Type: TokenNumber, Value: "-123", Pos: 0},
			},
		},
		{
			name:  "小数",
			input: "123.456",
			expected: []Token{
				{Type: TokenNumber, Value: "123.456", Pos: 0},
			},
		},
		{
			name:  "负小数",
			input: "-123.456",
			expected: []Token{
				{Type: TokenNumber, Value: "-123.456", Pos: 0},
			},
		},
		{
			name:  "单个变量",
			input: "x",
			expected: []Token{
				{Type: TokenVariable, Value: "x", Pos: 0},
			},
		},
		{
			name:  "单个函数调用",
			input: "sin(x)",
			expected: []Token{
				{Type: TokenFunc, Value: "sin", Pos: 0},
				{Type: TokenLParen, Value: "(", Pos: 3},
				{Type: TokenVariable, Value: "x", Pos: 4},
				{Type: TokenRParen, Value: ")", Pos: 5},
			},
		},
		{
			name:  "简单表达式",
			input: "x + y",
			expected: []Token{
				{Type: TokenVariable, Value: "x", Pos: 0},
				{Type: TokenPlus, Value: "+", Pos: 2},
				{Type: TokenVariable, Value: "y", Pos: 4},
			},
		},
		{
			name:  "复杂表达式",
			input: "sqrt(x) * (y + 2.5) - abs(-5)",
			expected: []Token{
				{Type: TokenFunc, Value: "sqrt", Pos: 0},
				{Type: TokenLParen, Value: "(", Pos: 4},
				{Type: TokenVariable, Value: "x", Pos: 5},
				{Type: TokenRParen, Value: ")", Pos: 6},
				{Type: TokenAsterisk, Value: "*", Pos: 8},
				{Type: TokenLParen, Value: "(", Pos: 10},
				{Type: TokenVariable, Value: "y", Pos: 11},
				{Type: TokenPlus, Value: "+", Pos: 13},
				{Type: TokenNumber, Value: "2.5", Pos: 15},
				{Type: TokenRParen, Value: ")", Pos: 18},
				{Type: TokenMinus, Value: "-", Pos: 20},
				{Type: TokenFunc, Value: "abs", Pos: 22},
				{Type: TokenLParen, Value: "(", Pos: 25},
				{Type: TokenNumber, Value: "-5", Pos: 26},
				{Type: TokenRParen, Value: ")", Pos: 28},
			},
		},
		{
			name:  "带空格的表达式",
			input: "  x  +  y  ",
			expected: []Token{
				{Type: TokenVariable, Value: "x", Pos: 2},
				{Type: TokenPlus, Value: "+", Pos: 5},
				{Type: TokenVariable, Value: "y", Pos: 8},
			},
		},
		{
			name:  "带特殊字符的表达式",
			input: "x + y * z ^ w / v",
			expected: []Token{
				{Type: TokenVariable, Value: "x", Pos: 0},
				{Type: TokenPlus, Value: "+", Pos: 2},
				{Type: TokenVariable, Value: "y", Pos: 4},
				{Type: TokenAsterisk, Value: "*", Pos: 6},
				{Type: TokenVariable, Value: "z", Pos: 8},
				{Type: TokenCaret, Value: "^", Pos: 10},
				{Type: TokenVariable, Value: "w", Pos: 12},
				{Type: TokenSlash, Value: "/", Pos: 14},
				{Type: TokenVariable, Value: "v", Pos: 16},
			},
		},
		{
			name:  "带括号的表达式",
			input: "(x + y) * (z - w)",
			expected: []Token{
				{Type: TokenLParen, Value: "(", Pos: 0},
				{Type: TokenVariable, Value: "x", Pos: 1},
				{Type: TokenPlus, Value: "+", Pos: 3},
				{Type: TokenVariable, Value: "y", Pos: 5},
				{Type: TokenRParen, Value: ")", Pos: 6},
				{Type: TokenAsterisk, Value: "*", Pos: 8},
				{Type: TokenLParen, Value: "(", Pos: 10},
				{Type: TokenVariable, Value: "z", Pos: 11},
				{Type: TokenMinus, Value: "-", Pos: 13},
				{Type: TokenVariable, Value: "w", Pos: 15},
				{Type: TokenRParen, Value: ")", Pos: 16},
			},
		},
		{
			name:  "带函数调用的表达式",
			input: "sin(x) + cos(y) * tan(z)",
			expected: []Token{
				{Type: TokenFunc, Value: "sin", Pos: 0},
				{Type: TokenLParen, Value: "(", Pos: 3},
				{Type: TokenVariable, Value: "x", Pos: 4},
				{Type: TokenRParen, Value: ")", Pos: 5},
				{Type: TokenPlus, Value: "+", Pos: 7},
				{Type: TokenFunc, Value: "cos", Pos: 9},
				{Type: TokenLParen, Value: "(", Pos: 12},
				{Type: TokenVariable, Value: "y", Pos: 13},
				{Type: TokenRParen, Value: ")", Pos: 14},
				{Type: TokenAsterisk, Value: "*", Pos: 16},
				{Type: TokenFunc, Value: "tan", Pos: 18},
				{Type: TokenLParen, Value: "(", Pos: 21},
				{Type: TokenVariable, Value: "z", Pos: 22},
				{Type: TokenRParen, Value: ")", Pos: 23},
			},
		},
		{
			name:  "带多参数函数调用的表达式",
			input: "min(x, y, z) + max(a, b, c)",
			expected: []Token{
				{Type: TokenFunc, Value: "min", Pos: 0},
				{Type: TokenLParen, Value: "(", Pos: 3},
				{Type: TokenVariable, Value: "x", Pos: 4},
				{Type: TokenComma, Value: ",", Pos: 5},
				{Type: TokenVariable, Value: "y", Pos: 7},
				{Type: TokenComma, Value: ",", Pos: 8},
				{Type: TokenVariable, Value: "z", Pos: 10},
				{Type: TokenRParen, Value: ")", Pos: 11},
				{Type: TokenPlus, Value: "+", Pos: 13},
				{Type: TokenFunc, Value: "max", Pos: 15},
				{Type: TokenLParen, Value: "(", Pos: 18},
				{Type: TokenVariable, Value: "a", Pos: 19},
				{Type: TokenComma, Value: ",", Pos: 20},
				{Type: TokenVariable, Value: "b", Pos: 22},
				{Type: TokenComma, Value: ",", Pos: 23},
				{Type: TokenVariable, Value: "c", Pos: 25},
				{Type: TokenRParen, Value: ")", Pos: 26},
			},
		},
		{
			name:  "带错误字符的表达式",
			input: "x + y @ z",
			expected: []Token{
				{Type: TokenVariable, Value: "x", Pos: 0},
				{Type: TokenPlus, Value: "+", Pos: 2},
				{Type: TokenVariable, Value: "y", Pos: 4},
				{Type: TokenError, Value: "@", Pos: 6},
				{Type: TokenVariable, Value: "z", Pos: 8},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(math_config.NewDefaultCalcConfig())
			tokens := lexer.Lex(tt.input)

			if !reflect.DeepEqual(tokens, tt.expected) {
				t.Errorf("Lex() = %v, want %v", tokens, tt.expected)
			}
		})
	}
}

func TestIsIdentifierChar(t *testing.T) {
	tests := []struct {
		name     string
		char     byte
		expected bool
	}{
		{"小写字母", 'a', true},
		{"大写字母", 'Z', true},
		{"数字", '5', true},
		{"下划线", '_', true},
		{"空格", ' ', false},
		{"加号", '+', false},
		{"减号", '-', false},
		{"乘号", '*', false},
		{"除号", '/', false},
		{"左括号", '(', false},
		{"右括号", ')', false},
		{"逗号", ',', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := math_utils.IsIdentifierChar(tt.char); got != tt.expected {
				t.Errorf("isIdentifierChar() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIsAlpha(t *testing.T) {
	tests := []struct {
		name     string
		char     byte
		expected bool
	}{
		{"小写字母a", 'a', true},
		{"小写字母z", 'z', true},
		{"大写字母A", 'A', true},
		{"大写字母Z", 'Z', true},
		{"数字", '5', false},
		{"下划线", '_', false},
		{"空格", ' ', false},
		{"加号", '+', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := math_utils.IsAlpha(tt.char); got != tt.expected {
				t.Errorf("isAlpha() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestIsDigit(t *testing.T) {
	tests := []struct {
		name     string
		char     byte
		expected bool
	}{
		{"数字0", '0', true},
		{"数字9", '9', true},
		{"小写字母", 'a', false},
		{"大写字母", 'A', false},
		{"下划线", '_', false},
		{"空格", ' ', false},
		{"加号", '+', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := math_utils.IsDigit(tt.char); got != tt.expected {
				t.Errorf("isDigit() = %v, want %v", got, tt.expected)
			}
		})
	}
}
