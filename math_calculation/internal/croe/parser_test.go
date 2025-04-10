package croe

import (
	"context"
	"strings"
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestParser_Parse(t *testing.T) {
	tests := []struct {
		name        string
		expression  string
		vars        map[string]decimal.Decimal
		wantErr     bool
		errContains string
	}{
		{
			name:        "空表达式",
			expression:  "",
			vars:        map[string]decimal.Decimal{},
			wantErr:     true,
			errContains: "空表达式",
		},
		{
			name:       "单个数字",
			expression: "123",
			vars:       map[string]decimal.Decimal{},
			wantErr:    false,
		},
		{
			name:       "负数",
			expression: "-123",
			vars:       map[string]decimal.Decimal{},
			wantErr:    false,
		},
		{
			name:       "小数",
			expression: "123.456",
			vars:       map[string]decimal.Decimal{},
			wantErr:    false,
		},
		{
			name:       "单个变量",
			expression: "x",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10)},
			wantErr:    false,
		},
		{
			name:        "未定义变量",
			expression:  "x",
			vars:        map[string]decimal.Decimal{},
			wantErr:     false, // 解析时不会检查变量是否定义，只有在计算时才会检查
			errContains: "",
		},
		{
			name:       "简单加法",
			expression: "x + y",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)},
			wantErr:    false,
		},
		{
			name:       "简单减法",
			expression: "x - y",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)},
			wantErr:    false,
		},
		{
			name:       "简单乘法",
			expression: "x * y",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)},
			wantErr:    false,
		},
		{
			name:       "简单除法",
			expression: "x / y",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)},
			wantErr:    false,
		},
		{
			name:        "除以零",
			expression:  "x / 0",
			vars:        map[string]decimal.Decimal{"x": decimal.NewFromInt(10)},
			wantErr:     false, // 解析时不会检查除数是否为零，只有在计算时才会检查
			errContains: "",
		},
		{
			name:       "幂运算",
			expression: "x ^ y",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(2)},
			wantErr:    false,
		},
		{
			name:       "带括号的表达式",
			expression: "(x + y) * z",
			vars: map[string]decimal.Decimal{
				"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20), "z": decimal.NewFromInt(2),
			},
			wantErr: false,
		},
		{
			name:       "括号不匹配",
			expression: "(x + y * z",
			vars: map[string]decimal.Decimal{
				"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20), "z": decimal.NewFromInt(2),
			},
			wantErr:     true,
			errContains: "缺少右括号",
		},
		{
			name:       "函数调用",
			expression: "sqrt(x)",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(25)},
			wantErr:    false,
		},
		{
			name:        "函数调用参数错误",
			expression:  "sqrt(-x)",
			vars:        map[string]decimal.Decimal{"x": decimal.NewFromInt(25)},
			wantErr:     false, // 解析时不会检查函数参数是否有效，只有在计算时才会检查
			errContains: "",
		},
		{
			name:       "多参数函数调用",
			expression: "min(x, y, z)",
			vars: map[string]decimal.Decimal{
				"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20), "z": decimal.NewFromInt(5),
			},
			wantErr: false,
		},
		{
			name:       "复杂表达式",
			expression: "sqrt(x) * (y + 2.5) - abs(-5) + pow(2, 3)",
			vars:       map[string]decimal.Decimal{"x": decimal.NewFromInt(25), "y": decimal.NewFromInt(10)},
			wantErr:    false,
		},
		{
			name:        "无效的表达式",
			expression:  "x + * y",
			vars:        map[string]decimal.Decimal{"x": decimal.NewFromInt(10), "y": decimal.NewFromInt(20)},
			wantErr:     true,
			errContains: "意外的标记",
		},
		{
			name:        "未知函数",
			expression:  "unknown(x)",
			vars:        map[string]decimal.Decimal{"x": decimal.NewFromInt(10)},
			wantErr:     false, // 解析时不会检查函数是否存在，只有在计算时才会检查
			errContains: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(tt.vars, math_config.NewDefaultCalcConfig())
			_, err := parser.Parse(tt.expression)

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil && tt.errContains != "" {
				if !containsString(err.Error(), tt.errContains) {
					t.Errorf("Parse() error = %v, want error containing %v", err, tt.errContains)
				}
			}
		})
	}
}

func TestParser_ParseWithCache(t *testing.T) {
	// 测试缓存功能
	expression := "x + y * z"
	vars := map[string]decimal.Decimal{
		"x": decimal.NewFromInt(10),
		"y": decimal.NewFromInt(20),
		"z": decimal.NewFromInt(30),
	}

	// 启用缓存
	config := math_config.NewDefaultCalcConfig()
	config.UseExprCache = true
	parser := NewParser(vars, config)

	// 第一次解析
	node1, err := parser.Parse(expression)
	if err != nil {
		t.Errorf("Parse() error = %v", err)
		return
	}

	// 第二次解析，应该使用缓存
	node2, err := parser.Parse(expression)
	if err != nil {
		t.Errorf("Parse() error = %v", err)
		return
	}

	// 两次结果应该相同
	ctx := context.Background()
	result1, err := node1.Eval(ctx, vars, config)
	if err != nil {
		t.Errorf("Eval() error = %v", err)
		return
	}

	result2, err := node2.Eval(ctx, vars, config)
	if err != nil {
		t.Errorf("Eval() error = %v", err)
		return
	}

	if !result1.Equal(result2) {
		t.Errorf("Parse() with cache = %v, want %v", result2, result1)
	}

	// 测试禁用缓存
	config.UseExprCache = false
	parser = NewParser(vars, config)

	// 第一次解析
	node1, err = parser.Parse(expression)
	if err != nil {
		t.Errorf("Parse() error = %v", err)
		return
	}

	// 第二次解析，不应该使用缓存
	node2, err = parser.Parse(expression)
	if err != nil {
		t.Errorf("Parse() error = %v", err)
		return
	}

	// 两次结果应该相同
	result1, err = node1.Eval(ctx, vars, config)
	if err != nil {
		t.Errorf("Eval() error = %v", err)
		return
	}

	result2, err = node2.Eval(ctx, vars, config)
	if err != nil {
		t.Errorf("Eval() error = %v", err)
		return
	}

	if !result1.Equal(result2) {
		t.Errorf("Parse() without cache = %v, want %v", result2, result1)
	}
}

func TestParser_ParseArguments(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		wantCount  int
		wantErr    bool
	}{
		{
			name:       "无参数",
			expression: "func()",
			wantCount:  0,
			wantErr:    false,
		},
		{
			name:       "单个参数",
			expression: "func(x)",
			wantCount:  1,
			wantErr:    false,
		},
		{
			name:       "多个参数",
			expression: "func(x, y, z)",
			wantCount:  3,
			wantErr:    false,
		},
		{
			name:       "嵌套函数调用",
			expression: "func(x, sqrt(y), z)",
			wantCount:  3,
			wantErr:    false,
		},
		{
			name:       "复杂表达式参数",
			expression: "func(x + y, z * w, a / b)",
			wantCount:  3,
			wantErr:    false,
		},
		{
			name:       "参数中有括号",
			expression: "func((x + y) * z)",
			wantCount:  1,
			wantErr:    false,
		},
		{
			name:       "参数错误",
			expression: "func(x,)",
			wantCount:  0,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := map[string]decimal.Decimal{
				"x": decimal.NewFromInt(1),
				"y": decimal.NewFromInt(2),
				"z": decimal.NewFromInt(3),
				"w": decimal.NewFromInt(4),
				"a": decimal.NewFromInt(5),
				"b": decimal.NewFromInt(6),
			}

			parser := NewParser(vars, math_config.NewDefaultCalcConfig())
			lexer := NewLexer(math_config.NewDefaultCalcConfig())
			parser.tokens = lexer.Lex(tt.expression)

			// 跳过函数名和左括号
			for parser.pos < len(parser.tokens) && parser.tokens[parser.pos].Type != TokenLParen {
				parser.pos++
			}
			if parser.pos < len(parser.tokens) {
				parser.pos++ // 跳过左括号
			}

			args, err := parser.parseArguments()

			if (err != nil) != tt.wantErr {
				t.Errorf("parseArguments() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(args) != tt.wantCount {
				t.Errorf("parseArguments() got %d arguments, want %d", len(args), tt.wantCount)
			}
		})
	}
}

// 辅助函数，检查字符串是否包含子串
func containsString(s, substr string) bool {
	return strings.Contains(s, substr)
}
