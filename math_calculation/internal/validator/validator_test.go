package validator

import (
	"testing"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
)

func TestValidateExpression(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		options    ValidationOptions
		wantErr    bool
	}{
		{
			name:       "有效表达式",
			expression: "x + y * z",
			options:    DefaultValidationOptions,
			wantErr:    false,
		},
		{
			name:       "表达式过长",
			expression: "x + y + z + a + b + c + d + e + f + g + h + i + j + k + l + m + n + o + p + q + r + s + t + u + v + w + x + y + z",
			options:    ValidationOptions{MaxExpressionLength: 10},
			wantErr:    true,
		},
		{
			name:       "嵌套括号过多",
			expression: "((((((x + y))))))",
			options:    ValidationOptions{MaxNestedParentheses: 3, MaxExpressionLength: 1000},
			wantErr:    true,
		},
		{
			name:       "括号不匹配",
			expression: "(x + y",
			options:    DefaultValidationOptions,
			wantErr:    true,
		},
		{
			name:       "函数参数过多",
			expression: "func(a, b, c, d, e)",
			options:    ValidationOptions{MaxFunctionArguments: 3, MaxExpressionLength: 1000},
			wantErr:    true,
		},
		{
			name:       "禁止的函数",
			expression: "sin(x) + cos(y)",
			options:    ValidationOptions{DisallowedFunctions: []string{"sin", "cos"}, MaxExpressionLength: 1000},
			wantErr:    true,
		},
		{
			name:       "不在允许列表中的函数",
			expression: "sin(x) + tan(y)",
			options:    ValidationOptions{AllowedFunctions: []string{"sin", "cos"}, MaxExpressionLength: 1000},
			wantErr:    true,
		},
		{
			name:       "不允许变量",
			expression: "x + y",
			options:    ValidationOptions{AllowVariables: false, MaxExpressionLength: 1000},
			wantErr:    true,
		},
		{
			name:       "变量名过长",
			expression: "verylongvariablename + y",
			options:    ValidationOptions{MaxVariableNameLength: 10, MaxExpressionLength: 1000},
			wantErr:    true,
		},
		{
			name:       "数字过长",
			expression: "123456789012345678901234567890 + y",
			options:    ValidationOptions{MaxNumberLength: 10, MaxExpressionLength: 1000},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateExpression(tt.expression, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateExpression() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateAndSanitizeExpression(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		options    ValidationOptions
		want       string
		wantErr    bool
	}{
		{
			name:       "有效表达式",
			expression: "x + y * z",
			options:    DefaultValidationOptions,
			want:       "x + y * z",
			wantErr:    false,
		},
		{
			name:       "带空格的表达式",
			expression: "  x  +  y  *  z  ",
			options:    DefaultValidationOptions,
			want:       "x  +  y  *  z",
			wantErr:    false,
		},
		{
			name:       "无效表达式",
			expression: "((x + y",
			options:    DefaultValidationOptions,
			want:       "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ValidateAndSanitizeExpression(tt.expression, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateAndSanitizeExpression() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ValidateAndSanitizeExpression() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidatorIsIdentifierChar(t *testing.T) {
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

func TestValidatorIsAlpha(t *testing.T) {
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

func TestValidatorIsDigit(t *testing.T) {
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

// 新增测试

func TestValidationError_Error(t *testing.T) {
	tests := []struct {
		name    string
		err     ValidationError
		wantMsg string
	}{
		{
			name: "基本错误",
			err: ValidationError{
				Message: "测试错误",
				Pos:     5,
			},
			wantMsg: "位置 5: 测试错误",
		},
		{
			name: "位置为0",
			err: ValidationError{
				Message: "表达式长度超过限制",
				Pos:     0,
			},
			wantMsg: "位置 0: 表达式长度超过限制",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.wantMsg {
				t.Errorf("ValidationError.Error() = %v, want %v", got, tt.wantMsg)
			}
		})
	}
}

func TestValidateFunctions(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		options    ValidationOptions
		wantErr    bool
	}{
		{
			name:       "无函数限制",
			expression: "sin(x) + cos(y) + tan(z)",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxFunctionArguments: 1000},
			wantErr:    false,
		},
		{
			name:       "函数参数过多",
			expression: "max(a, b, c, d, e)",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxFunctionArguments: 3},
			wantErr:    true,
		},
		{
			name:       "函数在允许列表中",
			expression: "sin(x) + cos(y)",
			options:    ValidationOptions{MaxExpressionLength: 1000, AllowedFunctions: []string{"sin", "cos"}, MaxFunctionArguments: 10},
			wantErr:    false,
		},
		{
			name:       "函数不在允许列表中",
			expression: "sin(x) + tan(y)",
			options:    ValidationOptions{MaxExpressionLength: 1000, AllowedFunctions: []string{"sin", "cos"}, MaxFunctionArguments: 10},
			wantErr:    true,
		},
		{
			name:       "函数在禁止列表中",
			expression: "sin(x) + cos(y)",
			options:    ValidationOptions{MaxExpressionLength: 1000, DisallowedFunctions: []string{"cos"}, MaxFunctionArguments: 10},
			wantErr:    true,
		},
		{
			name:       "嵌套函数调用",
			expression: "sin(cos(x))",
			options:    ValidationOptions{MaxExpressionLength: 1000, AllowedFunctions: []string{"sin", "cos"}, MaxFunctionArguments: 10},
			wantErr:    false,
		},
		{
			name:       "复杂表达式中的函数",
			expression: "sin(x) + (cos(y) * tan(z))",
			options:    ValidationOptions{MaxExpressionLength: 1000, DisallowedFunctions: []string{"tan"}, MaxFunctionArguments: 10},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateFunctions(tt.expression, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateFunctions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateNumberLength(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		options    ValidationOptions
		wantErr    bool
	}{
		{
			name:       "正常数字长度",
			expression: "123.45 + 67.89",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxNumberLength: 10},
			wantErr:    false,
		},
		{
			name:       "数字过长",
			expression: "123456789012345 + y",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxNumberLength: 10},
			wantErr:    true,
		},
		{
			name:       "负数长度正常",
			expression: "-123.45 + 67.89",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxNumberLength: 10},
			wantErr:    false,
		},
		{
			name:       "负数过长",
			expression: "-12345678901 + y",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxNumberLength: 10},
			wantErr:    true,
		},
		{
			name:       "小数点计入长度",
			expression: "123.456789 + y",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxNumberLength: 10},
			wantErr:    false,
		},
		{
			name:       "小数点计入长度超限",
			expression: "123.45678901 + y",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxNumberLength: 10},
			wantErr:    true,
		},
		{
			name:       "表达式中多个数字",
			expression: "123 + 456 + 789",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxNumberLength: 3},
			wantErr:    false,
		},
		{
			name:       "表达式中多个数字有超长",
			expression: "123 + 4567 + 789",
			options:    ValidationOptions{MaxExpressionLength: 1000, MaxNumberLength: 3},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNumberLength(tt.expression, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateNumberLength() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSanitizeExpression(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{
			name:       "正常表达式",
			expression: "x + y * z",
			want:       "x + y * z",
		},
		{
			name:       "前后有空格",
			expression: "  x + y * z  ",
			want:       "x + y * z",
		},
		{
			name:       "多余的空格",
			expression: "x  +  y  *  z",
			want:       "x  +  y  *  z",
		},
		{
			name:       "空表达式",
			expression: "",
			want:       "",
		},
		{
			name:       "只有空格",
			expression: "    ",
			want:       "",
		},
		{
			name:       "包含非UTF-8字符",
			expression: "x + y\xFFz",
			want:       "x + yz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeExpression(tt.expression)
			if got != tt.want {
				t.Errorf("sanitizeExpression() = %v, want %v", got, tt.want)
			}
		})
	}
}
