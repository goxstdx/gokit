package croe

import (
	"regexp"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_utils"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// TokenType 标记类型
type TokenType int

// 标记类型定义
const (
	TokenError TokenType = iota
	TokenNumber
	TokenVariable
	TokenFunc
	TokenPlus
	TokenMinus
	TokenAsterisk
	TokenSlash
	TokenLParen
	TokenRParen
	TokenCaret
	TokenComma
)

// Token 标记结构体
type Token struct {
	Type  TokenType // 标记类型
	Value string    // 标记值
	Pos   int       // 标记在表达式中的位置
}

// 正则表达式定义
var (
	numberRegex   = regexp.MustCompile(`^-?\d+(\.\d*)?$`)
	variableRegex = regexp.MustCompile(`^[a-zA-Z_]\w*$`)
	// 匹配后跟开括号的单词
	functionRegex = regexp.MustCompile(`^[a-zA-Z_]\w*\(`)
	// 分离数字、变量、运算符和括号的正则表达式
	tokenRegex = regexp.MustCompile(`\s*(-?\d+(\.\d*)?|[a-zA-Z_]\w*|[-+*/^(),])`)
)

// Lexer 词法分析器结构体
type Lexer struct {
	cache  *LexerCache
	config *math_config.CalcConfig
}

// NewLexer 创建新的词法分析器
func NewLexer(config *math_config.CalcConfig) *Lexer {
	// 空指针检查
	if config == nil {
		config = math_config.NewDefaultCalcConfig()
	}

	return &Lexer{
		cache:  globalLexerCache,
		config: config,
	}
}

// Lex 词法分析方法，将输入字符串分解为标记
func (l *Lexer) Lex(input string) []Token {
	// 空指针检查
	if l.config == nil {
		l.config = math_config.NewDefaultCalcConfig()
	}

	// 如果启用缓存，尝试从缓存中获取
	if l.config.UseLexerCache {
		l.cache.RLock()
		if tokens, ok := l.cache.cache[input]; ok {
			tokensCopy := make([]Token, len(tokens))
			copy(tokensCopy, tokens)
			l.cache.RUnlock()
			return tokensCopy
		}
		l.cache.RUnlock()
	}

	// 预分配标记切片，减少动态扩容
	tokens := make([]Token, 0, len(input)/4) // 假设平均每4个字符产生一个标记
	pos := 0

	// 使用字节切片进行处理，避免字符串操作
	bytes := []byte(input)
	len_bytes := len(bytes)

	for pos < len_bytes {
		// 跳过空白字符
		if bytes[pos] == ' ' {
			pos++
			continue
		}

		// 检查函数名或变量名（字母或下划线开头）
		if pos < len_bytes && (math_utils.IsAlpha(bytes[pos]) || bytes[pos] == '_') {
			start := pos
			// 读取标识符
			for pos < len_bytes && math_utils.IsIdentifierChar(bytes[pos]) {
				pos++
			}

			// 跳过空白字符
			tempPos := pos
			for tempPos < len_bytes && bytes[tempPos] == ' ' {
				tempPos++
			}

			// 检查是否是函数调用
			if tempPos < len_bytes && bytes[tempPos] == '(' {
				// 使用对象池获取Token对象
				token := GetToken()
				token.Type = TokenFunc
				token.Value = string(bytes[start:pos])
				token.Pos = start
				tokens = append(tokens, *token)
				PutToken(token) // 归还到对象池
				pos = tempPos   // 更新位置
				continue
			} else {
				// 这是一个变量
				token := GetToken()
				token.Type = TokenVariable
				token.Value = string(bytes[start:pos])
				token.Pos = start
				tokens = append(tokens, *token)
				PutToken(token)
				continue
			}
		}

		// 检查数字
		if math_utils.IsDigit(bytes[pos]) || (bytes[pos] == '-' && pos+1 < len_bytes && math_utils.IsDigit(bytes[pos+1])) {
			start := pos
			// 处理负号
			if bytes[pos] == '-' {
				pos++
			}

			// 读取整数部分
			for pos < len_bytes && math_utils.IsDigit(bytes[pos]) {
				pos++
			}

			// 处理小数部分
			if pos < len_bytes && bytes[pos] == '.' {
				pos++
				for pos < len_bytes && math_utils.IsDigit(bytes[pos]) {
					pos++
				}
			}

			token := GetToken()
			token.Type = TokenNumber
			token.Value = string(bytes[start:pos])
			token.Pos = start
			tokens = append(tokens, *token)
			PutToken(token)
			continue
		}

		// 处理运算符和其他符号
		token := GetToken()
		token.Pos = pos

		switch bytes[pos] {
		case '+':
			token.Type = TokenPlus
			token.Value = "+"
		case '-':
			token.Type = TokenMinus
			token.Value = "-"
		case '*':
			token.Type = TokenAsterisk
			token.Value = "*"
		case '/':
			token.Type = TokenSlash
			token.Value = "/"
		case '^':
			token.Type = TokenCaret
			token.Value = "^"
		case '(':
			token.Type = TokenLParen
			token.Value = "("
		case ')':
			token.Type = TokenRParen
			token.Value = ")"
		case ',':
			token.Type = TokenComma
			token.Value = ","
		default:
			// 只取当前字符作为错误标记，而不是尝试读取更多字符
			token.Type = TokenError
			token.Value = string([]byte{bytes[pos]})
		}

		tokens = append(tokens, *token)
		PutToken(token)
		pos++
	}

	// 如果启用缓存，将结果存入缓存
	if l.config != nil && l.config.UseLexerCache && len(input) < 1000 { // 只缓存较短的表达式
		l.cache.Lock()
		if len(l.cache.cache) > l.cache.capacity {
			// 简单的缓存清理策略：当缓存过大时清空
			l.cache.cache = make(map[string][]Token)
		}
		tokensCopy := make([]Token, len(tokens))
		copy(tokensCopy, tokens)
		l.cache.cache[input] = tokensCopy
		l.cache.Unlock()
	}

	return tokens
}
