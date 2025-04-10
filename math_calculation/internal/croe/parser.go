package croe

import (
	"fmt"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// Parser 解析器结构体
type Parser struct {
	tokens     []Token                    // 标记列表
	pos        int                        // 当前位置
	vars       map[string]decimal.Decimal // 变量映射
	expression string                     // 原始表达式
	config     *math_config.CalcConfig    // 计算配置
}

// NewParser 创建新的解析器
func NewParser(vars map[string]decimal.Decimal, config *math_config.CalcConfig) *Parser {
	// 空指针检查
	if config == nil {
		config = math_config.NewDefaultCalcConfig()
	}

	return &Parser{
		vars:   vars,
		config: config,
	}
}

// Parse 解析表达式
func (p *Parser) Parse(expression string) (math_node.Node, error) {
	// 检查表达式是否为空
	if len(expression) == 0 {
		return nil, &internal.ParseError{
			Pos:     0,
			Message: "空表达式",
			Cause:   internal.ErrInvalidExpression,
		}
	}

	// 如果表达式过长，可能是恶意输入，直接拒绝
	if len(expression) > 10000 {
		return nil, &internal.ParseError{
			Pos:     0,
			Message: "表达式过长",
			Cause:   internal.ErrInvalidExpression,
		}
	}

	// 空指针检查
	if p.config == nil {
		p.config = math_config.NewDefaultCalcConfig()
	}

	// 如果启用了缓存，尝试从缓存中获取解析树
	if p.config.UseExprCache {
		if node, ok := globalShardedCache.Get(expression); ok {
			return node, nil
		}
	}

	// 设置解析器状态
	p.expression = expression

	// 创建词法分析器并分析表达式
	lexer := NewLexer(p.config)
	p.tokens = lexer.Lex(expression)
	p.pos = 0

	// 检查标记数量，防止恶意输入
	if len(p.tokens) > 1000 {
		return nil, &internal.ParseError{
			Pos:     0,
			Message: "表达式复杂度过高",
			Cause:   internal.ErrInvalidExpression,
		}
	}

	// 解析表达式
	node, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	// 检查是否有未消耗的标记
	if p.pos < len(p.tokens) {
		token := p.tokens[p.pos]
		return nil, &internal.ParseError{
			Pos:     token.Pos,
			Message: fmt.Sprintf("意外的标记: %s", token.Value),
			Cause:   internal.ErrInvalidExpression,
		}
	}

	// 如果启用了缓存，将解析树存入缓存
	if p.config != nil && p.config.UseExprCache {
		globalShardedCache.Set(expression, node)
	}

	return node, nil
}

// parseExpr 解析表达式（加减运算）
func (p *Parser) parseExpr() (math_node.Node, error) {
	// 解析第一个项
	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	// 循环处理加减运算
	for p.pos < len(p.tokens) {
		token := p.tokens[p.pos]
		switch token.Type {
		case TokenPlus:
			p.pos++
			right, err := p.parseTerm()
			if err != nil {
				return nil, err
			}
			// 使用对象池获取BinaryOpNode
			node := GetBinaryOpNode()
			node.Left = left
			node.Operator = "+"
			node.Right = right
			node.Pos = token.Pos
			left = node
		case TokenMinus:
			p.pos++
			right, err := p.parseTerm()
			if err != nil {
				return nil, err
			}
			// 使用对象池获取BinaryOpNode
			node := GetBinaryOpNode()
			node.Left = left
			node.Operator = "-"
			node.Right = right
			node.Pos = token.Pos
			left = node
		default:
			return left, nil
		}
	}
	return left, nil
}

// parseTerm 解析项（乘除运算）
func (p *Parser) parseTerm() (math_node.Node, error) {
	// 解析第一个因子
	left, err := p.parsePower()
	if err != nil {
		return nil, err
	}

	// 循环处理乘除运算
	for p.pos < len(p.tokens) {
		token := p.tokens[p.pos]
		switch token.Type {
		case TokenAsterisk:
			p.pos++
			right, err := p.parsePower()
			if err != nil {
				return nil, err
			}
			// 使用对象池获取BinaryOpNode
			node := GetBinaryOpNode()
			node.Left = left
			node.Operator = "*"
			node.Right = right
			node.Pos = token.Pos
			left = node
		case TokenSlash:
			p.pos++
			right, err := p.parsePower()
			if err != nil {
				return nil, err
			}
			// 使用对象池获取BinaryOpNode
			node := GetBinaryOpNode()
			node.Left = left
			node.Operator = "/"
			node.Right = right
			node.Pos = token.Pos
			left = node
		default:
			return left, nil
		}
	}
	return left, nil
}

// parsePower 解析幂运算
func (p *Parser) parsePower() (math_node.Node, error) {
	// 解析第一个因子
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}

	// 循环处理幂运算
	for p.pos < len(p.tokens) {
		token := p.tokens[p.pos]
		switch token.Type {
		case TokenCaret:
			p.pos++
			right, err := p.parseFactor()
			if err != nil {
				return nil, err
			}
			// 使用对象池获取BinaryOpNode
			node := GetBinaryOpNode()
			node.Left = left
			node.Operator = "^"
			node.Right = right
			node.Pos = token.Pos
			left = node
		default:
			return left, nil
		}
	}
	return left, nil
}

// parseFactor 解析因子（数字、变量、括号表达式、函数调用等）
func (p *Parser) parseFactor() (math_node.Node, error) {
	// 检查是否到达表达式结尾
	if p.pos >= len(p.tokens) {
		return nil, &internal.ParseError{
			Pos:     len(p.expression),
			Message: "表达式意外结束",
			Cause:   internal.ErrInvalidExpression,
		}
	}

	// 获取当前标记
	token := p.tokens[p.pos]
	p.pos++

	// 根据标记类型处理
	switch token.Type {
	case TokenNumber:
		// 解析数字
		val, err := decimal.NewFromString(token.Value)
		if err != nil {
			return nil, &internal.ParseError{
				Pos:     token.Pos,
				Message: fmt.Sprintf("无效的数字: %s", token.Value),
				Cause:   err,
			}
		}
		// 使用对象池获取NumberNode
		node := GetNumberNode()
		node.Value = val
		return node, nil
	case TokenVariable:
		// 解析变量，使用对象池
		node := GetVariableNode()
		node.VarName = token.Value
		node.Pos = token.Pos
		return node, nil
	case TokenPlus, TokenMinus:
		// 解析一元运算符
		operand, err := p.parseFactor()
		if err != nil {
			return nil, err
		}
		// 使用对象池获取UnaryOpNode
		node := GetUnaryOpNode()
		node.Operator = token.Value
		node.Operand = operand
		node.Pos = token.Pos
		return node, nil
	case TokenLParen:
		// 解析括号表达式
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		// 检查右括号
		if p.pos >= len(p.tokens) || p.tokens[p.pos].Type != TokenRParen {
			return nil, &internal.ParseError{
				Pos:     token.Pos,
				Message: "缺少右括号",
				Cause:   internal.ErrInvalidExpression,
			}
		}
		p.pos++
		return expr, nil
	case TokenFunc:
		// 解析函数调用
		funcName := token.Value
		funcPos := token.Pos

		// 检查左括号
		if p.pos >= len(p.tokens) || p.tokens[p.pos].Type != TokenLParen {
			return nil, &internal.ParseError{
				Pos:     funcPos,
				Message: fmt.Sprintf("函数 %s 后缺少左括号", funcName),
				Cause:   internal.ErrInvalidExpression,
			}
		}
		p.pos++

		// 解析函数参数
		args, err := p.parseArguments()
		if err != nil {
			return nil, err
		}

		// 检查右括号
		if p.pos >= len(p.tokens) || p.tokens[p.pos].Type != TokenRParen {
			return nil, &internal.ParseError{
				Pos:     funcPos,
				Message: fmt.Sprintf("函数 %s 缺少右括号", funcName),
				Cause:   internal.ErrInvalidExpression,
			}
		}
		p.pos++

		// 使用对象池获取FunctionNode
		node := GetFunctionNode()
		node.FuncName = funcName
		node.Args = args
		node.Pos = funcPos
		return node, nil
	default:
		// 处理意外的标记
		return nil, &internal.ParseError{
			Pos:     token.Pos,
			Message: fmt.Sprintf("意外的标记: %s", token.Value),
			Cause:   internal.ErrInvalidExpression,
		}
	}
}

// parseArguments 解析函数参数列表
func (p *Parser) parseArguments() ([]math_node.Node, error) {
	// 预分配切片容量，减少动态扩容
	args := make([]math_node.Node, 0, 4) // 大多数函数参数少于4个

	// 如果不是右括号，则解析参数
	if p.pos < len(p.tokens) && p.tokens[p.pos].Type != TokenRParen {
		// 解析第一个参数
		expr, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		args = append(args, expr)

		// 循环解析逗号分隔的参数
		for p.pos < len(p.tokens) && p.tokens[p.pos].Type == TokenComma {
			p.pos++
			expr, err = p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, expr)
		}
	}

	return args, nil
}
