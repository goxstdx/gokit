// Package mathcalc 提供数学表达式解析和计算功能
package internal

import (
	"errors"
	"fmt"
)

// 错误类型定义
var (
	ErrDivisionByZero      = errors.New("除以零错误")
	ErrUndefinedVariable   = errors.New("未定义的变量")
	ErrUnsupportedOperator = errors.New("不支持的运算符")
	ErrInvalidExpression   = errors.New("无效的表达式")
	ErrInvalidArgument     = errors.New("无效的参数")
	ErrMaxRecursionDepth   = errors.New("超过最大递归深度")
	ErrExecutionTimeout    = errors.New("执行超时")
)

// ParseError 解析错误结构体，包含详细的错误信息
type ParseError struct {
	Pos     int    // 错误位置
	Message string // 错误消息
	Cause   error  // 原始错误
}

// Error 实现error接口
func (e *ParseError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("位置 %d: %s: %v", e.Pos, e.Message, e.Cause)
	}
	return fmt.Sprintf("位置 %d: %s", e.Pos, e.Message)
}
