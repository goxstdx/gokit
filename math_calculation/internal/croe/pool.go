package croe

import (
	"sync"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
)

// TokenPool 标记对象池
type TokenPool struct {
	pool sync.Pool
}

// 全局标记对象池
var globalTokenPool = &TokenPool{
	pool: sync.Pool{
		New: func() interface{} {
			return &Token{}
		},
	},
}

// GetToken 获取标记对象
func GetToken() *Token {
	return globalTokenPool.pool.Get().(*Token)
}

// PutToken 归还标记对象
func PutToken(token *Token) {
	// 重置标记对象
	token.Type = TokenError
	token.Value = ""
	token.Pos = 0
	globalTokenPool.pool.Put(token)
}

// NodePool 节点对象池
type NodePool struct {
	numberPool   sync.Pool
	variablePool sync.Pool
	binaryPool   sync.Pool
	unaryPool    sync.Pool
	functionPool sync.Pool
}

// 全局节点对象池
var globalNodePool = &NodePool{
	numberPool: sync.Pool{
		New: func() interface{} {
			return &math_node.NumberNode{}
		},
	},
	variablePool: sync.Pool{
		New: func() interface{} {
			return &math_node.VariableNode{}
		},
	},
	binaryPool: sync.Pool{
		New: func() interface{} {
			return &math_node.BinaryOpNode{}
		},
	},
	unaryPool: sync.Pool{
		New: func() interface{} {
			return &math_node.UnaryOpNode{}
		},
	},
	functionPool: sync.Pool{
		New: func() interface{} {
			return &math_node.FunctionNode{}
		},
	},
}

// GetNumberNode 获取数字节点
func GetNumberNode() *math_node.NumberNode {
	return globalNodePool.numberPool.Get().(*math_node.NumberNode)
}

// PutNumberNode 归还数字节点
func PutNumberNode(node *math_node.NumberNode) {
	// 重置节点
	node.Value = decimal.Zero
	globalNodePool.numberPool.Put(node)
}

// GetVariableNode 获取变量节点
func GetVariableNode() *math_node.VariableNode {
	return globalNodePool.variablePool.Get().(*math_node.VariableNode)
}

// PutVariableNode 归还变量节点
func PutVariableNode(node *math_node.VariableNode) {
	// 重置节点
	node.VarName = ""
	node.Pos = 0
	globalNodePool.variablePool.Put(node)
}

// GetBinaryOpNode 获取二元运算符节点
func GetBinaryOpNode() *math_node.BinaryOpNode {
	return globalNodePool.binaryPool.Get().(*math_node.BinaryOpNode)
}

// PutBinaryOpNode 归还二元运算符节点
func PutBinaryOpNode(node *math_node.BinaryOpNode) {
	// 重置节点
	node.Left = nil
	node.Right = nil
	node.Operator = ""
	node.Pos = 0
	globalNodePool.binaryPool.Put(node)
}

// GetUnaryOpNode 获取一元运算符节点
func GetUnaryOpNode() *math_node.UnaryOpNode {
	return globalNodePool.unaryPool.Get().(*math_node.UnaryOpNode)
}

// PutUnaryOpNode 归还一元运算符节点
func PutUnaryOpNode(node *math_node.UnaryOpNode) {
	// 重置节点
	node.Operand = nil
	node.Operator = ""
	node.Pos = 0
	globalNodePool.unaryPool.Put(node)
}

// GetFunctionNode 获取函数节点
func GetFunctionNode() *math_node.FunctionNode {
	return globalNodePool.functionPool.Get().(*math_node.FunctionNode)
}

// PutFunctionNode 归还函数节点
func PutFunctionNode(node *math_node.FunctionNode) {
	// 重置节点
	node.FuncName = ""
	node.Args = nil
	node.Pos = 0
	globalNodePool.functionPool.Put(node)
}
