package croe

import (
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
)

func TestTokenPool(t *testing.T) {
	// 获取标记
	token := GetToken()

	// 设置标记属性
	token.Type = TokenNumber
	token.Value = "123"
	token.Pos = 10

	// 归还标记
	PutToken(token)

	// 再次获取标记，应该是重置后的
	token = GetToken()

	// 检查标记是否被重置
	if token.Type != TokenError {
		t.Errorf("PutToken() did not reset token.Type, got %v, want %v", token.Type, TokenError)
	}

	if token.Value != "" {
		t.Errorf("PutToken() did not reset token.Value, got %v, want %v", token.Value, "")
	}

	if token.Pos != 0 {
		t.Errorf("PutToken() did not reset token.Pos, got %v, want %v", token.Pos, 0)
	}
}

func TestNodePools(t *testing.T) {
	// 测试NumberNode池
	t.Run("NumberNodePool", func(t *testing.T) {
		// 获取节点
		node := GetNumberNode()

		// 设置节点属性
		node.Value = decimal.NewFromInt(123)

		// 归还节点
		PutNumberNode(node)

		// 再次获取节点，应该是重置后的
		node = GetNumberNode()

		// 检查节点是否被重置
		if !node.Value.Equal(decimal.Zero) {
			t.Errorf("PutNumberNode() did not reset node.Value, got %v, want %v", node.Value, decimal.Zero)
		}
	})

	// 测试VariableNode池
	t.Run("VariableNodePool", func(t *testing.T) {
		// 获取节点
		node := GetVariableNode()

		// 设置节点属性
		node.VarName = "x"
		node.Pos = 10

		// 归还节点
		PutVariableNode(node)

		// 再次获取节点，应该是重置后的
		node = GetVariableNode()

		// 检查节点是否被重置
		if node.VarName != "" {
			t.Errorf("PutVariableNode() did not reset node.VarName, got %v, want %v", node.VarName, "")
		}

		if node.Pos != 0 {
			t.Errorf("PutVariableNode() did not reset node.Pos, got %v, want %v", node.Pos, 0)
		}
	})

	// 测试BinaryOpNode池
	t.Run("BinaryOpNodePool", func(t *testing.T) {
		// 获取节点
		node := GetBinaryOpNode()

		// 设置节点属性
		node.Left = &math_node.NumberNode{Value: decimal.NewFromInt(1)}
		node.Right = &math_node.NumberNode{Value: decimal.NewFromInt(2)}
		node.Operator = "+"
		node.Pos = 10

		// 归还节点
		PutBinaryOpNode(node)

		// 再次获取节点，应该是重置后的
		node = GetBinaryOpNode()

		// 检查节点是否被重置
		if node.Left != nil {
			t.Errorf("PutBinaryOpNode() did not reset node.Left, got %v, want %v", node.Left, nil)
		}

		if node.Right != nil {
			t.Errorf("PutBinaryOpNode() did not reset node.Right, got %v, want %v", node.Right, nil)
		}

		if node.Operator != "" {
			t.Errorf("PutBinaryOpNode() did not reset node.Operator, got %v, want %v", node.Operator, "")
		}

		if node.Pos != 0 {
			t.Errorf("PutBinaryOpNode() did not reset node.Pos, got %v, want %v", node.Pos, 0)
		}
	})

	// 测试UnaryOpNode池
	t.Run("UnaryOpNodePool", func(t *testing.T) {
		// 获取节点
		node := GetUnaryOpNode()

		// 设置节点属性
		node.Operand = &math_node.NumberNode{Value: decimal.NewFromInt(1)}
		node.Operator = "-"
		node.Pos = 10

		// 归还节点
		PutUnaryOpNode(node)

		// 再次获取节点，应该是重置后的
		node = GetUnaryOpNode()

		// 检查节点是否被重置
		if node.Operand != nil {
			t.Errorf("PutUnaryOpNode() did not reset node.Operand, got %v, want %v", node.Operand, nil)
		}

		if node.Operator != "" {
			t.Errorf("PutUnaryOpNode() did not reset node.Operator, got %v, want %v", node.Operator, "")
		}

		if node.Pos != 0 {
			t.Errorf("PutUnaryOpNode() did not reset node.Pos, got %v, want %v", node.Pos, 0)
		}
	})

	// 测试FunctionNode池
	t.Run("FunctionNodePool", func(t *testing.T) {
		// 获取节点
		node := GetFunctionNode()

		// 设置节点属性
		node.FuncName = "sqrt"
		node.Args = []math_node.Node{&math_node.NumberNode{Value: decimal.NewFromInt(25)}}
		node.Pos = 10

		// 归还节点
		PutFunctionNode(node)

		// 再次获取节点，应该是重置后的
		node = GetFunctionNode()

		// 检查节点是否被重置
		if node.FuncName != "" {
			t.Errorf("PutFunctionNode() did not reset node.FuncName, got %v, want %v", node.FuncName, "")
		}

		if node.Args != nil {
			t.Errorf("PutFunctionNode() did not reset node.Args, got %v, want %v", node.Args, nil)
		}

		if node.Pos != 0 {
			t.Errorf("PutFunctionNode() did not reset node.Pos, got %v, want %v", node.Pos, 0)
		}
	})
}

func BenchmarkWithPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		token := GetToken()
		token.Type = TokenNumber
		token.Value = "123.45"
		token.Pos = 0
		PutToken(token)
	}
}

func BenchmarkWithoutPool(b *testing.B) {
	for i := 0; i < b.N; i++ {
		token := &Token{
			Type:  TokenNumber,
			Value: "123.45",
			Pos:   0,
		}
		_ = token
	}
}
