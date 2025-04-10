package croe

import (
	"testing"
)

func TestSetLexerCacheCapacity(t *testing.T) {
	// 保存原始值
	originalCapacity := globalLexerCacheCapacity

	// 测试设置有效值
	SetLexerCacheCapacity(2000)
	if globalLexerCacheCapacity != 2000 {
		t.Errorf("SetLexerCacheCapacity(2000) resulted in globalLexerCacheCapacity = %v, want %v", globalLexerCacheCapacity, 2000)
	}

	// 测试设置无效值
	SetLexerCacheCapacity(0)
	if globalLexerCacheCapacity != 2000 {
		t.Errorf("SetLexerCacheCapacity(0) changed globalLexerCacheCapacity to %v, should not change", globalLexerCacheCapacity)
	}

	SetLexerCacheCapacity(-100)
	if globalLexerCacheCapacity != 2000 {
		t.Errorf("SetLexerCacheCapacity(-100) changed globalLexerCacheCapacity to %v, should not change", globalLexerCacheCapacity)
	}

	// 恢复原始值
	SetLexerCacheCapacity(originalCapacity)
}

func TestSetExprCacheCapacity(t *testing.T) {
	// 保存原始值
	originalCapacity := globalExprCacheCapacity

	// 测试设置有效值
	SetExprCacheCapacity(3000)
	if globalExprCacheCapacity != 3000 {
		t.Errorf("SetExprCacheCapacity(3000) resulted in globalExprCacheCapacity = %v, want %v", globalExprCacheCapacity, 3000)
	}

	// 测试设置无效值
	SetExprCacheCapacity(0)
	if globalExprCacheCapacity != 3000 {
		t.Errorf("SetExprCacheCapacity(0) changed globalExprCacheCapacity to %v, should not change", globalExprCacheCapacity)
	}

	SetExprCacheCapacity(-100)
	if globalExprCacheCapacity != 3000 {
		t.Errorf("SetExprCacheCapacity(-100) changed globalExprCacheCapacity to %v, should not change", globalExprCacheCapacity)
	}

	// 恢复原始值
	SetExprCacheCapacity(originalCapacity)
}
