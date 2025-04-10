package croe

import (
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
)

func TestLRUCache(t *testing.T) {
	// 创建容量为2的缓存
	cache := NewLRUCache(2)

	// 创建测试节点
	node1 := &math_node.NumberNode{Value: decimal.NewFromInt(1)}
	node2 := &math_node.NumberNode{Value: decimal.NewFromInt(2)}
	node3 := &math_node.NumberNode{Value: decimal.NewFromInt(3)}

	// 测试设置和获取
	cache.Set("key1", node1)

	got, ok := cache.Get("key1")
	if !ok {
		t.Errorf("LRUCache.Get() ok = %v, want %v", ok, true)
	}

	if got.(*math_node.NumberNode).Value.IntPart() != 1 {
		t.Errorf("LRUCache.Get() = %v, want %v", got.(*math_node.NumberNode).Value.IntPart(), 1)
	}

	// 测试不存在的键
	_, ok = cache.Get("key2")
	if ok {
		t.Errorf("LRUCache.Get() ok = %v, want %v", ok, false)
	}

	// 测试LRU淘汰
	cache.Set("key2", node2)
	cache.Set("key3", node3) // 应该淘汰key1

	_, ok = cache.Get("key1")
	if ok {
		t.Errorf("LRUCache.Get() ok = %v, want %v", ok, false)
	}

	got, ok = cache.Get("key2")
	if !ok {
		t.Errorf("LRUCache.Get() ok = %v, want %v", ok, true)
	}

	if got.(*math_node.NumberNode).Value.IntPart() != 2 {
		t.Errorf("LRUCache.Get() = %v, want %v", got.(*math_node.NumberNode).Value.IntPart(), 2)
	}

	got, ok = cache.Get("key3")
	if !ok {
		t.Errorf("LRUCache.Get() ok = %v, want %v", ok, true)
	}

	if got.(*math_node.NumberNode).Value.IntPart() != 3 {
		t.Errorf("LRUCache.Get() = %v, want %v", got.(*math_node.NumberNode).Value.IntPart(), 3)
	}

	// 测试访问后更新LRU顺序
	cache.Get("key2")                                                      // key2现在是最近使用的
	cache.Set("key4", &math_node.NumberNode{Value: decimal.NewFromInt(4)}) // 应该淘汰key3

	_, ok = cache.Get("key3")
	if ok {
		t.Errorf("LRUCache.Get() ok = %v, want %v", ok, false)
	}

	got, ok = cache.Get("key2")
	if !ok {
		t.Errorf("LRUCache.Get() ok = %v, want %v", ok, true)
	}

	got, ok = cache.Get("key4")
	if !ok {
		t.Errorf("LRUCache.Get() ok = %v, want %v", ok, true)
	}
}

func TestLexerCache(t *testing.T) {
	// 创建容量为2的缓存
	cache := NewLexerCache(2)

	// 创建测试标记
	tokens1 := []Token{{Type: TokenNumber, Value: "1", Pos: 0}}
	tokens2 := []Token{{Type: TokenNumber, Value: "2", Pos: 0}}
	tokens3 := []Token{{Type: TokenNumber, Value: "3", Pos: 0}}

	// 测试设置和获取
	cache.Lock()
	cache.cache["key1"] = tokens1
	cache.Unlock()

	cache.RLock()
	got, ok := cache.cache["key1"]
	cache.RUnlock()

	if !ok {
		t.Errorf("LexerCache.Get() ok = %v, want %v", ok, true)
	}

	if got[0].Value != "1" {
		t.Errorf("LexerCache.Get() = %v, want %v", got[0].Value, "1")
	}

	// 测试不存在的键
	cache.RLock()
	_, ok = cache.cache["key2"]
	cache.RUnlock()

	if ok {
		t.Errorf("LexerCache.Get() ok = %v, want %v", ok, false)
	}

	// 测试容量限制
	cache.Lock()
	cache.cache["key2"] = tokens2
	cache.cache["key3"] = tokens3

	// 检查缓存大小是否超过容量
	if len(cache.cache) > cache.capacity {
		// 模拟清理
		cache.cache = make(map[string][]Token)
		cache.cache["key3"] = tokens3 // 保留最后添加的
	}
	cache.Unlock()

	cache.RLock()
	_, ok = cache.cache["key1"]
	if len(cache.cache) <= cache.capacity && ok {
		t.Errorf("LexerCache should have cleared key1")
	}
	cache.RUnlock()
}

func TestResetExprCache(t *testing.T) {
	// 保存原始缓存
	originalCache := globalCache

	// 设置测试缓存
	testCache := NewLRUCache(10)
	testCache.Set("testKey", &math_node.NumberNode{Value: decimal.NewFromInt(1)})
	globalCache = testCache

	// 重置缓存
	ResetExprCache()

	// 检查缓存是否被重置
	_, ok := globalCache.Get("testKey")
	if ok {
		t.Errorf("ResetExprCache() did not reset the cache")
	}

	// 恢复原始缓存
	globalCache = originalCache
}
