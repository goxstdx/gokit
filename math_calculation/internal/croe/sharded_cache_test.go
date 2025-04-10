package croe

import (
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
)

func TestShardedLRUCache(t *testing.T) {
	// 创建分片缓存
	cache := NewShardedLRUCache(16)

	// 创建测试节点
	node1 := &math_node.NumberNode{Value: decimal.NewFromInt(1)}
	node2 := &math_node.NumberNode{Value: decimal.NewFromInt(2)}

	// 测试设置和获取
	cache.Set("key1", node1)

	got, ok := cache.Get("key1")
	if !ok {
		t.Errorf("ShardedLRUCache.Get() ok = %v, want %v", ok, true)
	}

	if got.(*math_node.NumberNode).Value.IntPart() != 1 {
		t.Errorf("ShardedLRUCache.Get() = %v, want %v", got.(*math_node.NumberNode).Value.IntPart(), 1)
	}

	// 测试不存在的键
	_, ok = cache.Get("key2")
	if ok {
		t.Errorf("ShardedLRUCache.Get() ok = %v, want %v", ok, false)
	}

	// 测试多个键
	cache.Set("key2", node2)

	got, ok = cache.Get("key2")
	if !ok {
		t.Errorf("ShardedLRUCache.Get() ok = %v, want %v", ok, true)
	}

	if got.(*math_node.NumberNode).Value.IntPart() != 2 {
		t.Errorf("ShardedLRUCache.Get() = %v, want %v", got.(*math_node.NumberNode).Value.IntPart(), 2)
	}

	// 测试更新
	node1Updated := &math_node.NumberNode{Value: decimal.NewFromInt(10)}
	cache.Set("key1", node1Updated)

	got, ok = cache.Get("key1")
	if !ok {
		t.Errorf("ShardedLRUCache.Get() ok = %v, want %v", ok, true)
	}

	if got.(*math_node.NumberNode).Value.IntPart() != 10 {
		t.Errorf("ShardedLRUCache.Get() = %v, want %v", got.(*math_node.NumberNode).Value.IntPart(), 10)
	}
}

func TestResetShardedCache(t *testing.T) {
	// 保存原始缓存
	originalCache := globalShardedCache

	// 设置测试缓存
	testCache := NewShardedLRUCache(10)
	testCache.Set("testKey", &math_node.NumberNode{Value: decimal.NewFromInt(1)})
	globalShardedCache = testCache

	// 重置缓存
	ResetShardedCache()

	// 检查缓存是否被重置
	_, ok := globalShardedCache.Get("testKey")
	if ok {
		t.Errorf("ResetShardedCache() did not reset the cache")
	}

	// 恢复原始缓存
	globalShardedCache = originalCache
}
