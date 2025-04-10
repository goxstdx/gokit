package croe

import (
	"container/list"
	"sync"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
)

// ShardedLRUCache 分片LRU缓存
type ShardedLRUCache struct {
	shards    [16]*lruCacheShard
	shardMask uint32
}

// lruCacheShard LRU缓存分片
type lruCacheShard struct {
	mutex    sync.RWMutex
	capacity int
	cache    map[string]math_node.Node
	lru      *list.List
	items    map[string]*list.Element
}

// lruCacheItem LRU缓存项
type lruCacheItem struct {
	key  string
	node math_node.Node
}

// NewShardedLRUCache 创建新的分片LRU缓存
func NewShardedLRUCache(capacity int) *ShardedLRUCache {
	shardCapacity := capacity / 16
	if shardCapacity < 1 {
		shardCapacity = 1
	}

	cache := &ShardedLRUCache{
		shardMask: 15, // 16-1
	}

	for i := 0; i < 16; i++ {
		cache.shards[i] = &lruCacheShard{
			capacity: shardCapacity,
			cache:    make(map[string]math_node.Node),
			lru:      list.New(),
			items:    make(map[string]*list.Element),
		}
	}

	return cache
}

// getShard 获取分片
func (c *ShardedLRUCache) getShard(key string) *lruCacheShard {
	// 简单的哈希函数
	h := uint32(0)
	for i := 0; i < len(key); i++ {
		h = h*31 + uint32(key[i])
	}
	return c.shards[h&c.shardMask]
}

// Get 获取缓存项
func (c *ShardedLRUCache) Get(key string) (math_node.Node, bool) {
	shard := c.getShard(key)
	shard.mutex.RLock()
	element, exists := shard.items[key]
	if !exists {
		shard.mutex.RUnlock()
		return nil, false
	}

	// 移动到队头（最近使用）
	shard.mutex.RUnlock()
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	// 再次检查，因为可能在获取读锁和写锁之间被修改
	element, exists = shard.items[key]
	if !exists {
		return nil, false
	}

	shard.lru.MoveToFront(element)
	return element.Value.(lruCacheItem).node, true
}

// Set 设置缓存项
func (c *ShardedLRUCache) Set(key string, node math_node.Node) {
	shard := c.getShard(key)
	shard.mutex.Lock()
	defer shard.mutex.Unlock()

	// 如果已存在，更新并移动到队头
	if element, exists := shard.items[key]; exists {
		shard.lru.MoveToFront(element)
		element.Value = lruCacheItem{key: key, node: node}
		return
	}

	// 添加新项到队头
	element := shard.lru.PushFront(lruCacheItem{key: key, node: node})
	shard.items[key] = element
	shard.cache[key] = node

	// 如果超过容量，移除最不常用的项
	if shard.lru.Len() > shard.capacity {
		c.removeOldest(shard)
	}
}

// removeOldest 移除最不常用的项
func (c *ShardedLRUCache) removeOldest(shard *lruCacheShard) {
	element := shard.lru.Back()
	if element != nil {
		shard.lru.Remove(element)
		item := element.Value.(lruCacheItem)
		delete(shard.items, item.key)
		delete(shard.cache, item.key)
	}
}

// 全局分片缓存实例
var globalShardedCache = NewShardedLRUCache(1000)

// ResetShardedCache 重置分片缓存
func ResetShardedCache() {
	globalShardedCache = NewShardedLRUCache(globalExprCacheCapacity)
}
