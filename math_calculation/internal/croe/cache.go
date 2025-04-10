package croe

import (
	"container/list"
	"sync"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_node"
)

// LRUCacheItem LRU缓存项
type LRUCacheItem struct {
	key  string
	node math_node.Node
}

// LRUCache LRU缓存结构体，用于存储已解析的表达式
type LRUCache struct {
	capacity int
	cache    map[string]*list.Element
	lru      *list.List
	mutex    sync.RWMutex
}

// NewLRUCache 创建新的LRU缓存
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// Get 从缓存中获取表达式节点
func (c *LRUCache) Get(key string) (math_node.Node, bool) {
	c.mutex.RLock()
	element, exists := c.cache[key]
	c.mutex.RUnlock()

	if !exists {
		return nil, false
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 移动到队头（最近使用）
	c.lru.MoveToFront(element)
	return element.Value.(LRUCacheItem).node, true
}

// Set 将表达式节点存入缓存
func (c *LRUCache) Set(key string, node math_node.Node) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 如果已存在，更新并移动到队头
	if element, exists := c.cache[key]; exists {
		c.lru.MoveToFront(element)
		element.Value = LRUCacheItem{key: key, node: node}
		return
	}

	// 添加新项到队头
	element := c.lru.PushFront(LRUCacheItem{key: key, node: node})
	c.cache[key] = element

	// 如果超过容量，移除最不常用的项
	if c.lru.Len() > c.capacity {
		c.removeOldest()
	}
}

// removeOldest 移除最不常用的项
func (c *LRUCache) removeOldest() {
	element := c.lru.Back()
	if element != nil {
		c.lru.Remove(element)
		item := element.Value.(LRUCacheItem)
		delete(c.cache, item.key)
	}
}

// 全局缓存实例
var globalCache = NewLRUCache(globalExprCacheCapacity)

// ResetExprCache 重置表达式缓存
func ResetExprCache() {
	globalCache = NewLRUCache(globalExprCacheCapacity)
}
