package croe

import (
	"sync"
)

// 词法分析缓存
type LexerCache struct {
	sync.RWMutex
	cache    map[string][]Token
	capacity int
}

// 创建新的词法分析器缓存
func NewLexerCache(capacity int) *LexerCache {
	return &LexerCache{
		cache:    make(map[string][]Token),
		capacity: capacity,
	}
}

// 全局词法分析器缓存
var globalLexerCache = NewLexerCache(globalLexerCacheCapacity)

// ResetLexerCache 重置词法分析器缓存
func ResetLexerCache() {
	globalLexerCache = NewLexerCache(globalLexerCacheCapacity)
}
