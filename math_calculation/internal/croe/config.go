package croe

// 全局缓存容量配置
var (
	globalLexerCacheCapacity = 1000 // 词法分析器缓存容量
	globalExprCacheCapacity  = 1000 // 表达式缓存容量
)

// SetLexerCacheCapacity 设置词法分析器缓存容量
func SetLexerCacheCapacity(capacity int) {
	if capacity > 0 {
		globalLexerCacheCapacity = capacity
		// 重新初始化词法分析器缓存
		ResetLexerCache()
	}
}

// SetExprCacheCapacity 设置表达式缓存容量
func SetExprCacheCapacity(capacity int) {
	if capacity > 0 {
		globalExprCacheCapacity = capacity
		// 重新初始化表达式缓存
		ResetExprCache()
		// 重新初始化分片缓存
		ResetShardedCache()
	}
}
