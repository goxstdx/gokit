package math_config

// DebugMode 调试模式
type DebugMode int

const (
	DebugNone     DebugMode = iota // 不调试
	DebugBasic                     // 基本调试信息
	DebugDetailed                  // 详细调试信息
)
