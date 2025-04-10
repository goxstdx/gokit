package math_config

import (
	"time"
)

// PrecisionMode 精度设置方式
type PrecisionMode int

const (
	// RoundPrecision 四舍五入
	RoundPrecision PrecisionMode = iota
	// CeilPrecision 向上取整
	CeilPrecision
	// FloorPrecision 向下取整
	FloorPrecision
)

// CalcConfig 计算配置
type CalcConfig struct {
	MaxRecursionDepth int           // 最大递归深度
	Timeout           time.Duration // 执行超时时间
	Precision         int32         // 计算精度
	PrecisionMode     PrecisionMode // 精度设置方式（四舍五入、向上取整、向下取整）
	// 每一步都控制，可以最大限度的控制溢出问题
	// 1/3 + 1/3 + 1/3，ture = 0.9999999999，false = 1
	ApplyPrecisionEachStep bool      // 是否在每一步应用精度控制
	UseExprCache           bool      // 是否使用表达式解析树缓存
	UseLexerCache          bool      // 是否使用词法分析器缓存
	DebugMode              DebugMode // debug 模式
}

// DefaultConfig 默认配置
var DefaultConfig = NewDefaultCalcConfig()

func NewDefaultCalcConfig() *CalcConfig {
	// 返回默认配置的副本，避免修改全局默认配置
	return &CalcConfig{
		MaxRecursionDepth:      100,
		Timeout:                time.Second * 5,
		Precision:              10,
		PrecisionMode:          RoundPrecision, // 默认使用四舍五入
		ApplyPrecisionEachStep: true,
		UseExprCache:           true,
		UseLexerCache:          true,
		DebugMode:              DebugNone,
	}
}
