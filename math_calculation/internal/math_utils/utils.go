package math_utils

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal/math_func"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

// 用于存储每个goroutine的递归深度
type depthShard struct {
	sync.Mutex
	depths map[uint64]int
}

var shards [16]*depthShard

func init() {
	for i := 0; i < 16; i++ {
		shards[i] = &depthShard{
			depths: make(map[uint64]int),
		}
	}
}

func GetShard(id uint64) *depthShard {
	return shards[id%16]
}

func GetGoroutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func IncRecursionDepth() int {
	id := GetGoroutineID()
	shard := GetShard(id)
	shard.Lock()
	defer shard.Unlock()
	shard.depths[id]++
	return shard.depths[id]
}

func DecRecursionDepth() {
	id := GetGoroutineID()
	shard := GetShard(id)
	shard.Lock()
	defer shard.Unlock()
	shard.depths[id]--
	if shard.depths[id] <= 0 {
		delete(shard.depths, id)
	}
}

func CheckContext(ctx context.Context, maxDepth int) error {
	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return internal.ErrExecutionTimeout
	default:
	}

	// 检查递归深度
	id := GetGoroutineID()
	shard := GetShard(id)
	shard.Lock()
	depth := shard.depths[id]
	shard.Unlock()

	if depth > maxDepth {
		return internal.ErrMaxRecursionDepth
	}

	return nil
}

// SetPrecision 根据精度设置方式应用精度控制
func SetPrecision(d decimal.Decimal, precision int32, mode math_config.PrecisionMode) decimal.Decimal {
	switch mode {
	case math_config.RoundPrecision:
		return math_func.RoundToPlaces(d, precision)
	case math_config.CeilPrecision:
		return math_func.CeilToPlaces(d, precision)
	case math_config.FloorPrecision:
		return math_func.FloorToPlaces(d, precision)
	default:
		return math_func.RoundToPlaces(d, precision) // 默认使用四舍五入
	}
}

// IsIdentifierChar 判断字符是否是标识符字符
func IsIdentifierChar(c byte) bool {
	return IsAlpha(c) || IsDigit(c) || c == '_'
}

// IsAlpha 判断字符是否是字母
func IsAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// IsDigit 判断字符是否是数字
func IsDigit(c byte) bool {
	return c >= '0' && c <= '9'
}
