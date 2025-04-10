package math_utils

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"

	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/internal"
	"gitlab.ops.haochezhu.club/mutual_public/go-mutual-common/math_calculation/math_config"
)

func TestGetShard(t *testing.T) {
	// 测试不同ID获取的分片
	shard1 := GetShard(1)
	shard2 := GetShard(2)
	shard17 := GetShard(17) // 17 % 16 = 1，应该与shard1相同

	if shard1 == nil {
		t.Errorf("GetShard(1) returned nil")
	}

	if shard2 == nil {
		t.Errorf("GetShard(2) returned nil")
	}

	if shard1 == shard2 {
		t.Errorf("GetShard(1) and GetShard(2) returned the same shard")
	}

	if shard1 != shard17 {
		t.Errorf("GetShard(1) and GetShard(17) should return the same shard")
	}
}

func TestGetGoroutineID(t *testing.T) {
	// 测试获取当前goroutine的ID
	id := GetGoroutineID()
	if id == 0 {
		t.Errorf("GetGoroutineID() returned 0, expected a positive number")
	}

	// 测试在不同goroutine中获取ID
	idChan := make(chan uint64)
	go func() {
		idChan <- GetGoroutineID()
	}()

	goID := <-idChan
	if goID == 0 {
		t.Errorf("GetGoroutineID() in goroutine returned 0, expected a positive number")
	}

	if id == goID {
		t.Errorf("GetGoroutineID() returned the same ID for different goroutines: %d", id)
	}
}

func TestRecursionDepth(t *testing.T) {
	// 测试递归深度计数
	id := GetGoroutineID()
	shard := GetShard(id)

	// 初始深度应该为0或不存在
	shard.Lock()
	initialDepth, exists := shard.depths[id]
	shard.Unlock()

	if exists && initialDepth != 0 {
		t.Errorf("Initial recursion depth should be 0, got %d", initialDepth)
	}

	// 增加深度
	depth1 := IncRecursionDepth()
	if depth1 != 1 {
		t.Errorf("IncRecursionDepth() first call should return 1, got %d", depth1)
	}

	depth2 := IncRecursionDepth()
	if depth2 != 2 {
		t.Errorf("IncRecursionDepth() second call should return 2, got %d", depth2)
	}

	// 检查存储的深度
	shard.Lock()
	storedDepth := shard.depths[id]
	shard.Unlock()

	if storedDepth != 2 {
		t.Errorf("Stored recursion depth should be 2, got %d", storedDepth)
	}

	// 减少深度
	DecRecursionDepth()

	shard.Lock()
	storedDepth = shard.depths[id]
	shard.Unlock()

	if storedDepth != 1 {
		t.Errorf("After DecRecursionDepth(), depth should be 1, got %d", storedDepth)
	}

	// 再次减少深度
	DecRecursionDepth()

	shard.Lock()
	_, exists = shard.depths[id]
	shard.Unlock()

	if exists {
		t.Errorf("After second DecRecursionDepth(), depth entry should be removed")
	}
}

func TestCheckContext(t *testing.T) {
	tests := []struct {
		name     string
		ctx      context.Context
		maxDepth int
		wantErr  error
	}{
		{
			name:     "正常上下文",
			ctx:      context.Background(),
			maxDepth: 10,
			wantErr:  nil,
		},
		{
			name: "已取消上下文",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			maxDepth: 10,
			wantErr:  internal.ErrExecutionTimeout,
		},
		{
			name:     "超过最大递归深度",
			ctx:      context.Background(),
			maxDepth: 0, // 设置为0，这样第一次调用就会超过
			wantErr:  internal.ErrMaxRecursionDepth,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 重置递归深度
			id := GetGoroutineID()
			shard := GetShard(id)
			shard.Lock()
			delete(shard.depths, id)
			shard.Unlock()

			// 如果是测试递归深度超限，先增加递归深度
			if tt.name == "超过最大递归深度" {
				IncRecursionDepth()
				defer DecRecursionDepth()
			}

			err := CheckContext(tt.ctx, tt.maxDepth)
			if err != tt.wantErr {
				t.Errorf("CheckContext() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSetPrecision(t *testing.T) {
	tests := []struct {
		name          string
		value         decimal.Decimal
		precision     int32
		precisionMode math_config.PrecisionMode
		want          decimal.Decimal
	}{
		// 四舍五入模式测试
		{
			name:          "四舍五入-整数",
			value:         decimal.NewFromInt(123),
			precision:     0,
			precisionMode: math_config.RoundPrecision,
			want:          decimal.NewFromInt(123),
		},
		{
			name:          "四舍五入-四舍",
			value:         decimal.NewFromFloat(123.454),
			precision:     2,
			precisionMode: math_config.RoundPrecision,
			want:          decimal.NewFromFloat(123.45),
		},
		{
			name:          "四舍五入-五入",
			value:         decimal.NewFromFloat(123.455),
			precision:     2,
			precisionMode: math_config.RoundPrecision,
			want:          decimal.NewFromFloat(123.46),
		},
		{
			name:          "四舍五入-负数",
			value:         decimal.NewFromFloat(-123.456),
			precision:     2,
			precisionMode: math_config.RoundPrecision,
			want:          decimal.NewFromFloat(-123.46),
		},

		// 向上取整模式测试
		{
			name:          "向上取整-整数",
			value:         decimal.NewFromInt(123),
			precision:     0,
			precisionMode: math_config.CeilPrecision,
			want:          decimal.NewFromInt(123),
		},
		{
			name:          "向上取整-有小数",
			value:         decimal.NewFromFloat(123.451),
			precision:     2,
			precisionMode: math_config.CeilPrecision,
			want:          decimal.NewFromFloat(123.46),
		},
		{
			name:          "向上取整-刚好两位小数",
			value:         decimal.NewFromFloat(123.45),
			precision:     2,
			precisionMode: math_config.CeilPrecision,
			want:          decimal.NewFromFloat(123.45),
		},
		{
			name:          "向上取整-负数",
			value:         decimal.NewFromFloat(-123.456),
			precision:     2,
			precisionMode: math_config.CeilPrecision,
			want:          decimal.NewFromFloat(-123.45),
		},

		// 向下取整模式测试
		{
			name:          "向下取整-整数",
			value:         decimal.NewFromInt(123),
			precision:     0,
			precisionMode: math_config.FloorPrecision,
			want:          decimal.NewFromInt(123),
		},
		{
			name:          "向下取整-有小数",
			value:         decimal.NewFromFloat(123.456),
			precision:     2,
			precisionMode: math_config.FloorPrecision,
			want:          decimal.NewFromFloat(123.45),
		},
		{
			name:          "向下取整-刚好两位小数",
			value:         decimal.NewFromFloat(123.45),
			precision:     2,
			precisionMode: math_config.FloorPrecision,
			want:          decimal.NewFromFloat(123.45),
		},
		{
			name:          "向下取整-负数",
			value:         decimal.NewFromFloat(-123.451),
			precision:     2,
			precisionMode: math_config.FloorPrecision,
			want:          decimal.NewFromFloat(-123.46),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SetPrecision(tt.value, tt.precision, tt.precisionMode)
			if !got.Equal(tt.want) {
				t.Errorf("SetPrecision() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsIdentifierChar(t *testing.T) {
	tests := []struct {
		name string
		c    byte
		want bool
	}{
		{"小写字母", 'a', true},
		{"大写字母", 'Z', true},
		{"数字", '5', true},
		{"下划线", '_', true},
		{"空格", ' ', false},
		{"加号", '+', false},
		{"减号", '-', false},
		{"乘号", '*', false},
		{"除号", '/', false},
		{"左括号", '(', false},
		{"右括号", ')', false},
		{"逗号", ',', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsIdentifierChar(tt.c); got != tt.want {
				t.Errorf("IsIdentifierChar() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsAlpha(t *testing.T) {
	tests := []struct {
		name string
		c    byte
		want bool
	}{
		{"小写字母a", 'a', true},
		{"小写字母z", 'z', true},
		{"大写字母A", 'A', true},
		{"大写字母Z", 'Z', true},
		{"数字", '5', false},
		{"下划线", '_', false},
		{"空格", ' ', false},
		{"加号", '+', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsAlpha(tt.c); got != tt.want {
				t.Errorf("IsAlpha() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsDigit(t *testing.T) {
	tests := []struct {
		name string
		c    byte
		want bool
	}{
		{"数字0", '0', true},
		{"数字9", '9', true},
		{"小写字母", 'a', false},
		{"大写字母", 'A', false},
		{"下划线", '_', false},
		{"空格", ' ', false},
		{"加号", '+', false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDigit(tt.c); got != tt.want {
				t.Errorf("IsDigit() = %v, want %v", got, tt.want)
			}
		})
	}
}
