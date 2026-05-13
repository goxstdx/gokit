package logger_factory

import "context"

type callerSkipContextKey struct{}

// WithCallerSkip 为当前日志调用设置额外 caller skip 层数。
// 最终生效值 = 驱动 base + Config.Caller.Skip + WithCallerSkip(ctx, n) 的 n。
func WithCallerSkip(ctx context.Context, skip int) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if skip < 0 {
		skip = 0
	}
	return context.WithValue(ctx, callerSkipContextKey{}, skip)
}

func callerSkipFromContext(ctx context.Context) int {
	if ctx == nil {
		return 0
	}
	v := ctx.Value(callerSkipContextKey{})
	skip, ok := v.(int)
	if !ok || skip < 0 {
		return 0
	}
	return skip
}
