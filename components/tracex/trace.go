package tracex

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

var (
	//version string
	incrNum uint64
	//pid     = os.Getpid()
)

const TraceIdKey = "trace_id"

type (
	TraceIDCtx struct{}
)

// NewTraceID New tracex id
func NewTraceID() string {
	return fmt.Sprintf("%d%s%d",
		os.Getpid(),
		time.Now().Format("20060102150405999"),
		atomic.AddUint64(&incrNum, 1))
}

func GetTraceIdByContext(ctx context.Context) string {
	v, ok := ctx.Value(TraceIdKey).(string)
	if ok {
		return v
	}

	return ""
}

func MustGetTraceIdByContext(ctx context.Context) string {
	if v := GetTraceIdByContext(ctx); v != "" {
		return v
	}

	return NewTraceID()
}
