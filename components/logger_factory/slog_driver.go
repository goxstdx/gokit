package logger_factory

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"time"

	grom_logger "gorm.io/gorm/logger"
)

type slogLogger struct {
	logger    *slog.Logger
	cfg       Config
	addCaller bool
}

const slogBaseCallerSkip = 3

func newSlogLogger(cfg Config, w io.Writer) *slogLogger {
	opts := &slog.HandlerOptions{
		Level:     toSlogLevel(cfg.Level),
		AddSource: cfg.AddCaller,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().Format(cfg.TimeFormat))
			}
			if a.Key == slog.SourceKey {
				if src, ok := a.Value.Any().(*slog.Source); ok && src != nil {
					// 与 zap 默认 caller 保持一致：统一为 file:line，避免额外函数名解析开销。
					a.Value = slog.StringValue(fmt.Sprintf("%s:%d", src.File, src.Line))
				}
			}
			if cfg.Caller != nil && cfg.Caller.Key != "" && a.Key == slog.SourceKey {
				a.Key = cfg.Caller.Key
			}
			return a
		},
	}

	var handler slog.Handler
	switch cfg.Format {
	case FormatText:
		handler = slog.NewTextHandler(w, opts)
	default:
		handler = slog.NewJSONHandler(w, opts)
	}

	return &slogLogger{
		logger:    slog.New(handler),
		cfg:       cfg,
		addCaller: cfg.AddCaller,
	}
}

// --- 结构化方法 ---

func (s *slogLogger) Debug(msg string, fields ...Field) {
	s.log(context.Background(), LevelDebug, msg, fields)
}

func (s *slogLogger) Info(msg string, fields ...Field) {
	s.log(context.Background(), LevelInfo, msg, fields)
}

func (s *slogLogger) Warn(msg string, fields ...Field) {
	s.log(context.Background(), LevelWarn, msg, fields)
}

func (s *slogLogger) Error(msg string, fields ...Field) {
	s.log(context.Background(), LevelError, msg, fields)
}

func (s *slogLogger) Fatal(msg string, fields ...Field) {
	s.log(context.Background(), LevelError, msg, fields)
	os.Exit(1)
}

// --- 格式化方法 ---

func (s *slogLogger) Debugf(format string, args ...any) {
	s.log(context.Background(), LevelDebug, fmt.Sprintf(format, args...), nil)
}

func (s *slogLogger) Infof(format string, args ...any) {
	s.log(context.Background(), LevelInfo, fmt.Sprintf(format, args...), nil)
}

func (s *slogLogger) Warnf(format string, args ...any) {
	s.log(context.Background(), LevelWarn, fmt.Sprintf(format, args...), nil)
}

func (s *slogLogger) Errorf(format string, args ...any) {
	s.log(context.Background(), LevelError, fmt.Sprintf(format, args...), nil)
}

func (s *slogLogger) Fatalf(format string, args ...any) {
	s.log(context.Background(), LevelError, fmt.Sprintf(format, args...), nil)
	os.Exit(1)
}

// --- Context 方法 ---

func (s *slogLogger) DebugCtx(ctx context.Context, msg string, fields ...Field) {
	s.log(ctx, LevelDebug, msg, fields)
}

func (s *slogLogger) InfoCtx(ctx context.Context, msg string, fields ...Field) {
	s.log(ctx, LevelInfo, msg, fields)
}

func (s *slogLogger) WarnCtx(ctx context.Context, msg string, fields ...Field) {
	s.log(ctx, LevelWarn, msg, fields)
}

func (s *slogLogger) ErrorCtx(ctx context.Context, msg string, fields ...Field) {
	s.log(ctx, LevelError, msg, fields)
}

func (s *slogLogger) FatalCtx(ctx context.Context, msg string, fields ...Field) {
	s.log(ctx, LevelError, msg, fields)
	os.Exit(1)
}

func (s *slogLogger) DebugCtxf(ctx context.Context, format string, args ...any) {
	s.log(ctx, LevelDebug, fmt.Sprintf(format, args...), nil)
}

func (s *slogLogger) InfoCtxf(ctx context.Context, format string, args ...any) {
	s.log(ctx, LevelInfo, fmt.Sprintf(format, args...), nil)
}

func (s *slogLogger) WarnCtxf(ctx context.Context, format string, args ...any) {
	s.log(ctx, LevelWarn, fmt.Sprintf(format, args...), nil)
}

func (s *slogLogger) ErrorCtxf(ctx context.Context, format string, args ...any) {
	s.log(ctx, LevelError, fmt.Sprintf(format, args...), nil)
}

func (s *slogLogger) FatalCtxf(ctx context.Context, format string, args ...any) {
	s.log(ctx, LevelError, fmt.Sprintf(format, args...), nil)
	os.Exit(1)
}

func (s *slogLogger) With(key string, val any) Logger {
	return &slogLogger{
		logger:    s.logger.With(slog.Any(key, val)),
		cfg:       s.cfg,
		addCaller: s.addCaller,
	}
}

func (s *slogLogger) WithField(fields Field) Logger {
	return &slogLogger{
		logger:    s.logger.With(toSlogAttr(fields)),
		cfg:       s.cfg,
		addCaller: s.addCaller,
	}
}

func (s *slogLogger) WithFields(fields []Field) Logger {
	return &slogLogger{
		logger:    s.logger.With(fieldsToSlogAttrs(fields)...),
		cfg:       s.cfg,
		addCaller: s.addCaller,
	}
}

func (s *slogLogger) WithMapFields(fields map[string]any) Logger {
	return &slogLogger{
		logger:    s.logger.With(fieldsToSlogAttrsMap(fields)...),
		cfg:       s.cfg,
		addCaller: s.addCaller,
	}
}

func (s *slogLogger) WithCtx(ctx context.Context) Logger {
	return &slogLogger{
		logger:    s.logger.With(fieldsToSlogAttrs(s.cfg.ContextExtractor(ctx))...),
		cfg:       s.cfg,
		addCaller: s.addCaller,
	}
}

func (s *slogLogger) Sync() error {
	return nil
}

func (s *slogLogger) GetGormLogger(config grom_logger.Config) grom_logger.Interface {
	return grom_logger.NewSlogLogger(s.logger, config)
}

// log 是核心写入方法，统一处理 caller skip、context 提取
func (s *slogLogger) log(ctx context.Context, lvl Level, msg string, fields []Field) {
	if ctx == nil {
		ctx = context.Background()
	}

	sLvl := toSlogLevel(lvl)
	if !s.logger.Enabled(ctx, sLvl) {
		return
	}

	var allFields []Field
	if s.cfg.ContextExtractor != nil {
		allFields = append(allFields, s.cfg.ContextExtractor(ctx)...)
	}
	allFields = append(allFields, fields...)

	// callerSkip = 驱动 base + config 额外层数 + 动态覆盖层数
	var pcs [1]uintptr
	runtime.Callers(s.effectiveCallerSkip(ctx), pcs[:])

	r := slog.NewRecord(time.Now(), sLvl, msg, pcs[0])
	r.Add(fieldsToSlogAttrs(allFields)...)
	_ = s.logger.Handler().Handle(ctx, r)
}

func (s *slogLogger) effectiveCallerSkip(ctx context.Context) int {
	cfgSkip := 0
	if s.cfg.Caller != nil {
		cfgSkip = s.cfg.Caller.Skip
	}
	return slogBaseCallerSkip + cfgSkip + callerSkipFromContext(ctx)
}

func toSlogLevel(lvl Level) slog.Level {
	switch lvl {
	case LevelDebug:
		return slog.LevelDebug
	case LevelInfo:
		return slog.LevelInfo
	case LevelWarn:
		return slog.LevelWarn
	case LevelError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func fieldsToSlogAttrs(fields []Field) []any {
	attrs := make([]any, 0, len(fields))
	for _, f := range fields {
		attrs = append(attrs, toSlogAttr(f))
	}
	return attrs
}

func fieldsToSlogAttrsMap(fields map[string]any) []any {
	attrs := make([]any, 0, len(fields))
	for k, v := range fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	return attrs
}

func toSlogAttr(f Field) slog.Attr {
	switch v := f.Value.(type) {
	case string:
		return slog.String(f.Key, v)
	case int:
		return slog.Int(f.Key, v)
	case int64:
		return slog.Int64(f.Key, v)
	case float64:
		return slog.Float64(f.Key, v)
	case bool:
		return slog.Bool(f.Key, v)
	case error:
		if v == nil {
			return slog.String(f.Key, "<nil>")
		}
		return slog.String(f.Key, fmt.Sprintf("%+v", v))
	case time.Duration:
		return slog.Duration(f.Key, v)
	case fmt.Stringer:
		return slog.String(f.Key, v.String())
	default:
		return slog.Any(f.Key, v)
	}
}
