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

func newSlogLogger(cfg Config, w io.Writer) *slogLogger {
	opts := &slog.HandlerOptions{
		Level:     toSlogLevel(cfg.Level),
		AddSource: cfg.AddCaller,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Value = slog.StringValue(a.Value.Time().Format(cfg.TimeFormat))
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

func (s *slogLogger) With(fields ...Field) Logger {
	return &slogLogger{
		logger:    s.logger.With(fieldsToSlogAttrs(fields)...),
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
	sLvl := toSlogLevel(lvl)
	if !s.logger.Enabled(ctx, sLvl) {
		return
	}

	var allFields []Field
	if s.cfg.ContextExtractor != nil {
		allFields = append(allFields, s.cfg.ContextExtractor(ctx)...)
	}
	allFields = append(allFields, fields...)

	// callerSkip=3: log -> public method -> caller
	var pcs [1]uintptr
	runtime.Callers(3, pcs[:])

	r := slog.NewRecord(time.Now(), sLvl, msg, pcs[0])
	r.Add(fieldsToSlogAttrs(allFields)...)
	_ = s.logger.Handler().Handle(ctx, r)
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
		attrs = append(attrs, slog.Any(f.Key, f.Value))
	}
	return attrs
}
