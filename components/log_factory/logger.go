package log_factory

import "context"

type DriverType string

const (
	DriverSlog DriverType = "slog"
	DriverZap  DriverType = "zap"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

type Format string

const (
	FormatJSON Format = "json"
	FormatText Format = "text"
)

type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)

	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)

	DebugCtx(ctx context.Context, msg string, fields ...Field)
	InfoCtx(ctx context.Context, msg string, fields ...Field)
	WarnCtx(ctx context.Context, msg string, fields ...Field)
	ErrorCtx(ctx context.Context, msg string, fields ...Field)
	FatalCtx(ctx context.Context, msg string, fields ...Field)

	With(fields ...Field) Logger
	Sync() error
}
