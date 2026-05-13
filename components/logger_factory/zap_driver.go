package logger_factory

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/gorm/logger"
)

type zapLogger struct {
	logger *zap.Logger
	sugar  *zap.SugaredLogger
	cfg    Config
}

const zapBaseCallerSkip = 1

func newZapLogger(cfg Config, w io.Writer) (*zapLogger, error) {
	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format(cfg.TimeFormat))
		},
	}

	if !cfg.AddCaller {
		encoderCfg.CallerKey = ""
	} else if cfg.Caller != nil && cfg.Caller.Key != "" {
		encoderCfg.CallerKey = cfg.Caller.Key
	}

	var encoder zapcore.Encoder
	switch cfg.Format {
	case FormatText:
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	default:
		encoder = zapcore.NewJSONEncoder(encoderCfg)
	}

	ws := zapcore.AddSync(w)
	core := zapcore.NewCore(encoder, ws, toZapLevel(cfg.Level))

	cfgCallerSkip := 0
	if cfg.Caller != nil {
		cfgCallerSkip = cfg.Caller.Skip
	}

	opts := []zap.Option{
		zap.AddCallerSkip(zapBaseCallerSkip + cfgCallerSkip),
	}
	if cfg.AddCaller {
		opts = append(opts, zap.AddCaller())
	}
	if cfg.Development {
		opts = append(opts, zap.Development())
	}

	l := zap.New(core, opts...)

	return &zapLogger{
		logger: l,
		sugar:  l.Sugar(),
		cfg:    cfg,
	}, nil
}

// --- 结构化方法 ---

func (z *zapLogger) Debug(msg string, fields ...Field) {
	z.logger.Debug(msg, toZapFields(fields)...)
}

func (z *zapLogger) Info(msg string, fields ...Field) {
	z.logger.Info(msg, toZapFields(fields)...)
}

func (z *zapLogger) Warn(msg string, fields ...Field) {
	z.logger.Warn(msg, toZapFields(fields)...)
}

func (z *zapLogger) Error(msg string, fields ...Field) {
	z.logger.Error(msg, toZapFields(fields)...)
}

func (z *zapLogger) Fatal(msg string, fields ...Field) {
	z.logger.Fatal(msg, toZapFields(fields)...)
}

// --- 格式化方法 ---

func (z *zapLogger) Debugf(format string, args ...any) {
	z.sugar.Debugf(format, args...)
}

func (z *zapLogger) Infof(format string, args ...any) {
	z.sugar.Infof(format, args...)
}

func (z *zapLogger) Warnf(format string, args ...any) {
	z.sugar.Warnf(format, args...)
}

func (z *zapLogger) Errorf(format string, args ...any) {
	z.sugar.Errorf(format, args...)
}

func (z *zapLogger) Fatalf(format string, args ...any) {
	z.sugar.Fatalf(format, args...)
}

// --- Context 方法 ---

func (z *zapLogger) DebugCtx(ctx context.Context, msg string, fields ...Field) {
	z.loggerWithCallerFromContext(ctx).Debug(msg, z.ctxFields(ctx, fields)...)
}

func (z *zapLogger) InfoCtx(ctx context.Context, msg string, fields ...Field) {
	z.loggerWithCallerFromContext(ctx).Info(msg, z.ctxFields(ctx, fields)...)
}

func (z *zapLogger) WarnCtx(ctx context.Context, msg string, fields ...Field) {
	z.loggerWithCallerFromContext(ctx).Warn(msg, z.ctxFields(ctx, fields)...)
}

func (z *zapLogger) ErrorCtx(ctx context.Context, msg string, fields ...Field) {
	z.loggerWithCallerFromContext(ctx).Error(msg, z.ctxFields(ctx, fields)...)
}

func (z *zapLogger) FatalCtx(ctx context.Context, msg string, fields ...Field) {
	z.loggerWithCallerFromContext(ctx).Fatal(msg, z.ctxFields(ctx, fields)...)
}

func (z *zapLogger) DebugCtxf(ctx context.Context, format string, args ...any) {
	z.loggerWithCallerFromContext(ctx).Debug(fmt.Sprintf(format, args...), z.ctxFields(ctx, nil)...)
}

func (z *zapLogger) InfoCtxf(ctx context.Context, format string, args ...any) {
	z.loggerWithCallerFromContext(ctx).Info(fmt.Sprintf(format, args...), z.ctxFields(ctx, nil)...)
}

func (z *zapLogger) WarnCtxf(ctx context.Context, format string, args ...any) {
	z.loggerWithCallerFromContext(ctx).Warn(fmt.Sprintf(format, args...), z.ctxFields(ctx, nil)...)
}

func (z *zapLogger) ErrorCtxf(ctx context.Context, format string, args ...any) {
	z.loggerWithCallerFromContext(ctx).Error(fmt.Sprintf(format, args...), z.ctxFields(ctx, nil)...)
}

func (z *zapLogger) FatalCtxf(ctx context.Context, format string, args ...any) {
	z.loggerWithCallerFromContext(ctx).Fatal(fmt.Sprintf(format, args...), z.ctxFields(ctx, nil)...)
}

func (z *zapLogger) With(key string, val any) Logger {
	newL := z.logger.With(zap.Any(key, val))
	return &zapLogger{
		logger: newL,
		sugar:  newL.Sugar(),
		cfg:    z.cfg,
	}
}

func (z *zapLogger) WithField(fields Field) Logger {
	newL := z.logger.With(toZapField(fields))
	return &zapLogger{
		logger: newL,
		sugar:  newL.Sugar(),
		cfg:    z.cfg,
	}
}

func (z *zapLogger) WithFields(fields []Field) Logger {
	newL := z.logger.With(toZapFields(fields)...)
	return &zapLogger{
		logger: newL,
		sugar:  newL.Sugar(),
		cfg:    z.cfg,
	}
}

func (z *zapLogger) WithCtx(ctx context.Context) Logger {
	newL := z.logger.With(toZapFields(z.cfg.ContextExtractor(ctx))...)
	return &zapLogger{
		logger: newL,
		sugar:  newL.Sugar(),
		cfg:    z.cfg,
	}
}

func (z *zapLogger) Sync() error {
	return z.logger.Sync()
}

func (z *zapLogger) GetGormLogger(config logger.Config) logger.Interface {
	return newZapGormLogger(z.logger, config)
}

func (z *zapLogger) ctxFields(ctx context.Context, fields []Field) []zap.Field {
	var all []Field
	if z.cfg.ContextExtractor != nil {
		all = append(all, z.cfg.ContextExtractor(ctx)...)
	}
	all = append(all, fields...)
	return toZapFields(all)
}

func (z *zapLogger) loggerWithCallerFromContext(ctx context.Context) *zap.Logger {
	dynamicSkip := callerSkipFromContext(ctx)
	if dynamicSkip <= 0 {
		return z.logger
	}
	return z.logger.WithOptions(zap.AddCallerSkip(dynamicSkip))
}

func toZapLevel(lvl Level) zapcore.Level {
	switch lvl {
	case LevelDebug:
		return zapcore.DebugLevel
	case LevelInfo:
		return zapcore.InfoLevel
	case LevelWarn:
		return zapcore.WarnLevel
	case LevelError:
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

func toZapFields(fields []Field) []zap.Field {
	zf := make([]zap.Field, 0, len(fields))
	for _, f := range fields {
		zf = append(zf, toZapField(f))
	}
	return zf
}

func toZapField(f Field) zap.Field {
	switch v := f.Value.(type) {
	case string:
		return zap.String(f.Key, v)
	case int:
		return zap.Int(f.Key, v)
	case int64:
		return zap.Int64(f.Key, v)
	case float64:
		return zap.Float64(f.Key, v)
	case bool:
		return zap.Bool(f.Key, v)
	case error:
		if v == nil {
			return zap.String(f.Key, "<nil>")
		}
		return zap.NamedError(f.Key, v)
	case time.Duration:
		return zap.Duration(f.Key, v)
	case fmt.Stringer:
		return zap.Stringer(f.Key, v)
	default:
		return zap.Any(f.Key, v)
	}
}

// 确保编译时检查接口实现
var _ Logger = (*zapLogger)(nil)
var _ Logger = (*slogLogger)(nil)

// exitFunc 用于测试时替换 os.Exit
var exitFunc = os.Exit
