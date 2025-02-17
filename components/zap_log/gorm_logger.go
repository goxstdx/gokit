package zap_log

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// GormLogger is a custom logger_core for Gorm that uses Zap
type GormLogger struct {
	Logger *zap.Logger
	logger.Config
}

func NewGormLogger(logger *zap.Logger, config logger.Config) *GormLogger {
	return &GormLogger{Logger: logger, Config: config}
}

// LogMode set log mode
func (l *GormLogger) LogMode(level logger.LogLevel) logger.Interface {
	l.LogLevel = level

	return l
}

// Info logs info level messages
func (l *GormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Info {
		l.Logger.Sugar().Infow(msg, data...)
	}
}

// Warn logs warning level messages
func (l *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Warn {
		l.Logger.Sugar().Warnw(msg, data...)
	}
}

// Error logs error level messages
func (l GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Error {
		l.Logger.Error(msg, zap.Any("data", data))
	}
}

// Trace logs trace level messages, including query details
func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}
	elapsed := time.Since(begin)
	sql, rows := fc()

	switch {
	case err != nil && l.LogLevel >= logger.Error:
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if !l.IgnoreRecordNotFoundError {
				l.Logger.Sugar().Warnw("SQL error: ", "err", err, "elapsed", elapsed, "sql", sql, "rows", rows)
			}
		} else {
			l.Logger.Sugar().Errorw("SQL error: ", "err", err, "elapsed", elapsed, "sql", sql, "rows", rows)
		}
	case elapsed > l.SlowThreshold && l.SlowThreshold != 0 && l.LogLevel >= logger.Warn:
		l.Logger.Sugar().Warnw("SQL Slow: ", "elapsed", elapsed, "sql", sql, "rows", rows)
	case l.LogLevel >= logger.Info:
		l.Logger.Sugar().Infow("SQL Trace: ", "elapsed", elapsed, "sql", sql, "rows", rows)
	}
}

// 实现 gorm.ParamsFilter 接口
func (l *GormLogger) ParamsFilter(ctx context.Context, sql string, params ...interface{}) (string, []interface{}) {
	if l.Config.ParameterizedQueries {
		return sql, nil
	}

	return sql, params
}
