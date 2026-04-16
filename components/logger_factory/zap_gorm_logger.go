package logger_factory

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm/logger"
)

type zapGormLogger struct {
	zl             *zap.Logger
	LogLevel       logger.LogLevel
	SlowThreshold  time.Duration
	Parameterized  bool
	IgnoreNotFound bool
}

func newZapGormLogger(zl *zap.Logger, config logger.Config) *zapGormLogger {
	return &zapGormLogger{
		zl:             zl,
		LogLevel:       config.LogLevel,
		SlowThreshold:  config.SlowThreshold,
		Parameterized:  config.ParameterizedQueries,
		IgnoreNotFound: config.IgnoreRecordNotFoundError,
	}
}

func (l *zapGormLogger) LogMode(level logger.LogLevel) logger.Interface {
	newLogger := *l
	newLogger.LogLevel = level
	return &newLogger
}

func (l *zapGormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Info {
		l.zl.Info(msg, zap.Any("data", data))
	}
}

func (l *zapGormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Warn {
		l.zl.Warn(msg, zap.Any("data", data))
	}
}

func (l *zapGormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= logger.Error {
		l.zl.Error(msg, zap.Any("data", data))
	}
}

func (l *zapGormLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()

	switch {
	case err != nil && l.LogLevel >= logger.Error:
		if errors.Is(err, logger.ErrRecordNotFound) {
			if !l.IgnoreNotFound {
				l.zl.Warn(
					"SQL error",
					zap.String("err", err.Error()),
					zap.Duration("elapsed", elapsed),
					zap.String("sql", sql),
					zap.Int64("rows", rows),
				)
			}
		} else {
			l.zl.Error(
				"SQL error",
				zap.String("err", err.Error()),
				zap.Duration("elapsed", elapsed),
				zap.String("sql", sql),
				zap.Int64("rows", rows),
			)
		}
	case l.SlowThreshold != 0 && elapsed > l.SlowThreshold && l.LogLevel >= logger.Warn:
		l.zl.Warn(
			"SQL slow",
			zap.Duration("elapsed", elapsed),
			zap.String("sql", sql),
			zap.Int64("rows", rows),
			zap.String("threshold", fmt.Sprintf("%v", l.SlowThreshold)),
		)
	case l.LogLevel >= logger.Info:
		l.zl.Info(
			"SQL trace",
			zap.Duration("elapsed", elapsed),
			zap.String("sql", sql),
			zap.Int64("rows", rows),
		)
	}
}

func (l *zapGormLogger) ParamsFilter(ctx context.Context, sql string, params ...interface{}) (string, []interface{}) {
	if l.Parameterized {
		return sql, nil
	}
	return sql, params
}
