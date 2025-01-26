package logger_core

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggerConfig struct {
	WriteFile bool

	LogDir   string
	LogLevel string
	FileName string

	TimeFormat string
	// TODO 切割大小、保留时间

	_level    zapcore.Level
	_linkName string
}

var _cfg LoggerConfig

func NewLogger(conf LoggerConfig) (*zap.Logger, error) {
	_cfg = conf

	if err := handleConfig(); err != nil {
		return nil, err
	}

	encodeConfig := zapcore.EncoderConfig{
		MessageKey: "message",
		LevelKey:   "level",
		TimeKey:    "time",
		NameKey:    "module",
		CallerKey:  "caller",
		//FunctionKey:    "func",	// 不打 func,太占地方
		StacktraceKey:  "stacktrace",
		SkipLineEnding: false,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.TimeEncoderOfLayout(_cfg.TimeFormat),
		EncodeDuration: zapcore.MillisDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeName:     zapcore.FullNameEncoder,
	}

	var zapCores = []zapcore.Core{}

	if _cfg.WriteFile {
		WriteSyncer := zapcore.AddSync(getWriter())
		zapCores = append(zapCores, zapcore.NewCore(
			zapcore.NewJSONEncoder(encodeConfig),
			zapcore.Lock(WriteSyncer),
			_cfg._level,
		))
	}

	encodeConfig.EncodeLevel = zapcore.LowercaseColorLevelEncoder

	zapCores = append(zapCores, zapcore.NewCore(
		zapcore.NewConsoleEncoder(encodeConfig),
		getConsoleWrite(),                 // 锁定输出到标准输出
		zap.NewAtomicLevelAt(_cfg._level), // 设置日志级别
	))

	// 合并多个核心
	return zap.New(zapcore.NewTee(zapCores...),
		zap.WithCaller(true),
		zap.AddStacktrace(zap.ErrorLevel),
	), nil
}

func getWriter() io.Writer {
	fileName := fmt.Sprintf("%s/%s.%s.log", _cfg.LogDir, _cfg.FileName, "%Y-%m-%d-%H")
	linkName := fmt.Sprintf("%s/%s.log", _cfg.LogDir, _cfg._linkName)

	hook, err := rotatelogs.New(
		fileName,
		rotatelogs.WithMaxAge(time.Hour*24*15),
		rotatelogs.WithRotationTime(time.Minute*1),
		rotatelogs.WithLinkName(linkName),
	)
	if err != nil {
		panic(err)
	}

	return hook
}

func handleConfig() error {
	if _cfg.LogDir == "" {
		return errors.New("log dir is empty")
	}
	if _cfg.FileName == "" {
		return errors.New("file name is empty")
	}

	if _cfg.TimeFormat == "" {
		_cfg.TimeFormat = "2006-01-02T15:04:05-07:00"
	}

	_cfg.FileName = strings.TrimSuffix(_cfg.FileName, ".log")

	_cfg._level = convLevel(_cfg.LogLevel)
	_cfg._linkName = _cfg.FileName + ".latest"

	return nil
}

func convLevel(level string) zapcore.Level {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return zapcore.DebugLevel
	case "INFO":
		return zapcore.InfoLevel
	case "WARN", "WARNING":
		return zapcore.WarnLevel
	case "ERROR":
		return zapcore.ErrorLevel
	case "PANIC":
		return zapcore.PanicLevel
	case "FATAL":
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}
