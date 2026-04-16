package logger_factory

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

func NewLogger(cfg Config) (Logger, error) {
	cfg.applyDefaults()

	w, err := buildWriter(cfg)
	if err != nil {
		return nil, err
	}

	switch cfg.DriverType {
	case DriverSlog:
		return newSlogLogger(cfg, w), nil
	case DriverZap:
		return newZapLogger(cfg, w)
	default:
		return nil, fmt.Errorf("log_factory: unsupported driver type: %q", cfg.DriverType)
	}
}

func buildWriter(cfg Config) (io.Writer, error) {
	var writers []io.Writer

	if shouldWriteConsole(cfg) {
		consoleWriter, err := getConsoleWriter(cfg.Console)
		if err != nil {
			return nil, err
		}
		writers = append(writers, consoleWriter)
	}

	if shouldWriteFile(cfg) {
		fileWriter, err := buildFileWriter(cfg)
		if err != nil {
			return nil, err
		}
		writers = append(writers, fileWriter)
	}

	if len(writers) == 0 {
		return nil, fmt.Errorf("log_factory: no output configured")
	}

	if len(writers) == 1 {
		return writers[0], nil
	}

	return &multiWriter{writers: writers}, nil
}

func shouldWriteConsole(cfg Config) bool {
	if cfg.Development {
		return true
	}
	return cfg.OutputMode == OutputModeBoth || cfg.OutputMode == OutputModeConsole
}

func shouldWriteFile(cfg Config) bool {
	return cfg.File != nil && (cfg.OutputMode == OutputModeBoth || cfg.OutputMode == OutputModeFile)
}

func getConsoleWriter(console string) (io.Writer, error) {
	switch console {
	case "stdout", "":
		return os.Stdout, nil
	case "stderr":
		return os.Stderr, nil
	default:
		return nil, fmt.Errorf("log_factory: unsupported console output: %q", console)
	}
}

func buildFileWriter(cfg Config) (io.Writer, error) {
	if cfg.File == nil {
		return nil, fmt.Errorf("log_factory: file config is nil")
	}

	filePath := filepath.Join(cfg.File.Path, cfg.File.Name)

	if cfg.Rotation == nil {
		if err := os.MkdirAll(cfg.File.Path, 0755); err != nil {
			return nil, fmt.Errorf("log_factory: create directory: %w", err)
		}
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("log_factory: open log file: %w", err)
		}
		return f, nil
	}

	lj := &lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    cfg.Rotation.MaxSize,
		MaxAge:     cfg.Rotation.MaxAge,
		MaxBackups: cfg.Rotation.MaxBackups,
		Compress:   cfg.Rotation.Compress,
		LocalTime:  true,
	}

	if cfg.Rotation.RotationTime != "" {
		d, err := time.ParseDuration(cfg.Rotation.RotationTime)
		if err != nil {
			return nil, fmt.Errorf("log_factory: invalid rotation_time %q: %w", cfg.Rotation.RotationTime, err)
		}
		return &timeRotationWriter{inner: lj, interval: d, filePath: filePath, rotation: cfg.Rotation}, nil
	}

	return lj, nil
}

type multiWriter struct {
	writers []io.Writer
}

func (m *multiWriter) Write(p []byte) (n int, err error) {
	for _, w := range m.writers {
		if _, err := w.Write(p); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

type timeRotationWriter struct {
	inner    *lumberjack.Logger
	interval time.Duration
	filePath string
	rotation *RotationConfig
	lastTime time.Time
}

func (w *timeRotationWriter) Write(p []byte) (n int, err error) {
	now := time.Now()
	if w.lastTime.IsZero() {
		w.lastTime = now
	}
	if now.Sub(w.lastTime) >= w.interval {
		_ = w.inner.Rotate()
		w.lastTime = now
	}
	return w.inner.Write(p)
}

var _ Logger = (*slogLogger)(nil)
var _ Logger = (*zapLogger)(nil)
