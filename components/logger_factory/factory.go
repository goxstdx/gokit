package logger_factory

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lestrrat-go/file-rotatelogs"
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

	if err := os.MkdirAll(cfg.File.Path, 0755); err != nil {
		return nil, fmt.Errorf("log_factory: create directory: %w", err)
	}

	if cfg.Rotation == nil {
		f, err := os.OpenFile(filepath.Join(cfg.File.Path, cfg.File.Name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("log_factory: open log file: %w", err)
		}
		return f, nil
	}

	pattern := fmt.Sprintf("%s/%s.%s.log", cfg.File.Path, cfg.File.Name, "%Y-%m-%d-%H")
	opts := []rotatelogs.Option{}

	if cfg.Rotation.MaxAge > 0 {
		opts = append(opts, rotatelogs.WithMaxAge(time.Duration(cfg.Rotation.MaxAge)*24*time.Hour))
	}
	if cfg.Rotation.MaxBackups > 0 {
		opts = append(opts, rotatelogs.WithRotationCount(uint(cfg.Rotation.MaxBackups)))
	}

	if cfg.Rotation.RotationTime != "" {
		d, err := time.ParseDuration(cfg.Rotation.RotationTime)
		if err != nil {
			return nil, fmt.Errorf("log_factory: invalid rotation_time %q: %w", cfg.Rotation.RotationTime, err)
		}
		opts = append(opts, rotatelogs.WithRotationTime(d))
	}

	if cfg.Rotation.LinkName != "" {
		linkName := filepath.Join(cfg.File.Path, cfg.Rotation.LinkName+".log")
		opts = append(opts, rotatelogs.WithLinkName(linkName))
	}

	rl, err := rotatelogs.New(pattern, opts...)
	if err != nil {
		return nil, fmt.Errorf("log_factory: create rotatelogs: %w", err)
	}

	return rl, nil
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

var _ Logger = (*slogLogger)(nil)
var _ Logger = (*zapLogger)(nil)
