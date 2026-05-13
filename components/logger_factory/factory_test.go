package logger_factory

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm/logger"
)

const testLogDir = "/tmp/log_factory_test"

var gormConfig = logger.Config{
	SlowThreshold:             200 * time.Millisecond,
	LogLevel:                  logger.Info,
	IgnoreRecordNotFoundError: true,
	ParameterizedQueries:      false,
}

func init() {
	os.MkdirAll(testLogDir, 0755)
}

func readRotatedLogContent(t *testing.T, dir, baseName string) string {
	t.Helper()
	pattern := filepath.Join(dir, baseName+".*.log")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		t.Fatalf("glob rotated log failed: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no rotated log found with pattern: %s", pattern)
	}
	sort.Strings(matches)
	content, err := os.ReadFile(matches[len(matches)-1])
	if err != nil {
		t.Fatalf("read rotated log failed: %v", err)
	}
	return string(content)
}

func TestSlogDriver_ConsoleOnly(t *testing.T) {
	logger, err := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeConsole,
		},
	)
	if err != nil {
		t.Fatalf("NewLogger(slog) failed: %v", err)
	}
	if logger == nil {
		t.Fatal("logger is nil")
	}

	logger.Info("slog console test")
}

func TestSlogDriver_FileOnly(t *testing.T) {
	tmpDir := testLogDir

	logger, err := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-test.log",
			},
		},
	)
	if err != nil {
		t.Fatalf("NewLogger(slog) failed: %v", err)
	}

	logger.Info("slog file test")
	logger.Sync()

	content := readRotatedLogContent(t, tmpDir, "slog-test")
	if !strings.Contains(content, "slog file test") {
		t.Errorf("expected 'slog file test' in output")
	}
}

func TestSlogDriver_Both(t *testing.T) {
	tmpDir := testLogDir

	logger, err := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeBoth,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-both.log",
			},
		},
	)
	if err != nil {
		t.Fatalf("NewLogger(slog) failed: %v", err)
	}

	logger.Info("slog both test")
	logger.Sync()

	content := readRotatedLogContent(t, tmpDir, "slog-both")
	if !strings.Contains(content, "slog both test") {
		t.Errorf("expected 'slog both test' in output")
	}
}

func TestSlogDriver_Development(t *testing.T) {
	tmpDir := testLogDir

	logger, err := NewLogger(
		Config{
			DriverType:  DriverSlog,
			OutputMode:  OutputModeFile,
			Development: true,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-dev.log",
			},
		},
	)
	if err != nil {
		t.Fatalf("NewLogger(slog) failed: %v", err)
	}

	logger.Info("slog development mode test")
	t.Log("Development mode: should output to console regardless of OutputMode")
}

func TestSlogDriver_Levels(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeFile,
			Level:      LevelDebug,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-levels.log",
			},
		},
	)

	logger.Debug("debug msg")
	logger.Info("info msg")
	logger.Warn("warn msg")
	logger.Error("error msg")

	t.Logf("Log file: %s", filepath.Join(tmpDir, "slog-levels.log"))
}

func TestSlogDriver_FormattedLogs(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-formatted.log",
			},
		},
	)

	logger.Infof("user: %s, age: %d", "Alice", 30)

	t.Logf("Log file: %s", filepath.Join(tmpDir, "slog-formatted.log"))
}

func TestSlogDriver_WithFields(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-fields.log",
			},
		},
	)

	logger.Info("request", String("method", "GET"), Int("status", 200))

	t.Logf("Log file: %s", filepath.Join(tmpDir, "slog-fields.log"))
}

func TestSlogDriver_With(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-with.log",
			},
		},
	)

	child := logger.With("service", "api")
	child.Info("request")

	t.Logf("Log file: %s", filepath.Join(tmpDir, "slog-with.log"))
}

func TestSlogDriver_ContextLogs(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-ctx.log",
			},
		},
	)

	ctx := context.WithValue(context.Background(), "trace_id", "abc123")
	logger.InfoCtx(ctx, "request", String("method", "GET"))

	t.Logf("Log file: %s", filepath.Join(tmpDir, "slog-ctx.log"))
}

func TestSlogDriver_Sync(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "slog-sync.log",
			},
		},
	)
	if err := logger.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}

func TestZapDriver_ConsoleOnly(t *testing.T) {
	logger, err := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeConsole,
		},
	)
	if err != nil {
		t.Fatalf("NewLogger(zap) failed: %v", err)
	}
	if logger == nil {
		t.Fatal("logger is nil")
	}

	logger.Info("zap console test")
}

func TestZapDriver_FileOnly(t *testing.T) {
	tmpDir := testLogDir

	logger, err := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-test.log",
			},
		},
	)
	if err != nil {
		t.Fatalf("NewLogger(zap) failed: %v", err)
	}

	logger.Info("zap file test")
	logger.Sync()

	content := readRotatedLogContent(t, tmpDir, "zap-test")
	if !strings.Contains(content, "zap file test") {
		t.Errorf("expected 'zap file test' in output")
	}
}

func TestZapDriver_Both(t *testing.T) {
	tmpDir := testLogDir

	logger, err := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeBoth,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-both.log",
			},
		},
	)
	if err != nil {
		t.Fatalf("NewLogger(zap) failed: %v", err)
	}

	logger.Info("zap both test")
	logger.Sync()

	content := readRotatedLogContent(t, tmpDir, "zap-both")
	if !strings.Contains(content, "zap both test") {
		t.Errorf("expected 'zap both test' in output")
	}
}

func TestZapDriver_Development(t *testing.T) {
	tmpDir := testLogDir

	logger, err := NewLogger(
		Config{
			DriverType:  DriverZap,
			OutputMode:  OutputModeFile,
			Development: true,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-dev.log",
			},
		},
	)
	if err != nil {
		t.Fatalf("NewLogger(zap) failed: %v", err)
	}

	logger.Info("zap development mode test")
	t.Log("Development mode: should output to console regardless of OutputMode")
}

func TestZapDriver_Levels(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeFile,
			Level:      LevelDebug,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-levels.log",
			},
		},
	)

	logger.Debug("debug msg")
	logger.Info("info msg")
	logger.Warn("warn msg")
	logger.Error("error msg")

	t.Logf("Log file: %s", filepath.Join(tmpDir, "zap-levels.log"))
}

func TestZapDriver_FormattedLogs(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-formatted.log",
			},
		},
	)

	logger.Infof("user: %s, age: %d", "Bob", 25)

	t.Logf("Log file: %s", filepath.Join(tmpDir, "zap-formatted.log"))
}

func TestZapDriver_WithFields(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-fields.log",
			},
		},
	)

	logger.Info("response", String("status", "ok"), Int("code", 200))

	t.Logf("Log file: %s", filepath.Join(tmpDir, "zap-fields.log"))
}

func TestZapDriver_With(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-with.log",
			},
		},
	)

	child := logger.With("service", "worker")
	child.Info("task started")

	t.Logf("Log file: %s", filepath.Join(tmpDir, "zap-with.log"))
}

func TestZapDriver_ContextLogs(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-ctx.log",
			},
		},
	)

	ctx := context.WithValue(context.Background(), "request_id", "xyz789")
	logger.InfoCtx(ctx, "processing", String("step", "1"))

	t.Logf("Log file: %s", filepath.Join(tmpDir, "zap-ctx.log"))
}

func TestZapDriver_Sync(t *testing.T) {
	tmpDir := testLogDir

	logger, _ := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeFile,
			File: &FileConfig{
				Path: tmpDir,
				Name: "zap-sync.log",
			},
		},
	)
	if err := logger.Sync(); err != nil {
		t.Errorf("Sync failed: %v", err)
	}
}

func TestBothDrivers_Formats(t *testing.T) {
	for _, tc := range []struct {
		driverType DriverType
		format     Format
		name       string
	}{
		{DriverSlog, FormatJSON, "slog-json"},
		{DriverSlog, FormatText, "slog-text"},
		{DriverZap, FormatJSON, "zap-json"},
		{DriverZap, FormatText, "zap-text"},
	} {
		t.Run(
			tc.name, func(t *testing.T) {
				tmpDir := testLogDir

				logger, err := NewLogger(
					Config{
						DriverType: tc.driverType,
						Format:     tc.format,
						OutputMode: OutputModeFile,
						File: &FileConfig{
							Path: tmpDir,
							Name: tc.name + ".log",
						},
					},
				)
				if err != nil {
					t.Fatalf("NewLogger failed: %v", err)
				}
				logger.Info("format test", String("key", "value"))

				t.Logf("Log file: %s", filepath.Join(tmpDir, tc.name+".log"))
			},
		)
	}
}

func TestBothDrivers_FieldTypes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field Field
	}{
		{"String", String("str", "hello")},
		{"Int", Int("num", 42)},
		{"Bool", Bool("flag", true)},
	} {
		t.Run(
			tc.name, func(t *testing.T) {
				for _, driver := range []DriverType{DriverSlog, DriverZap} {
					t.Run(
						string(driver), func(t *testing.T) {
							tmpDir := testLogDir
							fileName := string(driver) + "-" + tc.name + ".log"

							logger, _ := NewLogger(
								Config{
									DriverType: driver,
									OutputMode: OutputModeFile,
									File: &FileConfig{
										Path: tmpDir,
										Name: fileName,
									},
								},
							)
							logger.Info("test", tc.field)

							t.Logf("Log file: %s", filepath.Join(tmpDir, fileName))
						},
					)
				}
			},
		)
	}
}

func TestNewLogger_UnsupportedDriver(t *testing.T) {
	_, err := NewLogger(
		Config{
			DriverType: "unsupported",
		},
	)
	if err == nil {
		t.Error("expected error for unsupported driver")
	}
}

func TestConfig_ApplyDefaults(t *testing.T) {
	cfg := Config{
		DriverType: DriverSlog,
	}
	cfg.applyDefaults()

	if cfg.TimeFormat != defaultTimeFormat {
		t.Errorf("expected TimeFormat %q, got %q", defaultTimeFormat, cfg.TimeFormat)
	}
	if cfg.Format != FormatJSON {
		t.Errorf("expected Format %q, got %q", FormatJSON, cfg.Format)
	}
	if cfg.Console != "stdout" {
		t.Errorf("expected Console %q, got %q", "stdout", cfg.Console)
	}
	if cfg.OutputMode != OutputModeConsole {
		t.Errorf("expected OutputMode %q, got %q", OutputModeConsole, cfg.OutputMode)
	}
	if cfg.Caller == nil {
		t.Fatal("expected Caller default config, got nil")
	}
	if cfg.Caller.Key != "caller" {
		t.Errorf("expected Caller.Key %q, got %q", "caller", cfg.Caller.Key)
	}
	if cfg.Caller.Skip != 0 {
		t.Errorf("expected Caller.Skip %d, got %d", 0, cfg.Caller.Skip)
	}
}

func TestOutputMode_FileConfig(t *testing.T) {
	_, err := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeFile,
		},
	)
	if err == nil {
		t.Error("expected error when OutputMode is file but File is nil")
	}
}

func TestGormLogger_Slog(t *testing.T) {
	logger, err := NewLogger(
		Config{
			DriverType: DriverSlog,
			OutputMode: OutputModeConsole,
		},
	)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	gormLog := logger.GetGormLogger(gormConfig)
	if gormLog == nil {
		t.Fatal("NewGormLogger returned nil")
	}

	gormLog.Info(context.Background(), "gorm info test")
}

func TestGormLogger_Zap(t *testing.T) {
	logger, err := NewLogger(
		Config{
			DriverType: DriverZap,
			OutputMode: OutputModeConsole,
		},
	)
	if err != nil {
		t.Fatalf("NewLogger failed: %v", err)
	}

	gormLog := logger.GetGormLogger(gormConfig)
	if gormLog == nil {
		t.Fatal("NewGormLogger returned nil")
	}

	gormLog.Info(context.Background(), "gorm zap info test")
}
