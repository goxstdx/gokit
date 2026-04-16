package log_factory

import "context"

const (
	defaultTimeFormat = "2006-01-02 15:04:05.000"
	defaultMaxSize    = 100
	defaultMaxAge     = 30
	defaultMaxBackups = 10
)

type ContextExtractor func(ctx context.Context) []Field

type OutputMode string

const (
	OutputModeBoth    OutputMode = "both"    // 同时写入文件和 console
	OutputModeConsole OutputMode = "console" // 只写入 console
	OutputModeFile    OutputMode = "file"    // 只写入文件
)

type Config struct {
	DriverType       DriverType
	Level            Level
	Format           Format
	Console          string           // console 输出目标："stdout" | "stderr"，空则默认 stdout
	File             *FileConfig      // 文件配置，为 nil 则不写文件
	OutputMode       OutputMode       // 输出模式
	TimeFormat       string           // 时间格式，默认 "2006-01-02 15:04:05.000"
	AddCaller        bool             // 是否显示调用位置
	Development      bool             // 开发模式，强制输出到 console
	Rotation         *RotationConfig  // 文件切割，File 不为空时生效
	ContextExtractor ContextExtractor // 从 context 中提取自定义字段
}

type FileConfig struct {
	Path string
	Name string
}

type RotationConfig struct {
	MaxSize      int
	MaxAge       int
	MaxBackups   int
	Compress     bool
	RotationTime string
}

func (c *Config) applyDefaults() {
	if c.TimeFormat == "" {
		c.TimeFormat = defaultTimeFormat
	}
	if c.Format == "" {
		c.Format = FormatJSON
	}
	if c.Console == "" {
		c.Console = "stdout"
	}
	if c.OutputMode == "" {
		c.OutputMode = OutputModeConsole
	}
	if c.Rotation != nil {
		if c.Rotation.MaxSize <= 0 {
			c.Rotation.MaxSize = defaultMaxSize
		}
		if c.Rotation.MaxAge <= 0 {
			c.Rotation.MaxAge = defaultMaxAge
		}
		if c.Rotation.MaxBackups <= 0 {
			c.Rotation.MaxBackups = defaultMaxBackups
		}
	}
}
