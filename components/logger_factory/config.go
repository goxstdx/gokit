package logger_factory

import (
	"context"
	"strings"
)

const (
	defaultTimeFormat   = "2006-01-02 15:04:05.000"
	defaultMaxAge       = 7
	defaultRotationTime = "1h"
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
	MaxAge       int    // 保留旧日志文件的天数（优先级高于 MaxBackups，两者同时存在时以 MaxAge 为准）
	MaxBackups   int    // 保留旧日志文件的最大数量
	RotationTime string // 按时间切割的间隔，如 "1h", "30m", "1d"
	LinkName     string // 软链名称，为空则不创建软链，如 "latest"
}

func GetDefaultRotationConfig() *RotationConfig {
	return &RotationConfig{
		MaxAge:       defaultMaxAge,
		RotationTime: defaultRotationTime,
	}
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
	if c.File != nil {
		c.File.Path = strings.TrimRight(c.File.Path, "/")
		c.File.Name = strings.TrimRight(c.File.Name, ".log")
	}

	if c.Rotation != nil {
		if c.Rotation.MaxAge > 0 && c.Rotation.MaxBackups > 0 {
			c.Rotation.MaxBackups = 0
		}
		if c.Rotation.MaxAge <= 0 && c.Rotation.MaxBackups <= 0 {
			c.Rotation.MaxAge = defaultMaxAge
		}
		if c.Rotation.LinkName == "" {
			c.Rotation.LinkName = c.File.Name + ".latest"
		}
		c.Rotation.LinkName = strings.TrimRight(c.Rotation.LinkName, ".log")
	} else {
		c.Rotation = GetDefaultRotationConfig()
	}
}
