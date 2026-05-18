package producer

import (
	"strings"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/driver"
)

// Config Producer 配置
type Config struct {
	EventDriver driver.EventQueueDriver
	DelayDriver driver.DelayQueueDriver
	KeyPrefix   string
	Logger      core.Logger
	OnAlert     core.AlertFunc

	// ResolveEventGroup 用于将 runnerName 解析为事件队列组名。
	// 返回 (groupName, registered)；若为 nil，所有 event 推入默认组且视为已注册。
	ResolveEventGroup EventGroupResolver

	// IsDelayRegistered 检查 delay runner 是否已注册。
	// 若为 nil，所有 delay runner 视为已注册（不做死信检查）。
	IsDelayRegistered DelayRegisteredChecker
}

func (c *Config) normalize() {
	c.KeyPrefix = strings.TrimRight(c.KeyPrefix, ":")
	if c.KeyPrefix == "" {
		c.KeyPrefix = "taskx"
	}
}

// Option Producer 配置选项
type Option func(*Config)

func WithEventQueueDriver(d driver.EventQueueDriver) Option {
	return func(c *Config) { c.EventDriver = d }
}

func WithDelayQueueDriver(d driver.DelayQueueDriver) Option {
	return func(c *Config) { c.DelayDriver = d }
}

func WithKeyPrefix(prefix string) Option {
	return func(c *Config) { c.KeyPrefix = prefix }
}

func WithLogger(l core.Logger) Option {
	return func(c *Config) { c.Logger = l }
}

func WithAlertFunc(f core.AlertFunc) Option {
	return func(c *Config) { c.OnAlert = f }
}

func WithEventGroupResolver(fn EventGroupResolver) Option {
	return func(c *Config) { c.ResolveEventGroup = fn }
}

func WithDelayRegisteredChecker(fn DelayRegisteredChecker) Option {
	return func(c *Config) { c.IsDelayRegistered = fn }
}

// NewConfig 基于 Option 函数式构建 Config。
func NewConfig(opts ...Option) Config {
	var cfg Config
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}
