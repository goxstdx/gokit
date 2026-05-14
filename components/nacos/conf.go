package nacos

import (
	"fmt"
	"time"
)

type Scheme string

const (
	SchemeHTTP  Scheme = "http"
	SchemeHTTPS Scheme = "https"
)

type AuthMode string

const (
	AuthModeAuto     AuthMode = "auto"
	AuthModeRequired AuthMode = "required"
	AuthModeDisabled AuthMode = "disabled"
)

const (
	DefaultScheme        = SchemeHTTP
	DefaultGroup         = "DEFAULT_GROUP"
	DefaultAuthMode      = AuthModeAuto
	DefaultTimeoutMs     = 3000
	DefaultPollInterval  = 5 * time.Second
	DefaultRetryCount    = 3
	DefaultRetryInterval = 500 * time.Millisecond
	DefaultLogLevel      = "info"
)

// RetryConf controls retry behavior for HTTP requests.
type RetryConf struct {
	// MaxRetries is the maximum number of retry attempts (0 means no retry).
	MaxRetries int
	// Interval is the base wait duration between retries.
	Interval time.Duration
}

// AuthConf controls authentication behavior.
type AuthConf struct {
	// Mode controls whether auth is auto-detected, required, or disabled.
	Mode AuthMode
	// UserName is the username used for auth login.
	UserName string
	// Password is the password used for auth login.
	Password string
}

// ConfigFileConf controls the target nacos config file info.
type ConfigFileConf struct {
	NamespaceId string
	DataId      string
	Group       string
}

// Conf holds all configuration for a NacosHTTP client.
type Conf struct {
	Scheme Scheme
	Ipaddr string
	Port   uint64
	File   *ConfigFileConf

	Auth      *AuthConf
	TimeoutMs uint64
	Retry     *RetryConf

	PollIntervalMs      uint64
	NotLoadCacheAtStart bool
	LogDir              string
	CacheDir            string
	LogLevel            string
}

// applyDefaults fills zero-value fields with sensible defaults.
func (c *Conf) applyDefaults() {
	if c.Retry == nil {
		c.Retry = &RetryConf{
			MaxRetries: DefaultRetryCount,
			Interval:   DefaultRetryInterval,
		}
	}
	if c.File == nil {
		c.File = &ConfigFileConf{}
	}
	if c.Auth == nil {
		c.Auth = &AuthConf{}
	}
	if c.Scheme == "" {
		c.Scheme = DefaultScheme
	}
	if c.File.Group == "" {
		c.File.Group = string(DefaultGroup)
	}
	if c.Auth.Mode == "" {
		c.Auth.Mode = DefaultAuthMode
	}
	if c.TimeoutMs == 0 {
		c.TimeoutMs = DefaultTimeoutMs
	}
	if c.Retry.MaxRetries > 0 && c.Retry.Interval == 0 {
		c.Retry.Interval = DefaultRetryInterval
	}
	if c.LogLevel == "" {
		c.LogLevel = DefaultLogLevel
	}
}

// Validate checks required fields and applies defaults for optional ones.
func (c *Conf) Validate() error {
	c.applyDefaults()

	if c.Scheme != SchemeHTTP && c.Scheme != SchemeHTTPS {
		return fmt.Errorf("nacos: Scheme must be %q or %q, got %q", SchemeHTTP, SchemeHTTPS, c.Scheme)
	}
	if c.Auth.Mode != AuthModeAuto && c.Auth.Mode != AuthModeRequired && c.Auth.Mode != AuthModeDisabled {
		return fmt.Errorf("nacos: Auth.Mode must be %q, %q or %q, got %q", AuthModeAuto, AuthModeRequired, AuthModeDisabled, c.Auth.Mode)
	}
	if c.Ipaddr == "" {
		return fmt.Errorf("nacos: Ipaddr is required")
	}
	if c.Port == 0 {
		return fmt.Errorf("nacos: Port is required")
	}
	if (c.Auth.UserName == "") != (c.Auth.Password == "") {
		return fmt.Errorf("nacos: Auth.UserName and Auth.Password must be both set or both empty")
	}
	if c.Auth.Mode == AuthModeRequired && (c.Auth.UserName == "" || c.Auth.Password == "") {
		return fmt.Errorf("nacos: Auth.UserName and Auth.Password are required when Auth.Mode is %q", AuthModeRequired)
	}
	if c.Auth.Mode == AuthModeDisabled && (c.Auth.UserName != "" || c.Auth.Password != "") {
		return fmt.Errorf("nacos: Auth.UserName/Auth.Password must be empty when Auth.Mode is %q", AuthModeDisabled)
	}
	if c.File == nil {
		return fmt.Errorf("nacos: File is required")
	}
	if c.Retry.MaxRetries < 0 {
		return fmt.Errorf("nacos: Retry.MaxRetries must be >= 0")
	}
	return nil
}

// ValidateWithDataId extends Validate to also require File.DataId and File.Group.
func (c *Conf) ValidateWithDataId() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.File.DataId == "" {
		return fmt.Errorf("nacos: File.DataId is required")
	}
	if c.File.Group == "" {
		return fmt.Errorf("nacos: File.Group is required")
	}
	return nil
}
