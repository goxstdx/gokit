package nacos

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	maxResponseBodySize = 10 << 20 // 10 MB
	tokenRefreshBuffer  = 30 * time.Second
)

// NacosError represents an HTTP-level error returned by Nacos.
type NacosError struct {
	StatusCode int
	Body       string
	Operation  string // "login" or "get_config"
}

func (e *NacosError) Error() string {
	return fmt.Sprintf("nacos %s failed: status=%d body=%s", e.Operation, e.StatusCode, e.Body)
}

type NacosHTTP struct {
	conf          Conf
	client        *http.Client
	listenHandles map[string]*listenHandle

	mu sync.Mutex

	tokenMu      sync.Mutex
	cachedToken  string
	tokenExpires time.Time
}

type listenHandle struct {
	cancel context.CancelFunc
}

type nacosLoginResponse struct {
	AccessToken string `json:"accessToken"`
	TokenTTL    int64  `json:"tokenTtl"`
}

// NewNacosHTTP creates a new NacosHTTP client.
// It validates configuration and applies defaults for optional fields.
func NewNacosHTTP(conf Conf) (*NacosHTTP, error) {
	if err := conf.Validate(); err != nil {
		return nil, err
	}

	return &NacosHTTP{
		conf: conf,
		client: &http.Client{
			Timeout: time.Duration(conf.TimeoutMs) * time.Millisecond,
		},
		listenHandles: make(map[string]*listenHandle),
	}, nil
}

// GetDefaultConfig fetches configuration using Conf.File.
func (n *NacosHTTP) GetDefaultConfig() (string, error) {
	return n.GetDefaultConfigWithContext(context.Background())
}

// GetDefaultConfigWithContext is like GetDefaultConfig but accepts a context.
func (n *NacosHTTP) GetDefaultConfigWithContext(ctx context.Context) (string, error) {
	if err := n.conf.ValidateWithDataId(); err != nil {
		return "", err
	}

	return n.getConfigWithRetry(ctx, n.conf.File)
}

// GetConfig fetches configuration for the specified dataId and group.
func (n *NacosHTTP) GetConfig(fileConf ConfigFile) (string, error) {
	return n.GetConfigWithContext(context.Background(), fileConf)
}

// GetConfigWithContext is like GetConfig but accepts a context.
func (n *NacosHTTP) GetConfigWithContext(ctx context.Context, fileConf ConfigFile) (string, error) {
	if err := fileConf.Validate(); err != nil {
		return "", err
	}

	return n.getConfigWithRetry(ctx, fileConf)
}

// ListenConfig starts polling for configuration changes using Conf.File.
func (n *NacosHTTP) ListenConfig(conf ListenConfig) error {
	return n.ListenConfigWithTarget(conf)
}

// ListenConfigWithTarget starts polling for configuration changes for the specified dataId and group.
func (n *NacosHTTP) ListenConfigWithTarget(conf ListenConfig) error {
	if err := conf.File.Validate(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	handle := &listenHandle{cancel: cancel}
	key := conf.File.Key()

	n.mu.Lock()
	if old, exists := n.listenHandles[key]; exists {
		old.cancel()
	}
	n.listenHandles[key] = handle
	n.mu.Unlock()

	lastData, err := n.getConfigWithRetry(ctx, conf.File)
	if err != nil {
		cancel()
		n.mu.Lock()
		if current, exists := n.listenHandles[key]; exists && current == handle {
			delete(n.listenHandles, key)
		}
		n.mu.Unlock()
		return err
	}

	go n.listenLoop(ctx, key, handle, conf.File, hashContent(lastData), conf.OnChange, conf.OnErr)

	return nil
}

// StopListenConfig stops all active listen loops.
func (n *NacosHTTP) StopListenConfig() {
	n.mu.Lock()
	handles := make([]*listenHandle, 0, len(n.listenHandles))
	for key, handle := range n.listenHandles {
		handles = append(handles, handle)
		delete(n.listenHandles, key)
	}
	n.mu.Unlock()

	for _, handle := range handles {
		handle.cancel()
	}
}

// StopListenConfigWithTarget stops the active listen loop for the specified target file.
func (n *NacosHTTP) StopListenConfigWithTarget(file ConfigFile) {
	key := file.Key()

	n.mu.Lock()
	handle, exists := n.listenHandles[key]
	if exists {
		delete(n.listenHandles, key)
	}
	n.mu.Unlock()

	if exists {
		handle.cancel()
	}
}

func (n *NacosHTTP) listenLoop(
	ctx context.Context,
	key string,
	handle *listenHandle,
	file ConfigFile,
	lastHash string,
	fn func(fileConf ConfigFile, data string),
	onErr func(err error),
) {
	ticker := time.NewTicker(n.pollInterval())
	defer func() {
		ticker.Stop()
		n.mu.Lock()
		if current, exists := n.listenHandles[key]; exists && current == handle {
			delete(n.listenHandles, key)
		}
		n.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current, getErr := n.getConfigWithRetry(ctx, file)
			if getErr != nil {
				if ctx.Err() != nil {
					return
				}
				n.reportErr(onErr, getErr)
				continue
			}

			currentHash := hashContent(current)
			if currentHash == lastHash {
				continue
			}

			lastHash = currentHash
			n.safeCallback(fn, file, current, onErr)
		}
	}
}

// safeCallback invokes fn with panic recovery; any panic is reported via onErr.
func (n *NacosHTTP) safeCallback(
	fn func(fileConf ConfigFile, data string),
	fileConf ConfigFile,
	data string,
	onErr func(err error),
) {
	if fn == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			n.reportErr(onErr, fmt.Errorf("nacos: callback panicked: %v", r))
		}
	}()
	fn(fileConf, data)
}

// getConfigWithRetry wraps getConfig with retry and 401-token-refresh logic.
func (n *NacosHTTP) getConfigWithRetry(ctx context.Context, file ConfigFile) (string, error) {
	var lastErr error

	for attempt := 0; attempt <= n.conf.Retry.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(n.conf.Retry.Interval):
			}
		}

		result, err := n.getConfig(ctx, file)
		if err == nil {
			return result, nil
		}
		lastErr = err

		if nacosErr, ok := err.(*NacosError); ok && nacosErr.StatusCode == http.StatusUnauthorized {
			if n.conf.Auth.Mode == AuthModeDisabled {
				return "", fmt.Errorf("nacos: received 401 while auth mode is disabled: %w", err)
			}
			if !n.hasAuthCredentials() {
				return "", fmt.Errorf(
					"nacos: received 401 but auth credentials are not configured (mode=%s): %w",
					n.conf.Auth.Mode,
					err,
				)
			}
			n.invalidateToken()
			continue
		}

		if ctx.Err() != nil {
			return "", ctx.Err()
		}
	}

	return "", fmt.Errorf("nacos: all %d attempts failed: %w", n.conf.Retry.MaxRetries+1, lastErr)
}

func (n *NacosHTTP) getConfig(ctx context.Context, file ConfigFile) (string, error) {
	token, err := n.fetchToken(ctx)
	if err != nil {
		return "", err
	}

	query := url.Values{}
	query.Set("dataId", file.DataId)
	query.Set("group", file.Group)
	if file.NamespaceId != "" {
		query.Set("tenant", file.NamespaceId)
	}
	if token != "" {
		query.Set("accessToken", token)
	}

	endpoint := fmt.Sprintf("%s/nacos/v1/cs/configs?%s", n.baseURL(), query.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", &NacosError{
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(body)),
			Operation:  "get_config",
		}
	}

	return string(body), nil
}

// fetchToken returns a cached token or fetches a new one when expired.
func (n *NacosHTTP) fetchToken(ctx context.Context) (string, error) {
	if n.conf.Auth.Mode == AuthModeDisabled {
		return "", nil
	}

	userName, password := n.authCredentials()
	if userName == "" || password == "" {
		return "", nil
	}

	n.tokenMu.Lock()
	defer n.tokenMu.Unlock()

	if n.cachedToken != "" && time.Now().Before(n.tokenExpires) {
		return n.cachedToken, nil
	}

	form := url.Values{}
	form.Set("username", userName)
	form.Set("password", password)

	endpoint := fmt.Sprintf("%s/nacos/v1/auth/login", n.baseURL())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := n.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", &NacosError{
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(body)),
			Operation:  "login",
		}
	}

	var loginResp nacosLoginResponse
	if unmarshalErr := json.Unmarshal(body, &loginResp); unmarshalErr != nil {
		return "", fmt.Errorf("nacos: parse login response failed: %w", unmarshalErr)
	}

	n.cachedToken = loginResp.AccessToken
	if loginResp.TokenTTL > 0 {
		n.tokenExpires = time.Now().Add(time.Duration(loginResp.TokenTTL)*time.Second - tokenRefreshBuffer)
	} else {
		n.tokenExpires = time.Now().Add(5 * time.Minute)
	}

	return n.cachedToken, nil
}

// invalidateToken clears the cached token, forcing a re-login on the next request.
func (n *NacosHTTP) invalidateToken() {
	n.tokenMu.Lock()
	defer n.tokenMu.Unlock()
	n.cachedToken = ""
	n.tokenExpires = time.Time{}
}

func (n *NacosHTTP) hasAuthCredentials() bool {
	userName, password := n.authCredentials()
	return userName != "" && password != ""
}

func (n *NacosHTTP) authCredentials() (string, string) {
	return n.conf.Auth.UserName, n.conf.Auth.Password
}

func (n *NacosHTTP) baseURL() string {
	return fmt.Sprintf("%s://%s:%d", n.conf.Scheme, n.conf.Ipaddr, n.conf.Port)
}

func (n *NacosHTTP) pollInterval() time.Duration {
	if n.conf.PollIntervalMs > 0 {
		return time.Duration(n.conf.PollIntervalMs) * time.Millisecond
	}
	return DefaultPollInterval
}

func (n *NacosHTTP) reportErr(onErr func(err error), err error) {
	if onErr != nil {
		onErr(err)
	}
}

func hashContent(content string) string {
	sum := md5.Sum([]byte(content))
	return hex.EncodeToString(sum[:])
}
