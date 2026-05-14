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
	defaultHTTPTimeout  = 3 * time.Second
	defaultPollInterval = 5 * time.Second
)

type NacosHTTP struct {
	Conf Conf

	client *http.Client

	mu           sync.Mutex
	listenCancel context.CancelFunc
}

type nacosLoginResponse struct {
	AccessToken string `json:"accessToken"`
	TokenTTL    int64  `json:"tokenTtl"`
}

func NewNacosHTTP(conf Conf) *NacosHTTP {
	timeout := defaultHTTPTimeout
	if conf.TimeoutMs > 0 {
		timeout = time.Duration(conf.TimeoutMs) * time.Millisecond
	}

	return &NacosHTTP{
		Conf: conf,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (n *NacosHTTP) GetConfig() (string, error) {
	return n.getConfig(context.Background())
}

func (n *NacosHTTP) ListenConfig(fn func(dataId, data string), onErr func(err error)) error {
	n.mu.Lock()
	if n.listenCancel != nil {
		n.listenCancel()
		n.listenCancel = nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.listenCancel = cancel
	n.mu.Unlock()

	lastData, err := n.getConfig(ctx)
	if err != nil {
		n.mu.Lock()
		n.listenCancel = nil
		n.mu.Unlock()
		cancel()
		return err
	}

	go n.listenLoop(ctx, n.pollInterval(), hashContent(lastData), fn, onErr)
	return nil
}

func (n *NacosHTTP) StopListenConfig() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.listenCancel != nil {
		n.listenCancel()
		n.listenCancel = nil
	}
}

func (n *NacosHTTP) listenLoop(ctx context.Context, pollInterval time.Duration, lastHash string, fn func(dataId, data string), onErr func(err error)) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current, getErr := n.getConfig(ctx)
			if getErr != nil {
				n.reportErr(onErr, getErr)
				continue
			}

			currentHash := hashContent(current)
			if currentHash == lastHash {
				continue
			}

			lastHash = currentHash
			if fn != nil {
				fn(n.Conf.DataId, current)
			}
		}
	}
}

func (n *NacosHTTP) getConfig(ctx context.Context) (string, error) {
	token, err := n.fetchToken(ctx)
	if err != nil {
		return "", err
	}

	query := url.Values{}
	query.Set("dataId", n.Conf.DataId)
	query.Set("group", n.Conf.Group)
	if n.Conf.NamespaceId != "" {
		query.Set("tenant", n.Conf.NamespaceId)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("nacos get config failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return string(body), nil
}

func (n *NacosHTTP) fetchToken(ctx context.Context) (string, error) {
	if n.Conf.UserName == "" || n.Conf.Password == "" {
		return "", nil
	}

	form := url.Values{}
	form.Set("username", n.Conf.UserName)
	form.Set("password", n.Conf.Password)

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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("nacos login failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var loginResp nacosLoginResponse
	if unmarshalErr := json.Unmarshal(body, &loginResp); unmarshalErr != nil {
		return "", fmt.Errorf("parse nacos login response failed: %w", unmarshalErr)
	}

	return loginResp.AccessToken, nil
}

func (n *NacosHTTP) baseURL() string {
	return fmt.Sprintf("http://%s:%d", n.Conf.Ipaddr, n.Conf.Port)
}

func (n *NacosHTTP) pollInterval() time.Duration {
	if n.Conf.PollIntervalMs > 0 {
		return time.Duration(n.Conf.PollIntervalMs) * time.Millisecond
	}
	return defaultPollInterval
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
