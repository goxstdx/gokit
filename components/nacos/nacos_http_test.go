package nacos

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func getTestConf(t *testing.T) Conf {
	t.Helper()

	conf := Conf{
		Ipaddr: "192.168.96.247",
		Port:   18848,
		File: &ConfigFileConf{
			NamespaceId: "local",
			DataId:      "vehicle-rate-factor.yaml",
			Group:       "DEFAULT_GROUP",
		},
		Auth: &AuthConf{
			Mode:     AuthModeAuto,
			UserName: "test",
			Password: "8hJ1mxWS6LnntyT0",
		},
		PollIntervalMs:      5000,
		NotLoadCacheAtStart: true,
		LogDir:              "tmp/nacos/log/",
		CacheDir:            "tmp/nacos/cache",
	}
	if conf.Ipaddr == "" || conf.File == nil || conf.File.DataId == "" || conf.File.Group == "" {
		t.Skip("skip real nacos test: set NACOS_TEST_IP/NACOS_TEST_DATA_ID/NACOS_TEST_GROUP/NACOS_TEST_PORT")
	}

	return conf
}

func TestNacosHTTPGetDefaultConfig(t *testing.T) {
	client, err := NewNacosHTTP(getTestConf(t))
	if err != nil {
		t.Fatalf("NewNacosHTTP returned error: %v", err)
	}

	got, err := client.GetDefaultConfig()
	if err != nil {
		t.Fatalf("GetDefaultConfig returned error: %v", err)
	}
	if got == "" {
		t.Fatal("GetDefaultConfig returned empty content")
	}
}

func TestNacosHTTPGetConfig(t *testing.T) {
	conf := getTestConf(t)
	client, err := NewNacosHTTP(conf)
	if err != nil {
		t.Fatalf("NewNacosHTTP returned error: %v", err)
	}

	got, err := client.GetConfig(conf.File.DataId, conf.File.Group)
	if err != nil {
		t.Fatalf("GetConfig returned error: %v", err)
	}
	if got == "" {
		t.Fatal("GetConfig returned empty content")
	}
}

func TestNacosHTTPListenConfigHotUpdateByPolling(t *testing.T) {
	conf := getTestConf(t)
	client, err := NewNacosHTTP(conf)
	if err != nil {
		t.Fatalf("NewNacosHTTP returned error: %v", err)
	}
	defer client.StopListenConfig()

	origin, err := client.GetDefaultConfig()
	if err != nil {
		t.Fatalf("GetDefaultConfig before listen failed: %v", err)
	}

	updateCh := make(chan string, 4)
	errCh := make(chan error, 2)

	err = client.ListenConfig(
		func(dataID, data string) {
			if dataID != conf.File.DataId {
				errCh <- &testError{msg: "unexpected dataId: " + dataID}
				return
			}
			updateCh <- data
		}, func(listenErr error) {
			errCh <- listenErr
		},
	)
	if err != nil {
		t.Fatalf("ListenConfig returned error: %v", err)
	}

	time.Sleep(5 * time.Second)

	latest, err := client.GetDefaultConfig()
	if err != nil {
		t.Fatalf("GetDefaultConfig after 5s failed: %v", err)
	}
	if latest == origin {
		t.Fatal("config is unchanged after 5s, treat as failure by default")
	}

	timeout := time.After(10 * time.Second)
	for {
		select {
		case e := <-errCh:
			t.Fatalf("listen returned async error: %v", e)
		case data := <-updateCh:
			if data == latest {
				return
			}
		case <-timeout:
			t.Fatalf("timeout waiting hot update callback, latest=%q", latest)
		}
	}
}

func TestNacosHTTPGetConfigSpecifiedMissingFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("dataId") == "not-exist.yaml" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("config data not exist"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	conf := mockConfFromServer(t, server.URL)
	client, err := NewNacosHTTP(conf)
	if err != nil {
		t.Fatalf("NewNacosHTTP returned error: %v", err)
	}

	_, err = client.GetConfig("not-exist.yaml", "DEFAULT_GROUP")
	if err == nil {
		t.Fatal("expected error for missing config file")
	}

	var nacosErr *NacosError
	if !errors.As(err, &nacosErr) {
		t.Fatalf("expected NacosError, got %T: %v", err, err)
	}
	if nacosErr.StatusCode != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", nacosErr.StatusCode)
	}
}

func TestNacosHTTPListenConfigWithTargetSpecifiedMissingFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("dataId") == "not-exist.yaml" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("config data not exist"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	conf := mockConfFromServer(t, server.URL)
	client, err := NewNacosHTTP(conf)
	if err != nil {
		t.Fatalf("NewNacosHTTP returned error: %v", err)
	}

	err = client.ListenConfigWithTarget("not-exist.yaml", "DEFAULT_GROUP", nil, nil)
	if err == nil {
		t.Fatal("expected ListenConfigWithTarget to fail for missing config file")
	}
}

func mockConfFromServer(t *testing.T, serverURL string) Conf {
	t.Helper()

	parsed, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("parse server url failed: %v", err)
	}
	host, portStr, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("parse host:port failed: %v", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		t.Fatalf("parse port failed: %v", err)
	}

	return Conf{
		Scheme: Scheme(parsed.Scheme),
		Ipaddr: host,
		Port:   port,
		File: &ConfigFileConf{
			DataId: "exist.yaml",
			Group:  "DEFAULT_GROUP",
		},
		Auth: &AuthConf{
			Mode: AuthModeDisabled,
		},
		Retry: &RetryConf{
			MaxRetries: 0,
		},
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
