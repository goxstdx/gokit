package nacos

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"
)

func getTestConf(t *testing.T) Conf {
	t.Helper()

	conf := Conf{
		Ipaddr: "192.168.96.247",
		Port:   18848,
		File: &ConfigFile{
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

	got, err := client.GetConfig(*conf.File)
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
		ListenConfig{
			File: *conf.File,
			OnChange: func(file ConfigFile, data string) {
				if file.DataId != conf.File.DataId {
					errCh <- &testError{msg: "unexpected dataId: " + file.DataId}
					return
				}
				updateCh <- data
			},
			OnErr: func(listenErr error) {
				errCh <- listenErr
			},
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

func TestNacosHTTPListenConfigWithMultipleTargetsAndStopByTarget(t *testing.T) {
	type event struct {
		file ConfigFile
		data string
	}

	state := struct {
		sync.Mutex
		data map[string]string
	}{
		data: map[string]string{
			"a.yaml": "a-v1",
			"b.yaml": "b-v1",
		},
	}

	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				dataID := r.URL.Query().Get("dataId")
				state.Lock()
				content, ok := state.data[dataID]
				state.Unlock()
				if !ok {
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte("config data not exist"))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(content))
			},
		),
	)
	defer server.Close()

	conf := mockConfFromServer(t, server.URL)
	conf.PollIntervalMs = 50
	client, err := NewNacosHTTP(conf)
	if err != nil {
		t.Fatalf("NewNacosHTTP returned error: %v", err)
	}
	defer client.StopListenConfig()

	fileA := ConfigFile{
		NamespaceId: "local",
		DataId:      "a.yaml",
		Group:       "DEFAULT_GROUP",
	}
	fileB := ConfigFile{
		NamespaceId: "local",
		DataId:      "b.yaml",
		Group:       "DEFAULT_GROUP",
	}

	events := make(chan event, 16)
	errCh := make(chan error, 4)
	onChange := func(file ConfigFile, data string) {
		events <- event{file: file, data: data}
	}

	if err := client.ListenConfigWithTarget(ListenConfig{File: fileA, OnChange: onChange, OnErr: func(err error) { errCh <- err }}); err != nil {
		t.Fatalf("ListenConfigWithTarget(fileA) returned error: %v", err)
	}
	if err := client.ListenConfigWithTarget(ListenConfig{File: fileB, OnChange: onChange, OnErr: func(err error) { errCh <- err }}); err != nil {
		t.Fatalf("ListenConfigWithTarget(fileB) returned error: %v", err)
	}

	state.Lock()
	state.data["a.yaml"] = "a-v2"
	state.data["b.yaml"] = "b-v2"
	state.Unlock()

	gotA2 := false
	gotB2 := false
	timeout := time.After(3 * time.Second)
	for !(gotA2 && gotB2) {
		select {
		case asyncErr := <-errCh:
			t.Fatalf("listen returned async error: %v", asyncErr)
		case ev := <-events:
			if ev.file.Key() == fileA.Key() && ev.data == "a-v2" {
				gotA2 = true
			}
			if ev.file.Key() == fileB.Key() && ev.data == "b-v2" {
				gotB2 = true
			}
		case <-timeout:
			t.Fatalf("timeout waiting both listeners, gotA2=%v gotB2=%v", gotA2, gotB2)
		}
	}

	client.StopListenConfigWithTarget(fileA)

	for len(events) > 0 {
		<-events
	}

	state.Lock()
	state.data["a.yaml"] = "a-v3"
	state.data["b.yaml"] = "b-v3"
	state.Unlock()

	gotB3 := false
	timeout = time.After(3 * time.Second)
	for !gotB3 {
		select {
		case asyncErr := <-errCh:
			t.Fatalf("listen returned async error after stop target: %v", asyncErr)
		case ev := <-events:
			if ev.file.Key() == fileA.Key() && ev.data == "a-v3" {
				t.Fatalf("fileA should have been stopped, but received update %q", ev.data)
			}
			if ev.file.Key() == fileB.Key() && ev.data == "b-v3" {
				gotB3 = true
			}
		case <-timeout:
			t.Fatal("timeout waiting fileB update after stopping fileA listener")
		}
	}
}

func TestNacosHTTPGetConfigSpecifiedMissingFile(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("dataId") == "not-exist.yaml" {
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte("config data not exist"))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
			},
		),
	)
	defer server.Close()

	conf := mockConfFromServer(t, server.URL)
	client, err := NewNacosHTTP(conf)
	if err != nil {
		t.Fatalf("NewNacosHTTP returned error: %v", err)
	}

	_, err = client.GetConfig(
		ConfigFile{
			NamespaceId: conf.File.NamespaceId,
			DataId:      "not-exist.yaml",
			Group:       "DEFAULT_GROUP",
		},
	)
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
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("dataId") == "not-exist.yaml" {
					w.WriteHeader(http.StatusNotFound)
					_, _ = w.Write([]byte("config data not exist"))
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ok"))
			},
		),
	)
	defer server.Close()

	conf := mockConfFromServer(t, server.URL)
	client, err := NewNacosHTTP(conf)
	if err != nil {
		t.Fatalf("NewNacosHTTP returned error: %v", err)
	}

	err = client.ListenConfigWithTarget(
		ListenConfig{
			File: ConfigFile{
				NamespaceId: conf.File.NamespaceId,
				DataId:      "not-exist.yaml",
				Group:       "DEFAULT_GROUP",
			},
		},
	)
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
		File: &ConfigFile{
			NamespaceId: "local",
			DataId:      "exist.yaml",
			Group:       "DEFAULT_GROUP",
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
