package nacos

import (
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

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
