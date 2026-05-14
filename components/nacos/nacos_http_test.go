package nacos

import (
	"testing"
	"time"
)

func getConfig(t *testing.T) Conf {
	t.Helper()

	conf := Conf{
		Ipaddr:              "192.168.96.247",
		Port:                18848,
		NamespaceId:         "local",
		DataId:              "vehicle-rate-factor.yaml",
		Group:               "DEFAULT_GROUP",
		UserName:            "test",
		Password:            "8hJ1mxWS6LnntyT0",
		TimeoutMs:           0,
		PollIntervalMs:      5000,
		NotLoadCacheAtStart: true,
		LogDir:              "tmp/nacos/log/",
		CacheDir:            "tmp/nacos/cache",
		LogLevel:            "info",
	}
	if conf.Ipaddr == "" || conf.DataId == "" || conf.Group == "" {
		t.Skip("skip real nacos test: set NACOS_TEST_IP/NACOS_TEST_DATA_ID/NACOS_TEST_GROUP/NACOS_TEST_PORT")
	}

	return conf
}

func TestNacosHTTPGetConfig(t *testing.T) {
	client := NewNacosHTTP(getConfig(t))

	got, err := client.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig returned error: %v", err)
	}
	if got == "" {
		t.Fatal("GetConfig returned empty content")
	}
}

func TestNacosHTTPListenConfigHotUpdateByPolling(t *testing.T) {
	conf := getConfig(t)
	client := NewNacosHTTP(conf)
	defer client.StopListenConfig()

	origin, err := client.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig before listen failed: %v", err)
	}

	updateCh := make(chan string, 4)
	errCh := make(chan error, 2)

	err = client.ListenConfig(
		func(dataID, data string) {
			if dataID != conf.DataId {
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

	latest, err := client.GetConfig()
	if err != nil {
		t.Fatalf("GetConfig after 5s failed: %v", err)
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
