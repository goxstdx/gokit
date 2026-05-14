package nacos

import "testing"

func TestConfValidateApplyDefaults(t *testing.T) {
	conf := Conf{
		Ipaddr: "127.0.0.1",
		Port:   8848,
	}

	if err := conf.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	if conf.Scheme != DefaultScheme {
		t.Fatalf("expected default scheme %q, got %q", DefaultScheme, conf.Scheme)
	}
	if conf.File == nil {
		t.Fatal("expected File to be initialized")
	}
	if conf.File.Group != DefaultGroup {
		t.Fatalf("expected default group %q, got %q", DefaultGroup, conf.File.Group)
	}
	if conf.Auth == nil {
		t.Fatal("expected Auth to be initialized")
	}
	if conf.Auth.Mode != DefaultAuthMode {
		t.Fatalf("expected default auth mode %q, got %q", DefaultAuthMode, conf.Auth.Mode)
	}
	if conf.Retry == nil {
		t.Fatal("expected Retry to be initialized")
	}
	if conf.Retry.MaxRetries != DefaultRetryCount {
		t.Fatalf("expected default retry count %d, got %d", DefaultRetryCount, conf.Retry.MaxRetries)
	}
	if conf.Retry.Interval != DefaultRetryInterval {
		t.Fatalf("expected default retry interval %s, got %s", DefaultRetryInterval, conf.Retry.Interval)
	}
}

func TestConfValidateRetryZeroMeansNoRetry(t *testing.T) {
	conf := Conf{
		Ipaddr: "127.0.0.1",
		Port:   8848,
		Retry: &RetryConf{
			MaxRetries: 0,
		},
	}

	if err := conf.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
	if conf.Retry.MaxRetries != 0 {
		t.Fatalf("expected retry count to keep 0, got %d", conf.Retry.MaxRetries)
	}
	if conf.Retry.Interval != 0 {
		t.Fatalf("expected retry interval to keep 0 when retries are disabled, got %s", conf.Retry.Interval)
	}
}

func TestConfValidateAuthRequiredNeedsCredentials(t *testing.T) {
	conf := Conf{
		Ipaddr: "127.0.0.1",
		Port:   8848,
		Auth: &AuthConf{
			Mode: AuthModeRequired,
		},
	}

	if err := conf.Validate(); err == nil {
		t.Fatal("expected error when auth mode is required without credentials")
	}
}

func TestConfValidateWithDataIdRequiresFileTarget(t *testing.T) {
	conf := Conf{
		Ipaddr: "127.0.0.1",
		Port:   8848,
		File: &ConfigFileConf{
			Group: "G1",
		},
	}

	if err := conf.ValidateWithDataId(); err == nil {
		t.Fatal("expected error when File.DataId is empty")
	}
}
