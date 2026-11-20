package main

import (
	"testing"

	"github.com/resurgentech/hourch/daemon/internal/config"
)

// check-config's success line: unchanged in htpasswd mode (and with the UI
// off); proxy mode is made visible.
func TestCheckSummary(t *testing.T) {
	cfg := config.Default()
	if got := checkSummary(cfg); got != "config ok" {
		t.Errorf("htpasswd: %q", got)
	}
	cfg.UI.Enabled = false
	cfg.UI.Auth = "proxy"
	if got := checkSummary(cfg); got != "config ok" {
		t.Errorf("ui disabled: %q", got)
	}
	cfg.UI.Enabled = true
	if got := checkSummary(cfg); got != "config ok ui=proxy" {
		t.Errorf("proxy: %q", got)
	}
	// personal_keys.request_url set: " key_request=on", after ui=proxy.
	cfg.PersonalKeys.RequestURL = "https://provider.example/v1/services/example/requests"
	if got := checkSummary(cfg); got != "config ok ui=proxy key_request=on" {
		t.Errorf("proxy and key request: %q", got)
	}
	cfg.UI.Auth = "htpasswd"
	if got := checkSummary(cfg); got != "config ok key_request=on" {
		t.Errorf("key request: %q", got)
	}
}

// personal_keys.url set: " key_list=on" after key_request=on; plain http with
// allow_insecure_http adds " key_list_insecure_http=on"; nothing without url.
func TestCheckSummaryKeyList(t *testing.T) {
	cfg := config.Default()
	cfg.PersonalKeys.AllowInsecureHTTP = true // meaningless without url
	if got := checkSummary(cfg); got != "config ok" {
		t.Errorf("no url: %q", got)
	}
	cfg.PersonalKeys.AllowInsecureHTTP = false
	cfg.PersonalKeys.URL = "https://keys.example/v1/lists/hourchd"
	if got := checkSummary(cfg); got != "config ok key_list=on" {
		t.Errorf("https: %q", got)
	}
	cfg.PersonalKeys.AllowInsecureHTTP = true
	if got := checkSummary(cfg); got != "config ok key_list=on" {
		t.Errorf("https with allow_insecure_http: %q", got)
	}
	cfg.PersonalKeys.URL = "http://keys.example/v1/lists/hourchd"
	if got := checkSummary(cfg); got != "config ok key_list=on key_list_insecure_http=on" {
		t.Errorf("http with allow_insecure_http: %q", got)
	}
	cfg.PersonalKeys.URL = "http://127.0.0.1:8080/v1/lists/hourchd"
	if got := checkSummary(cfg); got != "config ok key_list=on key_list_insecure_http=on" {
		t.Errorf("loopback http with allow_insecure_http: %q", got)
	}
	cfg.PersonalKeys.AllowInsecureHTTP = false
	if got := checkSummary(cfg); got != "config ok key_list=on" {
		t.Errorf("loopback http: %q", got)
	}
	cfg.UI.Auth = "proxy"
	cfg.PersonalKeys.RequestURL = "https://keys.example/v1/services/hourchd/requests"
	cfg.PersonalKeys.URL = "http://keys.example/x"
	cfg.PersonalKeys.AllowInsecureHTTP = true
	if got := checkSummary(cfg); got != "config ok ui=proxy key_request=on key_list=on key_list_insecure_http=on" {
		t.Errorf("all: %q", got)
	}
}
