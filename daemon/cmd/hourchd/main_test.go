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
