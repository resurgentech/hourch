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
}
