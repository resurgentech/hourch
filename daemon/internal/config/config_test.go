package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExampleConfigMatchesDefaults(t *testing.T) {
	data, err := os.ReadFile("../../config.yaml.example")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := Parse(data)
	if err != nil {
		t.Fatalf("example config: %v", err)
	}
	// minio.endpoint is required and powerwarden.url is optional with no
	// default, so those two take the example's values; every other key is the default.
	def := Default()
	def.MinIO.Endpoint = cfg.MinIO.Endpoint
	def.Powerwarden.URL = cfg.Powerwarden.URL
	if cfg.MinIO.Endpoint == "" || cfg.Powerwarden.URL == "" || !reflect.DeepEqual(cfg, def) {
		t.Fatalf("example != defaults\nexample: %+v\ndefault: %+v", cfg, def)
	}
	// Every other key in the example is explicit: a document with only the
	// required key gives the same, except powerwarden is disabled.
	min, err := parseReq("")
	if err != nil {
		t.Fatal(err)
	}
	def.MinIO.Endpoint = reqEndpoint
	def.Powerwarden.URL = ""
	if !reflect.DeepEqual(min, def) {
		t.Fatalf("minimal config differs from defaults\nminimal: %+v\ndefault: %+v", min, def)
	}
}

const reqEndpoint = "minio.example:9000"

// parseReq parses y after the one key that is required and has no default,
// so tests of other keys are not decided by minio.endpoint.
func parseReq(y string) (Config, error) {
	return Parse([]byte("minio:\n  endpoint: " + reqEndpoint + "\n" + y))
}

func TestMinioEndpointRequiredPowerwardenOptional(t *testing.T) {
	d := Default()
	if d.MinIO.Endpoint != "" || d.Powerwarden.URL != "" {
		t.Fatalf("lab values in defaults: %q %q", d.MinIO.Endpoint, d.Powerwarden.URL)
	}
	if _, err := Parse(nil); err == nil || !strings.Contains(err.Error(), "minio.endpoint is required") {
		t.Fatalf("Parse(nil) = %v", err)
	}
	cfg, err := parseReq("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Powerwarden.URL != "" {
		t.Fatalf("powerwarden should be disabled by default: %q", cfg.Powerwarden.URL)
	}
}

func TestParseUnitsAndOverrides(t *testing.T) {
	cfg, err := parseReq("retention:\n  rows: 7d\nlimits:\n  context_max_bytes: 10MiB\n  artifact_max_bytes: 1234\ndispatch:\n  lease: 60s\n")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Retention.Rows.D() != 7*24*time.Hour {
		t.Fatalf("rows = %v", cfg.Retention.Rows.D())
	}
	if cfg.Limits.ContextMaxBytes != 10<<20 || cfg.Limits.ArtifactMaxBytes != 1234 {
		t.Fatalf("limits = %+v", cfg.Limits)
	}
	if cfg.Dispatch.Lease.D() != time.Minute || cfg.Dispatch.Heartbeat.D() != 10*time.Second {
		t.Fatalf("dispatch = %+v", cfg.Dispatch)
	}
}

func TestDisableOptionalParts(t *testing.T) {
	cfg, err := parseReq("powerwarden:\n  url: \"\"\n  token_file: \"\"\nui:\n  enabled: false\n  htpasswd_file: \"\"\n")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Powerwarden.URL != "" || cfg.UI.Enabled {
		t.Fatalf("not disabled: %+v %+v", cfg.Powerwarden, cfg.UI)
	}
}

// The secrets key file is gone: the key is generated at startup. A config
// that still sets it fails with an explanation, not the generic error.
func TestRemovedSecretsKeyFile(t *testing.T) {
	for _, doc := range []string{"secrets:\n  key_file: /etc/hourchd/secrets.key\n", "secrets: {}\n"} {
		_, err := parseReq(doc)
		if err == nil || !strings.Contains(err.Error(), "was removed") || !strings.Contains(err.Error(), "generated at startup") {
			t.Fatalf("%q: want a removed-key error, got %v", doc, err)
		}
	}
	_, err := parseReq("secrets:\n  key_file: /x\n")
	if !strings.Contains(err.Error(), `"secrets.key_file"`) {
		t.Fatalf("error does not name secrets.key_file: %v", err)
	}
}

func TestParseRejects(t *testing.T) {
	cases := map[string]string{
		"unknown top-level key":    "listne: \":1\"\n",
		"unknown nested key":       "dispatch:\n  leese: 1s\n",
		"duration without unit":    "dispatch:\n  lease: 45\n",
		"bad duration":             "retention:\n  rows: forever\n",
		"zero duration":            "dispatch:\n  reaper_interval: 0s\n",
		"bad size":                 "limits:\n  context_max_bytes: lots\n",
		"bad log level":            "log_level: loud\n",
		"heartbeat >= lease":       "dispatch:\n  heartbeat: 60s\n",
		"fractional lease":         "dispatch:\n  lease: 45500ms\n",
		"secret-looking key":       "minio:\n  endpoint: " + reqEndpoint + "\n  secret_key: abc\n",
		"prefix without slash":     "backup:\n  minio_prefix: backups\n",
		"zero jobs per run":        "limits:\n  max_jobs_per_run: 0\n",
		"missing required setting": "state_dir: \"\"\n",
		"powerwarden url scheme":   "powerwarden:\n  url: powerwarden.labtwo\n",
		"powerwarden no token":     "powerwarden:\n  url: http://powerwarden.example\n  token_file: \"\"\n",
		"ui without htpasswd":      "ui:\n  htpasswd_file: \"\"\n",
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if strings.HasPrefix(doc, "minio:") {
				if _, err := Parse([]byte(doc)); err == nil {
					t.Fatalf("want error for %q", doc)
				}
				return
			}
			if _, err := parseReq(doc); err == nil {
				t.Fatalf("want error for %q", doc)
			}
		})
	}
}

func TestUnschedulableTimeout(t *testing.T) {
	if Default().Dispatch.UnschedulableTimeout.D() != 30*time.Minute {
		t.Fatalf("default = %v", Default().Dispatch.UnschedulableTimeout.D())
	}
	cfg, err := parseReq("dispatch:\n  unschedulable_timeout: 0s\n")
	if err != nil || cfg.Dispatch.UnschedulableTimeout != 0 {
		t.Fatalf("0s: %v %v", cfg.Dispatch.UnschedulableTimeout.D(), err)
	}
	if _, err := parseReq("dispatch:\n  unschedulable_timeout: -1m\n"); err == nil || !strings.Contains(err.Error(), "unschedulable_timeout") {
		t.Fatalf("negative: %v", err)
	}
}

func TestUIPathPrefix(t *testing.T) {
	if got := Default().UI.PathPrefix; got != "" {
		t.Fatalf("default path_prefix = %q", got)
	}
	for _, p := range []string{"", "/p", "/a/b"} {
		c, err := parseReq("ui:\n  path_prefix: \"" + p + "\"\n")
		if err != nil || c.UI.PathPrefix != p {
			t.Errorf("path_prefix %q: %v, %q", p, err, c.UI.PathPrefix)
		}
	}
	for _, p := range []string{"p", "/p/", "/", "/a/../b", "/.."} {
		_, err := parseReq("ui:\n  path_prefix: \"" + p + "\"\n")
		if err == nil || !strings.Contains(err.Error(), "ui.path_prefix") {
			t.Errorf("path_prefix %q: want ui.path_prefix error, got %v", p, err)
		}
	}
}

const proxyOK = "ui:\n  auth: proxy\n  proxy:\n    secret_file: /etc/hourchd/proxy.secret\n    secret_header: X-Auth-Proxy-Secret\n    user_header: Remote-User\n"

func TestUIAuth(t *testing.T) {
	d := Default().UI
	if d.Auth != "htpasswd" || d.Proxy != (UIProxy{}) {
		t.Fatalf("defaults: %+v", d)
	}
	c, err := parseReq(proxyOK)
	if err != nil || c.UI.Auth != "proxy" || c.UI.Proxy.UserHeader != "Remote-User" || c.UI.Proxy.SecretHeader != "X-Auth-Proxy-Secret" {
		t.Fatalf("proxy ok: %v %+v", err, c.UI)
	}
	// htpasswd_file is not needed in proxy mode.
	if _, err := parseReq(proxyOK + "  htpasswd_file: \"\"\n"); err != nil {
		t.Fatalf("proxy without htpasswd_file: %v", err)
	}
	// Unknown auth value.
	if _, err := parseReq("ui:\n  auth: ldap\n"); err == nil || !strings.Contains(err.Error(), "ui.auth") {
		t.Fatalf("unknown auth: %v", err)
	}
	// htpasswd mode still needs the file, and ignores ui.proxy.
	if _, err := parseReq("ui:\n  htpasswd_file: \"\"\n"); err == nil || !strings.Contains(err.Error(), "ui.htpasswd_file") {
		t.Fatalf("htpasswd without file: %v", err)
	}
	if _, err := parseReq("ui:\n  proxy:\n    user_header: \"bad header\"\n"); err != nil {
		t.Fatalf("proxy keys ignored under htpasswd: %v", err)
	}
	// Each proxy key is required.
	for _, key := range []string{"secret_file", "secret_header", "user_header"} {
		var y strings.Builder
		for _, line := range strings.SplitAfter(proxyOK, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), key+":") {
				y.WriteString(line)
			}
		}
		if _, err := parseReq(y.String()); err == nil || !strings.Contains(err.Error(), "ui.proxy."+key) {
			t.Errorf("missing %s: %v", key, err)
		}
	}
	// Header names: valid tokens, and different.
	for _, bad := range []string{"a b", "a:b", "a/b", "é", "a\\tb"} {
		y := strings.Replace(proxyOK, "Remote-User", "\""+bad+"\"", 1)
		if _, err := parseReq(y); err == nil || !strings.Contains(err.Error(), "ui.proxy.user_header") {
			t.Errorf("user_header %q: %v", bad, err)
		}
	}
	if _, err := parseReq(strings.Replace(proxyOK, "Remote-User", "x-auth-proxy-secret", 1)); err == nil || !strings.Contains(err.Error(), "differ") {
		t.Errorf("equal headers: %v", err)
	}
	// ui.enabled: false skips all of it.
	if _, err := parseReq("ui:\n  enabled: false\n  auth: bogus\n"); err != nil {
		t.Errorf("disabled UI: %v", err)
	}
}
