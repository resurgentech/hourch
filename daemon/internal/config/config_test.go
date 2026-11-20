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
	for _, p := range []string{"", "/p", "/a/b", "/svc-1/a.b_c~d"} {
		c, err := parseReq("ui:\n  path_prefix: \"" + p + "\"\n")
		if err != nil || c.UI.PathPrefix != p {
			t.Errorf("path_prefix %q: %v, %q", p, err, c.UI.PathPrefix)
		}
	}
	for _, p := range []string{"p", "/p/", "/", "/a/../b", "/..", "/a//b", "/a b", "/a{b}", "/a?b", "/a#b", "/a%2fb", "/./a", "//p"} {
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
	// htpasswd mode still needs the file.
	if _, err := parseReq("ui:\n  htpasswd_file: \"\"\n"); err == nil || !strings.Contains(err.Error(), "ui.htpasswd_file") {
		t.Fatalf("htpasswd without file: %v", err)
	}
	// ui.proxy.* under htpasswd (unset or explicit) is an error naming the key.
	for _, auth := range []string{"", "  auth: htpasswd\n"} {
		for _, key := range []string{"secret_file", "secret_header", "user_header"} {
			y := "ui:\n" + auth + "  proxy:\n    " + key + ": X-Something\n"
			if _, err := parseReq(y); err == nil || !strings.Contains(err.Error(), "ui.proxy."+key) {
				t.Errorf("%q: want ui.proxy.%s error, got %v", y, key, err)
			}
		}
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

// personal_keys.request_url: unset by default (GET /v1/key-request is 404);
// when set it must be an absolute http or https URL, as the key-request
// contract requires of what discovery returns.
func TestPersonalKeysRequestURL(t *testing.T) {
	want := PersonalKeys{Refresh: Duration(time.Minute), RefreshOnMiss: Duration(5 * time.Second)}
	if got := Default().PersonalKeys; got != want {
		t.Fatalf("default personal_keys = %+v, want %+v", got, want)
	}
	for _, u := range []string{"", "https://provider.example/v1/services/example/requests", "http://10.0.0.1:8080/r", "HTTPS://provider.example/r", "Http://provider.example/r?x=1"} {
		c, err := parseReq("personal_keys:\n  request_url: \"" + u + "\"\n")
		if err != nil || c.PersonalKeys.RequestURL != u {
			t.Errorf("request_url %q: %v, %q", u, err, c.PersonalKeys.RequestURL)
		}
	}
	for _, u := range []string{"provider.example/r", "/v1/r", "ftp://provider.example/r", "https://", "https:///r", "http//x", " https://provider.example/r",
		"https://user:pass@provider.example/r", "https://user@provider.example/r", "https://provider.example/r#frag", "https://provider.example/r#"} {
		_, err := parseReq("personal_keys:\n  request_url: \"" + u + "\"\n")
		if err == nil || !strings.Contains(err.Error(), "personal_keys.request_url") {
			t.Errorf("request_url %q: want personal_keys.request_url error, got %v", u, err)
		}
	}
	// A refused URL with user info is not echoed (it may hold a password).
	for _, u := range []string{"https://user:s3cretpw@provider.example/r", "ftp://user:s3cretpw@provider.example/r"} {
		if _, err := parseReq("personal_keys:\n  request_url: \"" + u + "\"\n"); err == nil || strings.Contains(err.Error(), "s3cretpw") {
			t.Errorf("request_url %q: error %v must exist and not contain the password", u, err)
		}
	}
}

const pkOK = "personal_keys:\n  url: https://keys.example/v1/lists/hourchd\n  cache_file: /var/lib/hourchd/personal-keys.json\n"

func TestPersonalKeysKeyListDefaultsAndValues(t *testing.T) {
	c, err := parseReq(pkOK)
	if err != nil {
		t.Fatal(err)
	}
	p := c.PersonalKeys
	if p.URL != "https://keys.example/v1/lists/hourchd" || p.CacheFile != "/var/lib/hourchd/personal-keys.json" ||
		p.TokenFile != "" || p.Refresh.D() != time.Minute || p.RefreshOnMiss.D() != 5*time.Second || p.AllowInsecureHTTP {
		t.Fatalf("defaults: %+v", p)
	}
	c, err = parseReq(pkOK + "  token_file: /etc/t\n  refresh: 30s\n  refresh_on_miss: 0s\n  allow_insecure_http: true\n")
	if err != nil {
		t.Fatal(err)
	}
	p = c.PersonalKeys
	if p.TokenFile != "/etc/t" || p.Refresh.D() != 30*time.Second || p.RefreshOnMiss != 0 || !p.AllowInsecureHTTP {
		t.Fatalf("values: %+v", p)
	}
	// request_url and the key list are independent.
	if c, err = parseReq(pkOK + "  request_url: https://keys.example/r\n"); err != nil || c.PersonalKeys.RequestURL == "" {
		t.Fatalf("both: %v", err)
	}
}

// TestPersonalKeysDurationNeedsUnit: like every hourchd duration, a bare
// number is refused when decoding; "0s" turns refresh on miss off.
func TestPersonalKeysDurationNeedsUnit(t *testing.T) {
	for _, y := range []string{"  refresh_on_miss: 0\n", "  refresh: 60\n"} {
		_, err := parseReq(pkOK + y)
		if err == nil || !strings.Contains(err.Error(), "must be a string with a unit, like 30s or 90d") {
			t.Errorf("%q: err = %v, want the unit error", y, err)
		}
	}
	c, err := parseReq(pkOK + "  refresh_on_miss: 0s\n")
	if err != nil || c.PersonalKeys.RefreshOnMiss != 0 {
		t.Fatalf("0s: %v %+v", err, c.PersonalKeys)
	}
}

// SR-2: the exact messages.
func TestPersonalKeysKeyListErrors(t *testing.T) {
	const wantURL = `want an absolute http(s)://host/... URL (or "" to turn the key list off)`
	cases := []struct{ name, y, want string }{
		{"relative", "url: keys.example/x\ncache_file: /c\n", `personal_keys.url "keys.example/x": ` + wantURL},
		{"scheme", "url: ftp://keys.example/x\ncache_file: /c\n", `personal_keys.url "ftp://keys.example/x": ` + wantURL},
		{"no host", "url: https:///x\ncache_file: /c\n", `personal_keys.url "https:///x": ` + wantURL},
		{"userinfo redacted", "url: https://bob:s3cretpw@keys.example/x\ncache_file: /c\n", `personal_keys.url "https://bob:xxxxx@keys.example/x": ` + wantURL},
		{"fragment", "url: https://keys.example/x#f\ncache_file: /c\n", `personal_keys.url "https://keys.example/x#f": ` + wantURL},
		{"unparsable", "url: \"http://[::1\"\ncache_file: /c\n", `personal_keys.url "xxxxx": ` + wantURL},
		{"no cache", "url: https://keys.example/x\n", "personal_keys.cache_file: required when personal_keys.url is set"},
		{"refresh zero", "url: https://keys.example/x\ncache_file: /c\nrefresh: 0s\n", "personal_keys.refresh: must be greater than zero"},
		{"refresh negative", "url: https://keys.example/x\ncache_file: /c\nrefresh: -1s\n", "personal_keys.refresh: must be greater than zero"},
		{"miss negative", "url: https://keys.example/x\ncache_file: /c\nrefresh_on_miss: -1s\n", "personal_keys.refresh_on_miss: must not be negative"},
	}
	for _, c := range cases {
		y := "personal_keys:\n  " + strings.ReplaceAll(strings.TrimSuffix(c.y, "\n"), "\n", "\n  ") + "\n"
		_, err := parseReq(y)
		if err == nil || err.Error() != c.want {
			t.Errorf("%s: got %v, want %q", c.name, err, c.want)
		}
		if err != nil && strings.Contains(err.Error(), "s3cretpw") {
			t.Errorf("%s: password echoed: %v", c.name, err)
		}
	}
}

// "Set" means present in the file, whatever the value, with url empty.
func TestPersonalKeysSubKeyWithoutURL(t *testing.T) {
	for _, kv := range []string{"token_file: \"\"", "token_file: /t", "cache_file: /c", "refresh: 1m", "refresh_on_miss: 5s", "refresh_on_miss: 0s", "allow_insecure_http: false", "allow_insecure_http: true"} {
		key := kv[:strings.Index(kv, ":")]
		for _, urlLine := range []string{"", "  url: \"\"\n"} {
			_, err := parseReq("personal_keys:\n" + urlLine + "  " + kv + "\n")
			want := "personal_keys." + key + " is set but personal_keys.url is empty"
			if err == nil || err.Error() != want {
				t.Errorf("%q (url line %q): got %v, want %q", kv, urlLine, err, want)
			}
		}
	}
	// The first present key wins, in the documented order.
	_, err := parseReq("personal_keys:\n  allow_insecure_http: true\n  refresh: 1m\n  token_file: /t\n")
	if err == nil || err.Error() != "personal_keys.token_file is set but personal_keys.url is empty" {
		t.Errorf("order: %v", err)
	}
	// A sub-key of another block of the file does not count, nor does request_url.
	if _, err := parseReq("personal_keys:\n  request_url: https://keys.example/r\n"); err != nil {
		t.Errorf("request_url only: %v", err)
	}
	if _, err := parseReq("personal_keys:\n  url: \"\"\n"); err != nil {
		t.Errorf("url empty only: %v", err)
	}
}

func TestPersonalKeysUnknownSubKeyRejected(t *testing.T) {
	for _, y := range []string{"personal_keys:\n  bogus: 1\n", pkOK + "  cache_fil: /x\n"} {
		if _, err := parseReq(y); err == nil || !strings.Contains(err.Error(), "not found in type config.PersonalKeys") {
			t.Errorf("%q: want an unknown-key error, got %v", y, err)
		}
	}
}

// Load adds the path in front of the message; the message text stays intact.
func TestPersonalKeysLoadWrapsWithPath(t *testing.T) {
	f := t.TempDir() + "/config.yaml"
	if err := os.WriteFile(f, []byte("minio:\n  endpoint: m:9000\npersonal_keys:\n  url: https://keys.example/x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(f)
	if err == nil || err.Error() != f+": personal_keys.cache_file: required when personal_keys.url is set" {
		t.Fatalf("Load: %v", err)
	}
}
