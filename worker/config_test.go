package hourchworker

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestExampleConfigMatchesDefaults(t *testing.T) {
	data, err := os.ReadFile("worker.yaml.example")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := ParseConfig(data)
	if err != nil {
		t.Fatalf("example config: %v", err)
	}
	// The four per-host identity keys carry illustrative values.
	if cfg.WorkerID != "node01" || cfg.Hostname != "node01.lab.example" ||
		!reflect.DeepEqual(cfg.Tags, []string{"kernel"}) || !reflect.DeepEqual(cfg.Capabilities, []string{"docker"}) {
		t.Fatalf("identity keys: %+v", cfg)
	}
	cfg.WorkerID, cfg.Hostname, cfg.Tags, cfg.Capabilities = "", "", nil, nil
	// hourchd_url is required, so it has no default: take the example's.
	if cfg.HourchdURL == "" {
		t.Fatal("example must set hourchd_url")
	}
	def := DefaultConfig()
	def.HourchdURL = cfg.HourchdURL
	if !reflect.DeepEqual(cfg, def) {
		t.Fatalf("example != defaults\nexample: %+v\ndefault: %+v", cfg, def)
	}
}

func TestHourchdURLIsRequired(t *testing.T) {
	if u := DefaultConfig().HourchdURL; u != "" {
		t.Fatalf("hourchd_url has a default: %q", u)
	}
	if _, err := ParseConfig([]byte("worker_id: a\n")); err == nil || !strings.Contains(err.Error(), "hourchd_url is required") {
		t.Fatalf("missing hourchd_url: %v", err)
	}
	if _, err := ParseConfig([]byte("worker_id: a\n" + reqURL)); err != nil {
		t.Fatalf("with hourchd_url: %v", err)
	}
}

// reqURL is the one key that is required and has no default; tests of other
// keys include it so they are not decided by its absence.
const reqURL = "hourchd_url: ws://hourchd.example/v1/worker/connect\n"

func TestConfigErrors(t *testing.T) {
	for name, body := range map[string]string{
		"unknown key":     "worker_id: a\nbogus: 1\n",
		"no worker id":    "hostname: h\n",
		"bad url":         "worker_id: a\nhourchd_url: https://x/\n",
		"bare duration":   "worker_id: a\nreconnect:\n  initial: 5\n",
		"bad capability":  "worker_id: a\ncapabilities: [gpu]\n",
		"bad tag":         "worker_id: a\ntags: [\"a b\"]\n",
		"small buffer":    "worker_id: a\nlog_buffer: 1KiB\n",
		"max < initial":   "worker_id: a\nreconnect:\n  initial: 10s\n  max: 1s\n",
		"bad size":        "worker_id: a\nlog_buffer: 64 potatoes\n",
		"nested unknown":  "worker_id: a\nreconnect:\n  jitter: 1\n",
		"env allowlist =": "worker_id: a\nenv_allowlist: [\"A=B\"]\n",
	} {
		if !strings.Contains(body, "hourchd_url") {
			body += reqURL
		}
		if _, err := ParseConfig([]byte(body)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestConfigHostnameDefault(t *testing.T) {
	cfg, err := ParseConfig([]byte("worker_id: a\n" + reqURL))
	if err != nil {
		t.Fatal(err)
	}
	h, _ := os.Hostname()
	if cfg.Hostname != h {
		t.Fatalf("hostname %q, want %q", cfg.Hostname, h)
	}
	if cfg.HasDocker() {
		t.Fatal("docker capability without config")
	}
}

func TestParseByteSize(t *testing.T) {
	for in, want := range map[string]ByteSize{"65536": 65536, "64KiB": 65536, "64MiB": 64 << 20, "1GB": 1e9, "2 GiB": 2 << 30} {
		got, err := ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("%q: %d, %v; want %d", in, got, err, want)
		}
	}
}

func TestHTTPBase(t *testing.T) {
	for in, want := range map[string]string{
		"wss://hourchd.labtwo/v1/worker/connect": "https://hourchd.labtwo",
		"ws://127.0.0.1:8093/v1/worker/connect":  "http://127.0.0.1:8093",
	} {
		c := Config{HourchdURL: in}
		got, err := c.HTTPBase()
		if err != nil || got != want {
			t.Errorf("%s: %q %v, want %q", in, got, err, want)
		}
	}
}

func TestReadTokenFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "token")
	os.WriteFile(p, []byte("  s3cret\n"), 0o600)
	if tok, err := ReadTokenFile(p); err != nil || tok != "s3cret" {
		t.Fatalf("got %q %v", tok, err)
	}
	os.Chmod(p, 0o640)
	if _, err := ReadTokenFile(p); err == nil || !strings.Contains(err.Error(), "0600") {
		t.Fatalf("group-readable token accepted: %v", err)
	}
	os.WriteFile(filepath.Join(dir, "empty"), nil, 0o600)
	if _, err := ReadTokenFile(filepath.Join(dir, "empty")); err == nil {
		t.Fatal("empty token accepted")
	}
}
