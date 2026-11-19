// Package hourchworker is the hourch worker: it holds one websocket to
// hourchd, runs the jobs hourchd assigns (one at a time), streams their logs
// and reports outputs, artifacts and the result.
//
// See docs/DESIGN-hourchd.md (Worker protocol).
package hourchworker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	hourchdprotocol "github.com/resurgentech/hourch/protocol"
)

// Duration is a time.Duration written in Go syntax ("500ms", "10s") in YAML.
type Duration time.Duration

// D returns the time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// UnmarshalYAML parses a Go duration string. Bare numbers are rejected.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
		return fmt.Errorf("line %d: duration %q must be a string with a unit, like 10s", n.Line, n.Value)
	}
	v, err := time.ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %v", n.Line, err)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML writes the Go duration string.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// ByteSize is a size in bytes written as an integer or with a unit
// (KiB, MiB, GiB, KB, MB, GB) in YAML.
type ByteSize int64

var byteSizeRE = regexp.MustCompile(`^([0-9]+)\s*([KMG]i?B)?$`)

// ParseByteSize parses "65536", "64KiB", "64MiB", "1GB".
func ParseByteSize(s string) (ByteSize, error) {
	m := byteSizeRE.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("size %q: want an integer with an optional unit (KiB, MiB, GiB, KB, MB, GB)", s)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("size %q: %v", s, err)
	}
	mult := map[string]int64{"": 1, "KB": 1e3, "MB": 1e6, "GB": 1e9, "KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30}[m[2]]
	return ByteSize(n * mult), nil
}

// UnmarshalYAML parses an integer or a string with a unit.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: size must be a scalar", n.Line)
	}
	v, err := ParseByteSize(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %v", n.Line, err)
	}
	*b = v
	return nil
}

// ReconnectConfig is the reconnect backoff: initial, doubling to max, with
// jitter.
type ReconnectConfig struct {
	Initial Duration `yaml:"initial"`
	Max     Duration `yaml:"max"`
}

// Config is worker.yaml. Every key is listed in worker.yaml.example.
type Config struct {
	HourchdURL   string          `yaml:"hourchd_url"`
	WorkerID     string          `yaml:"worker_id"`
	Hostname     string          `yaml:"hostname"`
	Tags         []string        `yaml:"tags"`
	Capabilities []string        `yaml:"capabilities"`
	TokenFile    string          `yaml:"token_file"`
	WorkDir      string          `yaml:"work_dir"`
	EnvAllowlist []string        `yaml:"env_allowlist"`
	Reconnect    ReconnectConfig `yaml:"reconnect"`
	LogBuffer    ByteSize        `yaml:"log_buffer"`
}

// DefaultConfig returns the defaults. hourchd_url has none (it is required).
// worker_id, hostname, tags and capabilities are per-host identity:
// worker_id has no default (it is required), hostname defaults to
// os.Hostname() at load, tags and capabilities default to empty.
// worker.yaml.example shows illustrative values for those four.
func DefaultConfig() Config {
	return Config{
		TokenFile:    "/etc/hourch/worker.token",
		WorkDir:      "/opt/hourch/work",
		EnvAllowlist: []string{"PATH", "HOME", "LANG", "TERM", "TMPDIR"},
		Reconnect:    ReconnectConfig{Initial: Duration(500 * time.Millisecond), Max: Duration(10 * time.Second)},
		LogBuffer:    64 << 20,
	}
}

// LoadConfig reads and parses a config file.
func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	cfg, err := ParseConfig(data)
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// ParseConfig decodes YAML over the defaults, fills hostname from
// os.Hostname() when empty, and validates. Unknown keys are errors.
func ParseConfig(data []byte) (Config, error) {
	cfg := DefaultConfig()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, err
	}
	if cfg.Hostname == "" {
		h, err := os.Hostname()
		if err != nil {
			return Config{}, fmt.Errorf("hostname: %w", err)
		}
		cfg.Hostname = h
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

var tagRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// Validate checks the config.
func (c Config) Validate() error {
	if c.HourchdURL == "" {
		return errors.New("hourchd_url is required")
	}
	u, err := url.Parse(c.HourchdURL)
	if err != nil {
		return fmt.Errorf("hourchd_url: %w", err)
	}
	if u.Scheme != "ws" && u.Scheme != "wss" || u.Host == "" {
		return fmt.Errorf("hourchd_url %q: want ws:// or wss://host/...", c.HourchdURL)
	}
	if c.WorkerID == "" {
		return errors.New("worker_id is required")
	}
	if c.Hostname == "" {
		return errors.New("hostname is required")
	}
	for _, t := range c.Tags {
		if !tagRE.MatchString(t) {
			return fmt.Errorf("tags: bad tag %q", t)
		}
	}
	for _, cp := range c.Capabilities {
		if cp != hourchdprotocol.CapabilityDocker {
			return fmt.Errorf("capabilities: unknown capability %q (known: %s)", cp, hourchdprotocol.CapabilityDocker)
		}
	}
	if c.TokenFile == "" {
		return errors.New("token_file is required")
	}
	if c.WorkDir == "" {
		return errors.New("work_dir is required")
	}
	for _, k := range c.EnvAllowlist {
		if k == "" || strings.ContainsAny(k, "=\x00") {
			return fmt.Errorf("env_allowlist: bad name %q", k)
		}
	}
	if c.Reconnect.Initial <= 0 || c.Reconnect.Max < c.Reconnect.Initial {
		return fmt.Errorf("reconnect: want 0 < initial <= max, got %s..%s", c.Reconnect.Initial.D(), c.Reconnect.Max.D())
	}
	if c.LogBuffer < minLogBuffer {
		return fmt.Errorf("log_buffer: at least %d bytes", minLogBuffer)
	}
	return nil
}

// minLogBuffer is two maximal chunks.
const minLogBuffer = 2 * ChunkMaxBytes

// HasDocker reports whether the worker advertises the docker capability.
func (c Config) HasDocker() bool {
	for _, cp := range c.Capabilities {
		if cp == hourchdprotocol.CapabilityDocker {
			return true
		}
	}
	return false
}

// HTTPBase returns hourchd's HTTP(S) base URL derived from hourchd_url:
// wss→https, ws→http, path dropped (assumption 16).
func (c Config) HTTPBase() (string, error) {
	u, err := url.Parse(c.HourchdURL)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "wss":
		u.Scheme = "https"
	case "ws":
		u.Scheme = "http"
	default:
		return "", fmt.Errorf("hourchd_url %q: want ws:// or wss://", c.HourchdURL)
	}
	return u.Scheme + "://" + u.Host, nil
}

// ReadTokenFile reads the worker token. The file must not be readable or
// writable by group or others (0600 or stricter) and must not be empty.
func ReadTokenFile(path string) (string, error) {
	st, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if st.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("%s: mode %04o, want 0600 (no group or other access)", path, st.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", fmt.Errorf("%s: empty token", path)
	}
	return tok, nil
}
