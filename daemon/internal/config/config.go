// Package config loads hourchd's config.yaml. Every key is documented, with
// its default, in config.yaml.example; DefaultConfig returns the same values
// (a test keeps them equal). Unknown keys are an error.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a Go duration string ("45s", "2h") or a whole number of days
// ("90d") in YAML. Bare numbers are rejected.
type Duration time.Duration

// D returns the time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// UnmarshalYAML parses a duration with a unit.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode || n.Tag != "!!str" {
		return fmt.Errorf("line %d: duration %q must be a string with a unit, like 30s or 90d", n.Line, n.Value)
	}
	v, err := ParseDuration(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %v", n.Line, err)
	}
	*d = Duration(v)
	return nil
}

// ParseDuration accepts Go durations plus "<n>d" for days.
func ParseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		n, err := strconv.ParseInt(strings.TrimSuffix(s, "d"), 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("duration %q: want <days>d", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

// Size is a byte count: a plain integer or a number with a unit
// (KiB, MiB, GiB, TiB, KB, MB, GB, TB, B).
type Size int64

// UnmarshalYAML parses a size.
func (s *Size) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: size must be a scalar", n.Line)
	}
	v, err := ParseSize(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %v", n.Line, err)
	}
	*s = Size(v)
	return nil
}

// ParseSize parses "65536", "64MiB", "2GiB".
func ParseSize(s string) (int64, error) {
	units := []struct {
		suffix string
		mult   int64
	}{
		{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
		{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000}, {"TB", 1000 * 1000 * 1000 * 1000},
		{"B", 1},
	}
	num, mult := s, int64(1)
	for _, u := range units {
		if strings.HasSuffix(s, u.suffix) {
			num, mult = strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("size %q: want a non-negative integer with an optional unit like MiB or GiB", s)
	}
	return n * mult, nil
}

// MinIO configures the object store. Credentials come from the named
// environment variables, never the file.
type MinIO struct {
	Endpoint     string `yaml:"endpoint"`
	Bucket       string `yaml:"bucket"`
	UseSSL       bool   `yaml:"use_ssl"`
	AccessKeyEnv string `yaml:"access_key_env"`
	SecretKeyEnv string `yaml:"secret_key_env"`
}

// Dispatch sets the worker protocol timings.
type Dispatch struct {
	Heartbeat      Duration `yaml:"heartbeat"`
	Lease          Duration `yaml:"lease"`
	AcceptTimeout  Duration `yaml:"accept_timeout"`
	ReaperInterval Duration `yaml:"reaper_interval"`
	// UnschedulableTimeout fails a ready job (no_worker) that no connected
	// worker, idle or busy, could run for this long. 0 disables.
	UnschedulableTimeout Duration `yaml:"unschedulable_timeout"`
}

// Logs sets the log chunk bounds (enforced by the worker; hourchd sizes its
// websocket read limit from chunk_max_bytes).
type Logs struct {
	ChunkMaxBytes int      `yaml:"chunk_max_bytes"`
	ChunkMaxDelay Duration `yaml:"chunk_max_delay"`
}

// Limits bound submissions and transfers.
type Limits struct {
	MaxJobsPerRun    int      `yaml:"max_jobs_per_run"`
	OutputValueBytes int      `yaml:"output_value_bytes"`
	ContextMaxBytes  Size     `yaml:"context_max_bytes"`
	ArtifactMaxBytes Size     `yaml:"artifact_max_bytes"`
	DefaultTimeout   Duration `yaml:"default_timeout"`
}

// Retention sets how long rows, logs and artifacts are kept.
type Retention struct {
	Rows          Duration `yaml:"rows"`
	Logs          Duration `yaml:"logs"`
	Artifacts     Duration `yaml:"artifacts"`
	PruneInterval Duration `yaml:"prune_interval"`
}

// Backup sets the SQLite backup to MinIO.
type Backup struct {
	Interval    Duration `yaml:"interval"`
	MinioPrefix string   `yaml:"minio_prefix"`
}

// Powerwarden configures the demand and liveness publisher. An empty URL
// disables it (healthz and stats report "disabled").
type Powerwarden struct {
	URL       string   `yaml:"url"`
	TokenFile string   `yaml:"token_file"`
	Refresh   Duration `yaml:"refresh"`
}

// Events sizes the SSE ring and heartbeat.
type Events struct {
	RingSize  int      `yaml:"ring_size"`
	Heartbeat Duration `yaml:"heartbeat"`
}

// UI configures the web UI at /ui: htpasswd basic auth (bcrypt only) or,
// with Auth "proxy", a trusted reverse proxy that names the user.
type UI struct {
	Enabled bool `yaml:"enabled"`
	// Auth is "htpasswd" (default) or "proxy".
	Auth string `yaml:"auth"`
	// HtpasswdFile applies only to Auth "htpasswd".
	HtpasswdFile string `yaml:"htpasswd_file"`
	// Proxy applies only to Auth "proxy", where every key is required.
	Proxy UIProxy `yaml:"proxy"`
	// PathPrefix moves the UI (and the site-root redirect) under a reverse
	// proxy's path: "/p" serves /p/ui/.... "" serves /ui. The API is never
	// prefixed.
	PathPrefix string `yaml:"path_prefix"`
}

// Config is config.yaml.
type Config struct {
	Listen      string      `yaml:"listen"`
	StateDir    string      `yaml:"state_dir"`
	TokensFile  string      `yaml:"tokens_file"`
	LogLevel    string      `yaml:"log_level"`
	MinIO       MinIO       `yaml:"minio"`
	Dispatch    Dispatch    `yaml:"dispatch"`
	Logs        Logs        `yaml:"logs"`
	Limits      Limits      `yaml:"limits"`
	Retention   Retention   `yaml:"retention"`
	Backup      Backup      `yaml:"backup"`
	Powerwarden Powerwarden `yaml:"powerwarden"`
	Events      Events      `yaml:"events"`
	UI          UI          `yaml:"ui"`
}

func sec(n int) Duration { return Duration(time.Duration(n) * time.Second) }

const day = 24 * 60 * 60

// Default returns the defaults, equal to config.yaml.example.
func Default() Config {
	return Config{
		Listen:     ":8093",
		StateDir:   "/var/lib/hourchd",
		TokensFile: "/etc/hourchd/tokens.yaml",
		LogLevel:   "info",
		MinIO: MinIO{
			Endpoint:     "", // required: Validate rejects it
			Bucket:       "hourch",
			UseSSL:       false,
			AccessKeyEnv: "HOURCHD_MINIO_ACCESS_KEY",
			SecretKeyEnv: "HOURCHD_MINIO_SECRET_KEY",
		},
		Dispatch: Dispatch{Heartbeat: sec(10), Lease: sec(45), AcceptTimeout: sec(10), ReaperInterval: sec(5), UnschedulableTimeout: sec(30 * 60)},
		Logs:     Logs{ChunkMaxBytes: 65536, ChunkMaxDelay: Duration(250 * time.Millisecond)},
		Limits: Limits{
			MaxJobsPerRun:    200,
			OutputValueBytes: 4096,
			ContextMaxBytes:  2 << 30,
			ArtifactMaxBytes: 4 << 30,
			DefaultTimeout:   sec(2 * 60 * 60),
		},
		Retention: Retention{Rows: sec(90 * day), Logs: sec(90 * day), Artifacts: sec(90 * day), PruneInterval: sec(60 * 60)},
		Backup:    Backup{Interval: sec(day), MinioPrefix: "backups/hourchd/"},
		Powerwarden: Powerwarden{
			URL:       "", // "" disables the publisher
			TokenFile: "/etc/hourchd/powerwarden.token",
			Refresh:   sec(15),
		},
		Events: Events{RingSize: 1000, Heartbeat: sec(15)},
		UI:     UI{Enabled: true, Auth: "htpasswd", HtpasswdFile: "/etc/hourchd/ui.htpasswd"},
	}
}

// Load reads and parses a config file.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	cfg, err := Parse(data)
	if err != nil {
		return Config{}, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

// removedKeys are keys of earlier versions. They are rejected with an
// explanation rather than the generic unknown-key error, so an old config
// fails clearly at startup.
var removedKeys = map[string]string{
	"secrets": "the run-secrets key is generated at startup and held only in memory; " +
		"there is no key file any more",
}

// Parse decodes YAML over the defaults and validates the result.
func Parse(data []byte) (Config, error) {
	var top map[string]any
	if yaml.Unmarshal(data, &top) == nil {
		var errs []error
		for _, k := range []string{"secrets"} {
			if v, ok := top[k]; ok {
				name := k
				if m, ok := v.(map[string]any); ok && m["key_file"] != nil {
					name = k + ".key_file"
				}
				errs = append(errs, fmt.Errorf("key %q was removed: %s; delete it", name, removedKeys[k]))
			}
		}
		if len(errs) > 0 {
			return Config{}, errors.Join(errs...)
		}
	}
	cfg := Default()
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks bounds.
func (c Config) Validate() error {
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log_level %q: want debug, info, warn or error", c.LogLevel)
	}
	req := map[string]string{
		"listen": c.Listen, "state_dir": c.StateDir, "tokens_file": c.TokensFile,
		"minio.endpoint": c.MinIO.Endpoint, "minio.bucket": c.MinIO.Bucket,
		"minio.access_key_env": c.MinIO.AccessKeyEnv, "minio.secret_key_env": c.MinIO.SecretKeyEnv,
	}
	for k, v := range req {
		if v == "" {
			return fmt.Errorf("%s is required", k)
		}
	}
	pos := map[string]Duration{
		"dispatch.heartbeat":       c.Dispatch.Heartbeat,
		"dispatch.lease":           c.Dispatch.Lease,
		"dispatch.accept_timeout":  c.Dispatch.AcceptTimeout,
		"dispatch.reaper_interval": c.Dispatch.ReaperInterval,
		"logs.chunk_max_delay":     c.Logs.ChunkMaxDelay,
		"limits.default_timeout":   c.Limits.DefaultTimeout,
		"retention.rows":           c.Retention.Rows,
		"retention.logs":           c.Retention.Logs,
		"retention.artifacts":      c.Retention.Artifacts,
		"retention.prune_interval": c.Retention.PruneInterval,
		"backup.interval":          c.Backup.Interval,
		"powerwarden.refresh":      c.Powerwarden.Refresh,
		"events.heartbeat":         c.Events.Heartbeat,
	}
	for k, v := range pos {
		if v <= 0 {
			return fmt.Errorf("%s must be > 0", k)
		}
	}
	for k, v := range map[string]int64{
		"logs.chunk_max_bytes":      int64(c.Logs.ChunkMaxBytes),
		"limits.max_jobs_per_run":   int64(c.Limits.MaxJobsPerRun),
		"limits.output_value_bytes": int64(c.Limits.OutputValueBytes),
		"limits.context_max_bytes":  int64(c.Limits.ContextMaxBytes),
		"limits.artifact_max_bytes": int64(c.Limits.ArtifactMaxBytes),
		"events.ring_size":          int64(c.Events.RingSize),
	} {
		if v < 1 {
			return fmt.Errorf("%s must be >= 1", k)
		}
	}
	if c.Dispatch.UnschedulableTimeout < 0 {
		return fmt.Errorf("dispatch.unschedulable_timeout must be >= 0 (0 disables)")
	}
	if c.Dispatch.Heartbeat >= c.Dispatch.Lease {
		return fmt.Errorf("dispatch.heartbeat (%v) must be shorter than dispatch.lease (%v)", c.Dispatch.Heartbeat.D(), c.Dispatch.Lease.D())
	}
	if c.Dispatch.Heartbeat.D()%time.Second != 0 || c.Dispatch.Lease.D()%time.Second != 0 {
		return fmt.Errorf("dispatch.heartbeat and dispatch.lease must be whole seconds (sent as *_seconds)")
	}
	if c.Powerwarden.URL != "" {
		u, err := url.Parse(c.Powerwarden.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("powerwarden.url %q: want http(s)://host (or \"\" to disable the publisher)", c.Powerwarden.URL)
		}
		if c.Powerwarden.TokenFile == "" {
			return fmt.Errorf("powerwarden.token_file is required when powerwarden.url is set")
		}
	}
	if err := c.UI.validate(); err != nil {
		return err
	}
	if p := c.UI.PathPrefix; p != "" {
		switch {
		case !strings.HasPrefix(p, "/"):
			return fmt.Errorf("ui.path_prefix %q must start with /", p)
		case strings.HasSuffix(p, "/"):
			return fmt.Errorf("ui.path_prefix %q must not end with /", p)
		case strings.Contains(p, ".."):
			return fmt.Errorf("ui.path_prefix %q must not contain ..", p)
		}
	}
	if c.Backup.MinioPrefix != "" && !strings.HasSuffix(c.Backup.MinioPrefix, "/") {
		return fmt.Errorf("backup.minio_prefix must end with /")
	}
	return nil
}

// UIProxy is ui.proxy: how hourchd trusts a reverse proxy that authenticates
// users (forward-auth style). No key has a default.
type UIProxy struct {
	// SecretFile holds the shared secrets, one per line; mode 0600.
	SecretFile string `yaml:"secret_file"`
	// SecretHeader carries the shared secret the proxy adds to each request.
	SecretHeader string `yaml:"secret_header"`
	// UserHeader carries the username the proxy sets.
	UserHeader string `yaml:"user_header"`
}

// validHeaderName reports whether s is an HTTP field name (RFC 9110 token).
func validHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

func (u UI) validate() error {
	if !u.Enabled {
		return nil
	}
	switch u.Auth {
	case "htpasswd":
		if u.HtpasswdFile == "" {
			return fmt.Errorf("ui.htpasswd_file is required when ui.enabled is true and ui.auth is htpasswd")
		}
	case "proxy":
		for _, k := range []struct{ name, v string }{
			{"secret_file", u.Proxy.SecretFile}, {"secret_header", u.Proxy.SecretHeader}, {"user_header", u.Proxy.UserHeader},
		} {
			if k.v == "" {
				return fmt.Errorf("ui.proxy.%s is required when ui.auth is proxy", k.name)
			}
		}
		for _, k := range []struct{ name, v string }{{"secret_header", u.Proxy.SecretHeader}, {"user_header", u.Proxy.UserHeader}} {
			if !validHeaderName(k.v) {
				return fmt.Errorf("ui.proxy.%s %q is not a valid HTTP header name", k.name, k.v)
			}
		}
		if strings.EqualFold(u.Proxy.SecretHeader, u.Proxy.UserHeader) {
			return fmt.Errorf("ui.proxy.secret_header and ui.proxy.user_header must differ (got %q and %q)", u.Proxy.SecretHeader, u.Proxy.UserHeader)
		}
	default:
		return fmt.Errorf("ui.auth %q: want htpasswd or proxy", u.Auth)
	}
	return nil
}
