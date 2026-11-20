// Package app wires hourchd's components from a config: store, secrets,
// object store, live logs, event hub, engine, websocket server, HTTP API,
// web UI, powerwarden publisher and maintenance.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/resurgentech/keyclient/keylist"

	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/events"
	"github.com/resurgentech/hourch/daemon/internal/httpapi"
	"github.com/resurgentech/hourch/daemon/internal/logs"
	"github.com/resurgentech/hourch/daemon/internal/maint"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/pwpub"
	"github.com/resurgentech/hourch/daemon/internal/secrets"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/ui"
	"github.com/resurgentech/hourch/daemon/internal/ulid"
	"github.com/resurgentech/hourch/daemon/internal/wsserver"
)

// Deps are injectable dependencies; zero values mean production ones.
type Deps struct {
	Clock   clock.Clock
	Objects objects.Store
	Log     *slog.Logger
	// PowerwardenHTTP is the publisher's HTTP client (default: 10s timeout).
	PowerwardenHTTP *http.Client
}

// App is a wired daemon.
type App struct {
	Config    config.Config
	Store     *store.Store
	Engine    *dispatch.Engine
	API       *httpapi.Server
	Maint     *maint.Maint
	Objects   objects.Store
	Publisher *pwpub.Publisher // nil when powerwarden.url is empty
	KeyList   *keylist.Source  // nil when personal_keys.url is empty

	handler http.Handler
	loops   sync.WaitGroup
	log     *slog.Logger
}

// Check validates everything that can be checked without serving: the
// config (already parsed), the tokens file, the powerwarden token and the
// UI credentials (the htpasswd file, or the proxy secret file and its mode). (The run-secrets key has no file: Build generates it.)
func Check(cfg config.Config) error {
	tokens, err := httpapi.LoadTokens(cfg.TokensFile)
	if err != nil {
		return err
	}
	// Builds the key list exactly as serve does, but never starts it: nothing
	// is sent.
	if _, err := newKeyList(cfg, tokens, slog.Default()); err != nil {
		return err
	}
	if cfg.Powerwarden.URL != "" {
		if _, err := pwpub.ReadToken(cfg.Powerwarden.TokenFile); err != nil {
			return err
		}
	}
	if _, _, err := loadUIAuth(cfg); err != nil {
		return err
	}
	return nil
}

// newKeyList builds the key list from personal_keys (nil when url is empty)
// without starting it. Both Check and Build use it, so check-config refuses
// what serve refuses. The Accept function takes only role submitter and skips
// every entry that clashes with a token of tokens_file.
func newKeyList(cfg config.Config, tokens *httpapi.Tokens, log *slog.Logger) (*keylist.Source, error) {
	p := cfg.PersonalKeys
	if p.URL == "" {
		return nil, nil
	}
	src, err := keylist.New(keylist.Options{
		URL: p.URL, TokenFile: p.TokenFile, CacheFile: p.CacheFile,
		Refresh: p.Refresh.D(), RefreshOnMiss: p.RefreshOnMiss.D(),
		AllowInsecureHTTP: p.AllowInsecureHTTP,
		Accept:            keylist.AcceptRoles([]string{httpapi.KindSubmitter}, tokens.TakenLocally),
		Logger:            log,
	})
	if err != nil {
		msg := fmt.Sprintf("personal_keys: %v", err)
		if u, perr := url.Parse(p.URL); perr == nil && u.Scheme == "http" && !p.AllowInsecureHTTP && !isLoopbackHost(u.Hostname()) {
			msg += "; set personal_keys.allow_insecure_http: true only for a network path you trust"
		}
		return nil, errors.New(msg)
	}
	return src, nil
}

// isLoopbackHost is keyclient's rule for plain http: "localhost" by name, an
// address in 127.0.0.0/8, or ::1.
func isLoopbackHost(h string) bool {
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// loadUIAuth loads what the configured UI auth mode needs: the htpasswd
// file, or the proxy secret file (whose permissions it checks). Both are nil
// when the UI is disabled.
func loadUIAuth(cfg config.Config) (*ui.Htpasswd, *ui.ProxyAuth, error) {
	if !cfg.UI.Enabled {
		return nil, nil, nil
	}
	if cfg.UI.Auth == "proxy" {
		p, err := ui.NewProxyAuth(cfg.UI.Proxy.SecretFile, cfg.UI.Proxy.SecretHeader, cfg.UI.Proxy.UserHeader)
		return nil, p, err
	}
	u, err := ui.LoadHtpasswd(cfg.UI.HtpasswdFile)
	return u, nil, err
}

// Build opens the state and wires the components.
func Build(cfg config.Config, d Deps) (*App, error) {
	if d.Clock == nil {
		d.Clock = clock.Real{}
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	tokens, err := httpapi.LoadTokens(cfg.TokensFile)
	if err != nil {
		return nil, err
	}
	// A refusal of the key list stops the start before any state is touched.
	kl, err := newKeyList(cfg, tokens, d.Log)
	if err != nil {
		return nil, err
	}
	// A new instance id and a volatile key per process: secrets sealed by
	// an earlier process are unreadable and Start deletes them.
	instance := ulid.New().Make(d.Clock.Now())
	box, err := secrets.NewVolatile(instance)
	if err != nil {
		return nil, err
	}
	d.Log.Info("hourchd instance", "instance", instance)
	var pwToken string
	if cfg.Powerwarden.URL != "" {
		if pwToken, err = pwpub.ReadToken(cfg.Powerwarden.TokenFile); err != nil {
			return nil, err
		}
	}
	users, proxy, err := loadUIAuth(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.UI.Enabled {
		d.Log.Info("ui auth", "mode", cfg.UI.Auth)
	}
	if d.Objects == nil {
		d.Objects, err = objects.NewMinIO(objects.MinIOConfig{
			Endpoint: cfg.MinIO.Endpoint, Bucket: cfg.MinIO.Bucket, UseSSL: cfg.MinIO.UseSSL,
			AccessKeyEnv: cfg.MinIO.AccessKeyEnv, SecretKeyEnv: cfg.MinIO.SecretKeyEnv,
		})
		if err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, err
	}
	st, err := store.Open(filepath.Join(cfg.StateDir, "state.db"))
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*App, error) {
		st.Close()
		return nil, err
	}
	lm, err := logs.New(filepath.Join(cfg.StateDir, "logs"))
	if err != nil {
		return fail(err)
	}
	eng, err := dispatch.New(dispatch.Options{
		Store: st, Objects: d.Objects, Box: box, Logs: lm, Hub: events.NewHub(cfg.Events.RingSize),
		Clock: d.Clock, Log: d.Log,
		Heartbeat: cfg.Dispatch.Heartbeat.D(), Lease: cfg.Dispatch.Lease.D(), AcceptTimeout: cfg.Dispatch.AcceptTimeout.D(),
		ReaperInterval: cfg.Dispatch.ReaperInterval.D(), DefaultTimeout: cfg.Limits.DefaultTimeout.D(),
		MaxJobsPerRun: cfg.Limits.MaxJobsPerRun, OutputValueBytes: cfg.Limits.OutputValueBytes,
		UnschedulableTimeout: cfg.Dispatch.UnschedulableTimeout.D(),
	})
	if err != nil {
		return fail(err)
	}
	var pub *pwpub.Publisher
	if cfg.Powerwarden.URL != "" {
		pub, err = pwpub.New(pwpub.Options{URL: cfg.Powerwarden.URL, Token: pwToken, Source: eng,
			Refresh: cfg.Powerwarden.Refresh.D(), Log: d.Log.With("component", "powerwarden"), HTTPClient: d.PowerwardenHTTP})
		if err != nil {
			return fail(err)
		}
		eng.SetNotify(pub.Kick)
		eng.SetHosts(pub)
	}
	pwState := func() (string, string) {
		if pub == nil {
			return string(pwpub.Disabled), ""
		}
		s, msg, _ := pub.State()
		return string(s), msg
	}
	ws := wsserver.New(wsserver.Options{Engine: eng, Log: d.Log, ChunkMaxBytes: cfg.Logs.ChunkMaxBytes})
	srv, err := httpapi.New(httpapi.Options{
		Engine: eng, Tokens: tokens, WS: ws, Log: d.Log, Heartbeat: cfg.Events.Heartbeat.D(),
		ContextMaxBytes: int64(cfg.Limits.ContextMaxBytes), ArtifactMaxBytes: int64(cfg.Limits.ArtifactMaxBytes),
		WorkerRetention: cfg.Retention.Rows.D(),
		Powerwarden:     func() string { s, _ := pwState(); return s },
		KeyRequestURL:   cfg.PersonalKeys.RequestURL,
		KeyList:         kl,
	})
	if err != nil {
		return fail(err)
	}
	mux := http.NewServeMux()
	if cfg.UI.Enabled {
		u, err := ui.New(ui.Options{Engine: eng, API: srv, Users: users, Proxy: proxy, PathPrefix: cfg.UI.PathPrefix, Powerwarden: pwState,
			WorkerRetention: cfg.Retention.Rows.D(), Log: d.Log.With("component", "ui")})
		if err != nil {
			return fail(err)
		}
		u.Register(mux)
	}
	mux.Handle("/", srv)
	m := maint.New(maint.Options{
		Engine: eng, Store: st, Objects: d.Objects, Clock: d.Clock, Log: d.Log, StateDir: cfg.StateDir,
		Rows: cfg.Retention.Rows.D(), Logs: cfg.Retention.Logs.D(), Artifacts: cfg.Retention.Artifacts.D(),
		PruneInterval: cfg.Retention.PruneInterval.D(), BackupEvery: cfg.Backup.Interval.D(), BackupPrefix: cfg.Backup.MinioPrefix,
	})
	return &App{Config: cfg, Store: st, Engine: eng, API: srv, Maint: m, Objects: d.Objects, Publisher: pub, KeyList: kl,
		handler: mux, log: d.Log}, nil
}

// Handler is the HTTP handler: the web UI under /ui (and the site-root
// redirect to it) when ui.enabled, everything else the API.
func (a *App) Handler() http.Handler { return a.handler }

// Start recovers state and starts the reaper, maintenance and powerwarden
// publisher loops until ctx ends.
func (a *App) Start(ctx context.Context) error {
	if err := a.Engine.Start(ctx); err != nil {
		return fmt.Errorf("recover: %w", err)
	}
	a.Maint.Start(ctx)
	a.StartPublisher(ctx)
	if a.KeyList != nil {
		if err := a.KeyList.Start(ctx); err != nil {
			return fmt.Errorf("personal_keys: %v", err)
		}
		st := a.KeyList.Status()
		a.log.Info(fmt.Sprintf("personal_keys: list origin=%s entries=%d age=%s", st.Origin, st.Entries, st.Age))
	}
	return nil
}

// StartPublisher starts the powerwarden publisher until ctx ends (no-op
// when disabled). Start calls it; tests that drive the reaper by hand call
// it directly.
func (a *App) StartPublisher(ctx context.Context) {
	if a.Publisher == nil {
		return
	}
	a.loops.Add(1)
	go func() {
		defer a.loops.Done()
		a.Publisher.Run(ctx)
	}()
}

// Close closes worker connections, waits for background work (after the
// Start ctx ended) and closes the store.
func (a *App) Close() error {
	a.API.CloseStreams()
	a.Engine.CloseAll("hourchd shutting down")
	a.Maint.Wait()
	a.Engine.Wait()
	a.loops.Wait()
	return a.Store.Close()
}
