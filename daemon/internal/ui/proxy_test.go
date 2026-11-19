package ui_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/resurgentech/hourch/daemon/internal/app"
	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/ui"
	"golang.org/x/crypto/bcrypt"
)

const (
	pSecretH = "X-Test-Proxy-Secret"
	pUserH   = "X-Test-User"
	pSecret1 = "proxy-secret-one"
	pSecret2 = "proxy-secret-two"
)

func proxyConfig(t *testing.T, mode os.FileMode, content string) config.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StateDir = filepath.Join(dir, "state")
	cfg.TokensFile = filepath.Join(dir, "tokens.yaml")
	cfg.Powerwarden.URL = ""
	cfg.Events.Heartbeat = config.Duration(50 * time.Millisecond)
	if err := os.WriteFile(cfg.TokensFile, []byte("submitters:\n  ops: tok-ops\nworkers:\n  w1: tok-w1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	sf := filepath.Join(dir, "proxy.secret")
	if err := os.WriteFile(sf, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(sf, mode); err != nil {
		t.Fatal(err)
	}
	cfg.UI.Auth = "proxy"
	cfg.UI.HtpasswdFile = "" // not needed in proxy mode
	cfg.UI.Proxy = config.UIProxy{SecretFile: sf, SecretHeader: pSecretH, UserHeader: pUserH}
	return cfg
}

func proxySetup(t *testing.T, prefix string) *uenv {
	t.Helper()
	cfg := proxyConfig(t, 0o600, "\n  "+pSecret1+"  \r\n"+pSecret2+"\n")
	cfg.UI.PathPrefix = prefix
	clk := clock.NewFake(time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC))
	obj := objects.NewMem()
	obj.Now = clk.Now
	a, err := app.Build(cfg, app.Deps{Clock: clk, Objects: obj, Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	seedSeen(t, a.Store)
	if err := a.Engine.Recover(); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(func() {
		a.Engine.CloseAll("test")
		srv.CloseClientConnections()
		srv.Close()
		_ = a.Close()
	})
	return &uenv{t: t, a: a, srv: srv, clk: clk}
}

func (e *uenv) preq(method, path string, hdr ...string) (*http.Response, string) {
	e.t.Helper()
	req, _ := http.NewRequest(method, e.srv.URL+path, strings.NewReader(""))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	resp, err := noRedirect.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func good(user string) []string { return []string{pSecretH, pSecret1, pUserH, user} }

func TestProxyAuthRequired(t *testing.T) {
	e := proxySetup(t, "")
	run := e.submit(graph.Submission{Workflow: "wf", Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	jid := e.jobs(run.ID)["a"].ID
	paths := []string{"/ui/runs", "/ui/runs/" + run.ID, "/ui/jobs/" + jid, "/ui/workers", "/ui/events",
		"/ui/jobs/" + jid + "/log", "/ui/jobs/" + jid + "/log/stream", "/ui/jobs/" + jid + "/artifacts/x", "/ui/static/app.js"}
	long := strings.Repeat("a", 65)
	cases := map[string][]string{
		"no headers":          nil,
		"wrong secret":        {pSecretH, "nope", pUserH, "alice"},
		"secret only":         {pSecretH, pSecret1},
		"user only":           {pUserH, "alice"},
		"empty user":          {pSecretH, pSecret1, pUserH, ""},
		"space in user":       {pSecretH, pSecret1, pUserH, "a b"},
		"colon in user":       {pSecretH, pSecret1, pUserH, "a:b"},
		"ui: prefix in user":  {pSecretH, pSecret1, pUserH, "ui:x"},
		"65-char user":        {pSecretH, pSecret1, pUserH, long},
		"duplicate user":      {pSecretH, pSecret1, pUserH, "alice", pUserH, "bob"},
		"duplicate secret":    {pSecretH, pSecret1, pSecretH, pSecret1, pUserH, "alice"},
		"prefix of a secret":  {pSecretH, pSecret1[:5], pUserH, "alice"},
		"basic auth alone":    {"Authorization", "Basic YWxpY2U6cHc="},
		"bearer token alone":  {"Authorization", "Bearer tok-ops"},
		"comment-ish secret":  {pSecretH, "# comment", pUserH, "alice"},
		"secret with padding": {pSecretH, pSecret1 + "x", pUserH, "alice"},
	}
	for name, hdr := range cases {
		for _, p := range paths {
			resp, _ := e.preq("GET", p, hdr...)
			if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("WWW-Authenticate") != "" {
				t.Errorf("%s: GET %s = %d, WWW-Authenticate %q", name, p, resp.StatusCode, resp.Header.Get("WWW-Authenticate"))
			}
		}
		if resp, _ := e.preq("POST", "/ui/runs/"+run.ID+"/cancel", append(hdr, "Origin", e.srv.URL)...); resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("%s: POST = %d", name, resp.StatusCode)
		}
	}
	// Valid headers, with unrelated basic-auth credentials present: accepted.
	if resp, _ := e.preq("GET", "/ui/runs", append(good("alice"), "Authorization", "Basic YWxpY2U6cHc=")...); resp.StatusCode != 200 {
		t.Errorf("headers + stray basic auth = %d", resp.StatusCode)
	}
}

func TestProxyAcceptsAnySecretLine(t *testing.T) {
	e := proxySetup(t, "")
	for _, s := range []string{pSecret1, pSecret2} {
		resp, body := e.preq("GET", "/ui/runs", pSecretH, s, pUserH, "alice@example.com")
		if resp.StatusCode != 200 || resp.Header.Get("X-Frame-Options") != "DENY" || !strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'self'") {
			t.Errorf("secret %q: %d %v", s, resp.StatusCode, body)
		}
	}
}

func TestProxyUserIsCaller(t *testing.T) {
	e := proxySetup(t, "")
	run := e.submit(graph.Submission{Workflow: "wf", Params: map[string]string{"p": "1"}, Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	if err := e.a.Engine.CancelRun(run.ID); err != nil {
		t.Fatal(err)
	}
	for _, pg := range []string{"/ui/runs", "/ui/runs/" + run.ID, "/ui/workers"} {
		if resp, body := e.preq("GET", pg, good("alice")...); resp.StatusCode != 200 {
			t.Fatalf("GET %s = %d %s", pg, resp.StatusCode, body)
		}
	}
	// Cross-site POST is refused even with valid proxy headers.
	if resp, _ := e.preq("POST", "/ui/runs/"+run.ID+"/retry", append(good("alice"), "Origin", "https://evil.example")...); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin retry = %d", resp.StatusCode)
	}
	resp, _ := e.preq("POST", "/ui/runs/"+run.ID+"/retry", append(good("alice"), "Origin", e.srv.URL)...)
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("retry = %d", resp.StatusCode)
	}
	runs, _ := e.a.Store.ListRuns(store.RunFilter{Workflow: "wf", Limit: 10})
	if len(runs) != 2 || runs[0].SubmittedBy != "ui:alice" {
		t.Fatalf("runs after retry: %+v", runs)
	}
	// Cancel works.
	if resp, _ := e.preq("POST", "/ui/runs/"+runs[0].ID+"/cancel", append(good("bob"), "Origin", e.srv.URL)...); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("cancel = %d", resp.StatusCode)
	}
}

func TestProxyForwardedRoutes(t *testing.T) {
	e := proxySetup(t, "")
	run := e.submit(graph.Submission{Workflow: "wf", Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	jid := e.jobs(run.ID)["a"].ID
	// The log route is the API's own handler, reached as the UI user with no bearer token.
	// The job never started, so the API answers 404 "never started": it
	// authorized the UI user (a missing principal would be 401).
	if resp, body := e.preq("GET", "/ui/jobs/"+jid+"/log", good("alice")...); resp.StatusCode != 404 || !strings.Contains(body, "never started") {
		t.Fatalf("log = %d %s", resp.StatusCode, body)
	}
	// The same API route directly, with the same proxy headers and no
	// bearer token, is not authenticated: proxy identity does not reach /v1.
	if resp, _ := e.preq("GET", "/v1/jobs/"+jid+"/log", good("alice")...); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/v1 with proxy headers = %d", resp.StatusCode)
	}
	if resp, _ := e.preq("GET", "/v1/runs", good("alice")...); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("/v1/runs with proxy headers = %d", resp.StatusCode)
	}
}

// The shared secret must never reach the API handler.
func TestProxyHeadersStrippedBeforeAPI(t *testing.T) {
	e := proxySetup(t, "")
	var seen http.Header
	a := e.a
	spy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		a.API.ServeHTTP(w, r)
	})
	srv := httptest.NewServer(e.uiOver(spy))
	t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
	run := e.submit(graph.Submission{Workflow: "wf", Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	jid := e.jobs(run.ID)["a"].ID
	req, _ := http.NewRequest("GET", srv.URL+"/ui/jobs/"+jid+"/log", nil)
	req.Header.Set(pSecretH, pSecret1)
	req.Header.Set(pUserH, "alice")
	req.Header.Set("Authorization", "Basic YWxpY2U6cHc=")
	resp, err := noRedirect.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 || seen == nil {
		t.Fatalf("status %d (want 404 never started), API reached: %v", resp.StatusCode, seen != nil)
	}
	for _, h := range []string{pSecretH, pUserH, "Authorization"} {
		if v := seen.Values(h); len(v) != 0 {
			t.Errorf("API saw %s: %v", h, v)
		}
	}
}

func TestProxyPathPrefix(t *testing.T) {
	e := proxySetup(t, "/p")
	if resp, _ := e.preq("GET", "/p/ui/runs"); resp.StatusCode != 401 {
		t.Fatalf("no headers = %d", resp.StatusCode)
	}
	resp, body := e.preq("GET", "/p/ui/runs", good("alice")...)
	if resp.StatusCode != 200 || !strings.Contains(body, `href="/p/ui/runs"`) {
		t.Fatalf("prefixed proxy page = %d", resp.StatusCode)
	}
	if resp, _ := e.preq("GET", "/p", good("alice")...); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("/p = %d", resp.StatusCode)
	}
}

func TestProxySecretFilePermissions(t *testing.T) {
	for _, mode := range []os.FileMode{0o640, 0o604, 0o644, 0o660} {
		cfg := proxyConfig(t, mode, pSecret1+"\n")
		if err := app.Check(cfg); err == nil || !strings.Contains(err.Error(), "ui.proxy.secret_file") || strings.Contains(err.Error(), pSecret1) {
			t.Errorf("Check mode %o: %v", mode, err)
		}
		a, err := app.Build(cfg, app.Deps{Objects: objects.NewMem(), Log: quiet()})
		if err == nil {
			a.Close()
			t.Errorf("Build mode %o succeeded", mode)
		}
	}
	for name, content := range map[string]string{"empty": "", "blank lines": "\n  \r\n\n"} {
		cfg := proxyConfig(t, 0o600, content)
		if err := app.Check(cfg); err == nil {
			t.Errorf("Check %s secret file succeeded", name)
		}
		if a, err := app.Build(cfg, app.Deps{Objects: objects.NewMem(), Log: quiet()}); err == nil {
			a.Close()
			t.Errorf("Build %s secret file succeeded", name)
		}
	}
	cfg := proxyConfig(t, 0o600, pSecret1+"\n")
	if err := app.Check(cfg); err != nil {
		t.Errorf("Check ok: %v", err)
	}
	cfg.UI.Proxy.SecretFile += ".missing"
	if err := app.Check(cfg); err == nil {
		t.Error("Check missing file succeeded")
	}
}

// uiOver serves a proxy-mode UI (same config as e) over a different API handler.
func (e *uenv) uiOver(api http.Handler) http.Handler {
	e.t.Helper()
	c := e.a.Config.UI.Proxy
	p, err := ui.NewProxyAuth(c.SecretFile, c.SecretHeader, c.UserHeader)
	if err != nil {
		e.t.Fatal(err)
	}
	u, err := ui.New(ui.Options{Engine: e.a.Engine, API: api, Proxy: p, Log: quiet()})
	if err != nil {
		e.t.Fatal(err)
	}
	mux := http.NewServeMux()
	u.Register(mux)
	mux.Handle("/", api)
	return mux
}

func TestNewRequiresExactlyOneAuth(t *testing.T) {
	e := proxySetup(t, "")
	c := e.a.Config.UI.Proxy
	p, err := ui.NewProxyAuth(c.SecretFile, c.SecretHeader, c.UserHeader)
	if err != nil {
		t.Fatal(err)
	}
	users, _ := ui.LoadHtpasswd(writeHtpasswd(t))
	for name, o := range map[string]ui.Options{
		"neither": {},
		"both":    {Users: users, Proxy: p},
	} {
		o.Engine, o.API = e.a.Engine, e.a.API
		if _, err := ui.New(o); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

func writeHtpasswd(t *testing.T) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "h")
	h, _ := bcrypt.GenerateFromPassword([]byte("pw"), bcrypt.MinCost)
	if err := os.WriteFile(f, []byte("alice:"+string(h)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}
