package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/resurgentech/keyclient/keyclienttest"
	"github.com/resurgentech/keyclient/keylist"

	"github.com/resurgentech/hourch/daemon/internal/app"
	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/httpapi"
	"github.com/resurgentech/hourch/daemon/internal/objects"
)

const (
	keyTok  = "personal-key-alice-0123456789"
	keyName = "alice@laptop"
)

// keyEnv is an env whose key list points at list. refresh is 1h so the timer
// never pulls during a test; the pull at Start is waited for by polling.
func keyEnv(t *testing.T, list *keyclienttest.List, tweak func(*config.Config)) *env {
	t.Helper()
	cache := filepath.Join(t.TempDir(), "personal-keys.json")
	e := newEnv(t, func(c *config.Config) {
		c.PersonalKeys.URL = list.URL()
		c.PersonalKeys.CacheFile = cache
		c.PersonalKeys.Refresh = config.Duration(time.Hour)
		if tweak != nil {
			tweak(c)
		}
	})
	e.startList()
	return e
}

// startList starts the key list with a context that ends with the env.
func (e *env) startList() {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	e.stop = cancel
	if err := e.a.KeyList.Start(ctx); err != nil {
		e.t.Fatal(err)
	}
}

// restart is a daemon restart: same config, same cache file.
func (e *env) restart() {
	e.t.Helper()
	e.close()
	e.start()
	e.startList()
}

func (e *env) waitPulled() {
	e.t.Helper()
	waitFor(e.t, "the first good pull", func() bool { return !e.a.KeyList.Status().LastSuccess.IsZero() })
}

func (e *env) waitPullFailed() {
	e.t.Helper()
	waitFor(e.t, "the first failed pull", func() bool { return e.a.KeyList.Status().LastError != nil })
}

func (e *env) status(tok, method, path string) int {
	e.t.Helper()
	r, _ := e.do(method, path, tok, nil, nil)
	return r.StatusCode
}

func (e *env) wantStatus(want int, tok, method, path string) {
	e.t.Helper()
	if got := e.status(tok, method, path); got != want {
		e.t.Fatalf("%s %s = %d, want %d", method, path, got, want)
	}
}

func TestK1_CacheServesBeforeFirstPull(t *testing.T) {
	list := keyclienttest.NewList(t)
	list.Add(keyName, "submitter", keyTok)
	e := keyEnv(t, list, nil)
	e.waitPulled()
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
	e.close()

	// The held pull is never released by the test: closing the env ends the
	// source's context and cuts it short (the list releases it at cleanup).
	list.Block()
	before := list.Pulls()
	e.start()
	e.startList()
	waitFor(t, "the held pull to arrive", func() bool { return list.Pulls() > before })
	if !e.a.KeyList.Status().LastSuccess.IsZero() {
		t.Fatal("the pull finished although the list is blocked")
	}
	if st := e.a.KeyList.Status(); st.Origin != keylist.OriginCache {
		t.Fatalf("origin %q, want cache", st.Origin)
	}
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
	e.close()
}

func TestK10_ProviderDownCacheKeepsWorking(t *testing.T) {
	list := keyclienttest.NewList(t)
	list.Add(keyName, "submitter", keyTok)
	e := keyEnv(t, list, nil)
	e.waitPulled()
	list.FailWith(keyclienttest.Status(500, ""))
	if err := e.a.KeyList.Pull(context.Background()); err == nil {
		t.Fatal("a pull against a failing provider returned nil")
	}
	e.wantStatus(200, keyTok, "GET", "/v1/runs")

	e.restart() // the provider is still down
	e.waitPullFailed()
	if st := e.a.KeyList.Status(); st.Origin != keylist.OriginCache {
		t.Fatalf("origin %q, want cache", st.Origin)
	}
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
}

func TestK5_K8_200Then304(t *testing.T) {
	list := keyclienttest.NewList(t)
	list.Add(keyName, "submitter", keyTok)
	e := keyEnv(t, list, nil)
	e.waitPulled()
	reqs := list.Requests()
	if len(reqs) != 1 || reqs[0].IfNoneMatch != "" {
		t.Fatalf("first pull: %+v", reqs)
	}
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
	if err := e.a.KeyList.Pull(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs = list.Requests()
	if len(reqs) != 2 || reqs[1].IfNoneMatch == "" {
		t.Fatalf("second pull must be conditional: %+v", reqs)
	}
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
	pk := e.statsPersonalKeys()
	if pk["last_success"] == nil || pk["last_error"] != nil || pk["entries"] != float64(1) {
		t.Fatalf("stats after the 304: %v", pk)
	}
}

func TestRevokeDropsKeyAtNextPull(t *testing.T) {
	list := keyclienttest.NewList(t)
	list.Add(keyName, "submitter", keyTok)
	e := keyEnv(t, list, func(c *config.Config) { c.PersonalKeys.RefreshOnMiss = 0 })
	e.waitPulled()
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
	list.Remove(keyName)
	e.wantStatus(200, keyTok, "GET", "/v1/runs") // still in memory until the next pull
	if err := e.a.KeyList.Pull(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.wantStatus(401, keyTok, "GET", "/v1/runs")
}

func TestR2_KeyAddedAfterTickWorksOnFirstUse(t *testing.T) {
	list := keyclienttest.NewList(t)
	e := keyEnv(t, list, nil)
	e.waitPulled()
	list.Add(keyName, "submitter", keyTok)
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
}

func TestR7_BurstOfUnknownKeysOnePullPerWindow(t *testing.T) {
	list := keyclienttest.NewList(t)
	e := keyEnv(t, list, func(c *config.Config) { c.PersonalKeys.RefreshOnMiss = config.Duration(time.Hour) })
	e.waitPulled()
	before := list.Pulls()
	var wg sync.WaitGroup
	codes := make([]int, 20)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes[i] = e.status("unknown-key-"+string(rune('a'+i)), "GET", "/v1/runs")
		}()
	}
	wg.Wait()
	for i, c := range codes {
		if c != 401 {
			t.Fatalf("request %d = %d, want 401", i, c)
		}
	}
	if grew := list.Pulls() - before; grew > 1 {
		t.Fatalf("%d pulls for a burst of unknown keys, want at most 1", grew)
	}
}

func TestSR4_LocalTokenCollision(t *testing.T) {
	list := keyclienttest.NewList(t)
	list.Add("jared", "submitter", "a-different-key-for-jared") // name taken by a local submitter
	list.Add("someone", "submitter", w1Tok)                     // key of a local worker
	list.Add("w2", "submitter", "a-different-key-for-w2")       // name of a local worker
	list.Add("someone-else", "submitter", subTok)               // key of a local submitter
	list.Add(keyName, "submitter", keyTok)                      // the only good one
	e := keyEnv(t, list, nil)
	e.waitPulled()
	if n := e.a.KeyList.Status().Entries; n != 1 {
		t.Fatalf("entries = %d, want 1 (the four clashes are skipped)", n)
	}
	e.wantStatus(401, "a-different-key-for-jared", "GET", "/v1/runs")
	e.wantStatus(401, "a-different-key-for-w2", "GET", "/v1/runs")
	e.wantStatus(200, subTok, "GET", "/v1/runs")
	e.wantStatus(403, w1Tok, "GET", "/v1/runs")
	if c := e.status(w1Tok, "GET", "/v1/worker/jobs/nojob/context"); c == 401 || c == 403 {
		t.Fatalf("local worker token on a worker route = %d", c)
	}
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
	// The local token keeps its own role: the caller of a counter is still jared.
	e.wantStatus(200, subTok, "POST", "/v1/counters/c/next")
}

func TestSR4_UnknownRoleSkipped(t *testing.T) {
	list := keyclienttest.NewList(t)
	list.Add("w", "worker", "key-with-role-worker")
	list.Add("a", "agent", "key-with-role-agent")
	e := keyEnv(t, list, nil)
	e.waitPulled()
	if n := e.a.KeyList.Status().Entries; n != 0 {
		t.Fatalf("entries = %d, want 0", n)
	}
	for _, k := range []string{"key-with-role-worker", "key-with-role-agent"} {
		for _, rt := range [][2]string{{"GET", "/v1/runs"}, {"GET", "/v1/worker/jobs/x/context"}, {"PUT", "/v1/builds/t/f/1"}, {"GET", "/v1/worker/connect"}} {
			e.wantStatus(401, k, rt[0], rt[1])
		}
	}
}

func TestB7_PulledEntryNeverWorkerOrJob(t *testing.T) {
	list := keyclienttest.NewList(t)
	list.Add(keyName, "submitter", keyTok)
	e := keyEnv(t, list, nil)
	e.waitPulled()
	e.wantStatus(200, keyTok, "GET", "/v1/runs")
	e.wantStatus(200, keyTok, "POST", "/v1/counters/c/next") // a submitter route that also takes job tokens
	for _, rt := range [][2]string{
		{"GET", "/v1/worker/connect"}, {"GET", "/v1/worker/jobs/x/context"},
		{"GET", "/v1/worker/jobs/x/inputs/y/z"}, {"PUT", "/v1/worker/jobs/x/artifacts/p"}, {"PUT", "/v1/builds/t/f/1"},
	} {
		e.wantStatus(401, keyTok, rt[0], rt[1])
	}
	if _, _, err := e.dial(keyTok); err == nil {
		t.Fatal("a pulled key opened the worker websocket")
	}
}

func TestB7_WorkerRouteNeverStartsMissPull(t *testing.T) {
	list := keyclienttest.NewList(t)
	e := keyEnv(t, list, nil) // refresh_on_miss 5s: a miss on a submitter route would pull
	e.waitPulled()
	before := list.Pulls()
	for _, rt := range [][2]string{
		{"GET", "/v1/worker/connect"}, {"GET", "/v1/worker/jobs/x/context"},
		{"GET", "/v1/worker/jobs/x/inputs/y/z"}, {"PUT", "/v1/worker/jobs/x/artifacts/p"}, {"PUT", "/v1/builds/t/f/1"},
	} {
		e.wantStatus(401, "unknown-key", rt[0], rt[1])
	}
	// A job token is never looked up in the list, even on a submitter route.
	e.wantStatus(401, "hjt_not-a-real-job-token", "GET", "/v1/runs")
	e.wantStatus(401, "hjt_not-a-real-job-token", "POST", "/v1/counters/c/next")
	e.wantStatus(401, "", "GET", "/v1/runs") // an empty bearer is never looked up
	if got := list.Pulls(); got != before {
		t.Fatalf("%d pulls started by requests that must not consult the list", got-before)
	}
	// Control: an unknown key on a submitter route does pull.
	e.wantStatus(401, "unknown-key", "GET", "/v1/runs")
	if got := list.Pulls(); got != before+1 {
		t.Fatalf("control: pulls grew by %d, want 1", got-before)
	}
}

func keyListCfgError(t *testing.T, tweak func(*config.Config)) *env {
	t.Helper()
	e := newEnv(t, nil) // a valid config and tokens file; nothing is started with the key list
	e.close()
	tweak(&e.cfg)
	return e
}

func TestInsecureHTTP(t *testing.T) {
	const insecureURL = "http://keys.example/v1/lists/hourchd"
	const suffix = "; set personal_keys.allow_insecure_http: true only for a network path you trust"
	e := keyListCfgError(t, func(c *config.Config) {
		c.PersonalKeys.URL = insecureURL
		c.PersonalKeys.CacheFile = filepath.Join(t.TempDir(), "c.json")
	})
	_, nerr := keylist.New(keylist.Options{URL: insecureURL, CacheFile: e.cfg.PersonalKeys.CacheFile,
		Accept: func(keylist.Entry) (bool, string) { return true, "" }})
	if nerr == nil {
		t.Fatal("keyclient accepts plain http to a non-loopback host")
	}
	want := "personal_keys: " + nerr.Error() + suffix
	if err := app.Check(e.cfg); err == nil || err.Error() != want {
		t.Fatalf("Check: %v\nwant %q", err, want)
	}
	if err := os.RemoveAll(e.cfg.StateDir); err != nil { // newEnv made it
		t.Fatal(err)
	}
	if _, err := app.Build(e.cfg, app.Deps{Objects: objects.NewMem(), Log: testLog()}); err == nil || err.Error() != want {
		t.Fatalf("Build: %v\nwant %q", err, want)
	}
	if _, err := os.Stat(e.cfg.StateDir); err == nil {
		t.Fatal("Build touched the state dir before refusing the key list")
	}
	// Allowed: Check passes. Nothing is started, nothing is sent.
	e.cfg.PersonalKeys.AllowInsecureHTTP = true
	if err := app.Check(e.cfg); err != nil {
		t.Fatalf("Check with allow_insecure_http: %v", err)
	}
	// Loopback needs no flag.
	e.cfg.PersonalKeys.AllowInsecureHTTP = false
	e.cfg.PersonalKeys.URL = "http://127.0.0.1:9/x"
	if err := app.Check(e.cfg); err != nil {
		t.Fatalf("Check with a loopback http url: %v", err)
	}
}

func TestUnreadableTokenFile(t *testing.T) {
	list := keyclienttest.NewList(t)
	e := keyListCfgError(t, func(c *config.Config) {
		c.PersonalKeys.URL = list.URL()
		c.PersonalKeys.CacheFile = filepath.Join(t.TempDir(), "c.json")
		c.PersonalKeys.TokenFile = filepath.Join(t.TempDir(), "missing.token")
	})
	_, nerr := keylist.New(keylist.Options{URL: list.URL(), CacheFile: e.cfg.PersonalKeys.CacheFile, TokenFile: e.cfg.PersonalKeys.TokenFile,
		Accept: func(keylist.Entry) (bool, string) { return true, "" }})
	if nerr == nil {
		t.Fatal("keyclient accepts a missing token file")
	}
	want := "personal_keys: " + nerr.Error()
	if err := app.Check(e.cfg); err == nil || err.Error() != want {
		t.Fatalf("Check: %v, want %q", err, want)
	}
	if _, err := app.Build(e.cfg, app.Deps{Objects: objects.NewMem(), Log: testLog()}); err == nil || err.Error() != want {
		t.Fatalf("Build: %v, want %q", err, want)
	}
}

func (e *env) statsPersonalKeys() map[string]any {
	e.t.Helper()
	b := e.json("GET", "/v1/stats", subTok, nil, 200, nil)
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		e.t.Fatal(err)
	}
	pk, _ := m["personal_keys"].(map[string]any)
	return pk
}

func TestStatsPersonalKeysAbsentWithoutURL(t *testing.T) {
	e := newEnv(t, nil)
	b := e.json("GET", "/v1/stats", subTok, nil, 200, nil)
	if strings.Contains(string(b), "personal_keys") {
		t.Fatalf("stats without url: %s", b)
	}
	if e.a.KeyList != nil {
		t.Fatal("KeyList is set without personal_keys.url")
	}
	// No change: an unknown token is 401 as before, a job-prefixed one too.
	e.wantStatus(401, "unknown-token", "GET", "/v1/runs")
	e.wantStatus(401, "hjt_unknown", "GET", "/v1/runs")
	e.wantStatus(200, subTok, "GET", "/v1/runs")
}

func TestStatsPersonalKeysWithURL(t *testing.T) {
	const pullToken = "pull-token-0a1b2c3d4e5f"
	list := keyclienttest.NewList(t)
	list.RequireToken(pullToken)
	list.Add(keyName, "submitter", keyTok)
	tokFile := filepath.Join(t.TempDir(), "pull.token")
	if err := os.WriteFile(tokFile, []byte(pullToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e := keyEnv(t, list, func(c *config.Config) { c.PersonalKeys.TokenFile = tokFile })
	e.waitPulled()
	b := e.json("GET", "/v1/stats", subTok, nil, 200, nil)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	pk, ok := m["personal_keys"].(map[string]any)
	if !ok {
		t.Fatalf("no personal_keys in %s", b)
	}
	if pk["origin"] != "provider" || pk["entries"] != float64(1) || pk["cache_failing"] != false ||
		pk["age_seconds"] == nil || pk["list_time"] == nil || pk["last_success"] == nil || pk["last_error"] != nil {
		t.Fatalf("personal_keys = %v", pk)
	}
	for _, secret := range []string{keyTok, sha([]byte(keyTok)), pullToken, tokFile, e.cfg.PersonalKeys.CacheFile} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("stats leaks %q: %s", secret, b)
		}
	}
	// A failing provider shows as a (safe) last_error and the list stays.
	list.FailWith(keyclienttest.Status(500, ""))
	_ = e.a.KeyList.Pull(context.Background())
	pk = e.statsPersonalKeys()
	if s, _ := pk["last_error"].(string); s == "" || len([]rune(s)) > 200 || strings.Contains(s, pullToken) {
		t.Fatalf("last_error = %v", pk["last_error"])
	}
	if pk["origin"] != "provider" || pk["entries"] != float64(1) {
		t.Fatalf("after a failed pull: %v", pk)
	}
	// healthz is unchanged: no personal_keys there.
	_, hbody := e.do("GET", "/v1/healthz", "", nil, nil)
	if strings.Contains(string(hbody), "personal_keys") {
		t.Fatalf("healthz: %s", hbody)
	}
}

func TestStatsPersonalKeysOriginNone(t *testing.T) {
	list := keyclienttest.NewList(t)
	list.FailWith(keyclienttest.Status(500, ""))
	e := keyEnv(t, list, nil)
	e.waitPullFailed()
	pk := e.statsPersonalKeys()
	if pk["origin"] != "none" || pk["entries"] != float64(0) || pk["age_seconds"] != nil || pk["list_time"] != nil || pk["last_success"] != nil || pk["last_error"] == nil {
		t.Fatalf("personal_keys = %v", pk)
	}
}

func TestTokensTakenLocally(t *testing.T) {
	tk, err := httpapi.NewTokens(httpapi.TokensFile{
		Submitters: map[string]string{"alice": "s-token"}, Workers: map[string]string{"w1": "w-token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	hash := func(s string) string { return strings.ToUpper(sha([]byte(s))) } // case must not matter
	for _, e := range []keylist.Entry{
		{Name: "alice"}, {Name: "w1"}, {Name: "x", SHA256: sha([]byte("s-token"))}, {Name: "x", SHA256: hash("w-token")},
	} {
		if !tk.TakenLocally(e) {
			t.Errorf("%+v not taken", e)
		}
	}
	if tk.TakenLocally(keylist.Entry{Name: "bob", SHA256: sha([]byte("other"))}) {
		t.Error("an unrelated entry is taken")
	}
}

// The startup log line: exactly one, after Start, only with a key list.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func startLines(t *testing.T, withList bool) []string {
	return startLinesWith(t, withList, nil)
}

// startLinesWith is startLines with a hook that runs before Build, given the
// list and the cache path (for example to write an aged cache and hold pulls).
func startLinesWith(t *testing.T, withList bool, prep func(list *keyclienttest.List, cache string)) []string {
	t.Helper()
	list := keyclienttest.NewList(t)
	list.Add(keyName, "submitter", keyTok)
	cache := filepath.Join(t.TempDir(), "c.json")
	e := newEnv(t, func(c *config.Config) {
		if withList {
			c.PersonalKeys.URL = list.URL()
			c.PersonalKeys.CacheFile = cache
			c.PersonalKeys.Refresh = config.Duration(time.Hour)
		}
	})
	if prep != nil {
		prep(list, cache)
	}
	e.close()
	buf := &syncBuf{}
	a, err := app.Build(e.cfg, app.Deps{Clock: e.clk, Objects: e.obj, Log: slog.New(slog.NewTextHandler(buf, nil))})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := buf.String() // what Start itself logged
	cancel()
	a.API.CloseStreams()
	_ = a.Close()
	var out []string
	for _, l := range strings.Split(snapshot, "\n") {
		if strings.Contains(l, "personal_keys") {
			out = append(out, l)
		}
	}
	return out
}

func TestStartLogsOneLine(t *testing.T) {
	lines := startLines(t, true)
	if len(lines) != 1 || !strings.Contains(lines[0], `msg="personal_keys: list origin=`) || !strings.Contains(lines[0], " entries=") || !strings.Contains(lines[0], " age=") {
		t.Fatalf("lines: %q", lines)
	}
	if lines := startLines(t, false); len(lines) != 0 {
		t.Fatalf("without url: %q", lines)
	}
}

// TestStartLogLineAge: the start line's age is whole seconds, and exactly 0s
// when nothing is loaded (origin none).
func TestStartLogLineAge(t *testing.T) {
	// Origin none: no cache, and the first pull is held so it cannot land.
	lines := startLinesWith(t, true, func(list *keyclienttest.List, _ string) { list.Block() })
	if len(lines) != 1 || !strings.Contains(lines[0], `msg="personal_keys: list origin=none entries=0 age=0s"`) {
		t.Fatalf("origin none: %q", lines)
	}

	// Origin cache: a cache written 90.5 s ago is reported as 1m30s or 1m31s,
	// never with a fraction.
	lines = startLinesWith(t, true, func(list *keyclienttest.List, cache string) {
		src, err := keylist.New(keylist.Options{URL: list.URL(), CacheFile: cache,
			Accept: func(keylist.Entry) (bool, string) { return true, "" }})
		if err != nil {
			t.Fatal(err)
		}
		if err := src.Pull(context.Background()); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-90*time.Second - 500*time.Millisecond)
		if err := os.Chtimes(cache, old, old); err != nil {
			t.Fatal(err)
		}
		list.Block()
	})
	re := regexp.MustCompile(`msg="personal_keys: list origin=cache entries=1 age=1m3[01]s"$`)
	if len(lines) != 1 || !re.MatchString(lines[0]) {
		t.Fatalf("origin cache: %q, want a match for %s", lines, re)
	}
}
