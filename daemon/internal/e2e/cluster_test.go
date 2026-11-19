package e2e

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	api "github.com/resurgentech/hourch/api"
	worker "github.com/resurgentech/hourch/worker"

	"github.com/resurgentech/hourch/daemon/internal/app"
	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/pwfake"
)

const (
	opsToken = "tok-e2e-ops"
	uiUser   = "ops"
	uiPass   = "e2e-ui-pass"
)

var workerTokens = map[string]string{"tiny-1": "tok-e2e-tiny-1", "kernel-1": "tok-e2e-kernel-1"}

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == helperArg {
		os.Exit(helperMain(os.Args[2:]))
	}
	os.Exit(m.Run())
}

func logger(t *testing.T, name string) *slog.Logger {
	if os.Getenv("HOURCHD_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})).With("who", name)
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// cluster is an in-process hourchd behind a stable URL (so it can be
// restarted under the workers), real workers and a fake powerwarden.
type cluster struct {
	t       *testing.T // the current step
	root    *testing.T
	dir     string
	shared  string // E2E_DIR for the jobs
	dockRec string // what the fake docker recorded
	cfg     config.Config
	obj     *objects.Mem
	front   *httptest.Server
	pw      *pwfake.Fake

	handler atomic.Pointer[http.Handler]
	a       *app.App
	stopApp context.CancelFunc

	mu      sync.Mutex
	workers map[string]*runningWorker
}

type runningWorker struct {
	w      *worker.Worker
	cancel context.CancelFunc
	done   chan struct{}
}

// fakeDocker records create's argv and the env file (mode and content;
// the worker deletes it before start) and "runs" the container.
const fakeDocker = `#!/bin/sh
rec='@REC@'
case "$1" in
create)
  printf '%s\n' "$@" > "$rec/create.argv"
  while [ $# -gt 0 ]; do
    if [ "$1" = "--env-file" ]; then cp "$2" "$rec/env-file"; stat -c %a "$2" > "$rec/env-file.mode"; fi
    shift
  done
  echo fakecontainerid
  ;;
start)
  echo "fake container ran: $3"
  ;;
*)
  printf '%s\n' "$*" >> "$rec/other.log"
  ;;
esac
exit 0
`

func newCluster(t *testing.T) *cluster {
	t.Helper()
	dir := t.TempDir()
	c := &cluster{t: t, root: t, dir: dir, shared: filepath.Join(dir, "shared"), dockRec: filepath.Join(dir, "docker-rec"),
		obj: objects.NewMem(), workers: map[string]*runningWorker{}}
	bin := filepath.Join(dir, "bin")
	for _, d := range []string{c.shared, c.dockRec, bin} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(strings.ReplaceAll(fakeDocker, "@REC@", c.dockRec)), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	// Fake powerwarden.
	c.pw = pwfake.New("pw-token")
	pws := httptest.NewServer(c.pw)
	t.Cleanup(pws.Close)

	cfg := config.Default()
	cfg.StateDir = filepath.Join(dir, "state")
	cfg.TokensFile = filepath.Join(dir, "tokens.yaml")
	cfg.MinIO.Endpoint = "minio.test:9000" // required, no default; never connected to (an in-memory store is injected)
	cfg.Powerwarden.URL = pws.URL
	cfg.Powerwarden.TokenFile = filepath.Join(dir, "powerwarden.token")
	cfg.Powerwarden.Refresh = config.Duration(300 * time.Millisecond)
	cfg.UI.HtpasswdFile = filepath.Join(dir, "ui.htpasswd")
	cfg.Dispatch.Heartbeat = config.Duration(time.Second)
	cfg.Dispatch.Lease = config.Duration(4 * time.Second)
	cfg.Dispatch.AcceptTimeout = config.Duration(5 * time.Second)
	cfg.Dispatch.ReaperInterval = config.Duration(100 * time.Millisecond)
	cfg.Events.Heartbeat = config.Duration(200 * time.Millisecond)
	cfg.Limits.DefaultTimeout = config.Duration(5 * time.Minute)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	c.cfg = cfg
	tokens := "submitters:\n  ops: " + opsToken + "\nworkers:\n"
	for id, tok := range workerTokens {
		tokens += "  " + id + ": " + tok + "\n"
	}
	hash, _ := bcrypt.GenerateFromPassword([]byte(uiPass), bcrypt.MinCost)
	for p, body := range map[string]string{
		cfg.TokensFile:            tokens,
		cfg.Powerwarden.TokenFile: "pw-token\n",
		cfg.UI.HtpasswdFile:       uiUser + ":" + string(hash) + "\n",
	} {
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	c.front = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := c.handler.Load()
		if h == nil {
			http.Error(w, "hourchd restarting", http.StatusServiceUnavailable)
			return
		}
		(*h).ServeHTTP(w, r)
	}))
	c.startHourchd()
	t.Cleanup(func() {
		c.stopAllWorkers()
		c.stopHourchd()
		c.front.CloseClientConnections()
		c.front.Close()
	})
	return c
}

func (c *cluster) startHourchd() {
	c.t.Helper()
	a, err := app.Build(c.cfg, app.Deps{Objects: c.obj, Log: logger(c.t, "hourchd")})
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := a.Start(ctx); err != nil {
		cancel()
		c.t.Fatal(err)
	}
	c.a, c.stopApp = a, cancel
	h := a.Handler()
	c.handler.Store(&h)
}

// stopHourchd is a clean daemon stop: new requests get 503, the worker
// connections close, background loops end, the store closes.
func (c *cluster) stopHourchd() {
	if c.a == nil {
		return
	}
	c.handler.Store(nil)
	c.stopApp()
	if err := c.a.Close(); err != nil {
		c.root.Errorf("close hourchd: %v", err)
	}
	c.a = nil
}

func (c *cluster) restartHourchd() {
	c.t.Helper()
	c.stopHourchd()
	c.startHourchd()
}

func (c *cluster) wsURL() string {
	return "ws" + strings.TrimPrefix(c.front.URL, "http") + "/v1/worker/connect"
}

func (c *cluster) startWorker(id, hostname string, tags, caps []string) {
	c.t.Helper()
	cfg := worker.DefaultConfig()
	cfg.HourchdURL = c.wsURL()
	cfg.WorkerID = id
	cfg.Hostname = hostname
	cfg.Tags = tags
	cfg.Capabilities = caps
	cfg.TokenFile = "/unused/in/process"
	cfg.WorkDir = filepath.Join(c.dir, "work-"+id)
	cfg.EnvAllowlist = []string{"PATH", "HOME", "TMPDIR"}
	cfg.Reconnect = worker.ReconnectConfig{Initial: worker.Duration(50 * time.Millisecond), Max: worker.Duration(300 * time.Millisecond)}
	w, err := worker.New(cfg, workerTokens[id], worker.Options{
		Logger:        logger(c.t, id),
		HeartbeatUnit: 100 * time.Millisecond, // heartbeats every 100ms against a 4s lease
		KillGrace:     500 * time.Millisecond,
		ChunkMaxDelay: 50 * time.Millisecond,
		Version:       "e2e",
	})
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rw := &runningWorker{w: w, cancel: cancel, done: make(chan struct{})}
	root := c.root
	go func() {
		defer close(rw.done)
		if err := w.Run(ctx); err != nil {
			root.Errorf("worker %s: %v", id, err)
		}
	}()
	c.mu.Lock()
	c.workers[id] = rw
	c.mu.Unlock()
	c.waitWorker(id, true)
}

// killWorker stops a worker the hard way: its job is killed and no result
// is sent (a worker process dying mid-job).
func (c *cluster) killWorker(id string) {
	c.t.Helper()
	c.mu.Lock()
	rw := c.workers[id]
	delete(c.workers, id)
	c.mu.Unlock()
	rw.cancel()
	select {
	case <-rw.done:
	case <-time.After(20 * time.Second):
		c.t.Fatalf("worker %s did not stop", id)
	}
}

// drainWorker stops an idle worker cleanly.
func (c *cluster) drainWorker(id string) {
	c.t.Helper()
	c.mu.Lock()
	rw := c.workers[id]
	delete(c.workers, id)
	c.mu.Unlock()
	rw.w.Drain()
	select {
	case <-rw.done:
	case <-time.After(20 * time.Second):
		c.t.Fatalf("worker %s did not drain", id)
	}
	rw.cancel()
}

func (c *cluster) stopAllWorkers() {
	c.mu.Lock()
	ws := c.workers
	c.workers = map[string]*runningWorker{}
	c.mu.Unlock()
	for _, rw := range ws {
		rw.cancel()
		<-rw.done
	}
}

// ---- API access ----

func (c *cluster) do(method, path, token string, body io.Reader) (*http.Response, []byte) {
	c.t.Helper()
	req, err := http.NewRequest(method, c.front.URL+path, body)
	if err != nil {
		c.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (c *cluster) getJSON(path string, out any) {
	c.t.Helper()
	resp, b := c.do("GET", path, opsToken, nil)
	if resp.StatusCode != 200 {
		c.t.Fatalf("GET %s = %d: %s", path, resp.StatusCode, b)
	}
	if err := json.Unmarshal(b, out); err != nil {
		c.t.Fatalf("GET %s: %v: %s", path, err, b)
	}
}

func (c *cluster) job(id string) api.JobDetail {
	c.t.Helper()
	var j api.JobDetail
	c.getJSON("/v1/jobs/"+id, &j)
	return j
}

func (c *cluster) run(id string) api.Run {
	c.t.Helper()
	var r api.Run
	c.getJSON("/v1/runs/"+id, &r)
	return r
}

func (c *cluster) jobByName(r api.Run, name string) api.JobSummary {
	c.t.Helper()
	for _, j := range r.Jobs {
		if j.Name == name {
			return j
		}
	}
	c.t.Fatalf("run %s has no job %s", r.Id, name)
	return api.JobSummary{}
}

func (c *cluster) log(jobID string, attempt int) string {
	c.t.Helper()
	resp, b := c.do("GET", "/v1/jobs/"+jobID+"/log?attempt="+itoa(attempt), opsToken, nil)
	if resp.StatusCode != 200 {
		c.t.Fatalf("log %s/%d = %d %s", jobID, attempt, resp.StatusCode, b)
	}
	return string(b)
}

func itoa(n int) string { return strconv.Itoa(n) }

// step runs one stage as a subtest; a failed stage stops the test (later
// stages depend on the cluster state it leaves).
func (c *cluster) step(name string, fn func(t *testing.T)) {
	c.root.Helper()
	ok := c.root.Run(name, func(t *testing.T) {
		c.t = t
		defer func() { c.t = c.root }()
		fn(t)
	})
	if !ok {
		c.root.FailNow()
	}
}

// uiPage fetches a UI page with basic auth.
func (c *cluster) uiPage(path string) string {
	c.t.Helper()
	req, _ := http.NewRequest("GET", c.front.URL+path, nil)
	req.SetBasicAuth(uiUser, uiPass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		c.t.Fatalf("UI %s = %d: %s", path, resp.StatusCode, b)
	}
	return string(b)
}

func (c *cluster) waitWorker(id string, connected bool) {
	c.t.Helper()
	waitFor(c.t, "worker "+id+" connected="+boolStr(connected), 20*time.Second, func() bool {
		resp, b := c.do("GET", "/v1/workers", opsToken, nil)
		if resp.StatusCode != 200 {
			return false
		}
		var ws []api.Worker
		_ = json.Unmarshal(b, &ws)
		for _, w := range ws {
			if w.Id == id {
				return w.Connected == connected
			}
		}
		return !connected
	})
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *cluster) waitShared(name string) string {
	c.t.Helper()
	var s string
	waitFor(c.t, name, 30*time.Second, func() bool {
		b, err := os.ReadFile(filepath.Join(c.shared, name))
		s = string(b)
		return err == nil
	})
	return s
}

func (c *cluster) release(name string) {
	c.t.Helper()
	if err := os.WriteFile(filepath.Join(c.shared, name+".release"), nil, 0o644); err != nil {
		c.t.Fatal(err)
	}
}
