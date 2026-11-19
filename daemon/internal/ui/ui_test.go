package ui_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/app"
	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/ui"
)

const (
	uiUser = "jared"
	uiPass = "s3cret-ui-password"
	// secretValue is submitted as a run secret; no page may show it.
	secretValue = "TOPSECRET-value-9f3a1c"
)

type uenv struct {
	t   *testing.T
	a   *app.App
	srv *httptest.Server
	clk *clock.Fake
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func setup(t *testing.T) *uenv {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StateDir = filepath.Join(dir, "state")
	cfg.TokensFile = filepath.Join(dir, "tokens.yaml")
	cfg.UI.HtpasswdFile = filepath.Join(dir, "ui.htpasswd")
	cfg.Powerwarden.URL = ""
	cfg.Events.Heartbeat = config.Duration(50 * time.Millisecond)
	if err := os.WriteFile(cfg.TokensFile, []byte("submitters:\n  ops: tok-ops\nworkers:\n  w1: tok-w1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	hash, _ := bcrypt.GenerateFromPassword([]byte(uiPass), bcrypt.MinCost)
	if err := os.WriteFile(cfg.UI.HtpasswdFile, []byte("# ops\n"+uiUser+":"+string(hash)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
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

// get fetches path with basic auth (auth false: none) and returns the
// response and body.
func (e *uenv) get(path string, auth bool, hdr ...string) (*http.Response, string) {
	e.t.Helper()
	req, _ := http.NewRequest("GET", e.srv.URL+path, nil)
	if auth {
		req.SetBasicAuth(uiUser, uiPass)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := noRedirect.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func (e *uenv) page(path string) string {
	e.t.Helper()
	resp, body := e.get(path, true)
	if resp.StatusCode != 200 {
		e.t.Fatalf("GET %s = %d: %s", path, resp.StatusCode, body)
	}
	return body
}

func (e *uenv) post(path, origin string) *http.Response {
	e.t.Helper()
	req, _ := http.NewRequest("POST", e.srv.URL+path, strings.NewReader(""))
	req.SetBasicAuth(uiUser, uiPass)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	resp, err := noRedirect.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

var noRedirect = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func (e *uenv) submit(s graph.Submission) store.Run {
	e.t.Helper()
	r, err := e.a.Engine.Submit(context.Background(), "ops", s)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *uenv) jobs(runID string) map[string]store.Job {
	e.t.Helper()
	js, err := e.a.Store.RunJobs(runID)
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[string]store.Job{}
	for _, j := range js {
		out[j.Name] = j
	}
	return out
}

// conn is an in-process worker transport.
type conn struct {
	mu   sync.Mutex
	msgs []proto.Message
}

func (c *conn) Send(m proto.Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, m)
	return true
}
func (c *conn) Close(string) {}

func (c *conn) assigns() []*proto.Assign {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*proto.Assign
	for _, m := range c.msgs {
		if a, ok := m.(*proto.Assign); ok {
			out = append(out, a)
		}
	}
	return out
}

// runSecretJob drives a secret-using run to success through the engine:
// the job logs and outputs its secret (the daemon must redact both).
func (e *uenv) runSecretJob() (store.Run, store.Job) {
	e.t.Helper()
	run := e.submit(graph.Submission{
		Workflow: "deploy", Params: map[string]string{"target": "staging"},
		Secrets: map[string]string{"DEPLOY_KEY": secretValue},
		Jobs: []graph.Job{{Name: "push", Argv: []string{"push", "--key-from-env"}, Secrets: []string{"DEPLOY_KEY"},
			Artifacts: []string{"out/report.txt"}}},
	})
	c := &conn{}
	sess, err := e.a.Engine.Hello(c, &proto.Hello{WorkerID: "w1", Hostname: "sdf1.labtwo", Tags: []string{"tiny"}, Version: "t"})
	if err != nil {
		e.t.Fatal(err)
	}
	as := c.assigns()
	if len(as) != 1 || as[0].Secrets["DEPLOY_KEY"] != secretValue {
		e.t.Fatalf("assign did not deliver the secret: %+v", as)
	}
	jid := as[0].Job.ID
	for _, m := range []proto.Message{
		&proto.Accept{JobID: jid},
		&proto.Log{JobID: jid, Stream: "stdout", Seq: 1, Data: "using key " + secretValue + "\n"},
		&proto.Output{JobID: jid, Key: "leak", Value: "k=" + secretValue},
		&proto.Output{JobID: jid, Key: "version", Value: "1.2.3"},
	} {
		if err := e.a.Engine.Handle(sess, m); err != nil {
			e.t.Fatal(err)
		}
	}
	// An artifact, as the worker route would have stored it.
	body := "report " + time.Now().String()
	key := dispatch.ArtifactKey(run.ID, jid, "out/report.txt")
	if err := e.a.Objects.Put(context.Background(), key, strings.NewReader(body), int64(len(body))); err != nil {
		e.t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	if err := e.a.Store.PutArtifact(store.Artifact{JobID: jid, Path: "out/report.txt", Object: key, Size: int64(len(body)), Sha256: hex.EncodeToString(sum[:])}); err != nil {
		e.t.Fatal(err)
	}
	if err := e.a.Engine.Handle(sess, &proto.Result{JobID: jid, ExitCode: 0}); err != nil {
		e.t.Fatal(err)
	}
	e.a.Engine.WaitUploads()
	run, _ = e.a.Store.GetRun(run.ID)
	if run.Status != store.RunSucceeded {
		e.t.Fatalf("secret run status = %s", run.Status)
	}
	j, _ := e.a.Store.GetJob(jid)
	return run, j
}

func TestRootRedirectsToUI(t *testing.T) {
	e := setup(t)
	resp, _ := e.get("/", false)
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/ui/runs" {
		t.Fatalf("GET / = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	resp, _ = e.get("/ui/", true)
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/ui/runs" {
		t.Fatalf("GET /ui/ = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	// The API is still served next to the UI.
	if resp, _ := e.get("/v1/healthz", false); resp.StatusCode != 200 {
		t.Fatalf("healthz = %d", resp.StatusCode)
	}
}

func TestAuthRequired(t *testing.T) {
	e := setup(t)
	run := e.submit(graph.Submission{Workflow: "wf", Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	jid := e.jobs(run.ID)["a"].ID
	paths := []string{"/ui/runs", "/ui/runs/" + run.ID, "/ui/jobs/" + jid, "/ui/workers", "/ui/events",
		"/ui/jobs/" + jid + "/log", "/ui/jobs/" + jid + "/log/stream", "/ui/jobs/" + jid + "/artifacts/x", "/ui/static/app.js"}
	for _, p := range paths {
		resp, _ := e.get(p, false)
		if resp.StatusCode != http.StatusUnauthorized || !strings.Contains(resp.Header.Get("WWW-Authenticate"), "Basic") {
			t.Errorf("GET %s without auth = %d", p, resp.StatusCode)
		}
		req, _ := http.NewRequest("GET", e.srv.URL+p, nil)
		req.SetBasicAuth(uiUser, "wrong")
		r2, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		r2.Body.Close()
		if r2.StatusCode != http.StatusUnauthorized {
			t.Errorf("GET %s with a wrong password = %d", p, r2.StatusCode)
		}
	}
	// A bearer token is not UI auth.
	req, _ := http.NewRequest("GET", e.srv.URL+"/ui/runs", nil)
	req.Header.Set("Authorization", "Bearer tok-ops")
	r3, err := noRedirect.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r3.Body.Close()
	if r3.StatusCode != http.StatusUnauthorized {
		t.Errorf("bearer token on /ui = %d", r3.StatusCode)
	}
	// POSTs too.
	if resp := e.postNoAuth("/ui/runs/" + run.ID + "/cancel"); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("POST cancel without auth = %d", resp.StatusCode)
	}
}

func (e *uenv) postNoAuth(path string) *http.Response {
	e.t.Helper()
	req, _ := http.NewRequest("POST", e.srv.URL+path, nil)
	req.Header.Set("Origin", e.srv.URL)
	resp, err := noRedirect.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

func TestCSRFAndCancel(t *testing.T) {
	e := setup(t)
	run := e.submit(graph.Submission{Workflow: "wf", Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	for _, origin := range []string{"https://evil.example", ""} {
		if resp := e.post("/ui/runs/"+run.ID+"/cancel", origin); resp.StatusCode != http.StatusForbidden {
			t.Fatalf("cancel with Origin %q = %d", origin, resp.StatusCode)
		}
	}
	// Referer of another site is rejected as well.
	req, _ := http.NewRequest("POST", e.srv.URL+"/ui/runs/"+run.ID+"/cancel", nil)
	req.SetBasicAuth(uiUser, uiPass)
	req.Header.Set("Referer", "https://evil.example/ui/runs")
	if resp, err := noRedirect.Do(req); err != nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cancel with foreign Referer: %v %v", resp, err)
	}
	if r, _ := e.a.Store.GetRun(run.ID); r.Status != store.RunRunning {
		t.Fatalf("run %s after rejected posts", r.Status)
	}
	resp := e.post("/ui/runs/"+run.ID+"/cancel", e.srv.URL)
	if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(resp.Header.Get("Location"), "/ui/runs/"+run.ID) {
		t.Fatalf("same-origin cancel = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if r, _ := e.a.Store.GetRun(run.ID); r.Status != store.RunCancelled {
		t.Fatalf("run %s after cancel", r.Status)
	}
	// Job cancel.
	run2 := e.submit(graph.Submission{Workflow: "wf", Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	jid := e.jobs(run2.ID)["a"].ID
	if resp := e.post("/ui/jobs/"+jid+"/cancel", e.srv.URL); resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("job cancel = %d", resp.StatusCode)
	}
	if j, _ := e.a.Store.GetJob(jid); j.Status != store.JobCancelled {
		t.Fatalf("job %s after cancel", j.Status)
	}
}

func TestPagesRender(t *testing.T) {
	e := setup(t)
	_, secretJob := e.runSecretJob() // first: its worker takes the oldest ready job
	run := e.submit(graph.Submission{
		Workflow: "hello", Params: map[string]string{"variant": "static"},
		Jobs: []graph.Job{
			{Name: "settings", Argv: []string{"settings"}, Tags: []string{"tiny"}},
			{Name: "build", Argv: []string{"just", "package", "${outputs.settings.version}"}, Tags: []string{"kernel"},
				Image: "registry.labtwo/kbuild:1", Artifacts: []string{"output/"}},
			{Name: "publish", Argv: []string{"publish"}, Tags: []string{"tiny"}, DependsOn: []string{"build"}, ConcurrencyKey: "aptly"},
		},
	})
	other := e.submit(graph.Submission{Workflow: "flarp", Jobs: []graph.Job{{Name: "x", Argv: []string{"x"}}}})
	js := e.jobs(run.ID)

	body := e.page("/ui/runs")
	for _, want := range []string{"hello #1", "flarp #1", "deploy #1", "/ui/runs/" + run.ID, "ops", "ready <b>", "powerwarden <b class=\"muted\">disabled"} {
		if !strings.Contains(body, want) {
			t.Errorf("runs page lacks %q", want)
		}
	}
	body = e.page("/ui/runs?workflow=hello")
	if !strings.Contains(body, "hello #1") || strings.Contains(body, "flarp #1") {
		t.Errorf("workflow filter: %s", body)
	}
	body = e.page("/ui/runs?status=succeeded")
	if !strings.Contains(body, "deploy #1") || strings.Contains(body, "hello #1") {
		t.Errorf("status filter wrong")
	}
	if resp, _ := e.get("/ui/runs?status=bogus", true); resp.StatusCode != 200 {
		t.Errorf("bad filter = %d", resp.StatusCode)
	}

	body = e.page("/ui/runs/" + run.ID)
	for _, want := range []string{"<svg", "/ui/jobs/" + js["settings"].ID, "/ui/jobs/" + js["build"].ID, "/ui/jobs/" + js["publish"].ID,
		"class=\"edge", "n-leased", "n-pending", "variant", "static", "Cancel run"} {
		if !strings.Contains(body, want) {
			t.Errorf("run page lacks %q", want)
		}
	}
	if n := strings.Count(body, "class=\"edge"); n != 2 { // settings→build, build→publish
		t.Errorf("run page has %d edges, want 2", n)
	}
	if strings.Contains(body, "/retry") {
		t.Error("retry offered for a running run")
	}

	body = e.page("/ui/jobs/" + js["build"].ID)
	for _, want := range []string{"just package ${outputs.settings.version}", "registry.labtwo/kbuild:1", "kernel", "Not started", "output/ (not uploaded)"} {
		if !strings.Contains(body, want) {
			t.Errorf("job page lacks %q", want)
		}
	}

	body = e.page("/ui/jobs/" + secretJob.ID)
	for _, want := range []string{"id=\"log\"", "/ui/jobs/" + secretJob.ID + "/log/stream?attempt=1", "DEPLOY_KEY", "version", "1.2.3",
		"/ui/jobs/" + secretJob.ID + "/artifacts/out/report.txt", "w1"} {
		if !strings.Contains(body, want) {
			t.Errorf("secret job page lacks %q", want)
		}
	}
	body = e.page("/ui/workers")
	for _, want := range []string{"w1", "sdf1.labtwo", "tiny", "connected"} {
		if !strings.Contains(body, want) {
			t.Errorf("workers page lacks %q", want)
		}
	}
	// The live region alone, for the script's refresh.
	_, part := e.get("/ui/runs/"+other.ID, true, "X-Partial", "1")
	if strings.Contains(part, "<html") || !strings.Contains(part, "flarp #1") {
		t.Errorf("partial render: %s", part)
	}
	// Unknown ids.
	if resp, _ := e.get("/ui/runs/01JZZZZZZZZZZZZZZZZZZZZZZZ", true); resp.StatusCode != 404 {
		t.Errorf("unknown run = %d", resp.StatusCode)
	}
}

func TestArtifactAndLogProxied(t *testing.T) {
	e := setup(t)
	_, j := e.runSecretJob()
	resp, body := e.get("/ui/jobs/"+j.ID+"/artifacts/out/report.txt", true)
	if resp.StatusCode != 200 || !strings.HasPrefix(body, "report ") {
		t.Fatalf("artifact download = %d %q", resp.StatusCode, body)
	}
	resp, body = e.get("/ui/jobs/"+j.ID+"/log", true)
	if resp.StatusCode != 200 || !strings.Contains(body, "using key ***") {
		t.Fatalf("log = %d %q", resp.StatusCode, body)
	}
	resp, body = e.get("/ui/jobs/"+j.ID+"/log/stream?attempt=1", true)
	if resp.StatusCode != 200 || !strings.Contains(body, "event: log") || !strings.Contains(body, "event: end") {
		t.Fatalf("log stream = %d %q", resp.StatusCode, body)
	}
	// The event stream is the hub, behind basic auth.
	req, _ := http.NewRequest("GET", e.srv.URL+"/ui/events", nil)
	req.SetBasicAuth(uiUser, uiPass)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer ev.Body.Close()
	if ev.StatusCode != 200 || ev.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("/ui/events = %d %s", ev.StatusCode, ev.Header.Get("Content-Type"))
	}
	line, _ := bufio.NewReader(ev.Body).ReadString('\n')
	if !strings.HasPrefix(line, "id: ") && !strings.HasPrefix(line, ":") {
		t.Fatalf("first event line %q", line)
	}
}

func TestRetry(t *testing.T) {
	e := setup(t)
	run := e.submit(graph.Submission{Workflow: "wf", Params: map[string]string{"p": "1"}, Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	if err := e.a.Engine.CancelRun(run.ID); err != nil {
		t.Fatal(err)
	}
	body := e.page("/ui/runs/" + run.ID)
	if !strings.Contains(body, "/ui/runs/"+run.ID+"/retry") {
		t.Fatal("retry not offered for a finished run without secrets")
	}
	if resp := e.post("/ui/runs/"+run.ID+"/retry", "https://evil.example"); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin retry = %d", resp.StatusCode)
	}
	resp := e.post("/ui/runs/"+run.ID+"/retry", e.srv.URL)
	loc := resp.Header.Get("Location")
	if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(loc, "/ui/runs/") || strings.HasPrefix(loc, "/ui/runs/"+run.ID) {
		t.Fatalf("retry = %d %q", resp.StatusCode, loc)
	}
	runs, _ := e.a.Store.ListRuns(store.RunFilter{Workflow: "wf", Limit: 10})
	if len(runs) != 2 || runs[0].Number != 2 || runs[0].Params["p"] != "1" || runs[0].SubmittedBy != "ui:"+uiUser {
		t.Fatalf("runs after retry: %+v", runs)
	}
	if err := e.a.Engine.CancelRun(runs[0].ID); err != nil { // keep the worker below for the secret job
		t.Fatal(err)
	}

	// A run that used secrets which are gone (its secret-using job
	// succeeded, so they were deleted): no retry button, a POST is refused.
	srun, _ := e.runSecretJob()
	body = e.page("/ui/runs/" + srun.ID)
	if strings.Contains(body, "/retry") {
		t.Fatal("retry offered for a run whose secrets are gone")
	}
	if !strings.Contains(body, "run the workflow binary again") {
		t.Fatal("no explanation for the missing retry")
	}
	resp = e.post("/ui/runs/"+srun.ID+"/retry", e.srv.URL)
	loc = resp.Header.Get("Location")
	if resp.StatusCode != http.StatusSeeOther || !strings.HasPrefix(loc, "/ui/runs/"+srun.ID+"?") {
		t.Fatalf("secret retry = %d %q", resp.StatusCode, loc)
	}
	u, _ := url.Parse(loc)
	if u.Query().Get("err") != ui.ErrRetrySecrets.Error() {
		t.Fatalf("secret retry err = %q", u.Query().Get("err"))
	}
	if runs, _ := e.a.Store.ListRuns(store.RunFilter{Workflow: "deploy", Limit: 10}); len(runs) != 1 {
		t.Fatalf("a secret run was retried: %d runs", len(runs))
	}
	// The refusal is shown, escaped.
	if body := e.page(loc); !strings.Contains(body, "stored secrets are gone") {
		t.Fatal("refusal not shown")
	}
}

// A failed run whose stored secrets still exist can be retried from the UI:
// the retry reuses them. The header counts runs holding secrets.
func TestRetryReusesStoredSecrets(t *testing.T) {
	e := setup(t)
	run := e.submit(graph.Submission{
		Workflow: "deploy", Secrets: map[string]string{"DEPLOY_KEY": secretValue},
		Jobs: []graph.Job{{Name: "push", Argv: []string{"push"}, Secrets: []string{"DEPLOY_KEY"}, Tags: []string{"nobody"}}},
	})
	if body := e.page("/ui/runs"); !strings.Contains(body, "holding secrets <b class=\"warn\">1</b>") {
		t.Fatal("header does not count runs holding secrets")
	}
	if err := e.a.Engine.CancelRun(run.ID); err != nil {
		t.Fatal(err)
	}
	body := e.page("/ui/runs/" + run.ID)
	if !strings.Contains(body, "/ui/runs/"+run.ID+"/retry") {
		t.Fatal("retry not offered for a cancelled run that still holds its secrets")
	}
	resp := e.post("/ui/runs/"+run.ID+"/retry", e.srv.URL)
	loc := resp.Header.Get("Location")
	if resp.StatusCode != http.StatusSeeOther || strings.HasPrefix(loc, "/ui/runs/"+run.ID) || strings.Contains(loc, "err=") {
		t.Fatalf("retry = %d %q", resp.StatusCode, loc)
	}
	runs, _ := e.a.Store.ListRuns(store.RunFilter{Workflow: "deploy", Limit: 10})
	if len(runs) != 2 || !e.a.Engine.HoldsSecrets(runs[0].ID) {
		t.Fatalf("retry did not carry the secrets: %+v", runs)
	}
	if body := e.page("/ui/runs"); !strings.Contains(body, "holding secrets <b class=\"warn\">2</b>") {
		t.Fatal("header count after retry")
	}
	if strings.Contains(e.page("/ui/runs/"+runs[0].ID), secretValue) {
		t.Fatal("secret value on a page")
	}
}

func TestNoSecretValueInAnyPage(t *testing.T) {
	e := setup(t)
	run, j := e.runSecretJob()
	// Every page and route the UI serves, full and partial.
	paths := []string{
		"/ui/runs", "/ui/runs?workflow=deploy", "/ui/runs?status=succeeded", "/ui/runs/" + run.ID,
		"/ui/jobs/" + j.ID, "/ui/jobs/" + j.ID + "?attempt=1", "/ui/workers",
		"/ui/jobs/" + j.ID + "/log", "/ui/jobs/" + j.ID + "/log?attempt=1", "/ui/jobs/" + j.ID + "/log/stream?attempt=1",
		"/ui/jobs/" + j.ID + "/artifacts/out/report.txt", "/ui/static/app.js", "/ui/static/app.css",
	}
	sawRedacted := false
	for _, p := range paths {
		for _, partial := range []bool{false, true} {
			var hdr []string
			if partial {
				hdr = []string{"X-Partial", "1"}
			}
			resp, body := e.get(p, true, hdr...)
			if resp.StatusCode != 200 {
				t.Errorf("GET %s = %d", p, resp.StatusCode)
			}
			if strings.Contains(body, secretValue) {
				t.Errorf("GET %s (partial %v) shows the secret value", p, partial)
			}
			if strings.Contains(body, "***") {
				sawRedacted = true
			}
		}
	}
	if !sawRedacted {
		t.Error("no page shows the redacted log or output (the test did not exercise redaction)")
	}
	// The events backlog (served on /ui/events) carries views only.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", e.srv.URL+"/ui/events", nil)
	req.SetBasicAuth(uiUser, uiPass)
	req.Header.Set("Last-Event-ID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body) // ends at the ctx deadline
	resp.Body.Close()
	if !strings.Contains(string(b), "event: job") {
		t.Fatalf("no events in backlog: %q", b)
	}
	if strings.Contains(string(b), secretValue) {
		t.Error("/ui/events shows the secret value")
	}
}

func TestHtpasswdOnlyBcrypt(t *testing.T) {
	if _, err := ui.ParseHtpasswd([]byte("jared:{SHA}abc\n")); err == nil {
		t.Fatal("SHA hash accepted")
	}
	if _, err := ui.ParseHtpasswd([]byte("jared:$apr1$x$y\n")); err == nil {
		t.Fatal("MD5 hash accepted")
	}
	if _, err := ui.ParseHtpasswd([]byte("# nobody\n")); err == nil {
		t.Fatal("empty file accepted")
	}
}
