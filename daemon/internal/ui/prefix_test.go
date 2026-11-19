package ui_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/ui"
)

// prefixed serves a second UI with path prefix p over the same app, behind
// the same API fallback as the real handler, and returns a getter/poster.
type prefixed struct {
	t   *testing.T
	srv *httptest.Server
}

func (e *uenv) withPrefix(p string) *prefixed {
	e.t.Helper()
	users, err := ui.LoadHtpasswd(e.a.Config.UI.HtpasswdFile)
	if err != nil {
		e.t.Fatal(err)
	}
	u, err := ui.New(ui.Options{Engine: e.a.Engine, API: e.a.API, Users: users, PathPrefix: p, Log: quiet()})
	if err != nil {
		e.t.Fatal(err)
	}
	mux := http.NewServeMux()
	u.Register(mux)
	mux.Handle("/", e.a.API)
	srv := httptest.NewServer(mux)
	e.t.Cleanup(func() { srv.CloseClientConnections(); srv.Close() })
	return &prefixed{t: e.t, srv: srv}
}

func (p *prefixed) do(method, path string, auth bool, hdr ...string) (*http.Response, string) {
	p.t.Helper()
	req, _ := http.NewRequest(method, p.srv.URL+path, strings.NewReader(""))
	if auth {
		req.SetBasicAuth(uiUser, uiPass)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := noRedirect.Do(req)
	if err != nil {
		p.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func (p *prefixed) get(path string, hdr ...string) (*http.Response, string) {
	p.t.Helper()
	return p.do("GET", path, true, hdr...)
}

// fixture builds a populated queue and returns the paths of every page.
func (e *uenv) fixture() (pages []string, runID, jobID, otherRun string) {
	_, secretJob := e.runSecretJob()
	run := e.submit(graph.Submission{Workflow: "hello", Jobs: []graph.Job{
		{Name: "a", Argv: []string{"a"}},
		{Name: "b", Argv: []string{"b"}, DependsOn: []string{"a"}},
	}})
	other := e.submit(graph.Submission{Workflow: "flarp", Jobs: []graph.Job{{Name: "x", Argv: []string{"x"}}}})
	bid := e.jobs(run.ID)["b"].ID
	pages = []string{
		"/runs", "/runs?workflow=hello", "/runs/" + run.ID, "/runs/" + other.ID,
		"/jobs/" + bid, "/jobs/" + secretJob.ID, "/workers",
	}
	return pages, run.ID, secretJob.ID, other.ID
}

// ptrRe masks pointer addresses: the workers page header prints a Go value
// (an existing quirk, outside this step) whose pointer differs per render.
var ptrRe = regexp.MustCompile(`0x[0-9a-f]+`)

// Empty prefix is the existing UI; a prefixed UI renders the same pages
// with /p/ui where the plain one has /ui.
func TestPathPrefixPagesMatchUnprefixed(t *testing.T) {
	e := setup(t)
	pages, _, _, _ := e.fixture()
	px := e.withPrefix("/p")
	for _, pg := range pages {
		for _, partial := range []bool{false, true} {
			var hdr []string
			if partial {
				hdr = []string{"X-Partial", "1"}
			}
			resp, plain := e.get("/ui"+pg, true, hdr...)
			if resp.StatusCode != 200 {
				t.Fatalf("GET /ui%s = %d", pg, resp.StatusCode)
			}
			resp, pre := px.get("/p/ui"+pg, hdr...)
			if resp.StatusCode != 200 {
				t.Fatalf("GET /p/ui%s = %d: %s", pg, resp.StatusCode, pre)
			}
			if strings.Contains(pre, `"/ui/`) {
				t.Errorf("/p/ui%s (partial=%v) has a stray /ui/ URL", pg, partial)
			}
			if got := strings.ReplaceAll(pre, "/p/ui", "/ui"); ptrRe.ReplaceAllString(got, "0x") != ptrRe.ReplaceAllString(plain, "0x") {
				t.Errorf("%s (partial=%v): prefixed body differs from plain\nplain:    %s\nprefixed: %s", pg, partial, plain, got)
			}
			if !partial && !strings.Contains(plain, `href="/ui/runs"`) {
				t.Errorf("%s: plain page lost its /ui/runs link", pg)
			}
		}
	}
	// The script and stylesheet.
	for _, f := range []string{"app.js", "app.css"} {
		_, plain := e.get("/ui/static/"+f, true)
		resp, pre := px.get("/p/ui/static/" + f)
		if resp.StatusCode != 200 || pre != plain {
			t.Errorf("static %s: prefixed = %d, same as plain = %v", f, resp.StatusCode, pre == plain)
		}
		if strings.Contains(pre, `"/ui/`) || strings.Contains(pre, `'/ui/`) {
			t.Errorf("static %s has a stray /ui/ URL", f)
		}
	}
}

func TestNoStrayUIPathInSources(t *testing.T) {
	e := setup(t)
	pages, _, _, _ := e.fixture()
	px := e.withPrefix("/p")
	_, js := px.get("/p/ui/static/app.js")
	if !strings.Contains(js, "data-base") {
		t.Error("app.js does not read data-base")
	}
	for _, pg := range pages {
		_, body := px.get("/p/ui" + pg)
		for _, bad := range []string{`"/ui/`, `'/ui/`, `"/ui"`} {
			if strings.Contains(body, bad) {
				t.Errorf("%s contains %s", pg, bad)
			}
		}
		if !strings.Contains(body, `data-base="/p/ui"`) {
			t.Errorf("%s lacks data-base=/p/ui", pg)
		}
	}
}

func TestPathPrefixRoutes(t *testing.T) {
	e := setup(t)
	_, runID, jobID, _ := e.fixture()
	px := e.withPrefix("/p")

	// Redirects.
	if resp, _ := px.do("GET", "/", false); resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/p/ui/runs" {
		t.Errorf("GET / = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	for _, path := range []string{"/p", "/p/", "/p/ui", "/p/ui/"} {
		resp, _ := px.get(path)
		if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/p/ui/runs" {
			t.Errorf("GET %s = %d %q", path, resp.StatusCode, resp.Header.Get("Location"))
		}
		if resp, _ := px.do("GET", path, false); resp.StatusCode != 401 {
			t.Errorf("GET %s without auth = %d", path, resp.StatusCode)
		}
	}

	// Forwards behind auth.
	resp, body := px.get("/p/ui/jobs/" + jobID + "/artifacts/out/report.txt")
	if resp.StatusCode != 200 || !strings.HasPrefix(body, "report ") {
		t.Errorf("artifact = %d %q", resp.StatusCode, body)
	}
	resp, body = px.get("/p/ui/jobs/" + jobID + "/log")
	if resp.StatusCode != 200 || !strings.Contains(body, "using key ***") {
		t.Errorf("log = %d %q", resp.StatusCode, body)
	}
	resp, body = px.get("/p/ui/jobs/" + jobID + "/log/stream?attempt=1")
	if resp.StatusCode != 200 || !strings.Contains(body, "event: end") {
		t.Errorf("log stream = %d %q", resp.StatusCode, body)
	}
	if resp, _ := px.do("GET", "/p/ui/jobs/"+jobID+"/log", false); resp.StatusCode != 401 {
		t.Errorf("log without auth = %d", resp.StatusCode)
	}
	// The job page's log source points under the prefix.
	if _, page := px.get("/p/ui/jobs/" + jobID); !strings.Contains(page, `data-src="/p/ui/jobs/`+jobID+`/log/stream?attempt=1"`) {
		t.Errorf("job page log source not prefixed")
	}
	// Events: opens as the hub (read the headers only).
	req, _ := http.NewRequest("GET", px.srv.URL+"/p/ui/events", nil)
	req.SetBasicAuth(uiUser, uiPass)
	ev, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if ev.StatusCode != 200 || ev.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("events = %d %s", ev.StatusCode, ev.Header.Get("Content-Type"))
	}
	ev.Body.Close()

	// Unprefixed UI paths are not served; the API is.
	for _, path := range []string{"/ui/runs", "/ui/static/app.js", "/ui/events"} {
		if resp, _ := px.get(path); resp.StatusCode != 404 {
			t.Errorf("GET %s = %d, want 404", path, resp.StatusCode)
		}
	}
	if resp, _ := px.do("GET", "/v1/healthz", false); resp.StatusCode != 200 {
		t.Errorf("healthz = %d", resp.StatusCode)
	}

	// POST actions redirect under the prefix.
	post := func(path string) *http.Response {
		req, _ := http.NewRequest("POST", px.srv.URL+path, strings.NewReader(""))
		req.SetBasicAuth(uiUser, uiPass)
		req.Header.Set("Origin", px.srv.URL)
		resp, err := noRedirect.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	if resp := post("/p/ui/runs/" + runID + "/cancel"); resp.StatusCode != 303 || !strings.HasPrefix(resp.Header.Get("Location"), "/p/ui/runs/"+runID+"?") {
		t.Errorf("cancel run = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if resp := post("/p/ui/runs/" + runID + "/retry"); resp.StatusCode != 303 || !strings.HasPrefix(resp.Header.Get("Location"), "/p/ui/runs/") {
		t.Errorf("retry run = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	if resp := post("/p/ui/jobs/" + jobID + "/cancel"); resp.StatusCode != 303 || !strings.HasPrefix(resp.Header.Get("Location"), "/p/ui/jobs/"+jobID+"?") {
		t.Errorf("cancel job = %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
}

// The "older" pagination link is built in Go; it must carry the prefix.
func TestPathPrefixOlderLink(t *testing.T) {
	e := setup(t)
	for i := 0; i < 100; i++ { // the page size
		e.submit(graph.Submission{Workflow: "wf", Jobs: []graph.Job{{Name: "a", Argv: []string{"x"}}}})
	}
	px := e.withPrefix("/p")
	_, body := px.get("/p/ui/runs")
	if !strings.Contains(body, `href="/p/ui/runs?`) || !strings.Contains(body, "before=") {
		t.Errorf("no prefixed older link: %s", body)
	}
	_, plain := e.get("/ui/runs", true)
	if !strings.Contains(plain, `href="/ui/runs?`) || !strings.Contains(plain, "before=") {
		t.Errorf("no plain older link: %s", plain)
	}
}
