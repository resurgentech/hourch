// Package ui is hourchd's operator web UI at /ui, or {path_prefix}/ui (docs/DESIGN-hourchd.md
// "Web UI"): html/template pages, live updates over /ui/events (the same hub
// as /v1/events), the live job log over /ui/jobs/{id}/log/stream, and three
// actions (cancel run, retry run, cancel job). Everything is behind htpasswd
// basic auth (bcrypt only) or, with ui.auth: proxy, behind a reverse proxy
// that names the user and presents a shared secret (ProxyAuth); POSTs also
// pass an Origin/Referer check. Log,
// artifact and event routes are the API's own handlers, called in process
// with the UI user as an operator (httpapi.AsUIUser), so downloads stream
// through hourchd exactly as with a token.
//
// No page renders a secret value: views carry secret names only, outputs
// and logs are redacted when stored (secrets.Redactor, D11), and the assign
// message is never shown.
package ui

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	api "github.com/resurgentech/hourch/api"

	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/httpapi"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/ulid"
	"github.com/resurgentech/hourch/daemon/internal/view"
)

//go:embed templates/*.html static/*
var assets embed.FS

// Options configure the UI.
type Options struct {
	Engine *dispatch.Engine
	API    http.Handler // the httpapi server
	// Exactly one of Users (htpasswd basic auth) and Proxy (a trusted
	// reverse proxy names the user) is set.
	Users *Htpasswd
	Proxy *ProxyAuth
	// Powerwarden returns the publisher state and its last error.
	Powerwarden     func() (status, lastError string)
	WorkerRetention time.Duration
	// PathPrefix is the config's ui.path_prefix: the UI root is PathPrefix+"/ui".
	PathPrefix string
	Log        *slog.Logger
}

// UI serves /ui.
type UI struct {
	o     Options
	eng   *dispatch.Engine
	log   *slog.Logger
	pages map[string]*template.Template
	base  string // the UI root: PathPrefix + "/ui"
}

// New parses the embedded templates.
func New(o Options) (*UI, error) {
	if o.Engine == nil || o.API == nil {
		return nil, errors.New("ui: engine and api are required")
	}
	if (o.Users == nil) == (o.Proxy == nil) {
		return nil, errors.New("ui: exactly one of users and proxy is required")
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Powerwarden == nil {
		o.Powerwarden = func() (string, string) { return "disabled", "" }
	}
	if o.WorkerRetention <= 0 {
		o.WorkerRetention = 90 * 24 * time.Hour
	}
	u := &UI{o: o, eng: o.Engine, log: o.Log, pages: map[string]*template.Template{}, base: o.PathPrefix + "/ui"}
	funcs := template.FuncMap{
		"ts":    func(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05Z") },
		"tsp":   func(t *time.Time) string { return tsp(t) },
		"since": func(t time.Time) string { return human(u.eng.Clock().Now().Sub(t)) },
		// adur is an attempt's duration (running attempts: until now).
		"adur": func(a, b *time.Time) string {
			if a == nil {
				return ""
			}
			end := u.eng.Clock().Now()
			if b != nil {
				end = *b
			}
			return human(end.Sub(*a))
		},
		"ptr":     func(t time.Time) *time.Time { return &t },
		"join":    strings.Join,
		"artpath": artPath,
		"pe":      url.PathEscape,
		"base":    func() string { return u.base },
		// prefix is ui.path_prefix; layout.html adds data-base only when it is set,
		// so an unprefixed page is exactly as before the setting existed.
		"prefix": func() string { return u.o.PathPrefix },
		"exit": func(p *int) string {
			if p == nil {
				return ""
			}
			return strconv.Itoa(*p)
		},
		"terminal": func(s any) bool { return store.TerminalJob(fmt.Sprint(s)) },
	}
	root, err := template.New("layout.html").Funcs(funcs).ParseFS(assets, "templates/layout.html")
	if err != nil {
		return nil, err
	}
	for _, p := range []string{"runs", "run", "job", "workers"} {
		t, err := template.Must(root.Clone()).ParseFS(assets, "templates/"+p+".html")
		if err != nil {
			return nil, err
		}
		u.pages[p] = t
	}
	return u, nil
}

// Register adds the UI routes (under PathPrefix+"/ui") and the site-root
// redirect to mux.
func (u *UI) Register(mux *http.ServeMux) {
	static, _ := fs.Sub(assets, "static")
	b := u.base
	get := func(pattern string, h http.Handler) { mux.Handle("GET "+pattern, u.auth(h)) }
	post := func(pattern string, h http.HandlerFunc) { mux.Handle("POST "+pattern, u.auth(u.sameOrigin(h))) }
	toRuns := http.RedirectHandler(b+"/runs", http.StatusSeeOther)
	// The bare site root goes to the UI; the redirect itself needs no auth.
	mux.Handle("GET /{$}", toRuns)
	if p := u.o.PathPrefix; p != "" {
		get(p, toRuns)
		get(p+"/{$}", toRuns)
	}
	get(b+"/{$}", toRuns)
	get(b, toRuns)
	get(b+"/static/", http.StripPrefix(b+"/static/", http.FileServerFS(static)))
	get(b+"/events", u.forward(func(*http.Request) string { return "/v1/events" }))
	get(b+"/runs", http.HandlerFunc(u.runs))
	get(b+"/runs/{id}", http.HandlerFunc(u.run))
	get(b+"/jobs/{id}", http.HandlerFunc(u.job))
	get(b+"/jobs/{id}/log", u.forward(func(r *http.Request) string { return "/v1/jobs/" + url.PathEscape(r.PathValue("id")) + "/log" }))
	get(b+"/jobs/{id}/log/stream", u.forward(func(r *http.Request) string {
		return "/v1/jobs/" + url.PathEscape(r.PathValue("id")) + "/log/stream"
	}))
	get(b+"/jobs/{id}/artifacts/{path...}", u.forward(func(r *http.Request) string {
		return "/v1/jobs/" + url.PathEscape(r.PathValue("id")) + "/artifacts/" + artPath(r.PathValue("path"))
	}))
	get(b+"/workers", http.HandlerFunc(u.workers))
	post(b+"/runs/{id}/cancel", u.postCancelRun)
	post(b+"/runs/{id}/retry", u.postRetryRun)
	post(b+"/jobs/{id}/cancel", u.postCancelJob)
}

// ---- auth, CSRF, forwarding ----

func (u *UI) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var name string
		if u.o.Proxy != nil {
			// Proxy mode: no basic-auth prompt, and basic-auth credentials are ignored.
			n, ok := u.o.Proxy.User(r)
			if !ok {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			name = n
		} else {
			n, pass, ok := r.BasicAuth()
			if !ok || !u.o.Users.Check(n, pass) {
				w.Header().Set("WWW-Authenticate", `Basic realm="hourchd", charset="UTF-8"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			name = n
		}
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'")
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey{}, name)))
	})
}

// sameOrigin rejects a POST unless Origin (or, failing that, Referer) names
// the host the request was sent to. Browsers attach basic-auth credentials to
// cross-site form posts, so this is the CSRF guard.
func (u *UI) sameOrigin(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		src := r.Header.Get("Origin")
		if src == "" || src == "null" {
			src = r.Header.Get("Referer")
		}
		pu, err := url.Parse(src)
		if src == "" || err != nil || pu.Host == "" || !sameString(pu.Host, r.Host) {
			u.log.Warn("ui: cross-origin post rejected", "origin", r.Header.Get("Origin"), "referer", r.Header.Get("Referer"), "host", r.Host)
			http.Error(w, "cross-origin request rejected", http.StatusForbidden)
			return
		}
		next(w, r)
	})
}

// forward serves r with the API handler for apiPath, as the UI user.
func (u *UI) forward(apiPath func(*http.Request) string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r2 := httpapi.AsUIUser(r, user(r))
		nu := *r.URL
		nu.Path, nu.RawPath = apiPath(r), ""
		if p, err := url.PathUnescape(nu.Path); err == nil {
			nu.RawPath, nu.Path = nu.Path, p
		}
		r2.URL = &nu
		r2.RequestURI = nu.RequestURI()
		r2.Header = r.Header.Clone()
		r2.Header.Del("Authorization") // basic auth is not a bearer token
		if u.o.Proxy != nil {
			u.o.Proxy.strip(r2.Header) // the API never sees the shared secret
		}
		u.o.API.ServeHTTP(w, r2)
	})
}

type userKey struct{}

// user is the authenticated UI user, set by auth (either mode).
func user(r *http.Request) string {
	n, _ := r.Context().Value(userKey{}).(string)
	return n
}

// ---- pages ----

type header struct {
	Page         string
	Ready        int
	Leased       int
	OldestReady  string
	Workers      int
	HoldSecrets  int // runs whose stored secrets a restart would lose
	Powerwarden  string
	PWError      string
	Flash, Error string
}

func (u *UI) header(r *http.Request, page string) header {
	h := header{Page: page, Flash: r.URL.Query().Get("ok"), Error: r.URL.Query().Get("err")}
	if qs, err := u.eng.Store().QueueStats(); err == nil {
		h.Ready, h.Leased = qs.Ready, qs.Leased
		if qs.OldestReady != nil {
			h.OldestReady = human(u.eng.Clock().Now().Sub(*qs.OldestReady))
		}
	}
	h.Workers = len(u.eng.ConnectedWorkers())
	if n, err := u.eng.RunsHoldingSecrets(); err == nil {
		h.HoldSecrets = n
	}
	h.Powerwarden, h.PWError = u.o.Powerwarden()
	return h
}

func (u *UI) render(w http.ResponseWriter, r *http.Request, page string, data any) {
	name := "layout"
	if r.Header.Get("X-Partial") == "1" {
		name = "live"
	}
	var buf bytes.Buffer
	if err := u.pages[page].ExecuteTemplate(&buf, name, data); err != nil {
		u.log.Error("ui: render", "page", page, "error", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = buf.WriteTo(w)
}

func (u *UI) fail(w http.ResponseWriter, err error) {
	u.log.Error("ui: internal error", "error", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

type runRow struct {
	api.RunSummary
	Age, Duration string
}

var runStatuses = []string{store.RunRunning, store.RunSucceeded, store.RunFailed, store.RunCancelled}

func (u *UI) runs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.RunFilter{Workflow: strings.TrimSpace(q.Get("workflow")), Status: q.Get("status"), Before: q.Get("before"), Limit: 100}
	var bad string
	if f.Workflow != "" && !graph.WorkflowRe.MatchString(f.Workflow) {
		bad, f.Workflow = "invalid workflow name", ""
	}
	switch f.Status {
	case "", store.RunRunning, store.RunSucceeded, store.RunFailed, store.RunCancelled:
	default:
		bad, f.Status = "invalid status", ""
	}
	if f.Before != "" && !ulid.Valid(f.Before) {
		bad, f.Before = "invalid before", ""
	}
	st := u.eng.Store()
	runs, err := st.ListRuns(f)
	if err != nil {
		u.fail(w, err)
		return
	}
	now := u.eng.Clock().Now()
	rows := make([]runRow, 0, len(runs))
	for _, run := range runs {
		counts, err := st.JobCounts(run.ID)
		if err != nil {
			u.fail(w, err)
			return
		}
		end := now
		if run.FinishedAt != nil {
			end = *run.FinishedAt
		}
		rows = append(rows, runRow{RunSummary: view.RunSummary(run, counts), Age: human(now.Sub(run.CreatedAt)), Duration: human(end.Sub(run.CreatedAt))})
	}
	older := ""
	if len(runs) == f.Limit {
		v := url.Values{}
		if f.Workflow != "" {
			v.Set("workflow", f.Workflow)
		}
		if f.Status != "" {
			v.Set("status", f.Status)
		}
		v.Set("before", runs[len(runs)-1].ID)
		older = u.base + "/runs?" + v.Encode()
	}
	h := u.header(r, "runs")
	if bad != "" {
		h.Error = bad
	}
	u.render(w, r, "runs", struct {
		header
		Runs             []runRow
		Workflow, Status string
		Statuses         []string
		Older            string
	}{h, rows, f.Workflow, f.Status, runStatuses, older})
}

type jobRow struct {
	api.JobSummary
	Duration string
	Outputs  []kv
}

type kv struct{ K, V string }

func sortedKV(m map[string]string) []kv {
	out := make([]kv, 0, len(m))
	for k, v := range m {
		out = append(out, kv{k, v})
	}
	sort.Slice(out, func(i, k int) bool { return out[i].K < out[k].K })
	return out
}

func (u *UI) run(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st := u.eng.Store()
	run, err := st.GetRun(id)
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	} else if err != nil {
		u.fail(w, err)
		return
	}
	jobs, err := st.RunJobs(id)
	if err != nil {
		u.fail(w, err)
		return
	}
	now := u.eng.Clock().Now()
	rows := make([]jobRow, 0, len(jobs))
	for _, j := range jobs {
		outs, err := st.Outputs(j.ID)
		if err != nil {
			u.fail(w, err)
			return
		}
		row := jobRow{JobSummary: view.JobSummary(j), Outputs: sortedKV(outs)}
		if j.StartedAt != nil {
			end := now
			if j.FinishedAt != nil {
				end = *j.FinishedAt
			}
			row.Duration = human(end.Sub(*j.StartedAt))
		}
		rows = append(rows, row)
	}
	v := view.Run(run, jobs)
	end := now
	if run.FinishedAt != nil {
		end = *run.FinishedAt
	}
	u.render(w, r, "run", struct {
		header
		Run      api.Run
		Params   []kv
		Jobs     []jobRow
		Graph    Graph
		Age      string
		Duration string
		CanRetry bool
	}{u.header(r, "run"), v, sortedKV(run.Params), rows, layout(jobs), human(now.Sub(run.CreatedAt)), human(end.Sub(run.CreatedAt)),
		u.canRetry(run)})
}

type artLink struct {
	Path, Size string
	Dir        bool
}

func (u *UI) job(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	st := u.eng.Store()
	j, err := st.GetJob(id)
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	} else if err != nil {
		u.fail(w, err)
		return
	}
	run, err := st.GetRun(j.RunID)
	if err != nil {
		u.fail(w, err)
		return
	}
	outs, err := st.Outputs(id)
	if err != nil {
		u.fail(w, err)
		return
	}
	evs, err := st.JobEvents(id)
	if err != nil {
		u.fail(w, err)
		return
	}
	arts, err := st.Artifacts(id)
	if err != nil {
		u.fail(w, err)
		return
	}
	d := view.JobDetail(j, outs, evs, arts)
	attempt := j.Attempt
	if v := r.URL.Query().Get("attempt"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 && n <= j.Attempt {
			attempt = n
		}
	}
	var links []artLink
	for _, a := range arts {
		links = append(links, artLink{Path: a.Path, Size: bytesHuman(a.Size)})
	}
	if len(arts) > 0 {
		for _, decl := range j.Artifacts {
			if strings.HasSuffix(decl, "/") {
				links = append(links, artLink{Path: decl, Size: "tar", Dir: true})
			}
		}
	}
	u.render(w, r, "job", struct {
		header
		Job      api.JobDetail
		Run      store.Run
		Env      []kv
		Outputs  []kv
		Links    []artLink
		Attempt  int
		LogSrc   string
		Terminal bool
	}{u.header(r, "job"), d, run, sortedKV(j.Env), sortedKV(outs), links, attempt,
		fmt.Sprintf("%s/jobs/%s/log/stream?attempt=%d", u.base, url.PathEscape(id), attempt), store.TerminalJob(j.Status)})
}

func (u *UI) workers(w http.ResponseWriter, r *http.Request) {
	ws, err := u.eng.Store().ListWorkers(u.eng.Clock().Now().Add(-u.o.WorkerRetention))
	if err != nil {
		u.fail(w, err)
		return
	}
	out := make([]api.Worker, 0, len(ws))
	for _, wk := range ws {
		out = append(out, view.Worker(wk))
	}
	u.render(w, r, "workers", struct {
		header
		Workers []api.Worker
	}{u.header(r, "workers"), out})
}

// ---- actions ----

// back redirects to a UI path (relative to the UI root) with a flash message.
func (u *UI) back(w http.ResponseWriter, r *http.Request, to string, err error, ok string) {
	q := url.Values{}
	if err != nil {
		q.Set("err", err.Error())
	} else if ok != "" {
		q.Set("ok", ok)
	}
	http.Redirect(w, r, u.base+to+"?"+q.Encode(), http.StatusSeeOther)
}

func (u *UI) postCancelRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := u.eng.CancelRun(id)
	if errors.Is(err, dispatch.ErrNotFound) {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}
	u.log.Info("ui: cancel run", "run", id, "user", user(r), "error", err)
	u.back(w, r, "/runs/"+url.PathEscape(id), err, "cancel requested")
}

// ErrRetrySecrets: the run used secrets and its stored secrets are gone
// (hourchd restarted, or they were deleted after its secret-using jobs
// succeeded), so the UI cannot retry it: the UI never asks for values.
var ErrRetrySecrets = errors.New("this run's stored secrets are gone (hourchd restarted, or its secret-using jobs succeeded); " +
	"retry it by running the workflow binary again (it reads Vault)")

// canRetry: a finished run without secrets, or one whose stored secrets
// still exist (the retry reuses them).
func (u *UI) canRetry(run store.Run) bool {
	return len(run.SecretNames) == 0 || u.eng.HoldsSecrets(run.ID)
}

func (u *UI) postRetryRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	run, err := u.eng.Store().GetRun(id)
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	} else if err != nil {
		u.fail(w, err)
		return
	}
	if !u.canRetry(run) {
		u.log.Info("ui: retry refused (secrets gone)", "run", id, "user", user(r))
		u.back(w, r, "/runs/"+url.PathEscape(id), ErrRetrySecrets, "")
		return
	}
	nr, err := u.eng.Retry(r.Context(), "ui:"+user(r), id, nil)
	u.log.Info("ui: retry run", "run", id, "new_run", nr.ID, "user", user(r), "error", err)
	if err != nil {
		u.back(w, r, "/runs/"+url.PathEscape(id), fmt.Errorf("retry failed: %v", err), "")
		return
	}
	u.back(w, r, "/runs/"+url.PathEscape(nr.ID), nil, fmt.Sprintf("retry of %s #%d", run.Workflow, run.Number))
}

func (u *UI) postCancelJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := u.eng.CancelJob(id)
	if errors.Is(err, dispatch.ErrNotFound) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	u.log.Info("ui: cancel job", "job", id, "user", user(r), "error", err)
	u.back(w, r, "/jobs/"+url.PathEscape(id), err, "cancel requested")
}

// ---- formatting ----

func tsp(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05Z")
}

// human formats a duration compactly: 45s, 3m05s, 2h04m, 3d02h.
func human(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	s := int64(d.Round(time.Second) / time.Second)
	switch {
	case s < 60:
		return fmt.Sprintf("%ds", s)
	case s < 3600:
		return fmt.Sprintf("%dm%02ds", s/60, s%60)
	case s < 86400:
		return fmt.Sprintf("%dh%02dm", s/3600, (s%3600)/60)
	default:
		return fmt.Sprintf("%dd%02dh", s/86400, (s%86400)/3600)
	}
}

func bytesHuman(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// artPath escapes each segment of an artifact path for a URL, keeping the
// slashes (and a trailing slash for a directory).
func artPath(p string) string {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}
