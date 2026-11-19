package httpapi_test

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	api "github.com/resurgentech/hourch/api"
	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/httpapi"
	"github.com/resurgentech/hourch/daemon/internal/store"
)

func TestEverySpecOperationHasARoute(t *testing.T) {
	doc, err := api.GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			want = append(want, method+" "+path)
		}
	}
	e := newEnv(t, nil)
	got := e.a.API.Routes()
	sort.Strings(want)
	sort.Strings(got)
	if strings.Join(want, "\n") != strings.Join(got, "\n") {
		t.Fatalf("routes differ from spec\nspec:\n%s\nmux:\n%s", strings.Join(want, "\n"), strings.Join(got, "\n"))
	}
	// The mux dispatches each of them (no 404/405 from routing).
	for _, op := range got {
		method, path, _ := strings.Cut(op, " ")
		if strings.HasSuffix(path, "/stream") || strings.HasSuffix(path, "/events") || strings.HasSuffix(path, "/connect") {
			continue
		}
		path = strings.NewReplacer("{id}", "01JBF6Q9W2X7K3M4N5P6Q7R8S9", "{path}", "a/b", "{upstream_job}", "up",
			"{workflow}", "wf", "{number}", "1", "{name}", "build:hello:x", "{type}", "hello", "{flavour}", "f", "{num}", "1").Replace(path)
		resp, body := e.do(method, path, subTok, strings.NewReader("{}"), nil)
		if resp.StatusCode == http.StatusMethodNotAllowed || strings.Contains(string(body), "no such route") {
			t.Fatalf("%s not routed: %d %s", op, resp.StatusCode, body)
		}
	}
}

func TestAuthKinds(t *testing.T) {
	e := newEnv(t, nil)
	r1, b1 := e.do("GET", "/v1/runs", "", nil, nil)
	r2, b2 := e.do("GET", "/v1/runs", "wrong", nil, nil)
	r3, b3 := e.do("GET", "/v1/runs", "hjt_0000", nil, nil)
	if r1.StatusCode != 401 || r2.StatusCode != 401 || r3.StatusCode != 401 || string(b1) != string(b2) || string(b2) != string(b3) {
		t.Fatalf("401s differ: %d %s / %d %s / %d %s", r1.StatusCode, b1, r2.StatusCode, b2, r3.StatusCode, b3)
	}
	if r, b := e.do("GET", "/v1/runs", w1Tok, nil, nil); r.StatusCode != 403 || errCode(t, b) != api.ErrorCodeForbidden {
		t.Fatalf("worker token on /v1/runs: %d %s", r.StatusCode, b)
	}
	if r, b := e.do("GET", "/v1/worker/jobs/01JBF6Q9W2X7K3M4N5P6Q7R8S9/context", subTok, nil, nil); r.StatusCode != 403 {
		t.Fatalf("submitter token on worker route: %d %s", r.StatusCode, b)
	}
	if r, _ := e.do("PUT", "/v1/builds/kernel/f/1", subTok, strings.NewReader("{}"), nil); r.StatusCode != 403 {
		t.Fatalf("submitter token on PUT build: %d", r.StatusCode)
	}
	if r, _ := e.do("GET", "/v1/healthz", "", nil, nil); r.StatusCode != 200 {
		t.Fatal("healthz needs no token")
	}
	// hello.worker_id must equal the token's name.
	ws, _, err := e.dial(w1Tok)
	if err != nil {
		t.Fatal(err)
	}
	w := &worker{t: t, ws: ws, msgs: make(chan proto.Message, 10), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			if _, _, err := ws.Read(t.Context()); err != nil {
				return
			}
		}
	}()
	w.send(&proto.Hello{WorkerID: "w2", Hostname: "h", Version: "t"})
	w.closed()
	if _, _, err := e.dial(subTok); err == nil {
		t.Fatal("submitter token opened a worker socket")
	}
}

func TestLoadTokens(t *testing.T) {
	if _, err := httpapi.NewTokens(httpapi.TokensFile{}); err == nil {
		t.Fatal("empty tokens accepted")
	}
	if _, err := httpapi.NewTokens(httpapi.TokensFile{Submitters: map[string]string{"a": "x"}, Workers: map[string]string{"b": "x"}}); err == nil {
		t.Fatal("shared token accepted")
	}
	if _, err := httpapi.NewTokens(httpapi.TokensFile{Workers: map[string]string{"bad id": "x"}}); err == nil {
		t.Fatal("bad worker id accepted")
	}
	tk, err := httpapi.NewTokens(httpapi.TokensFile{Submitters: map[string]string{"a": "x"}, Workers: map[string]string{"w": "y"}})
	if err != nil {
		t.Fatal(err)
	}
	if n, k, ok := tk.Lookup("y"); !ok || n != "w" || k != httpapi.KindWorker {
		t.Fatalf("lookup = %s %s %v", n, k, ok)
	}
}

func submit(t *testing.T, e *env, rs api.RunSubmission) api.Run {
	t.Helper()
	var run api.Run
	e.json("POST", "/v1/runs", subTok, rs, http.StatusCreated, &run)
	return run
}

func ptr[T any](v T) *T { return &v }

func TestContexts(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.Limits.ContextMaxBytes = 1024 })
	body := []byte("context tarball bytes")
	var up api.ContextUpload
	resp, b := e.do("POST", "/v1/contexts", subTok, bytes.NewReader(body), nil)
	if resp.StatusCode != 201 || json.Unmarshal(b, &up) != nil || !up.Created || up.Sha256 != sha(body) || up.ContextRef != "sha256:"+sha(body) || up.Size != int64(len(body)) {
		t.Fatalf("first upload: %d %s", resp.StatusCode, b)
	}
	resp, b = e.do("POST", "/v1/contexts", subTok, bytes.NewReader(body), map[string]string{"X-Sha256": sha(body)})
	if resp.StatusCode != 200 || json.Unmarshal(b, &up) != nil || up.Created {
		t.Fatalf("dedupe upload: %d %s", resp.StatusCode, b)
	}
	n := 0
	for _, k := range e.obj.Keys() {
		if strings.HasPrefix(k, "contexts/") {
			n++
		}
		if strings.HasPrefix(k, "tmp/") {
			t.Fatalf("temp object left: %s", k)
		}
	}
	if n != 1 {
		t.Fatalf("%d context objects", n)
	}
	resp, b = e.do("POST", "/v1/contexts", subTok, bytes.NewReader([]byte("other")), map[string]string{"X-Sha256": sha(body)})
	if resp.StatusCode != 400 || errCode(t, b) != api.ErrorCodeChecksumMismatch {
		t.Fatalf("mismatch: %d %s", resp.StatusCode, b)
	}
	resp, b = e.do("POST", "/v1/contexts", subTok, bytes.NewReader(bytes.Repeat([]byte("x"), 2048)), nil)
	if resp.StatusCode != 413 || errCode(t, b) != api.ErrorCodeTooLarge {
		t.Fatalf("too large: %d %s", resp.StatusCode, b)
	}
	// A run naming an unknown context is refused.
	resp, b = e.do("POST", "/v1/runs", subTok, strings.NewReader(`{"workflow":"wf","context_ref":"sha256:`+strings.Repeat("0", 64)+`","jobs":[{"name":"a","argv":["x"]}]}`), nil)
	if resp.StatusCode != 422 || errCode(t, b) != api.ErrorCodeContextNotFound {
		t.Fatalf("unknown context: %d %s", resp.StatusCode, b)
	}
	// Submission errors map to their codes.
	for body, code := range map[string]api.ErrorCode{
		`{"workflow":"wf","jobs":[{"name":"a","argv":["x"],"depends_on":["zz"]}]}`:      api.ErrorCodeInvalidGraph,
		`{"workflow":"wf","jobs":[{"name":"a","argv":["x"],"secrets":["S"]}]}`:          api.ErrorCodeMissingSecret,
		`{"workflow":"wf","jobs":[{"name":"a","argv":["x"]}],"bogus":1}`:                api.ErrorCodeBadRequest,
		`{"workflow":"wf","jobs":[{"name":"a","argv":["x"],"env":{"HOURCH_JOB":"1"}}]}`: api.ErrorCodeBadRequest,
	} {
		if r, b := e.do("POST", "/v1/runs", subTok, strings.NewReader(body), nil); errCode(t, b) != code {
			t.Fatalf("%s: %d %s", body, r.StatusCode, b)
		}
	}
}

func readTar(t *testing.T, b []byte) map[string]string {
	t.Helper()
	out := map[string]string{}
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		c, _ := io.ReadAll(tr)
		out[h.Name] = string(c)
	}
}

// TestEndToEnd drives a two-job run over the real websocket and HTTP data
// routes: context, artifacts, inputs (directory as tar), secrets, outputs,
// logs, counters, builds and job tokens.
func TestEndToEnd(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.Limits.ArtifactMaxBytes = 4096 })
	ctxBody := []byte("the context")
	var up api.ContextUpload
	_, b := e.do("POST", "/v1/contexts", subTok, bytes.NewReader(ctxBody), nil)
	_ = json.Unmarshal(b, &up)
	run := submit(t, e, api.RunSubmission{
		Workflow: "hello", Params: &map[string]string{"variant": "static"}, ContextRef: &up.ContextRef,
		Secrets: &map[string]string{"APTLY": "s3cr3t-value"},
		Jobs: []api.JobSpec{
			{Name: "build", Tags: &[]string{"kernel"}, Argv: []string{"just", "package"}, Artifacts: &[]string{"output/", "meta.json"}},
			{Name: "record", Argv: []string{"rec", "${outputs.build.version}"}, Secrets: &[]string{"APTLY"},
				Inputs: &[]api.JobInput{{Job: "build", Path: "output/"}, {Job: "build", Path: "meta.json"}}},
		},
	})
	if run.Number != 1 || len(run.Jobs) != 2 || run.Jobs[1].DependsOn[0] != "build" || strings.Join(run.SecretNames, ",") != "APTLY" {
		t.Fatalf("run = %+v", run)
	}
	if strings.Contains(string(e.json("GET", "/v1/runs/"+run.Id, subTok, nil, 200, nil)), "s3cr3t") {
		t.Fatal("run view has a secret value")
	}

	w1, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1", Tags: []string{"kernel", "tiny"}})
	a := w1.assign()
	if a.Job.Name != "build" || len(a.Secrets) != 0 {
		t.Fatalf("assign = %+v", a)
	}
	jobPath := "/v1/worker/jobs/" + a.Job.ID
	w1.send(&proto.Accept{JobID: a.Job.ID})

	resp, got := e.do("GET", jobPath+"/context", w1Tok, nil, nil)
	if resp.StatusCode != 200 || string(got) != string(ctxBody) || resp.Header.Get("X-Sha256") != sha(ctxBody) {
		t.Fatalf("context: %d %q", resp.StatusCode, got)
	}
	if resp, b := e.do("GET", jobPath+"/context", w2Tok, nil, nil); resp.StatusCode != 409 || errCode(t, b) != api.ErrorCodeNotLeased {
		t.Fatalf("other worker: %d %s", resp.StatusCode, b)
	}
	put := func(path string, body []byte, sum string) (*http.Response, []byte) {
		return e.do("PUT", jobPath+"/artifacts/"+path, w1Tok, bytes.NewReader(body), map[string]string{"X-Sha256": sum})
	}
	if resp, b := put("output/a.deb", []byte("deb-a"), sha([]byte("nope"))); resp.StatusCode != 400 || errCode(t, b) != api.ErrorCodeChecksumMismatch {
		t.Fatalf("mismatch: %d %s", resp.StatusCode, b)
	}
	if _, ok := e.obj.Bytes(dispatch.ArtifactKey(run.Id, a.Job.ID, "output/a.deb")); ok {
		t.Fatal("mismatched artifact kept")
	}
	if resp, _ := put("other.txt", []byte("x"), sha([]byte("x"))); resp.StatusCode != 400 {
		t.Fatal("undeclared artifact accepted")
	}
	if resp, b := put("output/big", bytes.Repeat([]byte("x"), 5000), sha(bytes.Repeat([]byte("x"), 5000))); resp.StatusCode != 413 {
		t.Fatalf("too large: %d %s", resp.StatusCode, b)
	}
	for path, body := range map[string]string{"output/a.deb": "deb-a", "output/sub/b.deb": "deb-b", "meta.json": `{"v":1}`} {
		// Raw slashes and %2F both reach the same artifact.
		p := path
		if strings.Count(path, "/") == 2 {
			p = strings.ReplaceAll(path, "/", "%2F")
		}
		if resp, b := put(p, []byte(body), sha([]byte(body))); resp.StatusCode != 200 {
			t.Fatalf("put %s: %d %s", path, resp.StatusCode, b)
		}
	}

	// Job token: counters and builds while leased.
	jt := a.JobToken
	var ctr api.Counter
	e.json("POST", "/v1/counters/build:hello:debian12_amd64/next", jt, nil, 200, &ctr)
	if ctr.Value != 1 {
		t.Fatalf("counter = %+v", ctr)
	}
	e.json("POST", "/v1/counters/build:hello:debian12_amd64/next", subTok, nil, 200, &ctr)
	if ctr.Value != 2 {
		t.Fatalf("counter = %+v", ctr)
	}
	var bld api.Build
	e.json("PUT", "/v1/builds/hello/debian12_amd64/2", jt, map[string]any{"BuildNum": 2, "Note": "first"}, 200, &bld)
	if bld.RunId != run.Id || bld.JobId != a.Job.ID || bld.Body["Note"] != "first" {
		t.Fatalf("build = %+v", bld)
	}
	e.json("PUT", "/v1/builds/hello/debian12_amd64/2", jt, map[string]any{"BuildNum": 2, "Note": "second"}, 200, nil)
	var builds []api.Build
	e.json("GET", "/v1/builds?type=hello", subTok, nil, 200, &builds)
	if len(builds) != 1 || builds[0].Body["Note"] != "second" {
		t.Fatalf("builds = %+v", builds)
	}
	if r, _ := e.do("GET", "/v1/runs", jt, nil, nil); r.StatusCode != 403 {
		t.Fatalf("job token on /v1/runs: %d", r.StatusCode)
	}

	w1.send(&proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: "building\n"})
	w1.send(&proto.Output{JobID: a.Job.ID, Key: "version", Value: "0.2.0"})
	w1.send(&proto.Result{JobID: a.Job.ID, ExitCode: 0})
	waitFor(t, "build to finish", func() bool {
		j, _ := e.a.Store.GetJob(a.Job.ID)
		return j.Status == store.JobSucceeded
	})
	if r, _ := e.do("POST", "/v1/counters/x/next", jt, nil, nil); r.StatusCode != 401 {
		t.Fatalf("job token after the job ended: %d", r.StatusCode)
	}

	// record runs on the same worker with the substituted argv and the secret.
	rec := w1.assign()
	if rec.Job.Name != "record" || rec.Job.Argv[1] != "0.2.0" || rec.Secrets["APTLY"] != "s3cr3t-value" || len(rec.Job.Inputs) != 2 {
		t.Fatalf("record assign = %+v", rec.Job)
	}
	w1.send(&proto.Accept{JobID: rec.Job.ID})
	rp := "/v1/worker/jobs/" + rec.Job.ID + "/inputs/build/"
	resp, got = e.do("GET", rp+"output/", w1Tok, nil, nil)
	if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "application/x-tar" {
		t.Fatalf("dir input: %d", resp.StatusCode)
	}
	if files := readTar(t, got); len(files) != 2 || files["a.deb"] != "deb-a" || files["sub/b.deb"] != "deb-b" {
		t.Fatalf("tar = %v", files)
	}
	resp, got = e.do("GET", rp+"output%2Fsub%2Fb.deb", w1Tok, nil, nil)
	if resp.StatusCode != 200 || string(got) != "deb-b" || resp.Header.Get("X-Sha256") != sha([]byte("deb-b")) {
		t.Fatalf("file under dir input: %d %q", resp.StatusCode, got)
	}
	if resp, _ := e.do("GET", rp+"meta.json", w1Tok, nil, nil); resp.StatusCode != 200 {
		t.Fatal("file input")
	}
	if resp, _ := e.do("GET", "/v1/worker/jobs/"+rec.Job.ID+"/inputs/build/build.log", w1Tok, nil, nil); resp.StatusCode != 404 {
		t.Fatal("undeclared input served")
	}
	if resp, _ := e.do("GET", "/v1/worker/jobs/"+a.Job.ID+"/inputs/build/meta.json", w1Tok, nil, nil); resp.StatusCode != 409 {
		t.Fatal("input of a finished job served")
	}
	w1.send(&proto.Log{JobID: rec.Job.ID, Stream: "stderr", Seq: 1, Data: "using s3cr3t-value\n"})
	w1.send(&proto.Result{JobID: rec.Job.ID, ExitCode: 0})
	waitFor(t, "run to succeed", func() bool {
		r, _ := e.a.Store.GetRun(run.Id)
		return r.Status == store.RunSucceeded
	})
	e.a.Engine.WaitUploads()

	// Logs from the object store, redacted.
	resp, got = e.do("GET", "/v1/jobs/"+rec.Job.ID+"/log", subTok, nil, nil)
	if resp.StatusCode != 200 || string(got) != "using ***\n" {
		t.Fatalf("log: %d %q", resp.StatusCode, got)
	}
	if resp, _ := e.do("GET", "/v1/jobs/"+rec.Job.ID+"/log?attempt=2", subTok, nil, nil); resp.StatusCode != 404 {
		t.Fatal("attempt 2 log")
	}
	_, rd := e.sse("/v1/jobs/"+a.Job.ID+"/log/stream", "")
	evs := readSSE(t, rd, 2)
	if evs[0].event != "log" || evs[0].id != "9" || evs[1].event != "end" || !strings.Contains(evs[1].data, `"status":"succeeded"`) {
		t.Fatalf("finished stream = %+v", evs)
	}

	var jd api.JobDetail
	e.json("GET", "/v1/jobs/"+a.Job.ID, subTok, nil, 200, &jd)
	if jd.Outputs["version"] != "0.2.0" || len(jd.UploadedArtifacts) != 3 || len(jd.Attempts) != 1 || jd.Attempts[0].Worker != "w1" || *jd.Attempts[0].ExitCode != 0 {
		t.Fatalf("job detail = %+v", jd)
	}
	resp, got = e.do("GET", "/v1/jobs/"+a.Job.ID+"/artifacts/meta.json", subTok, nil, nil)
	if resp.StatusCode != 200 || string(got) != `{"v":1}` {
		t.Fatalf("download: %d %s", resp.StatusCode, got)
	}
	var byNum api.Run
	e.json("GET", "/v1/workflows/hello/runs/1", subTok, nil, 200, &byNum)
	if byNum.Id != run.Id || byNum.Status != api.RunStatusSucceeded {
		t.Fatalf("by number = %+v", byNum)
	}
	var runs []api.RunSummary
	e.json("GET", "/v1/runs?workflow=hello&limit=1", subTok, nil, 200, &runs)
	if len(runs) != 1 || runs[0].JobCounts.Succeeded != 2 {
		t.Fatalf("runs = %+v", runs)
	}
	var workers []api.Worker
	e.json("GET", "/v1/workers", subTok, nil, 200, &workers)
	if len(workers) != 1 || !workers[0].Connected || workers[0].CurrentJob != nil {
		t.Fatalf("workers = %+v", workers)
	}
	var st api.Stats
	e.json("GET", "/v1/stats", subTok, nil, 200, &st)
	if st.WorkersConnected != 1 || st.WorkersConnectedByTag["kernel"] != 1 || st.DispatchLatencyMs == nil || st.Powerwarden != api.Disabled ||
		st.RunsHoldingSecrets != 0 {
		t.Fatalf("stats = %+v", st)
	}
	// The run succeeded, so its secrets were deleted: a retry needs them again.
	if r, b := e.do("POST", "/v1/runs/"+run.Id+"/retry", subTok, nil, nil); r.StatusCode != 422 || errCode(t, b) != api.ErrorCodeMissingSecret ||
		!strings.Contains(string(b), "gone") {
		t.Fatalf("retry without secrets: %d %s", r.StatusCode, b)
	}
	var retried api.Run
	e.json("POST", "/v1/runs/"+run.Id+"/retry", subTok, map[string]any{"secrets": map[string]string{"APTLY": "v"}}, 201, &retried)
	if retried.Number != 2 || retried.ContextRef != up.ContextRef {
		t.Fatalf("retry = %+v", retried)
	}
}

func TestLiveLogStreamAndCancel(t *testing.T) {
	e := newEnv(t, nil)
	run := submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}}}})
	w, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1"})
	a := w.assign()
	w.send(&proto.Accept{JobID: a.Job.ID})
	w.send(&proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: "héllo\n"})
	waitFor(t, "first chunk", func() bool {
		j, _ := e.a.Store.GetJob(a.Job.ID)
		return j.LogSeq == 1
	})
	_, rd := e.sse("/v1/jobs/"+a.Job.ID+"/log/stream", "")
	first := readSSE(t, rd, 1)[0]
	var ch api.LogChunkEvent
	_ = json.Unmarshal([]byte(first.data), &ch)
	if ch.Data != "héllo\n" || ch.Offset != 7 || first.id != "7" {
		t.Fatalf("replayed = %+v", first)
	}
	w.send(&proto.Log{JobID: a.Job.ID, Stream: "stderr", Seq: 2, Data: "oops\n"})
	live := readSSE(t, rd, 1)[0]
	_ = json.Unmarshal([]byte(live.data), &ch)
	if ch.Stream != api.Stderr || ch.Data != "oops\n" || ch.Offset != 12 {
		t.Fatalf("live = %+v", live)
	}
	// Resume from an offset.
	_, rd2 := e.sse("/v1/jobs/"+a.Job.ID+"/log/stream", "7")
	if ev := readSSE(t, rd2, 1)[0]; !strings.Contains(ev.data, "oops") {
		t.Fatalf("resumed = %+v", ev)
	}
	var cancelled api.Run
	e.json("POST", "/v1/runs/"+run.Id+"/cancel", subTok, nil, 200, &cancelled)
	if cancelled.Status != api.RunStatusCancelled {
		t.Fatalf("cancel = %+v", cancelled)
	}
	c, ok := w.next().(*proto.Cancel)
	if !ok || c.Reason != "cancelled" {
		t.Fatalf("cancel msg = %+v", c)
	}
	w.send(&proto.Result{JobID: a.Job.ID, ExitCode: 143, Error: "killed"})
	end := readSSE(t, rd, 1)[0]
	if end.event != "end" || !strings.Contains(end.data, `"status":"cancelled"`) || !strings.Contains(end.data, `"exit_code":143`) {
		t.Fatalf("end = %+v", end)
	}
}

// Regression (H3 e2e): on shutdown the SSE streams (events, live log tail)
// kept running on the old process, so http.Server.Shutdown waited its full
// timeout and flow.Wait/StreamLog clients never noticed the restart.
func TestCloseStreamsEndsSSE(t *testing.T) {
	e := newEnv(t, nil)
	submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}}}})
	w, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1"})
	a := w.assign()
	w.send(&proto.Accept{JobID: a.Job.ID})
	w.send(&proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: "x\n"})
	waitFor(t, "first chunk", func() bool { j, _ := e.a.Store.GetJob(a.Job.ID); return j.LogSeq == 1 })
	_, logs := e.sse("/v1/jobs/"+a.Job.ID+"/log/stream", "")
	readSSE(t, logs, 1)
	_, evs := e.sse("/v1/events", "")
	e.a.API.CloseStreams()
	for name, rd := range map[string]*bufio.Reader{"log stream": logs, "events": evs} {
		done := make(chan error, 1)
		go func() { _, err := io.Copy(io.Discard, rd); done <- err }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s still open after CloseStreams", name)
		}
	}
}

func TestEventsStreamAndResync(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.Events.RingSize = 3 })
	_, rd := e.sse("/v1/events", "")
	run := submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}}}})
	evs := readSSE(t, rd, 2)
	if evs[0].event != "job" || evs[1].event != "run" || !strings.Contains(evs[1].data, run.Id) {
		t.Fatalf("events = %+v", evs)
	}
	for i := 0; i < 5; i++ {
		submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}}}})
	}
	_, rd2 := e.sse("/v1/events", "1")
	if ev := readSSE(t, rd2, 1)[0]; ev.event != "resync" {
		t.Fatalf("expected resync, got %+v", ev)
	}
	seq := e.a.Engine.Hub().Seq()
	_, rd3 := e.sse("/v1/events", jsonNum(seq-1))
	if ev := readSSE(t, rd3, 1)[0]; ev.id != jsonNum(seq) {
		t.Fatalf("resume = %+v", ev)
	}
}

func jsonNum(n uint64) string { b, _ := json.Marshal(n); return string(b) }

func TestNewerConnectionClosesOlder(t *testing.T) {
	e := newEnv(t, nil)
	old, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1"})
	nw, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1"})
	old.closed()
	submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}}}})
	if a := nw.assign(); a.Job.Name != "j" {
		t.Fatal("newer connection did not get the job")
	}
	var ws []api.Worker
	e.json("GET", "/v1/workers", subTok, nil, 200, &ws)
	if len(ws) != 1 || !ws[0].Connected {
		t.Fatalf("workers = %+v", ws)
	}
}

func TestAcceptTimeoutOverWebsocket(t *testing.T) {
	e := newEnv(t, nil)
	submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}}}})
	w, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1"})
	a := w.assign() // a busy worker ignores it
	e.clk.Advance(11 * time.Second)
	e.a.Engine.Reap()
	w.closed()
	w2, _ := e.worker(w2Tok, proto.Hello{WorkerID: "w2"})
	if a2 := w2.assign(); a2.Job.ID != a.Job.ID || a2.Attempt != 1 {
		t.Fatalf("reassigned = %+v", a2)
	}
}

// TestRestartReattach: hourchd restarts while a job runs; the worker
// re-attaches within the lease, resends after last_log_seq, and finishes.
func TestRestartReattach(t *testing.T) {
	e := newEnv(t, nil)
	run := submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}}}})
	w, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1"})
	a := w.assign()
	w.send(&proto.Accept{JobID: a.Job.ID})
	w.send(&proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: "one\n"})
	waitFor(t, "log stored", func() bool {
		j, _ := e.a.Store.GetJob(a.Job.ID)
		return j.LogSeq == 1 && j.AcceptBy == nil
	})
	e.close()
	w.closed()
	e.clk.Advance(10 * time.Minute) // longer than a lease
	e.start()
	e.a.Engine.Reap()
	w2, wel := e.worker(w1Tok, proto.Hello{WorkerID: "w1", RunningJobID: a.Job.ID})
	if wel.Abandon != "" || wel.LastLogSeq != 1 {
		t.Fatalf("welcome = %+v", wel)
	}
	w2.send(&proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 2, Data: "two\n"})
	w2.send(&proto.Result{JobID: a.Job.ID, ExitCode: 0})
	waitFor(t, "run to succeed", func() bool {
		r, _ := e.a.Store.GetRun(run.Id)
		return r.Status == store.RunSucceeded
	})
	e.a.Engine.WaitUploads()
	if _, got := e.do("GET", "/v1/jobs/"+a.Job.ID+"/log", subTok, nil, nil); string(got) != "one\ntwo\n" {
		t.Fatalf("log = %q", got)
	}
}

// Stored secrets survive neither a restart nor a retry needs them: stats
// and healthz count the runs holding secrets; after a restart the count is
// 0 and a retry without values is 422 missing_secret saying why.
func TestSecretsLostOnRestart(t *testing.T) {
	e := newEnv(t, nil)
	run := submit(t, e, api.RunSubmission{Workflow: "wf", Secrets: &map[string]string{"S": "v"},
		Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}, Secrets: &[]string{"S"}}}})
	if r, _ := e.do("POST", "/v1/runs/"+run.Id+"/cancel", subTok, nil, nil); r.StatusCode != 200 {
		t.Fatal("cancel")
	}
	var st api.Stats
	e.json("GET", "/v1/stats", subTok, nil, 200, &st)
	var h api.Healthz
	e.json("GET", "/v1/healthz", "", nil, 200, &h)
	if st.RunsHoldingSecrets != 1 || h.RunsHoldingSecrets == nil || *h.RunsHoldingSecrets != 1 {
		t.Fatalf("stats %+v healthz %+v", st, h)
	}
	// Before a restart the cancelled run's stored secret is reused.
	var re api.Run
	e.json("POST", "/v1/runs/"+run.Id+"/retry", subTok, nil, 201, &re)
	e.close()
	e.start()
	e.json("GET", "/v1/stats", subTok, nil, 200, &st)
	e.json("GET", "/v1/healthz", "", nil, 200, &h)
	if st.RunsHoldingSecrets != 0 || *h.RunsHoldingSecrets != 0 {
		t.Fatalf("after restart: stats %+v healthz %+v", st, h)
	}
	var after api.Run
	e.json("GET", "/v1/runs/"+re.Id, subTok, nil, 200, &after)
	if after.Status != api.RunStatusFailed || after.Jobs[0].Error != "secret_unavailable" {
		t.Fatalf("pending secret job after restart: %+v", after)
	}
	r, b := e.do("POST", "/v1/runs/"+run.Id+"/retry", subTok, nil, nil)
	if r.StatusCode != 422 || errCode(t, b) != api.ErrorCodeMissingSecret || !strings.Contains(string(b), "hourchd restarted") {
		t.Fatalf("retry after restart: %d %s", r.StatusCode, b)
	}
	e.json("POST", "/v1/runs/"+run.Id+"/retry", subTok, map[string]any{"secrets": map[string]string{"S": "v"}}, 201, &re)
}

func TestHealthzReportsMinio(t *testing.T) {
	e := newEnv(t, nil)
	var h api.Healthz
	e.json("GET", "/v1/healthz", "", nil, 200, &h)
	if h.Status != api.HealthzStatusOk || h.Minio != api.HealthzMinioConnected {
		t.Fatalf("healthz = %+v", h)
	}
	e.obj.SetFail(errors.New("down"))
	e.json("GET", "/v1/healthz", "", nil, 200, &h)
	if h.Minio != api.HealthzMinioUnreachable {
		t.Fatalf("healthz = %+v", h)
	}
	if r, b := e.do("POST", "/v1/contexts", subTok, strings.NewReader("x"), nil); r.StatusCode != 503 || errCode(t, b) != api.ErrorCodeUnavailable {
		t.Fatalf("upload with minio down: %d %s", r.StatusCode, b)
	}
}
