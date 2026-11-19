package httpapi_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	api "github.com/resurgentech/hourch/api"
	pw "github.com/resurgentech/powerwarden/powerwarden/api"
)

func errMsg(t *testing.T, b []byte) string {
	t.Helper()
	var e api.Error
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("not an Error body: %s", b)
	}
	return e.Message
}

func kernelJob(name string) api.JobSpec {
	return api.JobSpec{Name: name, Argv: []string{"x"}, Tags: ptr([]string{"kernel"})}
}

func (e *env) post(path string, rs api.RunSubmission) (int, []byte) {
	e.t.Helper()
	b, _ := json.Marshal(rs)
	resp, body := e.do("POST", path, subTok, bytes.NewReader(b), nil)
	return resp.StatusCode, body
}

func (e *env) countRows(table string) int {
	e.t.Helper()
	var n int
	if err := e.a.Store.DB().QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func TestSubmitUnsatisfiable(t *testing.T) {
	e := newEnv(t, nil)
	forgetSeen(t, e.a.Store)
	code, b := e.post("/v1/runs", api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{kernelJob("build")}})
	if code != 422 || errCode(t, b) != api.ErrorCodeUnsatisfiable {
		t.Fatalf("= %d %s", code, b)
	}
	if m := errMsg(t, b); m != `job "build" needs tags [kernel]; no worker has ever connected; powered-off hosts could not be checked (powerwarden is disabled)` {
		t.Fatalf("message = %q", m)
	}
	if n := e.countRows("runs"); n != 0 {
		t.Fatalf("%d runs stored", n)
	}
}

func TestRetryUnsatisfiable(t *testing.T) {
	e := newEnv(t, nil)
	run := submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{kernelJob("build")}})
	e.json("POST", "/v1/runs/"+run.Id+"/cancel", subTok, nil, 200, nil)
	forgetSeen(t, e.a.Store)
	resp, b := e.do("POST", "/v1/runs/"+run.Id+"/retry", subTok, nil, nil)
	if resp.StatusCode != 422 || errCode(t, b) != api.ErrorCodeUnsatisfiable {
		t.Fatalf("retry = %d %s", resp.StatusCode, b)
	}
}

func TestValidateOnly(t *testing.T) {
	e := newEnv(t, nil)
	valid := api.RunSubmission{Workflow: "wf", Secrets: ptr(map[string]string{"S": "v"}),
		Jobs: []api.JobSpec{{Name: "a", Argv: []string{"x"}, Secrets: ptr([]string{"S"})}, kernelJob("b")}}
	var vr api.ValidationResult
	e.json("POST", "/v1/runs?validate_only=true", subTok, valid, 200, &vr)
	if !vr.Valid {
		t.Fatalf("result = %+v", vr)
	}
	if r, ev, so := e.countRows("runs"), e.countRows("events"), e.countRows("secret_objects"); r+ev+so != 0 {
		t.Fatalf("stored: runs %d events %d secret_objects %d", r, ev, so)
	}
	if keys := e.obj.Keys(); len(keys) != 0 {
		t.Fatalf("objects stored: %v", keys)
	}
	// validate_only=false is a real submission.
	e.json("POST", "/v1/runs?validate_only=false", subTok, valid, 201, nil)

	missingRef := "sha256:" + strings.Repeat("a", 64)
	bad := map[string]api.RunSubmission{
		"invalid_graph":  {Workflow: "wf", Jobs: []api.JobSpec{{Name: "a", Argv: []string{"x"}, DependsOn: ptr([]string{"nope"})}}},
		"missing_secret": {Workflow: "wf", Jobs: []api.JobSpec{{Name: "a", Argv: []string{"x"}, Secrets: ptr([]string{"S"})}}},
		// A context_ref is optional, but checked when given.
		"context_not_found": {Workflow: "wf", ContextRef: &missingRef, Jobs: []api.JobSpec{{Name: "a", Argv: []string{"x"}}}},
		"unsatisfiable":     {Workflow: "wf", Jobs: []api.JobSpec{{Name: "a", Argv: []string{"x"}, Tags: ptr([]string{"quantum"})}}},
	}
	for want, rs := range bad {
		vc, vb := e.post("/v1/runs?validate_only=true", rs)
		sc, sb := e.post("/v1/runs", rs)
		if vc != 422 || string(errCode(t, vb)) != want || vc != sc || !bytes.Equal(vb, sb) {
			t.Errorf("%s: validate %d %s, submit %d %s", want, vc, vb, sc, sb)
		}
	}
	if code, b := e.post("/v1/runs?validate_only=maybe", valid); code != 400 || errCode(t, b) != api.ErrorCodeBadRequest {
		t.Fatalf("bad flag = %d %s", code, b)
	}
}

func TestStatsUnschedulableReady(t *testing.T) {
	e := newEnv(t, nil)
	submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{kernelJob("build")}})
	var st api.Stats
	e.json("GET", "/v1/stats", subTok, nil, 200, &st)
	if st.JobsReady != 1 || st.UnschedulableReady != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestPowerwardenHostsInSubmissionCheck(t *testing.T) {
	e, fake := pwEnv(t)
	forgetSeen(t, e.a.Store)
	fake.SetHosts(
		pw.HostView{Hostname: "veritech01", Tags: []string{"kernel"}},
		pw.HostView{Hostname: "gpu1", Tags: []string{"gpu"}, Locked: true},
		pw.HostView{Hostname: "gpu2", Tags: []string{"gpu"}, Exclude: true},
	)
	submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{kernelJob("build")}})
	img := api.JobSpec{Name: "build:hello", Argv: []string{"x"}, Tags: ptr([]string{"kernel"}), Image: ptr("img")}
	code, b := e.post("/v1/runs", api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{img}})
	if code != 422 || errMsg(t, b) != `job "build:hello" needs tags [kernel] + docker; no worker has ever connected; powerwarden hosts offer [kernel] (no docker known)` {
		t.Fatalf("image job = %d %s", code, b)
	}
	gpu := api.JobSpec{Name: "train", Argv: []string{"x"}, Tags: ptr([]string{"gpu"})}
	code, b = e.post("/v1/runs", api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{gpu}})
	if code != 422 || errMsg(t, b) != `job "train" needs tags [gpu]; no worker has ever connected; powerwarden hosts offer [kernel]` {
		t.Fatalf("locked/excluded host counted: %d %s", code, b)
	}
	if n := fake.HostGets(); n != 1 {
		t.Fatalf("GET /v1/hosts %d times, want 1 (cached)", n)
	}
}

func TestPowerwardenDownSubmissionCheck(t *testing.T) {
	e, fake := pwEnv(t)
	fake.SetDown(true)
	forgetSeen(t, e.a.Store)
	code, b := e.post("/v1/runs", api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{kernelJob("build")}})
	if code != 422 || errMsg(t, b) != `job "build" needs tags [kernel]; no worker has ever connected; powered-off hosts could not be checked (powerwarden unreachable: list hosts: 503 down)` {
		t.Fatalf("= %d %s", code, b)
	}
	seedSeen(t, e.a.Store) // the workers-only check still accepts what workers cover
	submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{kernelJob("build")}})
}
