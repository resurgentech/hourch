package hourchflow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	hourchdapi "github.com/resurgentech/hourch/api"
)

const (
	testToken    = "submitter-token"
	testJobToken = "job-token"
)

// fakeHourchd implements the routes flow uses, recording what it saw.
type fakeHourchd struct {
	t   *testing.T
	srv *httptest.Server

	mu            sync.Mutex
	contexts      map[string][]byte // sha256 hex -> body
	ctxReqs       []ctxReq
	submissions   []hourchdapi.RunSubmission
	runReqs       int
	numbers       map[string]int64
	runs          map[string]*hourchdapi.Run
	getRunHook    func(id string, n int) // called on each GET /v1/runs/{id} (n from 1)
	getRunCount   int
	events        http.HandlerFunc // GET /v1/events; nil = 404
	eventsReqs    []string         // Last-Event-ID of each events request
	logStream     http.HandlerFunc // GET /v1/jobs/{id}/log/stream
	ctxStatus     int              // force a status on POST /v1/contexts
	ctxBadSum     bool             // reply with a wrong sha256
	runStatus     int              // force a status on POST /v1/runs
	unsatisfiable bool             // with runStatus: answer unsatisfiable
	validateOnly  []string         // validate_only query of each POST /v1/runs
	counters      map[string]int64
	builds        map[string]json.RawMessage
}

type ctxReq struct {
	auth, contentType string
	contentLength     int64
	transferEncoding  []string
	body              []byte
}

func newFake(t *testing.T) *fakeHourchd {
	f := &fakeHourchd{
		t:        t,
		contexts: map[string][]byte{},
		numbers:  map[string]int64{},
		runs:     map[string]*hourchdapi.Run{},
		counters: map[string]int64{},
		builds:   map[string]json.RawMessage{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/contexts", f.postContext)
	mux.HandleFunc("POST /v1/runs", f.postRun)
	mux.HandleFunc("GET /v1/runs/{id}", f.getRun)
	mux.HandleFunc("GET /v1/events", func(w http.ResponseWriter, r *http.Request) {
		if !f.auth(w, r, testToken) {
			return
		}
		f.mu.Lock()
		f.eventsReqs = append(f.eventsReqs, r.Header.Get("Last-Event-ID"))
		h := f.events
		f.mu.Unlock()
		if h == nil {
			writeErr(w, 404, "not_found", "no events here")
			return
		}
		h(w, r)
	})
	mux.HandleFunc("GET /v1/jobs/{id}/log/stream", func(w http.ResponseWriter, r *http.Request) {
		if !f.auth(w, r, testToken) {
			return
		}
		f.logStream(w, r)
	})
	mux.HandleFunc("POST /v1/counters/{name}/next", f.nextCounter)
	mux.HandleFunc("PUT /v1/builds/{type}/{flavour}/{num}", f.putBuild)
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeHourchd) client(t *testing.T) *hourchdapi.Client {
	c, err := NewClient(f.srv.URL, testToken, hourchdapi.WithHTTPClient(f.srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(hourchdapi.Error{Error: hourchdapi.ErrorCode(code), Message: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (f *fakeHourchd) auth(w http.ResponseWriter, r *http.Request, want string) bool {
	if r.Header.Get("Authorization") != "Bearer "+want {
		writeErr(w, 401, "unauthorized", "bad token")
		return false
	}
	return true
}

func (f *fakeHourchd) postContext(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, 400, "bad_request", err.Error())
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ctxReqs = append(f.ctxReqs, ctxReq{
		auth: r.Header.Get("Authorization"), contentType: r.Header.Get("Content-Type"),
		contentLength: r.ContentLength, transferEncoding: r.TransferEncoding, body: body,
	})
	if r.Header.Get("Authorization") != "Bearer "+testToken {
		writeErr(w, 401, "unauthorized", "bad token")
		return
	}
	if f.ctxStatus != 0 {
		writeErr(w, f.ctxStatus, "too_large", "context over limit")
		return
	}
	sum := sha256.Sum256(body)
	hexsum := hex.EncodeToString(sum[:])
	reply := hourchdapi.ContextUpload{ContextRef: "sha256:" + hexsum, Sha256: hexsum, Size: int64(len(body))}
	if f.ctxBadSum {
		reply.Sha256 = strings.Repeat("0", 64)
		reply.ContextRef = "sha256:" + reply.Sha256
	}
	if _, ok := f.contexts[hexsum]; ok {
		reply.Created = false
		writeJSON(w, 200, reply)
		return
	}
	f.contexts[hexsum] = body
	reply.Created = true
	writeJSON(w, 201, reply)
}

func (f *fakeHourchd) postRun(w http.ResponseWriter, r *http.Request) {
	if !f.auth(w, r, testToken) {
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		writeErr(w, 400, "bad_request", "content type "+ct)
		return
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var sub hourchdapi.RunSubmission
	if err := dec.Decode(&sub); err != nil {
		writeErr(w, 400, "bad_request", err.Error())
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runReqs++
	f.submissions = append(f.submissions, sub)
	f.validateOnly = append(f.validateOnly, r.URL.Query().Get("validate_only"))
	if f.runStatus != 0 {
		code, msg := "invalid_graph", `job "record": depends_on names unknown job "biuld"`
		if f.unsatisfiable {
			code, msg = "unsatisfiable", `job "build" needs tags [kernel]; known workers offer [tiny]; powered-off hosts could not be checked (powerwarden is disabled)`
		}
		writeErr(w, f.runStatus, code, msg)
		return
	}
	if r.URL.Query().Get("validate_only") == "true" {
		if sub.ContextRef != nil {
			writeErr(w, 400, "bad_request", "test fake: Check must not send a context_ref")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"valid":true}`))
		return
	}
	if sub.ContextRef != nil {
		if _, ok := f.contexts[strings.TrimPrefix(*sub.ContextRef, "sha256:")]; !ok {
			writeErr(w, 422, "context_not_found", "unknown context")
			return
		}
	}
	f.numbers[sub.Workflow]++
	id := fmt.Sprintf("01JBF6Q9W2X7K3M4N5P6Q7R8%02d", len(f.runs))
	run := &hourchdapi.Run{
		Id: id, Workflow: sub.Workflow, Number: f.numbers[sub.Workflow], Status: "running",
		SubmittedBy: "test", CreatedAt: time.Unix(1700000000, 0).UTC(), Params: map[string]string{},
		SecretNames: []string{}, Jobs: []hourchdapi.JobSummary{},
	}
	if sub.ContextRef != nil {
		run.ContextRef = *sub.ContextRef
	}
	if sub.Secrets != nil {
		for k := range *sub.Secrets {
			run.SecretNames = append(run.SecretNames, k)
		}
	}
	for _, j := range sub.Jobs {
		run.Jobs = append(run.Jobs, hourchdapi.JobSummary{Name: j.Name, RunId: id, Status: "pending", Id: "J" + j.Name})
	}
	f.runs[id] = run
	writeJSON(w, 201, run)
}

func (f *fakeHourchd) getRun(w http.ResponseWriter, r *http.Request) {
	if !f.auth(w, r, testToken) {
		return
	}
	id := r.PathValue("id")
	f.mu.Lock()
	f.getRunCount++
	n := f.getRunCount
	hook := f.getRunHook
	f.mu.Unlock()
	if hook != nil {
		hook(id, n)
	}
	f.mu.Lock()
	run, ok := f.runs[id]
	var cp hourchdapi.Run
	if ok {
		cp = *run
		cp.Jobs = append([]hourchdapi.JobSummary(nil), run.Jobs...)
	}
	f.mu.Unlock()
	if !ok {
		writeErr(w, 404, "not_found", "no run "+id)
		return
	}
	writeJSON(w, 200, cp)
}

func (f *fakeHourchd) setRun(run *hourchdapi.Run) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[run.Id] = run
}

func (f *fakeHourchd) setStatus(id string, s hourchdapi.RunStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[id].Status = s
}

func (f *fakeHourchd) nextCounter(w http.ResponseWriter, r *http.Request) {
	if !f.auth(w, r, testJobToken) {
		return
	}
	name := r.PathValue("name")
	f.mu.Lock()
	f.counters[name]++
	v := f.counters[name]
	f.mu.Unlock()
	writeJSON(w, 200, hourchdapi.Counter{Name: name, Value: v})
}

func (f *fakeHourchd) putBuild(w http.ResponseWriter, r *http.Request) {
	if !f.auth(w, r, testJobToken) {
		return
	}
	body, _ := io.ReadAll(r.Body)
	var obj hourchdapi.BuildBody
	if err := json.Unmarshal(body, &obj); err != nil {
		writeErr(w, 400, "bad_request", err.Error())
		return
	}
	key := r.PathValue("type") + "/" + r.PathValue("flavour") + "/" + r.PathValue("num")
	f.mu.Lock()
	f.builds[key] = body
	f.mu.Unlock()
	var num int64
	fmt.Sscan(r.PathValue("num"), &num)
	writeJSON(w, 200, hourchdapi.Build{
		BuildType: r.PathValue("type"), Flavour: r.PathValue("flavour"), BuildNum: num,
		RunId: "01JBF6Q9W2X7K3M4N5P6Q7R8S9", JobId: "01JBF6Q9W2X7K3M4N5P6Q7R8SA",
		CreatedAt: time.Unix(1700000000, 0).UTC(), Body: obj,
	})
}

// sse writes one event and flushes.
func sse(w http.ResponseWriter, id, event string, data any) {
	b, _ := json.Marshal(data)
	var buf bytes.Buffer
	if id != "" {
		fmt.Fprintf(&buf, "id: %s\n", id)
	}
	fmt.Fprintf(&buf, "event: %s\ndata: %s\n\n", event, b)
	w.Write(buf.Bytes())
	w.(http.Flusher).Flush()
}

func sseHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(200)
	w.Write([]byte(": heartbeat\n\n"))
	w.(http.Flusher).Flush()
}
