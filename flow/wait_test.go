package hourchflow

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	hourchdapi "github.com/resurgentech/hourch/api"
)

const runID = "01JBF6Q9W2X7K3M4N5P6Q7R8S9"

func seedRun(f *fakeHourchd, status hourchdapi.RunStatus) {
	f.setRun(&hourchdapi.Run{
		Id: runID, Workflow: "wf", Number: 1, Status: status, Params: map[string]string{},
		Jobs: []hourchdapi.JobSummary{{Id: "JA", RunId: runID, Name: "a", Status: "ready"}},
	})
}

type jobLog struct {
	mu   sync.Mutex
	seen []string
}

func (l *jobLog) fn(j hourchdapi.JobSummary) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seen = append(l.seen, j.Name+"="+string(j.Status))
}

func (l *jobLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.seen...)
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestWaitViaSSE(t *testing.T) {
	f := newFake(t)
	seedRun(f, "running")
	subscribed := make(chan struct{})
	f.getRunHook = func(id string, n int) {
		if n == 2 { // the GET Wait makes after the stream opens
			close(subscribed)
		}
	}
	f.events = func(w http.ResponseWriter, r *http.Request) {
		sseHeaders(w)
		<-subscribed
		sse(w, "1", "job", hourchdapi.JobEvent{Seq: 1, Type: "job", Job: hourchdapi.JobSummary{Id: "JX", RunId: "OTHER", Name: "x", Status: "leased"}})
		sse(w, "2", "job", hourchdapi.JobEvent{Seq: 2, Type: "job", Job: hourchdapi.JobSummary{Id: "JA", RunId: runID, Name: "a", Status: "leased"}})
		sse(w, "3", "run", hourchdapi.RunEvent{Seq: 3, Type: "run", Run: hourchdapi.RunSummary{Id: "OTHER", Status: "succeeded"}})
		sse(w, "4", "job", hourchdapi.JobEvent{Seq: 4, Type: "job", Job: hourchdapi.JobSummary{Id: "JA", RunId: runID, Name: "a", Status: "succeeded"}})
		f.setStatus(runID, "succeeded")
		sse(w, "5", "run", hourchdapi.RunEvent{Seq: 5, Type: "run", Run: hourchdapi.RunSummary{Id: runID, Status: "succeeded"}})
		<-r.Context().Done() // keep the stream open; Wait must return on the event
	}
	var jl jobLog
	run, err := Wait(ctxT(t), f.client(t), runID, OnJob(jl.fn), PollInterval(time.Hour, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "succeeded" {
		t.Errorf("status = %s", run.Status)
	}
	want := []string{"a=ready", "a=leased", "a=succeeded"}
	if got := jl.get(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("OnJob saw %v, want %v", got, want)
	}
}

func TestWaitSSEResumesWithLastEventID(t *testing.T) {
	f := newFake(t)
	seedRun(f, "running")
	var mu sync.Mutex
	conn := 0
	f.events = func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conn++
		n := conn
		mu.Unlock()
		sseHeaders(w)
		if n == 1 {
			sse(w, "41", "job", hourchdapi.JobEvent{Seq: 41, Type: "job", Job: hourchdapi.JobSummary{Id: "JA", RunId: runID, Name: "a", Status: "leased"}})
			return // drop
		}
		f.setStatus(runID, "failed")
		sse(w, "42", "run", hourchdapi.RunEvent{Seq: 42, Type: "run", Run: hourchdapi.RunSummary{Id: runID, Status: "failed"}})
		<-r.Context().Done()
	}
	run, err := Wait(ctxT(t), f.client(t), runID, PollInterval(time.Millisecond, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "failed" {
		t.Errorf("status = %s", run.Status)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.eventsReqs) != 2 || f.eventsReqs[0] != "" || f.eventsReqs[1] != "41" {
		t.Errorf("Last-Event-ID per connection = %q, want [\"\" \"41\"]", f.eventsReqs)
	}
}

func TestWaitSSEResync(t *testing.T) {
	f := newFake(t)
	seedRun(f, "running")
	f.events = func(w http.ResponseWriter, r *http.Request) {
		sseHeaders(w)
		f.setStatus(runID, "cancelled")
		sse(w, "9", "resync", hourchdapi.ResyncEvent{Seq: 9, Type: "resync"})
		<-r.Context().Done()
	}
	run, err := Wait(ctxT(t), f.client(t), runID, PollInterval(time.Hour, time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "cancelled" {
		t.Errorf("status = %s", run.Status)
	}
}

func TestWaitRunFinishesBeforeSubscribe(t *testing.T) {
	f := newFake(t)
	seedRun(f, "running")
	// The run finishes between the first GET and the subscription; the
	// stream never mentions it.
	f.events = func(w http.ResponseWriter, r *http.Request) {
		f.setStatus(runID, "succeeded")
		sseHeaders(w)
		<-r.Context().Done()
	}
	run, err := Wait(ctxT(t), f.client(t), runID, PollInterval(time.Hour, time.Hour))
	if err != nil || run.Status != "succeeded" {
		t.Fatalf("run %v, err %v", run, err)
	}
}

func TestWaitPollingFallback(t *testing.T) {
	f := newFake(t)
	seedRun(f, "running")
	f.events = nil // 404: no SSE
	f.getRunHook = func(id string, n int) {
		switch n {
		case 3:
			f.mu.Lock()
			f.runs[runID].Jobs[0].Status = "leased"
			f.mu.Unlock()
		case 5:
			f.setStatus(runID, "succeeded")
		}
	}
	var jl jobLog
	run, err := Wait(ctxT(t), f.client(t), runID, OnJob(jl.fn), PollInterval(time.Millisecond, 4*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "succeeded" {
		t.Errorf("status = %s", run.Status)
	}
	if got := jl.get(); strings.Join(got, ",") != "a=ready,a=leased" {
		t.Errorf("OnJob saw %v", got)
	}
	if f.getRunCount != 5 {
		t.Errorf("%d GETs, want 5", f.getRunCount)
	}
}

func TestWaitStreamKeepsDroppingFallsBackToPolling(t *testing.T) {
	f := newFake(t)
	seedRun(f, "running")
	f.events = func(w http.ResponseWriter, r *http.Request) { sseHeaders(w) } // empty, closes
	f.getRunHook = func(id string, n int) {
		if n >= 6 {
			f.setStatus(runID, "succeeded")
		}
	}
	run, err := Wait(ctxT(t), f.client(t), runID, PollInterval(time.Millisecond, time.Millisecond))
	if err != nil || run.Status != "succeeded" {
		t.Fatalf("run %v, err %v", run, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.eventsReqs) != 3 {
		t.Errorf("%d event connections, want 3 before polling", len(f.eventsReqs))
	}
}

func TestWaitWithoutSSE(t *testing.T) {
	f := newFake(t)
	seedRun(f, "running")
	f.events = func(w http.ResponseWriter, r *http.Request) { t.Error("events called with WithoutSSE") }
	f.getRunHook = func(id string, n int) {
		if n == 2 {
			f.setStatus(runID, "failed")
		}
	}
	run, err := Wait(ctxT(t), f.client(t), runID, WithoutSSE(), PollInterval(time.Millisecond, time.Millisecond))
	if err != nil || run.Status != "failed" {
		t.Fatalf("run %v, err %v", run, err)
	}
}

func TestWaitAlreadyTerminal(t *testing.T) {
	f := newFake(t)
	seedRun(f, "succeeded")
	run, err := Wait(ctxT(t), f.client(t), runID)
	if err != nil || run.Status != "succeeded" {
		t.Fatalf("run %v, err %v", run, err)
	}
	if len(f.eventsReqs) != 0 {
		t.Error("subscribed to events for a finished run")
	}
}

func TestWaitErrors(t *testing.T) {
	f := newFake(t)
	_, err := Wait(ctxT(t), f.client(t), "01JBF6Q9W2X7K3M4N5P6Q7R8ZZ")
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 404 || ae.Code != "not_found" {
		t.Fatalf("err = %v", err)
	}

	seedRun(f, "running")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = Wait(ctx, f.client(t), runID, WithoutSSE(), PollInterval(time.Millisecond, time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline", err)
	}
}

func TestStreamLog(t *testing.T) {
	f := newFake(t)
	var mu sync.Mutex
	var lastIDs []string
	var attempts []string
	f.logStream = func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lastIDs = append(lastIDs, r.Header.Get("Last-Event-ID"))
		attempts = append(attempts, r.URL.Query().Get("attempt"))
		n := len(lastIDs)
		mu.Unlock()
		if r.PathValue("id") != "JOB1" {
			writeErr(w, 404, "not_found", "no job")
			return
		}
		sseHeaders(w)
		if n == 1 {
			sse(w, "6", "log", hourchdapi.LogChunkEvent{Offset: 6, Stream: "stdout", Data: "hello\n"})
			return // drop mid-attempt
		}
		sse(w, "12", "log", hourchdapi.LogChunkEvent{Offset: 12, Stream: "stderr", Data: "world\n"})
		code := 0
		sse(w, "12", "end", hourchdapi.LogEndEvent{Offset: 12, Status: "succeeded", ExitCode: &code})
	}
	var buf bytes.Buffer
	end, err := StreamLog(ctxT(t), f.client(t), "JOB1", 2, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if buf.String() != "hello\nworld\n" {
		t.Errorf("log = %q", buf.String())
	}
	if end == nil || end.Status != "succeeded" || end.ExitCode == nil || *end.ExitCode != 0 || end.Offset != 12 {
		t.Errorf("end = %+v", end)
	}
	mu.Lock()
	if strings.Join(lastIDs, ",") != ",6" || strings.Join(attempts, ",") != "2,2" {
		t.Errorf("Last-Event-ID %q attempts %q", lastIDs, attempts)
	}
	mu.Unlock()

	_, err = StreamLog(ctxT(t), f.client(t), "NOPE", 0, &buf)
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 404 {
		t.Fatalf("err = %v", err)
	}
}

func TestSSEReader(t *testing.T) {
	in := ": comment\n\nid: 1\nevent: log\ndata: a\ndata: b\n\nretry: 5\n\ndata:c\n\n"
	sr := newSSEReader(strings.NewReader(in))
	ev, err := sr.Next()
	if err != nil || ev.ID != "1" || ev.Event != "log" || ev.Data != "a\nb" {
		t.Fatalf("%+v %v", ev, err)
	}
	ev, err = sr.Next()
	if err != nil || ev.Data != "c" {
		t.Fatalf("%+v %v", ev, err)
	}
	if _, err := sr.Next(); err == nil {
		t.Fatal("no EOF")
	}
}
