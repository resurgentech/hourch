package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/resurgentech/keyclient/keylist"

	api "github.com/resurgentech/hourch/api"

	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/events"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/ulid"
	"github.com/resurgentech/hourch/daemon/internal/view"
)

const maxSubmissionBytes = 16 << 20

func deref[T any](p *T) T {
	var z T
	if p == nil {
		return z
	}
	return *p
}

func toSubmission(rs api.RunSubmission) graph.Submission {
	s := graph.Submission{
		Workflow: rs.Workflow, Params: deref(rs.Params), ContextRef: deref(rs.ContextRef), Secrets: deref(rs.Secrets),
	}
	for _, j := range rs.Jobs {
		gj := graph.Job{
			Name: j.Name, Tags: deref(j.Tags), Image: deref(j.Image), Argv: j.Argv, Workdir: deref(j.Workdir),
			Env: deref(j.Env), Secrets: deref(j.Secrets), Artifacts: deref(j.Artifacts), DependsOn: deref(j.DependsOn),
			ConcurrencyKey: deref(j.ConcurrencyKey), MaxAttempts: deref(j.MaxAttempts), TimeoutSeconds: deref(j.TimeoutSeconds),
		}
		if j.MaxAttempts != nil && *j.MaxAttempts < 1 {
			gj.MaxAttempts = -1 // explicit 0 is out of bounds
		}
		if j.TimeoutSeconds != nil && *j.TimeoutSeconds < 1 {
			gj.TimeoutSeconds = -1
		}
		for _, in := range deref(j.Inputs) {
			gj.Inputs = append(gj.Inputs, graph.Input{Job: in.Job, Path: in.Path, As: deref(in.As)})
		}
		s.Jobs = append(s.Jobs, gj)
	}
	return s
}

// submitError maps an engine submission error to a response.
func (s *Server) submitError(w http.ResponseWriter, err error) {
	var br *graph.BadRequest
	var ig *graph.InvalidGraph
	var ms *graph.MissingSecret
	var us *dispatch.Unsatisfiable
	switch {
	case errors.As(err, &br):
		badRequest(w, "%s", br.Msg)
	case errors.As(err, &ig):
		writeError(w, http.StatusUnprocessableEntity, api.ErrorCodeInvalidGraph, ig.Msg)
	case errors.As(err, &ms):
		writeError(w, http.StatusUnprocessableEntity, api.ErrorCodeMissingSecret, ms.Msg)
	case errors.As(err, &us):
		writeError(w, http.StatusUnprocessableEntity, api.ErrorCodeUnsatisfiable, us.Msg)
	case errors.Is(err, dispatch.ErrContextNotFound):
		writeError(w, http.StatusUnprocessableEntity, api.ErrorCodeContextNotFound, "context_ref is not stored")
	case errors.Is(err, dispatch.ErrUnavailable):
		s.unavailable(w, err)
	case errors.Is(err, dispatch.ErrNotFound):
		notFound(w, "run not found")
	default:
		s.internal(w, err)
	}
}

func (s *Server) runView(w http.ResponseWriter, status int, runID string) {
	q := s.eng.Store().Q
	r, err := q.GetRun(runID)
	if errors.Is(err, store.ErrNotFound) {
		notFound(w, "run %s not found", runID)
		return
	} else if err != nil {
		s.internal(w, err)
		return
	}
	jobs, err := q.RunJobs(runID)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, status, view.Run(r, jobs))
}

func (s *Server) submitRun(w http.ResponseWriter, r *http.Request, c caller) {
	validateOnly := false
	if v := r.URL.Query().Get("validate_only"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			badRequest(w, "validate_only must be true or false")
			return
		}
		validateOnly = b
	}
	var rs api.RunSubmission
	if !decodeBody(w, r, maxSubmissionBytes, &rs) {
		return
	}
	if validateOnly {
		if err := s.eng.Validate(r.Context(), toSubmission(rs)); err != nil {
			s.submitError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, api.ValidationResult{Valid: true})
		return
	}
	run, err := s.eng.Submit(r.Context(), c.name, toSubmission(rs))
	if err != nil {
		s.submitError(w, err)
		return
	}
	s.runView(w, http.StatusCreated, run.ID)
}

func (s *Server) retryRun(w http.ResponseWriter, r *http.Request, c caller) {
	id := r.PathValue("id")
	var rr api.RetryRequest
	body, err := io.ReadAll(io.LimitReader(r.Body, maxSubmissionBytes+1))
	if err != nil {
		badRequest(w, "cannot read body")
		return
	}
	if len(bytes.TrimSpace(body)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&rr); err != nil {
			badRequest(w, "invalid body: %v", err)
			return
		}
		if dec.More() {
			badRequest(w, "trailing data after body")
			return
		}
	}
	run, err := s.eng.Retry(r.Context(), c.name, id, deref(rr.Secrets))
	if err != nil {
		s.submitError(w, err)
		return
	}
	s.runView(w, http.StatusCreated, run.ID)
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request, _ caller) {
	q := r.URL.Query()
	f := store.RunFilter{Workflow: q.Get("workflow"), Status: q.Get("status"), Before: q.Get("before")}
	if f.Workflow != "" && !graph.WorkflowRe.MatchString(f.Workflow) {
		badRequest(w, "invalid workflow")
		return
	}
	switch f.Status {
	case "", store.RunRunning, store.RunSucceeded, store.RunFailed, store.RunCancelled:
	default:
		badRequest(w, "invalid status")
		return
	}
	if f.Before != "" && !ulid.Valid(f.Before) {
		badRequest(w, "before must be a run id")
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	f.Limit = limit
	st := s.eng.Store()
	runs, err := st.ListRuns(f)
	if err != nil {
		s.internal(w, err)
		return
	}
	out := make([]api.RunSummary, 0, len(runs))
	for _, run := range runs {
		counts, err := st.JobCounts(run.ID)
		if err != nil {
			s.internal(w, err)
			return
		}
		out = append(out, view.RunSummary(run, counts))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request, _ caller) {
	s.runView(w, http.StatusOK, r.PathValue("id"))
}

func (s *Server) getRunByNumber(w http.ResponseWriter, r *http.Request, _ caller) {
	wf := r.PathValue("workflow")
	n, err := strconv.ParseInt(r.PathValue("number"), 10, 64)
	if err != nil || n < 1 || !graph.WorkflowRe.MatchString(wf) {
		badRequest(w, "invalid workflow or number")
		return
	}
	run, err := s.eng.Store().GetRunByNumber(wf, n)
	if errors.Is(err, store.ErrNotFound) {
		notFound(w, "%s run %d not found", wf, n)
		return
	} else if err != nil {
		s.internal(w, err)
		return
	}
	s.runView(w, http.StatusOK, run.ID)
}

func (s *Server) cancelRun(w http.ResponseWriter, r *http.Request, c caller) {
	id := r.PathValue("id")
	if err := s.eng.CancelRun(id); errors.Is(err, dispatch.ErrNotFound) {
		notFound(w, "run %s not found", id)
		return
	} else if err != nil {
		s.internal(w, err)
		return
	}
	s.log.Info("run cancel requested", "run", id, "by", c.name)
	s.runView(w, http.StatusOK, id)
}

func (s *Server) jobDetail(w http.ResponseWriter, jobID string) {
	q := s.eng.Store().Q
	j, err := q.GetJob(jobID)
	if errors.Is(err, store.ErrNotFound) {
		notFound(w, "job %s not found", jobID)
		return
	} else if err != nil {
		s.internal(w, err)
		return
	}
	outs, err := q.Outputs(jobID)
	if err != nil {
		s.internal(w, err)
		return
	}
	evs, err := q.JobEvents(jobID)
	if err != nil {
		s.internal(w, err)
		return
	}
	arts, err := q.Artifacts(jobID)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view.JobDetail(j, outs, evs, arts))
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, _ caller) {
	s.jobDetail(w, r.PathValue("id"))
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request, c caller) {
	id := r.PathValue("id")
	if err := s.eng.CancelJob(id); errors.Is(err, dispatch.ErrNotFound) {
		notFound(w, "job %s not found", id)
		return
	} else if err != nil {
		s.internal(w, err)
		return
	}
	s.log.Info("job cancel requested", "job", id, "by", c.name)
	s.jobDetail(w, id)
}

func (s *Server) listWorkers(w http.ResponseWriter, r *http.Request, _ caller) {
	ws, err := s.eng.Store().ListWorkers(s.eng.Clock().Now().Add(-s.o.WorkerRetention))
	if err != nil {
		s.internal(w, err)
		return
	}
	out := make([]api.Worker, 0, len(ws))
	for _, wk := range ws {
		out = append(out, view.Worker(wk))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) nextCounter(w http.ResponseWriter, r *http.Request, c caller) {
	name := r.PathValue("name")
	if !graph.CounterRe.MatchString(name) {
		badRequest(w, "invalid counter name")
		return
	}
	v, err := s.eng.Store().NextCounter(name)
	if err != nil {
		s.internal(w, err)
		return
	}
	s.log.Info("counter incremented", "counter", name, "value", v, "by", c.name)
	writeJSON(w, http.StatusOK, api.Counter{Name: name, Value: v})
}

const maxBuildBody = 1 << 20

func (s *Server) putBuild(w http.ResponseWriter, r *http.Request, c caller) {
	typ, flavour := r.PathValue("type"), r.PathValue("flavour")
	num, err := strconv.ParseInt(r.PathValue("num"), 10, 64)
	if err != nil || num < 1 || !graph.BuildRe.MatchString(typ) || !graph.FlavourRe.MatchString(flavour) {
		badRequest(w, "invalid type, flavour or num")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBuildBody+1))
	if err != nil {
		badRequest(w, "cannot read body")
		return
	}
	if len(body) > maxBuildBody {
		writeError(w, http.StatusRequestEntityTooLarge, api.ErrorCodeTooLarge, "build body over 1 MiB")
		return
	}
	body = bytes.TrimSpace(body)
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil || obj == nil {
		badRequest(w, "body must be a JSON object")
		return
	}
	b := store.Build{BuildType: typ, Flavour: flavour, BuildNum: num, RunID: c.job.RunID, JobID: c.job.ID,
		Body: json.RawMessage(body), CreatedAt: s.eng.Clock().Now()}
	if err := s.eng.Store().PutBuild(b); err != nil {
		s.internal(w, err)
		return
	}
	s.log.Info("build stored", "type", typ, "flavour", flavour, "num", num, "run", c.job.RunID, "job", c.job.ID)
	writeJSON(w, http.StatusOK, view.Build(b))
}

func (s *Server) listBuilds(w http.ResponseWriter, r *http.Request, _ caller) {
	q := r.URL.Query()
	typ, flavour := q.Get("type"), q.Get("flavour")
	if (typ != "" && !graph.BuildRe.MatchString(typ)) || (flavour != "" && !graph.FlavourRe.MatchString(flavour)) {
		badRequest(w, "invalid type or flavour")
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	bs, err := s.eng.Store().ListBuilds(typ, flavour, limit)
	if err != nil {
		s.internal(w, err)
		return
	}
	out := make([]api.Build, 0, len(bs))
	for _, b := range bs {
		out = append(out, view.Build(b))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request, _ caller) {
	h := api.Healthz{Status: api.HealthzStatusOk, Minio: api.HealthzMinioConnected, Powerwarden: api.HealthzPowerwarden(s.powerwarden())}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.eng.Objects().Ping(ctx); err != nil {
		h.Minio = api.HealthzMinioUnreachable
	}
	if err := s.eng.Store().Ping(); err != nil {
		h.Status = api.HealthzStatusUnavailable
		writeJSON(w, http.StatusServiceUnavailable, h)
		return
	}
	if n, err := s.eng.RunsHoldingSecrets(); err == nil {
		h.RunsHoldingSecrets = &n
	}
	writeJSON(w, http.StatusOK, h)
}

// powerwarden is the publisher state for healthz and stats.
func (s *Server) powerwarden() string {
	if s.o.Powerwarden == nil {
		return string(api.HealthzPowerwardenDisabled)
	}
	return s.o.Powerwarden()
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request, _ caller) {
	st := s.eng.Store()
	qs, err := st.QueueStats()
	if err != nil {
		s.internal(w, err)
		return
	}
	running, err := st.CountRuns(store.RunRunning)
	if err != nil {
		s.internal(w, err)
		return
	}
	holding, err := s.eng.RunsHoldingSecrets()
	if err != nil {
		s.internal(w, err)
		return
	}
	unsched, err := s.eng.UnschedulableReady()
	if err != nil {
		s.internal(w, err)
		return
	}
	out := api.Stats{
		UnschedulableReady: unsched,
		RunsRunning:        running, RunsHoldingSecrets: holding, JobsReady: qs.Ready, JobsLeased: qs.Leased,
		WorkersConnectedByTag: map[string]int{}, Powerwarden: api.StatsPowerwarden(s.powerwarden()),
	}
	if qs.OldestReady != nil {
		age := int(s.eng.Clock().Now().Sub(*qs.OldestReady) / time.Second)
		if age < 0 {
			age = 0
		}
		out.OldestReadyAgeSeconds = &age
	}
	conn := s.eng.ConnectedWorkers()
	out.WorkersConnected = len(conn)
	for _, tags := range conn {
		for _, t := range tags {
			out.WorkersConnectedByTag[t]++
		}
	}
	if p50, p99, ok := s.eng.Latency(); ok {
		out.DispatchLatencyMs = &api.LatencyMs{P50: float32(p50), P99: float32(p99)}
	}
	if s.o.KeyList != nil {
		out.PersonalKeys = personalKeysStatus(s.o.KeyList.Status())
	}
	writeJSON(w, http.StatusOK, out)
}

// personalKeysStatus is the stats object for the key list. It holds no hash,
// token or URL: only counts, times and a safe error text.
func personalKeysStatus(st keylist.Status) *api.PersonalKeysStatus {
	out := &api.PersonalKeysStatus{
		Origin: api.PersonalKeysStatusOrigin(st.Origin), Entries: st.Entries, CacheFailing: st.CacheFailing,
	}
	if st.Origin != keylist.OriginNone {
		age := int(st.Age / time.Second)
		lt := st.ListTime.UTC()
		out.AgeSeconds, out.ListTime = &age, &lt
	}
	if !st.LastSuccess.IsZero() {
		ls := st.LastSuccess.UTC()
		out.LastSuccess = &ls
	}
	if st.LastError != nil {
		e := safeError(st.LastError)
		out.LastError = &e
	}
	return out
}

// safeError makes an error text fit for a stats field: every rune that is
// not printable becomes "?", and at most 200 runes are kept.
func safeError(err error) string {
	rs := []rune(err.Error())
	if len(rs) > 200 {
		rs = rs[:200]
	}
	for i, r := range rs {
		if !unicode.IsPrint(r) {
			rs[i] = '?'
		}
	}
	return string(rs)
}

func (s *Server) connectWorker(w http.ResponseWriter, r *http.Request, c caller) {
	s.o.WS.Serve(w, r, c.name)
}

// --- SSE ---

func sseHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request, _ caller) {
	var last uint64
	hasLast := false
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		n, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			badRequest(w, "Last-Event-ID must be an integer")
			return
		}
		last, hasLast = n, true
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.internal(w, errors.New("response writer cannot flush"))
		return
	}
	hub := s.eng.Hub()
	sub := hub.Subscribe(last, hasLast)
	defer sub.Close()
	sseHeaders(w)
	if sub.Resync {
		if writeEvent(w, hub.ResyncEvent()) != nil {
			return
		}
	}
	for _, ev := range sub.Backlog {
		if writeEvent(w, ev) != nil {
			return
		}
	}
	flusher.Flush()
	hb := time.NewTicker(s.o.Heartbeat)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done: // shutting down: end the stream so the client reconnects elsewhere
			return
		case ev, ok := <-sub.C:
			if !ok {
				return // dropped for lagging; the client resumes with Last-Event-ID
			}
			if writeEvent(w, ev) != nil {
				return
			}
			flusher.Flush()
		case <-hb.C:
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func writeEvent(w io.Writer, ev events.Event) error {
	_, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", ev.Seq, ev.Type, ev.Data)
	return err
}
