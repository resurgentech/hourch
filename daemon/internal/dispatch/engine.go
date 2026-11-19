// Package dispatch is hourchd's scheduler: run submission, the job
// lifecycle, push dispatch to connected workers, leases, the reaper,
// timeouts, cancellation, live logs and job tokens.
//
// Every state change runs under one engine mutex and one SQLite transaction;
// side effects (websocket sends, SSE publishes, log uploads) are applied after
// the transaction commits. Dispatch is event driven: it runs at the end of
// every change that can make a job assignable (a job becomes ready, a worker
// connects idle, a worker finishes, a concurrency key frees). There is no
// polling loop; the reaper runs every dispatch.reaper_interval only to find
// lapsed leases, missed accepts and timeouts.
package dispatch

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/events"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/logs"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/secrets"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/ulid"
	"github.com/resurgentech/hourch/daemon/internal/view"
)

// Failure reasons (JobSummary.error).
const (
	ReasonTimeout           = "timeout"
	ReasonLeaseExpired      = "lease_expired"
	ReasonAcceptTimeout     = "accept_timeout"
	ReasonMissingOutput     = "missing_output"
	ReasonSecretUnavailable = "secret_unavailable"
	ReasonDependencyFailed  = "dependency_failed"
	ReasonCancelled         = "cancelled"
	ReasonExitCode          = "exit_code"
	// ReasonNoWorker is in feasible.go.
)

// Errors returned to the API.
var (
	ErrNotFound        = errors.New("not found")
	ErrContextNotFound = errors.New("context not found")
	ErrUnavailable     = errors.New("object store unavailable")
)

// ContextKey is the object key of a context_ref ("sha256:<hex>").
func ContextKey(ref string) string {
	return "contexts/" + strings.TrimPrefix(ref, "sha256:") + ".tar.zst"
}

// LogKey is the object key of an attempt's finished log.
func LogKey(runID, jobID string, attempt int) string {
	return fmt.Sprintf("logs/%s/%s/%d.log", runID, jobID, attempt)
}

// ArtifactKey is the object key of an uploaded artifact.
func ArtifactKey(runID, jobID, path string) string {
	return fmt.Sprintf("artifacts/%s/%s/%s", runID, jobID, path)
}

// Options configure an Engine.
type Options struct {
	Store   *store.Store
	Objects objects.Store
	// Box seals run secrets under this process's volatile key; its
	// instance id names the objects it owns (secrets/<instance>/<run>).
	Box   *secrets.Box
	Logs  *logs.Manager
	Hub   *events.Hub
	Clock clock.Clock
	Log   *slog.Logger

	Heartbeat        time.Duration
	Lease            time.Duration
	AcceptTimeout    time.Duration
	ReaperInterval   time.Duration
	DefaultTimeout   time.Duration
	MaxJobsPerRun    int
	OutputValueBytes int
	// UnschedulableTimeout fails a ready job with no_worker once no
	// connected worker could run it for this long (0: never).
	UnschedulableTimeout time.Duration
}

// Conn is one worker transport (the websocket). Send enqueues without
// blocking and reports false if the connection is closed or backed up.
// Close closes the connection asynchronously.
type Conn interface {
	Send(m proto.Message) bool
	Close(reason string)
}

// Session is one connected worker.
type Session struct {
	WorkerID     string
	Hostname     string
	Tags         []string
	Capabilities []string

	conn      Conn
	gone      bool // replaced or disconnected
	job       string
	attempt   int
	runID     string
	logSeq    int64
	redactor  *secrets.Redactor
	idleSince time.Time
	lastRenew time.Time
	connSeq   uint64
}

// Engine is the scheduler.
type Engine struct {
	o   Options
	log *slog.Logger
	ids *ulid.Generator

	mu       sync.Mutex
	sessions map[string]*Session
	connSeq  uint64
	lat      []latSample

	uploads sync.WaitGroup
	loops   sync.WaitGroup

	notify func() // called after every committed change (SetNotify)

	started  time.Time           // engine creation: backstop coverage starts no earlier
	coverage map[string]coverage // ready job id -> reaper's coverage record (under mu)

	hostsMu sync.Mutex
	hosts   HostLister // powerwarden hosts for the submission check (SetHosts)
}

type latSample struct {
	at time.Time
	ms float64
}

// New returns an engine. Call Start before serving.
func New(o Options) (*Engine, error) {
	if o.Store == nil || o.Objects == nil || o.Box == nil || o.Logs == nil || o.Hub == nil {
		return nil, errors.New("dispatch: store, objects, box, logs and hub are required")
	}
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Lease <= 0 || o.Heartbeat <= 0 || o.AcceptTimeout <= 0 || o.ReaperInterval <= 0 || o.DefaultTimeout <= 0 {
		return nil, errors.New("dispatch: heartbeat, lease, accept timeout, reaper interval and default timeout must be > 0")
	}
	if o.MaxJobsPerRun <= 0 {
		o.MaxJobsPerRun = 200
	}
	if o.OutputValueBytes <= 0 {
		o.OutputValueBytes = 4096
	}
	if o.UnschedulableTimeout < 0 {
		return nil, errors.New("dispatch: unschedulable timeout must be >= 0")
	}
	return &Engine{o: o, log: o.Log, ids: ulid.New(), sessions: map[string]*Session{},
		started: o.Clock.Now(), coverage: map[string]coverage{}}, nil
}

// Store returns the store (read-only use by the API).
func (e *Engine) Store() *store.Store { return e.o.Store }

// Objects returns the object store.
func (e *Engine) Objects() objects.Store { return e.o.Objects }

// Logs returns the live log manager.
func (e *Engine) Logs() *logs.Manager { return e.o.Logs }

// Hub returns the event hub.
func (e *Engine) Hub() *events.Hub { return e.o.Hub }

// Clock returns the clock.
func (e *Engine) Clock() clock.Clock { return e.o.Clock }

// NewID returns a ULID.
func (e *Engine) NewID() string { return e.ids.Make(e.o.Clock.Now()) }

// ---- effects ----

type send struct {
	s *Session
	m proto.Message
}

type logEnd struct {
	runID, jobID string
	attempt      int
	end          logs.End
}

// fx collects side effects of one transaction, applied after commit.
type fx struct {
	jobs    map[string]bool
	runs    map[string]bool
	workers map[string]bool
	sends   []send
	closes  []closeReq
	logEnds []logEnd
	// dropSecrets are secrets objects whose rows this transaction deleted;
	// the objects are deleted after commit.
	dropSecrets []string
}

type closeReq struct {
	s      *Session
	reason string
}

func newFx() *fx {
	return &fx{jobs: map[string]bool{}, runs: map[string]bool{}, workers: map[string]bool{}}
}

// tx runs fn in a transaction and applies its effects after commit. Caller
// holds e.mu.
func (e *Engine) tx(fn func(q store.Q, f *fx) error) error {
	f := newFx()
	if err := e.o.Store.Tx(func(q store.Q) error { return fn(q, f) }); err != nil {
		return err
	}
	e.apply(f)
	return nil
}

func (e *Engine) apply(f *fx) {
	for _, s := range f.sends {
		if s.s.gone {
			continue
		}
		if !s.s.conn.Send(s.m) {
			e.log.Warn("worker send failed; closing", "worker", s.s.WorkerID, "type", s.m.MessageType())
			s.s.conn.Close("send failed")
		}
	}
	for _, c := range f.closes {
		c.s.conn.Close(c.reason)
	}
	for _, le := range f.logEnds {
		e.o.Logs.Finish(le.jobID, le.attempt, le.end)
		e.uploadLog(le.runID, le.jobID, le.attempt)
	}
	for _, key := range f.dropSecrets {
		e.deleteSecretsObject(key)
	}
	e.publish(f)
	if e.notify != nil {
		e.notify()
	}
}

// SetNotify registers fn, called (with the engine lock held, so it must not
// block or call back into the engine) after every committed state change.
// The powerwarden publisher uses it as a wake-up.
func (e *Engine) SetNotify(fn func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.notify = fn
}

func (e *Engine) publish(f *fx) {
	q := e.o.Store.Q
	for _, id := range sortedKeys(f.jobs) {
		j, err := q.GetJob(id)
		if err != nil {
			continue
		}
		e.o.Hub.Publish(events.TypeJob, map[string]any{"job": view.JobSummary(j)})
		f.runs[j.RunID] = true
	}
	for _, id := range sortedKeys(f.runs) {
		r, err := q.GetRun(id)
		if err != nil {
			continue
		}
		counts, _ := q.JobCounts(id)
		e.o.Hub.Publish(events.TypeRun, map[string]any{"run": view.RunSummary(r, counts)})
	}
	for _, id := range sortedKeys(f.workers) {
		w, err := q.GetWorker(id)
		if err != nil {
			continue
		}
		e.o.Hub.Publish(events.TypeWorker, map[string]any{"worker": view.Worker(w)})
	}
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func event(q store.Q, now time.Time, runID, jobID, worker, kind string, detail any) error {
	d := ""
	switch v := detail.(type) {
	case nil:
	case string:
		d = v
	default:
		b, _ := json.Marshal(v)
		d = string(b)
	}
	return q.InsertEvent(store.Event{TS: now, RunID: runID, JobID: jobID, Worker: worker, Kind: kind, Detail: d})
}

// ---- submission ----

// statContext checks that a context_ref is stored.
func (e *Engine) statContext(ctx context.Context, ref string) error {
	if _, err := e.o.Objects.Stat(ctx, ContextKey(ref)); errors.Is(err, objects.ErrNotFound) {
		return ErrContextNotFound
	} else if err != nil {
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return nil
}

// Submit validates and stores a run. Errors: *graph.BadRequest,
// *graph.InvalidGraph, *graph.MissingSecret, ErrContextNotFound,
// *Unsatisfiable, ErrUnavailable.
func (e *Engine) Submit(ctx context.Context, by string, s graph.Submission) (store.Run, error) {
	deps, err := e.prepare(ctx, s)
	if err != nil {
		return store.Run{}, err
	}
	now := e.o.Clock.Now()
	runID := e.ids.Make(now)
	secretNames := make([]string, 0, len(s.Secrets))
	for n := range s.Secrets {
		secretNames = append(secretNames, n)
	}
	sort.Strings(secretNames)
	// The sealed values go to the object store before the run exists, and
	// only when some job names a secret (nothing would ever read them
	// otherwise). The ownership row is written with the run below.
	secretsKey := ""
	if len(s.Secrets) > 0 && namesSecret(s.Jobs) {
		secretsKey, err = e.putSecrets(ctx, runID, s.Secrets)
		if err != nil {
			return store.Run{}, err
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	run := store.Run{
		ID: runID, Workflow: s.Workflow, Params: s.Params, Status: store.RunRunning, SubmittedBy: by,
		ContextRef: s.ContextRef, SecretNames: secretNames, CreatedAt: now,
	}
	if run.Params == nil {
		run.Params = map[string]string{}
	}
	err = e.tx(func(q store.Q, f *fx) error {
		n, err := q.InsertRun(run)
		if err != nil {
			return err
		}
		run.Number = n
		if secretsKey != "" {
			if err := q.InsertSecretObject(store.SecretObject{
				RunID: runID, Object: secretsKey, InstanceID: e.o.Box.Instance(), CreatedAt: now,
			}); err != nil {
				return err
			}
		}
		if err := event(q, now, runID, "", "", "submitted", map[string]any{"workflow": s.Workflow, "number": n, "by": by}); err != nil {
			return err
		}
		for i, sj := range s.Jobs {
			j := store.Job{
				ID: e.ids.Make(now), RunID: runID, Name: sj.Name, Tags: sj.Tags, Image: sj.Image,
				Argv: sj.Argv, SpecArgv: sj.Argv, Workdir: sj.Workdir, Env: sj.Env, SpecEnv: sj.Env,
				SecretNames: sj.Secrets, Artifacts: sj.Artifacts, DependsOn: deps[i], ConcurrencyKey: sj.ConcurrencyKey,
				Status: store.JobPending, MaxAttempts: sj.MaxAttempts, TimeoutS: sj.TimeoutSeconds, CreatedAt: now,
			}
			for _, in := range sj.Inputs {
				j.Inputs = append(j.Inputs, store.Input{Job: in.Job, Path: in.Path, As: in.As})
			}
			if j.MaxAttempts == 0 {
				j.MaxAttempts = 1
			}
			if j.TimeoutS == 0 {
				j.TimeoutS = int((e.o.DefaultTimeout + time.Second - 1) / time.Second)
			}
			if len(j.DependsOn) == 0 {
				j.Status = store.JobReady
				j.ReadyAt = &now
			}
			if err := q.InsertJob(j); err != nil {
				return err
			}
			if j.Status == store.JobReady {
				if err := event(q, now, runID, j.ID, "", "ready", nil); err != nil {
					return err
				}
			}
			f.jobs[j.ID] = true
		}
		f.runs[runID] = true
		return nil
	})
	if err != nil {
		if secretsKey != "" {
			e.deleteSecretsObject(secretsKey)
		}
		return store.Run{}, err
	}
	e.log.Info("run submitted", "run", runID, "workflow", s.Workflow, "number", run.Number, "jobs", len(s.Jobs), "by", by)
	e.dispatchLocked()
	return run, nil
}

// Retry submits a new run from a stored run's graph, params and context.
// The new run reuses the original run's stored secrets (re-sealed into its
// own object) while that object exists; values in secretValues override
// them. When the stored secrets are gone (hourchd restarted, or the run's
// secret-using jobs all succeeded) every needed value must be sent, else
// *graph.MissingSecret saying so.
func (e *Engine) Retry(ctx context.Context, by, runID string, secretValues map[string]string) (store.Run, error) {
	q := e.o.Store.Q
	r, err := q.GetRun(runID)
	if errors.Is(err, store.ErrNotFound) {
		return store.Run{}, ErrNotFound
	} else if err != nil {
		return store.Run{}, err
	}
	jobs, err := q.RunJobs(runID)
	if err != nil {
		return store.Run{}, err
	}
	s := graph.Submission{Workflow: r.Workflow, Params: r.Params, ContextRef: r.ContextRef, Secrets: map[string]string{}}
	lost := false
	if len(r.SecretNames) > 0 {
		stored, err := e.loadSecrets(ctx, q, runID)
		switch {
		case errors.Is(err, errSecretsGone):
			lost = true
		case err != nil:
			return store.Run{}, err
		}
		for k, v := range stored {
			s.Secrets[k] = v
		}
	}
	for k, v := range secretValues {
		s.Secrets[k] = v
	}
	for _, j := range jobs {
		gj := graph.Job{
			Name: j.Name, Tags: j.Tags, Image: j.Image, Argv: j.SpecArgv, Workdir: j.Workdir, Env: j.SpecEnv,
			Secrets: j.SecretNames, Artifacts: j.Artifacts, DependsOn: j.DependsOn, ConcurrencyKey: j.ConcurrencyKey,
			MaxAttempts: j.MaxAttempts, TimeoutSeconds: j.TimeoutS,
		}
		for _, in := range j.Inputs {
			gj.Inputs = append(gj.Inputs, graph.Input{Job: in.Job, Path: in.Path, As: in.As})
		}
		s.Jobs = append(s.Jobs, gj)
	}
	nr, err := e.Submit(ctx, by, s)
	var ms *graph.MissingSecret
	if lost && errors.As(err, &ms) {
		return store.Run{}, &graph.MissingSecret{Msg: fmt.Sprintf(
			"the stored secrets of run %s are gone (hourchd restarted since it was submitted, or its secret-using jobs "+
				"all succeeded); re-run the workflow binary, or send the values in `secrets`: %s", runID, ms.Msg)}
	}
	if err == nil {
		e.mu.Lock()
		_ = e.tx(func(q store.Q, f *fx) error {
			return event(q, e.o.Clock.Now(), nr.ID, "", "", "retry_of", runID)
		})
		e.mu.Unlock()
	}
	return nr, err
}

// ---- job tokens ----

func newToken() (token, hash string) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	token = "hjt_" + hex.EncodeToString(b)
	return token, HashToken(token)
}

// HashToken is the stored form of a job token.
func HashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// JobForToken returns the leased job holding token, or ErrNotFound when the
// token is unknown or its job is no longer leased.
func (e *Engine) JobForToken(token string) (store.Job, error) {
	if !strings.HasPrefix(token, "hjt_") {
		return store.Job{}, ErrNotFound
	}
	j, err := e.o.Store.JobByTokenHash(HashToken(token))
	if errors.Is(err, store.ErrNotFound) {
		return store.Job{}, ErrNotFound
	}
	return j, err
}

// LeasedTo returns job id if it is leased to worker. Errors: ErrNotFound
// (no such job) or ErrNotLeased.
func (e *Engine) LeasedTo(jobID, worker string) (store.Job, error) {
	j, err := e.o.Store.GetJob(jobID)
	if errors.Is(err, store.ErrNotFound) {
		return store.Job{}, ErrNotFound
	} else if err != nil {
		return store.Job{}, err
	}
	if j.Status != store.JobLeased || j.LeaseOwner != worker {
		return j, ErrNotLeased
	}
	return j, nil
}

// ErrNotLeased: the job is not leased to the caller.
var ErrNotLeased = errors.New("not leased to the caller")

// ---- lifecycle transitions (caller holds e.mu, inside a tx) ----

// endAttempt moves a leased job out of `leased`. requeue puts it back to
// ready (the attempt counted unless keepAttempt); otherwise status is the
// terminal status.
func (e *Engine) endAttempt(q store.Q, f *fx, j *store.Job, now time.Time, kind, status, reason string, exit *int, requeue, keepAttempt bool) error {
	attempt, owner := j.Attempt, j.LeaseOwner
	j.LeaseUntil, j.AcceptBy, j.JobTokenHash, j.CancelReason = nil, nil, "", ""
	j.ExitCode, j.Error = exit, reason
	endStatus := status
	if requeue {
		j.Status = store.JobReady
		j.ReadyAt = &now
		if keepAttempt {
			j.Attempt--
		}
		endStatus = store.JobFailed
	} else {
		j.Status = status
		j.FinishedAt = &now
	}
	if err := q.UpdateJob(*j); err != nil {
		return err
	}
	if err := q.ClearWorkerJob(owner, j.ID); err != nil {
		return err
	}
	if err := event(q, now, j.RunID, j.ID, owner, kind, view.AttemptDetail{Attempt: attempt, Status: j.Status, ExitCode: exit, Error: reason}); err != nil {
		return err
	}
	f.jobs[j.ID] = true
	f.workers[owner] = true
	f.logEnds = append(f.logEnds, logEnd{runID: j.RunID, jobID: j.ID, attempt: attempt,
		end: logs.End{Offset: j.LogSize, Status: endStatus, ExitCode: exit}})
	e.log.Info("job attempt ended", "run", j.RunID, "job", j.ID, "name", j.Name, "worker", owner, "attempt", attempt,
		"kind", kind, "status", j.Status, "error", reason)
	if !requeue {
		return e.onTerminal(q, f, *j, now)
	}
	return nil
}

// finishUnleased ends a pending or ready job terminally.
func (e *Engine) finishUnleased(q store.Q, f *fx, j *store.Job, now time.Time, kind, status, reason string) error {
	j.Status, j.Error, j.FinishedAt = status, reason, &now
	if err := q.UpdateJob(*j); err != nil {
		return err
	}
	if err := event(q, now, j.RunID, j.ID, "", kind, view.AttemptDetail{Status: status, Error: reason}); err != nil {
		return err
	}
	f.jobs[j.ID] = true
	return e.onTerminal(q, f, *j, now)
}

// onTerminal promotes or cancels dependents of a job that just became
// terminal, then settles the run.
func (e *Engine) onTerminal(q store.Q, f *fx, done store.Job, now time.Time) error {
	run, err := q.GetRun(done.RunID)
	if err != nil {
		return err
	}
	reason := ReasonDependencyFailed
	if run.Status == store.RunCancelled {
		reason = ReasonCancelled
	}
	jobs, err := q.RunJobs(done.RunID)
	if err != nil {
		return err
	}
	for _, snap := range jobs {
		if snap.Status != store.JobPending || !contains(snap.DependsOn, done.Name) {
			continue
		}
		// Re-read: a recursive transition may have changed it.
		j, err := q.GetJob(snap.ID)
		if err != nil {
			return err
		}
		if j.Status != store.JobPending {
			continue
		}
		if done.Status != store.JobSucceeded {
			if err := e.finishUnleased(q, f, &j, now, "cancelled", store.JobCancelled, reason); err != nil {
				return err
			}
			continue
		}
		fresh, err := q.RunJobs(done.RunID)
		if err != nil {
			return err
		}
		byName := map[string]store.Job{}
		for _, x := range fresh {
			byName[x.Name] = x
		}
		all := true
		for _, d := range j.DependsOn {
			if byName[d].Status != store.JobSucceeded {
				all = false
				break
			}
		}
		if all {
			if err := e.makeReady(q, f, &j, byName, now); err != nil {
				return err
			}
		}
	}
	if err := e.settleRun(q, f, done.RunID, now); err != nil {
		return err
	}
	return e.releaseSecrets(q, f, done.RunID)
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// makeReady substitutes output placeholders and marks a pending job ready,
// or fails it with missing_output.
func (e *Engine) makeReady(q store.Q, f *fx, j *store.Job, byName map[string]store.Job, now time.Time) error {
	names := map[string]bool{}
	for n := range byName {
		names[n] = true
	}
	outputs := map[string]map[string]string{}
	for _, d := range j.DependsOn {
		o, err := q.Outputs(byName[d].ID)
		if err != nil {
			return err
		}
		outputs[d] = o
	}
	var missing *graph.MissingOutput
	argv := make([]string, len(j.SpecArgv))
	for i, a := range j.SpecArgv {
		v, err := graph.Substitute(a, names, outputs)
		if err != nil {
			errors.As(err, &missing)
			break
		}
		argv[i] = v
	}
	env := map[string]string{}
	if missing == nil {
		for k, a := range j.SpecEnv {
			v, err := graph.Substitute(a, names, outputs)
			if err != nil {
				errors.As(err, &missing)
				break
			}
			env[k] = v
		}
	}
	if missing != nil {
		e.log.Warn("job missing output", "run", j.RunID, "job", j.ID, "name", j.Name, "output", missing.Job+"."+missing.Key)
		return e.finishUnleased(q, f, j, now, ReasonMissingOutput, store.JobFailed, ReasonMissingOutput)
	}
	j.Argv, j.Env, j.Status, j.ReadyAt = argv, env, store.JobReady, &now
	if err := q.UpdateJob(*j); err != nil {
		return err
	}
	f.jobs[j.ID] = true
	return event(q, now, j.RunID, j.ID, "", "ready", nil)
}

// settleRun marks a running run terminal once none of its jobs can run.
func (e *Engine) settleRun(q store.Q, f *fx, runID string, now time.Time) error {
	r, err := q.GetRun(runID)
	if err != nil {
		return err
	}
	if r.Status != store.RunRunning {
		return nil
	}
	counts, err := q.JobCounts(runID)
	if err != nil {
		return err
	}
	if counts[store.JobPending]+counts[store.JobReady]+counts[store.JobLeased] > 0 {
		return nil
	}
	status := store.RunCancelled
	switch {
	case counts[store.JobFailed] > 0:
		status = store.RunFailed
	case counts[store.JobCancelled] == 0:
		status = store.RunSucceeded
	}
	if err := q.SetRunStatus(runID, status, &now); err != nil {
		return err
	}
	f.runs[runID] = true
	e.log.Info("run finished", "run", runID, "workflow", r.Workflow, "number", r.Number, "status", status)
	return event(q, now, runID, "", "", "run_finished", status)
}

// ---- dispatch ----

func subset(need, have []string) bool {
	set := map[string]bool{}
	for _, h := range have {
		set[h] = true
	}
	for _, n := range need {
		if !set[n] {
			return false
		}
	}
	return true
}

// dispatchLocked assigns ready jobs to idle sessions until no assignment is
// possible. Caller holds e.mu.
func (e *Engine) dispatchLocked() {
	for {
		var idle []*Session
		for _, s := range e.sessions {
			if !s.gone && s.job == "" {
				idle = append(idle, s)
			}
		}
		if len(idle) == 0 {
			return
		}
		sort.Slice(idle, func(i, k int) bool {
			if !idle[i].idleSince.Equal(idle[k].idleSince) {
				return idle[i].idleSince.Before(idle[k].idleSince)
			}
			return idle[i].WorkerID < idle[k].WorkerID
		})
		progress := false
		for _, s := range idle {
			assigned, changed, err := e.assign(s)
			if err != nil {
				e.log.Error("dispatch failed", "worker", s.WorkerID, "error", err)
				return
			}
			if assigned || changed {
				progress = true
			}
		}
		if !progress {
			return
		}
	}
}

// assign gives s the oldest ready job it can run, in one transaction.
// changed reports other state changes (a job failed secret_unavailable).
func (e *Engine) assign(s *Session) (assigned, changed bool, err error) {
	now := e.o.Clock.Now()
	err = e.tx(func(q store.Q, f *fx) error {
		if held, err := q.WorkerLeased(s.WorkerID); err != nil {
			return err
		} else if len(held) > 0 {
			return nil // a worker never holds two jobs
		}
		ready, err := q.ReadyJobs()
		if err != nil {
			return err
		}
		for _, j := range ready {
			if !subset(j.Tags, s.Tags) {
				continue
			}
			if j.Image != "" && !contains(s.Capabilities, proto.CapabilityDocker) {
				continue
			}
			if j.ConcurrencyKey != "" {
				held, err := q.ConcurrencyHeld(j.ConcurrencyKey)
				if err != nil {
					return err
				}
				if held {
					continue
				}
			}
			// Secrets first: a job whose secrets are gone fails; one whose
			// object cannot be fetched right now (MinIO down) stays ready
			// and is tried again on the next dispatch (the reaper runs one
			// every reaper_interval).
			vals := map[string]string{}
			if len(j.SecretNames) > 0 {
				stored, err := e.loadSecrets(context.Background(), q, j.RunID)
				var bad error
				switch {
				case errors.Is(err, errSecretsGone):
					bad = err
				case err != nil:
					e.log.Warn("job secrets not fetched; job stays ready", "run", j.RunID, "job", j.ID, "error", err)
					continue
				}
				for _, name := range j.SecretNames {
					if bad != nil {
						break
					}
					v, ok := stored[name]
					if !ok {
						bad = fmt.Errorf("%w: secret %s is not in the run's object", errSecretsGone, name)
						break
					}
					vals[name] = v
				}
				if bad != nil {
					e.log.Error("job secrets unavailable", "run", j.RunID, "job", j.ID, "error", bad)
					jj := j
					if err := e.finishUnleased(q, f, &jj, now, ReasonSecretUnavailable, store.JobFailed, ReasonSecretUnavailable); err != nil {
						return err
					}
					changed = true
					continue
				}
			}
			token, hash := newToken()
			j.Status = store.JobLeased
			j.Attempt++
			j.LeaseOwner = s.WorkerID
			until, acceptBy := now.Add(e.o.Lease), now.Add(e.o.AcceptTimeout)
			j.LeaseUntil, j.AcceptBy = &until, &acceptBy
			j.StartedAt = &now
			j.JobTokenHash = hash
			j.CancelReason, j.ExitCode, j.Error = "", nil, ""
			j.LogSeq, j.LogSize = 0, 0
			if err := q.UpdateJob(j); err != nil {
				return err
			}
			if err := q.ClearOutputs(j.ID); err != nil {
				return err
			}
			if err := q.SetWorkerState(s.WorkerID, true, j.ID, now); err != nil {
				return err
			}
			if err := event(q, now, j.RunID, j.ID, s.WorkerID, view.KindAssigned, view.AttemptDetail{Attempt: j.Attempt}); err != nil {
				return err
			}
			if err := e.o.Logs.Reset(j.ID, j.Attempt); err != nil {
				return err
			}
			msg := &proto.Assign{
				Job: proto.AssignedJob{
					ID: j.ID, RunID: j.RunID, Name: j.Name, Image: j.Image, Argv: j.Argv, Workdir: j.Workdir,
					Env: j.Env, Artifacts: j.Artifacts, TimeoutSeconds: j.TimeoutS,
				},
				Secrets: vals, Attempt: j.Attempt, JobToken: token,
			}
			if len(msg.Job.Env) == 0 {
				msg.Job.Env = nil
			}
			if len(msg.Secrets) == 0 {
				msg.Secrets = nil
			}
			for _, in := range j.Inputs {
				msg.Job.Inputs = append(msg.Job.Inputs, proto.Input{Job: in.Job, Path: in.Path, As: in.As})
			}
			f.sends = append(f.sends, send{s: s, m: msg})
			f.jobs[j.ID] = true
			f.workers[s.WorkerID] = true
			s.job, s.attempt, s.runID, s.logSeq = j.ID, j.Attempt, j.RunID, 0
			s.redactor = secrets.NewRedactor(values(vals))
			s.lastRenew = now
			if j.ReadyAt != nil {
				e.lat = append(e.lat, latSample{at: now, ms: float64(now.Sub(*j.ReadyAt)) / float64(time.Millisecond)})
			}
			assigned = true
			e.log.Info("job assigned", "run", j.RunID, "job", j.ID, "name", j.Name, "worker", s.WorkerID, "attempt", j.Attempt)
			return nil
		}
		return nil
	})
	if err != nil && assigned {
		// The transaction rolled back: undo the session change.
		s.job, s.redactor = "", nil
		assigned = false
	}
	return assigned, changed, err
}

func values(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// ---- worker sessions ----

// Hello registers a worker connection, answers welcome (with abandon or
// last_log_seq as needed), closes an older connection for the same worker
// id, and dispatches. The caller has checked hello.worker_id against the
// worker token.
func (e *Engine) Hello(conn Conn, h *proto.Hello) (*Session, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.o.Clock.Now()
	if old := e.sessions[h.WorkerID]; old != nil {
		old.gone = true
		old.conn.Close("replaced by a newer connection")
		e.log.Info("worker connection replaced", "worker", h.WorkerID)
	}
	e.connSeq++
	s := &Session{
		WorkerID: h.WorkerID, Hostname: h.Hostname, Tags: append([]string{}, h.Tags...),
		Capabilities: append([]string{}, h.Capabilities...), conn: conn, idleSince: now, lastRenew: now, connSeq: e.connSeq,
	}
	welcome := &proto.Welcome{HeartbeatSeconds: int(e.o.Heartbeat / time.Second), LeaseSeconds: int(e.o.Lease / time.Second)}
	err := e.tx(func(q store.Q, f *fx) error {
		cur := ""
		held, err := q.WorkerLeased(h.WorkerID)
		if err != nil {
			return err
		}
		for _, j := range held {
			j := j
			if j.ID == h.RunningJobID {
				// Re-attach: keep the lease; a running job counts as accepted.
				until := now.Add(e.o.Lease)
				j.LeaseUntil, j.AcceptBy = &until, nil
				if err := q.UpdateJob(j); err != nil {
					return err
				}
				if err := event(q, now, j.RunID, j.ID, h.WorkerID, "reattached", view.AttemptDetail{Attempt: j.Attempt}); err != nil {
					return err
				}
				cur = j.ID
				s.job, s.attempt, s.runID, s.logSeq = j.ID, j.Attempt, j.RunID, j.LogSeq
				s.redactor = e.redactorFor(q, j)
				welcome.LastLogSeq = j.LogSeq
				if j.CancelReason != "" {
					f.sends = append(f.sends, send{s: s, m: &proto.Cancel{JobID: j.ID, Reason: j.CancelReason}})
				}
				f.jobs[j.ID] = true
				continue
			}
			// Leased to this worker, but the worker does not run it.
			if err := e.lost(q, f, &j, now); err != nil {
				return err
			}
		}
		if h.RunningJobID != "" && cur == "" {
			welcome.Abandon = h.RunningJobID
			if err := event(q, now, "", h.RunningJobID, h.WorkerID, "abandon", nil); err != nil {
				return err
			}
		}
		if err := q.UpsertWorker(store.Worker{
			ID: h.WorkerID, Hostname: h.Hostname, Tags: h.Tags, Capabilities: h.Capabilities,
			Connected: true, CurrentJob: cur, LastSeen: now,
		}); err != nil {
			return err
		}
		f.workers[h.WorkerID] = true
		// welcome goes out before anything else on this connection.
		f.sends = append([]send{{s: s, m: welcome}}, f.sends...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	e.sessions[h.WorkerID] = s
	e.log.Info("worker connected", "worker", h.WorkerID, "hostname", h.Hostname, "tags", h.Tags,
		"capabilities", h.Capabilities, "version", h.Version, "reattached", s.job, "abandon", welcome.Abandon)
	e.dispatchLocked()
	return s, nil
}

// lost ends a job leased to a worker that reconnected without it: a missed
// accept goes back to ready without using the attempt; otherwise as a lease
// lapse.
func (e *Engine) lost(q store.Q, f *fx, j *store.Job, now time.Time) error {
	if j.AcceptBy != nil {
		return e.endAttempt(q, f, j, now, ReasonAcceptTimeout, "", ReasonAcceptTimeout, nil, true, true)
	}
	return e.lapse(q, f, j, now)
}

// lapse handles a lease that ran out (or a worker that lost its job).
func (e *Engine) lapse(q store.Q, f *fx, j *store.Job, now time.Time) error {
	if j.CancelReason != "" {
		return e.endAttempt(q, f, j, now, "cancelled", store.JobCancelled, ReasonCancelled, nil, false, false)
	}
	if j.Attempt < j.MaxAttempts {
		return e.endAttempt(q, f, j, now, "requeued", "", ReasonLeaseExpired, nil, true, false)
	}
	return e.endAttempt(q, f, j, now, ReasonLeaseExpired, store.JobFailed, ReasonLeaseExpired, nil, false, false)
}

// redactorFor rebuilds the redactor of a re-attached job. After an hourchd
// restart its secrets are gone: hourchd then cannot redact the job's log,
// and relies on the worker, which redacts the values it was assigned before
// sending anything.
func (e *Engine) redactorFor(q store.Q, j store.Job) *secrets.Redactor {
	if len(j.SecretNames) == 0 {
		return nil
	}
	stored, err := e.loadSecrets(context.Background(), q, j.RunID)
	if err != nil {
		e.log.Warn("re-attached job's secrets unavailable; relying on worker-side redaction", "run", j.RunID, "job", j.ID, "error", err)
		return nil
	}
	var vals []string
	for _, n := range j.SecretNames {
		if v, ok := stored[n]; ok {
			vals = append(vals, v)
		}
	}
	return secrets.NewRedactor(vals)
}

// Disconnect unregisters a session (unless a newer one replaced it). Its
// job stays leased until the lease lapses or the worker re-attaches.
func (e *Engine) Disconnect(s *Session) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.sessions[s.WorkerID] != s {
		return
	}
	s.gone = true
	delete(e.sessions, s.WorkerID)
	now := e.o.Clock.Now()
	_ = e.tx(func(q store.Q, f *fx) error {
		f.workers[s.WorkerID] = true
		return q.SetWorkerState(s.WorkerID, false, s.job, now)
	})
	e.log.Info("worker disconnected", "worker", s.WorkerID, "job", s.job)
}

// dropSession unregisters a broken session and closes its connection; the
// worker reconnects fresh. Caller holds e.mu, inside a tx.
func (e *Engine) dropSession(q store.Q, f *fx, s *Session, now time.Time, reason string) error {
	s.gone, s.job, s.redactor = true, "", nil
	if e.sessions[s.WorkerID] == s {
		delete(e.sessions, s.WorkerID)
	}
	f.closes = append(f.closes, closeReq{s: s, reason: reason})
	f.workers[s.WorkerID] = true
	e.log.Warn("worker connection dropped", "worker", s.WorkerID, "reason", reason)
	return q.SetWorkerState(s.WorkerID, false, "", now)
}

// CloseAll closes every worker connection (shutdown).
func (e *Engine) CloseAll(reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.sessions {
		s.conn.Close(reason)
	}
}

// ErrProtocol is a message a worker must not send.
var ErrProtocol = errors.New("protocol violation")

// renewMinInterval throttles lease and last_seen writes.
const renewMinInterval = time.Second

// Handle processes one message from a worker. An error means the worker
// broke the protocol; the caller closes the connection.
func (e *Engine) Handle(s *Session, m proto.Message) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s.gone {
		return nil
	}
	now := e.o.Clock.Now()
	var herr error
	err := e.tx(func(q store.Q, f *fx) error {
		if now.Sub(s.lastRenew) >= renewMinInterval {
			s.lastRenew = now
			if s.job != "" {
				if err := q.SetLease(s.job, s.WorkerID, now.Add(e.o.Lease)); err != nil {
					return err
				}
			}
			if err := q.TouchWorker(s.WorkerID, now); err != nil {
				return err
			}
		}
		switch m := m.(type) {
		case *proto.Heartbeat:
			return nil
		case *proto.Accept:
			return e.onAccept(q, f, s, m, now)
		case *proto.Log:
			return e.onLog(q, s, m)
		case *proto.Output:
			return e.onOutput(q, f, s, m, now)
		case *proto.Result:
			return e.onResult(q, f, s, m, now)
		default:
			herr = fmt.Errorf("%w: worker sent %s", ErrProtocol, m.MessageType())
			return nil
		}
	})
	if herr != nil {
		return herr
	}
	if err != nil {
		e.log.Error("handling worker message", "worker", s.WorkerID, "type", m.MessageType(), "error", err)
		return nil
	}
	e.dispatchLocked()
	return nil
}

func (e *Engine) currentJob(q store.Q, s *Session, jobID string) (store.Job, bool, error) {
	if jobID == "" || jobID != s.job {
		return store.Job{}, false, nil
	}
	j, err := q.GetJob(jobID)
	if errors.Is(err, store.ErrNotFound) {
		return store.Job{}, false, nil
	}
	if err != nil {
		return store.Job{}, false, err
	}
	return j, j.Status == store.JobLeased && j.LeaseOwner == s.WorkerID && j.Attempt == s.attempt, nil
}

func (e *Engine) onAccept(q store.Q, f *fx, s *Session, m *proto.Accept, now time.Time) error {
	j, ok, err := e.currentJob(q, s, m.JobID)
	if err != nil || !ok || j.AcceptBy == nil {
		return err
	}
	j.AcceptBy = nil
	if err := q.UpdateJob(j); err != nil {
		return err
	}
	f.jobs[j.ID] = true
	return event(q, now, j.RunID, j.ID, s.WorkerID, "accepted", view.AttemptDetail{Attempt: j.Attempt})
}

func (e *Engine) onLog(q store.Q, s *Session, m *proto.Log) error {
	_, ok, err := e.currentJob(q, s, m.JobID)
	if err != nil || !ok {
		return err
	}
	if m.Seq <= s.logSeq {
		return nil // already stored (resent after a reconnect)
	}
	data := s.redactor.Redact(strings.ToValidUTF8(m.Data, "�"))
	off, err := e.o.Logs.Append(m.JobID, s.attempt, m.Stream, data)
	if err != nil {
		return err
	}
	s.logSeq = m.Seq
	return q.SetLogPos(m.JobID, m.Seq, off)
}

func (e *Engine) onOutput(q store.Q, f *fx, s *Session, m *proto.Output, now time.Time) error {
	j, ok, err := e.currentJob(q, s, m.JobID)
	if err != nil || !ok {
		return err
	}
	if !graph.OutputKey.MatchString(m.Key) {
		e.log.Warn("output key rejected", "job", j.ID, "key", m.Key)
		return event(q, now, j.RunID, j.ID, s.WorkerID, "output_rejected", "bad key")
	}
	if len(m.Value) > e.o.OutputValueBytes {
		e.log.Warn("output value too large", "job", j.ID, "key", m.Key, "bytes", len(m.Value))
		return event(q, now, j.RunID, j.ID, s.WorkerID, "output_rejected", fmt.Sprintf("%s: %d bytes > %d", m.Key, len(m.Value), e.o.OutputValueBytes))
	}
	f.jobs[j.ID] = true
	return q.SetOutput(j.ID, m.Key, s.redactor.Redact(m.Value))
}

func (e *Engine) onResult(q store.Q, f *fx, s *Session, m *proto.Result, now time.Time) error {
	if m.JobID != s.job {
		return nil
	}
	j, ok, err := e.currentJob(q, s, m.JobID)
	if err != nil {
		return err
	}
	s.job, s.attempt, s.runID, s.redactor, s.idleSince = "", 0, "", nil, now
	f.workers[s.WorkerID] = true
	if !ok {
		// Timed out or reaped already; the worker is idle again.
		return q.ClearWorkerJob(s.WorkerID, m.JobID)
	}
	exit := m.ExitCode
	switch {
	case j.CancelReason != "":
		return e.endAttempt(q, f, &j, now, "cancelled", store.JobCancelled, ReasonCancelled, &exit, false, false)
	case m.ExitCode == 0 && m.Error == "":
		return e.endAttempt(q, f, &j, now, "finished", store.JobSucceeded, "", &exit, false, false)
	}
	reason := m.Error
	if reason == "" {
		reason = ReasonExitCode
	}
	if j.Attempt < j.MaxAttempts {
		return e.endAttempt(q, f, &j, now, "requeued", "", reason, &exit, true, false)
	}
	return e.endAttempt(q, f, &j, now, "finished", store.JobFailed, reason, &exit, false, false)
}

// ---- reaper ----

// Reap ends lapsed leases, missed accepts and timed-out jobs, fails ready
// jobs no connected worker could run for dispatch.unschedulable_timeout
// (no_worker), then dispatches. The reaper loop calls it every dispatch.reaper_interval.
func (e *Engine) Reap() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.o.Clock.Now()
	err := e.tx(func(q store.Q, f *fx) error {
		leased, err := q.LeasedJobs()
		if err != nil {
			return err
		}
		for _, j := range leased {
			j := j
			sess := e.sessions[j.LeaseOwner]
			holds := sess != nil && sess.job == j.ID
			switch {
			case j.AcceptBy != nil && now.After(*j.AcceptBy):
				// Assumption 17: no attempt used; the worker is broken.
				if err := e.endAttempt(q, f, &j, now, ReasonAcceptTimeout, "", ReasonAcceptTimeout, nil, true, true); err != nil {
					return err
				}
				if holds {
					if err := e.dropSession(q, f, sess, now, "accept timeout"); err != nil {
						return err
					}
				}
			case j.LeaseUntil != nil && now.After(*j.LeaseUntil):
				if err := e.lapse(q, f, &j, now); err != nil {
					return err
				}
				if holds {
					if err := e.dropSession(q, f, sess, now, "lease expired"); err != nil {
						return err
					}
				}
			case j.StartedAt != nil && now.Sub(*j.StartedAt) > time.Duration(j.TimeoutS)*time.Second:
				if holds {
					// The worker stays busy until it reports the result.
					f.sends = append(f.sends, send{s: sess, m: &proto.Cancel{JobID: j.ID, Reason: ReasonTimeout}})
				}
				if err := e.endAttempt(q, f, &j, now, ReasonTimeout, store.JobFailed, ReasonTimeout, nil, false, false); err != nil {
					return err
				}
			}
		}
		return e.reapUnschedulable(q, f, now)
	})
	if err != nil {
		e.log.Error("reaper", "error", err)
		return
	}
	e.dispatchLocked()
}

// ---- cancellation ----

// CancelRun cancels every non-terminal job of a run.
func (e *Engine) CancelRun(runID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.o.Clock.Now()
	err := e.tx(func(q store.Q, f *fx) error {
		r, err := q.GetRun(runID)
		if err != nil {
			return err
		}
		if r.Status != store.RunRunning {
			return nil
		}
		if err := q.SetRunStatus(runID, store.RunCancelled, &now); err != nil {
			return err
		}
		if err := event(q, now, runID, "", "", "run_cancelled", nil); err != nil {
			return err
		}
		f.runs[runID] = true
		jobs, err := q.RunJobs(runID)
		if err != nil {
			return err
		}
		for _, j := range jobs {
			if err := e.cancelJob(q, f, j.ID, now); err != nil {
				return err
			}
		}
		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return ErrNotFound
	}
	if err == nil {
		e.log.Info("run cancelled", "run", runID)
		e.dispatchLocked()
	}
	return err
}

// CancelJob cancels one job; its dependents are cancelled.
func (e *Engine) CancelJob(jobID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.o.Clock.Now()
	err := e.tx(func(q store.Q, f *fx) error { return e.cancelJob(q, f, jobID, now) })
	if errors.Is(err, store.ErrNotFound) {
		return ErrNotFound
	}
	if err == nil {
		e.dispatchLocked()
	}
	return err
}

func (e *Engine) cancelJob(q store.Q, f *fx, jobID string, now time.Time) error {
	j, err := q.GetJob(jobID) // fresh: an earlier cancel may have changed it
	if err != nil {
		return err
	}
	switch j.Status {
	case store.JobPending, store.JobReady:
		return e.finishUnleased(q, f, &j, now, "cancelled", store.JobCancelled, ReasonCancelled)
	case store.JobLeased:
		if j.CancelReason != "" {
			return nil
		}
		j.CancelReason = ReasonCancelled
		if err := q.UpdateJob(j); err != nil {
			return err
		}
		if err := event(q, now, j.RunID, j.ID, j.LeaseOwner, "cancel_sent", view.AttemptDetail{Attempt: j.Attempt}); err != nil {
			return err
		}
		if s := e.sessions[j.LeaseOwner]; s != nil && s.job == j.ID {
			f.sends = append(f.sends, send{s: s, m: &proto.Cancel{JobID: j.ID, Reason: ReasonCancelled}})
		}
		f.jobs[j.ID] = true
	}
	return nil
}

// ---- startup, loops, logs ----

// Start recovers after a restart and runs the reaper until ctx ends: every
// worker is marked disconnected, every leased job gets one more lease (so
// downtime does not reap it; its worker re-attaches), and live log files of
// attempts that already ended are uploaded.
func (e *Engine) Start(ctx context.Context) error {
	if err := e.Recover(); err != nil {
		return err
	}
	e.loops.Add(1)
	go func() {
		defer e.loops.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-e.o.Clock.After(e.o.ReaperInterval):
				e.Reap()
			}
		}
	}()
	return nil
}

// Recover is Start without the reaper loop (tests call it directly).
//
// Secrets do not survive a restart (the key was in the old process's
// memory): Recover deletes every secrets object of another instance and its
// row, then fails every pending or ready job that names a secret with
// secret_unavailable (cancelling its dependents), so the queue is honest at
// once. Leased jobs keep running; their workers already hold the values.
func (e *Engine) Recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.o.Clock.Now()
	e.scrubSecretObjects()
	err := e.tx(func(q store.Q, f *fx) error {
		if err := q.DisconnectAll(); err != nil {
			return err
		}
		rows, err := q.DeleteSecretObjectsNotOf(e.o.Box.Instance())
		if err != nil {
			return err
		}
		stranded, err := q.UnleasedSecretJobs()
		if err != nil {
			return err
		}
		for _, sj := range stranded {
			j, err := q.GetJob(sj.ID) // fresh: an earlier failure may have cancelled it
			if err != nil {
				return err
			}
			if j.Status != store.JobPending && j.Status != store.JobReady {
				continue
			}
			e.log.Warn("job needs secrets lost in the restart", "run", j.RunID, "job", j.ID, "name", j.Name)
			if err := e.finishUnleased(q, f, &j, now, ReasonSecretUnavailable, store.JobFailed, ReasonSecretUnavailable); err != nil {
				return err
			}
		}
		if rows > 0 || len(stranded) > 0 {
			e.log.Warn("run secrets lost in the restart", "runs", rows, "failed_jobs", len(stranded))
		}
		leased, err := q.LeasedJobs()
		if err != nil {
			return err
		}
		for _, j := range leased {
			until := now.Add(e.o.Lease)
			j.LeaseUntil = &until
			if j.AcceptBy != nil {
				j.AcceptBy = &until
			}
			if err := q.UpdateJob(j); err != nil {
				return err
			}
			if err := event(q, now, j.RunID, j.ID, j.LeaseOwner, "lease_extended", view.AttemptDetail{Attempt: j.Attempt}); err != nil {
				return err
			}
		}
		e.log.Info("recovered", "leased_jobs", len(leased))
		return nil
	})
	if err != nil {
		return err
	}
	e.retryLogUploadsLocked()
	return nil
}

// RetryLogUploads uploads live files of attempts that already ended (after a
// restart or a failed upload).
func (e *Engine) RetryLogUploads() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retryLogUploadsLocked()
}

func (e *Engine) retryLogUploadsLocked() {
	files, err := e.o.Logs.Files()
	if err != nil {
		e.log.Error("listing live logs", "error", err)
		return
	}
	for _, lf := range files {
		j, err := e.o.Store.GetJob(lf.JobID)
		if errors.Is(err, store.ErrNotFound) {
			_ = e.o.Logs.Remove(lf.JobID, lf.Attempt)
			continue
		}
		if err != nil {
			continue
		}
		if j.Status == store.JobLeased && j.Attempt == lf.Attempt {
			continue // still live
		}
		e.uploadLog(j.RunID, j.ID, lf.Attempt)
	}
}

// uploadLog uploads an attempt's live file in the background and removes it
// on success. On failure the file stays and serves reads; the next retry
// uploads it.
func (e *Engine) uploadLog(runID, jobID string, attempt int) {
	path := e.o.Logs.Path(jobID, attempt)
	e.uploads.Add(1)
	go func() {
		defer e.uploads.Done()
		f, err := os.Open(path)
		if err != nil {
			if !os.IsNotExist(err) {
				e.log.Error("open live log", "job", jobID, "attempt", attempt, "error", err)
			}
			return
		}
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		err = e.o.Objects.Put(ctx, LogKey(runID, jobID, attempt), f, fi.Size())
		cancel()
		f.Close()
		if err != nil {
			e.log.Error("log upload failed; kept locally", "job", jobID, "attempt", attempt, "error", err)
			return
		}
		e.mu.Lock()
		defer e.mu.Unlock()
		// Do not remove a file that a new assignment of the same attempt
		// (after an accept timeout) is writing.
		if j, err := e.o.Store.GetJob(jobID); err == nil && j.Status == store.JobLeased && j.Attempt == attempt {
			return
		}
		if err := e.o.Logs.Remove(jobID, attempt); err != nil {
			e.log.Error("remove live log", "job", jobID, "error", err)
		}
	}()
}

// ---- secrets objects ----

// errSecretsGone: a run's secrets object is not recorded, missing, written
// by an earlier hourchd process, or does not decrypt. The job that needs it
// fails secret_unavailable; a retry must be sent the values.
var errSecretsGone = errors.New("run secrets unavailable")

// secretsIOTimeout bounds one secrets object read, write or delete. Reads
// and deletes run under the engine lock (objects are a few hundred bytes),
// so it is short: a hung MinIO stalls dispatch at most this long per call.
const secretsIOTimeout = 10 * time.Second

func namesSecret(jobs []graph.Job) bool {
	for _, j := range jobs {
		if len(j.Secrets) > 0 {
			return true
		}
	}
	return false
}

// putSecrets seals values into runID's object. A store failure is
// ErrUnavailable.
func (e *Engine) putSecrets(ctx context.Context, runID string, values map[string]string) (string, error) {
	obj, err := e.o.Box.Seal(runID, values)
	if err != nil {
		return "", err
	}
	key := e.o.Box.Key(runID)
	ctx, cancel := context.WithTimeout(ctx, secretsIOTimeout)
	defer cancel()
	if err := e.o.Objects.Put(ctx, key, bytes.NewReader(obj), int64(len(obj))); err != nil {
		return "", fmt.Errorf("%w: writing run secrets: %v", ErrUnavailable, err)
	}
	return key, nil
}

// loadSecrets reads and opens a run's secrets object: errSecretsGone when
// there is none this instance can read, ErrUnavailable when the store
// cannot be reached.
func (e *Engine) loadSecrets(ctx context.Context, q store.Q, runID string) (map[string]string, error) {
	row, err := q.GetSecretObject(runID)
	if errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("%w: run %s holds no secrets object", errSecretsGone, runID)
	} else if err != nil {
		return nil, err
	}
	inst, rid, ok := secrets.ParseObjectKey(row.Object)
	if !ok || rid != runID {
		return nil, fmt.Errorf("%w: bad object key %q", errSecretsGone, row.Object)
	}
	ctx, cancel := context.WithTimeout(ctx, secretsIOTimeout)
	defer cancel()
	rc, _, err := e.o.Objects.Get(ctx, row.Object)
	if errors.Is(err, objects.ErrNotFound) {
		return nil, fmt.Errorf("%w: object %s is missing", errSecretsGone, row.Object)
	} else if err != nil {
		return nil, fmt.Errorf("%w: reading run secrets: %v", ErrUnavailable, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("%w: reading run secrets: %v", ErrUnavailable, err)
	}
	vals, err := e.o.Box.Open(inst, runID, data)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errSecretsGone, err)
	}
	return vals, nil
}

// releaseSecrets deletes a run's secrets once nothing can need them: every
// job that names a secret has succeeded (always true once the run
// succeeded). A failed or cancelled job keeps them, for a retry, until the
// run is pruned or hourchd restarts. Caller holds e.mu, inside a tx.
func (e *Engine) releaseSecrets(q store.Q, f *fx, runID string) error {
	row, err := q.GetSecretObject(runID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	jobs, err := q.RunJobs(runID)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if len(j.SecretNames) > 0 && j.Status != store.JobSucceeded {
			return nil
		}
	}
	if err := q.DeleteSecretObject(runID); err != nil {
		return err
	}
	f.dropSecrets = append(f.dropSecrets, row.Object)
	f.runs[runID] = true
	return event(q, e.o.Clock.Now(), runID, "", "", "secrets_deleted", nil)
}

// deleteSecretsObject deletes an object whose row is gone. A failure is
// logged; maint's sweep deletes unrecorded objects later.
func (e *Engine) deleteSecretsObject(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), secretsIOTimeout)
	defer cancel()
	if err := e.o.Objects.Delete(ctx, key); err != nil {
		e.log.Error("deleting run secrets object failed; the maintenance sweep retries", "object", key, "error", err)
		return
	}
	e.log.Info("run secrets deleted", "object", key)
}

// scrubSecretObjects deletes every secrets object written by another
// instance. A store failure is logged: those objects are unreadable anyway,
// and maint's sweep deletes them once the store is back. Caller holds e.mu.
func (e *Engine) scrubSecretObjects() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*secretsIOTimeout)
	defer cancel()
	objs, err := e.o.Objects.List(ctx, secrets.Prefix)
	if err != nil {
		e.log.Error("listing secrets objects at startup failed; maintenance deletes stale ones later", "error", err)
		return
	}
	n := 0
	for _, o := range objs {
		if inst, _, ok := secrets.ParseObjectKey(o.Key); ok && inst == e.o.Box.Instance() {
			continue
		}
		if err := e.o.Objects.Delete(ctx, o.Key); err != nil {
			e.log.Error("deleting stale secrets object failed", "object", o.Key, "error", err)
			continue
		}
		n++
	}
	e.log.Info("secrets objects of earlier instances deleted", "instance", e.o.Box.Instance(), "deleted", n)
}

// Instance is this process's instance id (secrets objects are
// secrets/<instance>/<run>).
func (e *Engine) Instance() string { return e.o.Box.Instance() }

// RunsHoldingSecrets counts runs whose secrets object exists: what a
// restart would lose.
func (e *Engine) RunsHoldingSecrets() (int, error) { return e.o.Store.CountSecretObjects() }

// HoldsSecrets reports whether a run's secrets object is recorded for this
// instance (a retry can reuse it).
func (e *Engine) HoldsSecrets(runID string) bool {
	row, err := e.o.Store.GetSecretObject(runID)
	return err == nil && row.InstanceID == e.o.Box.Instance()
}

// WaitUploads waits for in-flight log uploads.
func (e *Engine) WaitUploads() { e.uploads.Wait() }

// Wait waits for log uploads and, once Start's ctx ended, the reaper.
func (e *Engine) Wait() {
	e.loops.Wait()
	e.uploads.Wait()
}

// ---- stats ----

// Latency returns dispatch latency p50/p99 in ms over the last hour (ok
// false with no assignments).
func (e *Engine) Latency() (p50, p99 float64, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cut := e.o.Clock.Now().Add(-time.Hour)
	keep := e.lat[:0]
	for _, s := range e.lat {
		if !s.at.Before(cut) {
			keep = append(keep, s)
		}
	}
	e.lat = keep
	if len(keep) == 0 {
		return 0, 0, false
	}
	ms := make([]float64, len(keep))
	for i, s := range keep {
		ms[i] = s.ms
	}
	sort.Float64s(ms)
	pick := func(p float64) float64 {
		i := int(p*float64(len(ms))+0.999999) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(ms) {
			i = len(ms) - 1
		}
		return ms[i]
	}
	return pick(0.50), pick(0.99), true
}

// ConnectedWorkers returns the connected sessions' ids and tags.
func (e *Engine) ConnectedWorkers() map[string][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[string][]string{}
	for id, s := range e.sessions {
		out[id] = append([]string{}, s.Tags...)
	}
	return out
}

// ---- powerwarden snapshot ----

// DemandJob is a ready job as the powerwarden publisher sees it.
type DemandJob struct {
	ID, RunID, Name string
	Tags            []string
	Docker          bool // needs a worker with the docker capability
	Blocked         bool // its concurrency key is held by a leased job
}

// LiveWorker is a connected worker session.
type LiveWorker struct {
	ID, Hostname       string
	Tags, Capabilities []string
	Job                string // the job it holds, or ""
}

// DemandSnapshot is one consistent view of ready jobs and connected
// workers, taken under the engine lock (so after any dispatch).
type DemandSnapshot struct {
	Ready   []DemandJob  // oldest first
	Workers []LiveWorker // by id
}

// DemandSnapshot returns the ready jobs and the connected workers.
func (e *Engine) DemandSnapshot() (DemandSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	q := e.o.Store.Q
	ready, err := q.ReadyJobs()
	if err != nil {
		return DemandSnapshot{}, err
	}
	var out DemandSnapshot
	held := map[string]bool{}
	for _, j := range ready {
		dj := DemandJob{ID: j.ID, RunID: j.RunID, Name: j.Name, Tags: append([]string{}, j.Tags...), Docker: j.Image != ""}
		if k := j.ConcurrencyKey; k != "" {
			h, ok := held[k]
			if !ok {
				if h, err = q.ConcurrencyHeld(k); err != nil {
					return DemandSnapshot{}, err
				}
				held[k] = h
			}
			dj.Blocked = h
		}
		out.Ready = append(out.Ready, dj)
	}
	for _, s := range e.sessions {
		if s.gone {
			continue
		}
		out.Workers = append(out.Workers, LiveWorker{
			ID: s.WorkerID, Hostname: s.Hostname, Tags: append([]string{}, s.Tags...),
			Capabilities: append([]string{}, s.Capabilities...), Job: s.job,
		})
	}
	sort.Slice(out.Workers, func(i, k int) bool { return out.Workers[i].ID < out.Workers[k].ID })
	return out, nil
}

// Covers reports whether worker w could run job j: its tags are a superset
// of the job's and it has the docker capability if the job needs it (the
// dispatch rule, and powerwarden's Covers for the tags).
func Covers(w LiveWorker, j DemandJob) bool {
	return subset(j.Tags, w.Tags) && (!j.Docker || contains(w.Capabilities, proto.CapabilityDocker))
}
