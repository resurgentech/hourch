// Package pwpub publishes hourchd's demand and worker liveness to
// powerwarden (docs/DESIGN-hourchd.md "Powerwarden", DESIGN-powerwarden.md).
//
// The publisher never blocks dispatch. The engine calls Kick after every
// committed change; Kick only sets a flag. One goroutine takes a snapshot of
// the ready jobs and connected workers, diffs it against what powerwarden was
// last told, and sends the difference (PUT/DELETE demands, PUT/DELETE
// workers). Changes that pile up while a request is in flight are coalesced
// into the next pass. A failed request is retried with backoff; what was not
// confirmed is sent again. Every refresh interval it re-PUTs every open
// demand and live worker and deletes source=hourchd demands it no longer has
// (stale rows after an hourchd or powerwarden restart).
package pwpub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	pw "github.com/resurgentech/powerwarden/powerwarden/api"

	"github.com/resurgentech/hourch/daemon/internal/dispatch"
)

// Status is the powerwarden connection state shown by healthz, stats and
// the UI.
type Status string

const (
	Connected   Status = "connected"
	Unreachable Status = "unreachable"
	Disabled    Status = "disabled"
)

// Lease lengths sent to powerwarden.
//
// DemandLeaseSeconds is powerwarden's demand_lease_default and maximum: a
// demand outlives an hourchd outage of up to an hour (two failed 15m boots),
// and stale demands are removed by the refresh's reconcile, not by expiry.
const DemandLeaseSeconds = 3600

// WorkerLeaseSeconds returns the worker liveness lease for a refresh
// interval: six refreshes (90s at the default 15s, powerwarden's
// worker_lease_default), at least 15s and at most 300s (powerwarden's
// worker_lease_min/max). Five refreshes can be missed before a live worker
// looks dead.
func WorkerLeaseSeconds(refresh time.Duration) int {
	s := int(6 * refresh / time.Second)
	return max(15, min(300, s))
}

// retryMax caps the delay between retries after a failure.
const retryMax = 5 * time.Second

// DemandID is the powerwarden demand id of a job.
func DemandID(jobID string) string { return "hourchd:" + jobID }

// Source is the engine.
type Source interface {
	DemandSnapshot() (dispatch.DemandSnapshot, error)
}

// Options configure a Publisher.
type Options struct {
	URL     string // powerwarden base URL
	Token   string // bearer token
	Source  Source
	Refresh time.Duration
	Log     *slog.Logger
	// HTTPClient defaults to a client with a 10s timeout.
	HTTPClient *http.Client
	// RetryMin is the first retry delay after a failure (default 500ms); it
	// doubles up to Refresh.
	RetryMin time.Duration
	// HostsTTL is how long Hosts reuses powerwarden's host list (and a
	// failure to get it). Default 15s.
	HostsTTL time.Duration
}

type demandState struct {
	tags []string
	ref  string
}

type workerState struct {
	hostname string
	tags     []string
	status   pw.JobStatus
	demandID string
}

// Publisher is the powerwarden demand and liveness publisher.
type Publisher struct {
	o           Options
	c           *pw.ClientWithResponses
	log         *slog.Logger
	workerLease int

	kick chan struct{}

	mu        sync.Mutex
	status    Status
	lastError string
	lastOK    time.Time

	// Hosts cache (GET /v1/hosts); hmu also serialises the calls.
	hmu   sync.Mutex
	hosts hostsCache

	// Owned by the Run goroutine: what powerwarden was last told.
	demands map[string]demandState
	workers map[string]workerState
}

// ReadToken reads a bearer token file (trimmed; empty is an error).
func ReadToken(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("powerwarden token: %w", err)
	}
	tok := strings.TrimSpace(string(b))
	if tok == "" {
		return "", fmt.Errorf("powerwarden token: %s is empty", path)
	}
	return tok, nil
}

// New returns a publisher; call Run to start it.
func New(o Options) (*Publisher, error) {
	if o.URL == "" || o.Token == "" || o.Source == nil {
		return nil, errors.New("pwpub: url, token and source are required")
	}
	if o.Refresh <= 0 {
		o.Refresh = 15 * time.Second
	}
	if o.HostsTTL <= 0 {
		o.HostsTTL = 15 * time.Second
	}
	if o.RetryMin <= 0 {
		o.RetryMin = 500 * time.Millisecond
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.HTTPClient == nil {
		o.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	tok := o.Token
	c, err := pw.NewClientWithResponses(strings.TrimRight(o.URL, "/"),
		pw.WithHTTPClient(o.HTTPClient),
		pw.WithRequestEditorFn(func(_ context.Context, r *http.Request) error {
			r.Header.Set("Authorization", "Bearer "+tok)
			return nil
		}))
	if err != nil {
		return nil, err
	}
	return &Publisher{
		o: o, c: c, log: o.Log, workerLease: WorkerLeaseSeconds(o.Refresh),
		kick: make(chan struct{}, 1), status: Unreachable,
		demands: map[string]demandState{}, workers: map[string]workerState{},
	}, nil
}

// Kick asks for a pass soon. It never blocks (the engine calls it with its
// lock held).
func (p *Publisher) Kick() {
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

// State returns the connection status, the last error (if the last pass
// failed) and when powerwarden last accepted a full pass.
func (p *Publisher) State() (Status, string, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.status, p.lastError, p.lastOK
}

// Status returns the connection status.
func (p *Publisher) Status() Status {
	s, _, _ := p.State()
	return s
}

func (p *Publisher) setStatus(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		if p.status != Unreachable || p.lastError != err.Error() {
			p.log.Warn("powerwarden publish failed", "error", err)
		}
		p.status, p.lastError = Unreachable, err.Error()
		return
	}
	if p.status != Connected {
		p.log.Info("powerwarden connected", "url", p.o.URL)
	}
	p.status, p.lastError, p.lastOK = Connected, "", time.Now()
}

// Run publishes until ctx ends. The first pass is a full refresh.
func (p *Publisher) Run(ctx context.Context) {
	tick := time.NewTicker(p.o.Refresh)
	defer tick.Stop()
	full := true
	fails := 0
	var retry <-chan time.Time
	for {
		err := p.Sync(ctx, full)
		if ctx.Err() != nil {
			return
		}
		p.setStatus(err)
		if err != nil {
			fails++
			d := min(p.o.RetryMin<<min(fails-1, 10), p.o.Refresh, retryMax)
			retry = time.After(d)
		} else {
			fails, full, retry = 0, false, nil
		}
		kick := p.kick
		if retry != nil {
			// While failing, changes wait for the retry (the pass sends
			// the latest snapshot anyway); a down powerwarden is not hit
			// on every job transition.
			kick = nil
		}
		select {
		case <-ctx.Done():
			return
		case <-kick:
		case <-tick.C:
			full = true
		case <-retry:
		}
	}
}

// Wanted computes the demands and worker records powerwarden should hold
// for a snapshot (exported for tests).
//
// A ready job is a demand when no connected idle worker covers it (tags and
// docker capability) and its concurrency key is free: powering on a host
// cannot help a job that waits for a key. Connected workers are idle, or
// busy with demand_id hourchd:<job id> of the job they hold.
func Wanted(s dispatch.DemandSnapshot) (map[string]demandState, map[string]workerState) {
	demands := map[string]demandState{}
	for _, j := range s.Ready {
		if j.Blocked {
			continue
		}
		covered := false
		for _, w := range s.Workers {
			if w.Job == "" && dispatch.Covers(w, j) {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		tags := slices.Clone(j.Tags)
		sort.Strings(tags)
		demands[DemandID(j.ID)] = demandState{tags: tags, ref: fmt.Sprintf("run %s job %s", j.RunID, j.Name)}
	}
	workers := map[string]workerState{}
	for _, w := range s.Workers {
		tags := slices.Clone(w.Tags)
		sort.Strings(tags)
		ws := workerState{hostname: w.Hostname, tags: tags, status: pw.JobStatusIdle}
		if w.Job != "" {
			ws.status, ws.demandID = pw.JobStatusBusy, DemandID(w.Job)
		}
		workers[w.ID] = ws
	}
	return demands, workers
}

func (d demandState) equal(o demandState) bool {
	return d.ref == o.ref && slices.Equal(d.tags, o.tags)
}

func (w workerState) equal(o workerState) bool {
	return w.hostname == o.hostname && w.status == o.status && w.demandID == o.demandID && slices.Equal(w.tags, o.tags)
}

// errs collects request failures of one pass.
type errs struct {
	n     int
	first error
}

func (e *errs) add(err error) {
	if err == nil {
		return
	}
	e.n++
	if e.first == nil {
		e.first = err
	}
}

func (e *errs) err() error {
	if e.first == nil {
		return nil
	}
	if e.n == 1 {
		return e.first
	}
	return fmt.Errorf("%w (and %d more failures)", e.first, e.n-1)
}

// check maps a response to an error. A 400 (powerwarden rejected the row)
// is permanent: it is logged and reported as permanent so the row is not
// resent in a tight loop.
func check(op string, resp any, err error) (permanent bool, _ error) {
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}
	status, body := meta(resp)
	switch {
	case status >= 200 && status < 300:
		return false, nil
	case status == http.StatusBadRequest:
		return true, fmt.Errorf("%s: 400 %s", op, strings.TrimSpace(string(body)))
	default:
		return false, fmt.Errorf("%s: %d %s", op, status, strings.TrimSpace(string(body)))
	}
}

// Sync runs one pass: send what changed since the last confirmed pass
// (everything when full) and, when full, delete stale source=hourchd
// demands. It returns the first failure.
func (p *Publisher) Sync(ctx context.Context, full bool) error {
	snap, err := p.o.Source.DemandSnapshot()
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	wantD, wantW := Wanted(snap)
	var fail errs

	// Demands that go away first (assigned, cancelled, finished), so a
	// worker that just took a job is not counted against a stale demand.
	for _, id := range sortedKeys(p.demands) {
		if _, ok := wantD[id]; ok {
			continue
		}
		resp, err := p.c.DeleteDemandWithResponse(ctx, id)
		if _, err := check("delete demand "+id, resp, err); err != nil {
			fail.add(err)
			continue
		}
		p.log.Debug("powerwarden demand deleted", "demand", id)
		delete(p.demands, id)
	}
	for _, id := range sortedKeys(wantD) {
		d := wantD[id]
		if old, ok := p.demands[id]; ok && !full && old.equal(d) {
			continue
		}
		lease, ref := DemandLeaseSeconds, d.ref
		resp, err := p.c.PutDemandWithResponse(ctx, id, pw.DemandPut{
			Tags: append(pw.Tags{}, d.tags...), Source: pw.DemandPutSourceHourchd, LeaseSeconds: &lease, ExternalRef: &ref,
		})
		perm, err := check("put demand "+id, resp, err)
		if err != nil && !perm {
			fail.add(err)
			continue
		}
		if err != nil {
			p.log.Error("powerwarden rejected demand", "demand", id, "error", err)
		} else if _, ok := p.demands[id]; !ok {
			p.log.Info("powerwarden demand published", "demand", id, "tags", d.tags)
		}
		p.demands[id] = d
	}
	if full {
		fail.add(p.deleteStale(ctx, wantD))
	}

	for _, id := range sortedKeys(wantW) {
		w := wantW[id]
		if old, ok := p.workers[id]; ok && !full && old.equal(w) {
			continue
		}
		lease := p.workerLease
		body := pw.WorkerPut{Hostname: w.hostname, Tags: append(pw.Tags{}, w.tags...), JobStatus: w.status, LeaseSeconds: &lease}
		if w.demandID != "" {
			did := w.demandID
			body.DemandId = &did
		}
		resp, err := p.c.PutWorkerWithResponse(ctx, id, body)
		perm, err := check("put worker "+id, resp, err)
		if err != nil && !perm {
			fail.add(err)
			continue
		}
		if err != nil {
			p.log.Error("powerwarden rejected worker", "worker", id, "error", err)
		}
		p.workers[id] = w
	}
	// Disconnected workers: an idle one left cleanly, so its row is deleted.
	// One that held a job is left to expire: powerwarden keeps its host
	// busy-held for busy_grace, which covers a worker that is restarting
	// or lost its network while the job may still run.
	for _, id := range sortedKeys(p.workers) {
		if _, ok := wantW[id]; ok {
			continue
		}
		if p.workers[id].status == pw.JobStatusBusy {
			delete(p.workers, id)
			continue
		}
		resp, err := p.c.DeleteWorkerWithResponse(ctx, id)
		if _, err := check("delete worker "+id, resp, err); err != nil {
			fail.add(err)
			continue
		}
		delete(p.workers, id)
	}
	return fail.err()
}

// deleteStale deletes source=hourchd demands powerwarden holds that hourchd
// does not want (left by a crash, a restart of either side, or a DELETE that
// was lost).
func (p *Publisher) deleteStale(ctx context.Context, want map[string]demandState) error {
	resp, err := p.c.ListDemandsWithResponse(ctx, &pw.ListDemandsParams{})
	if _, err := check("list demands", resp, err); err != nil {
		return err
	}
	if resp.JSON200 == nil {
		return errors.New("list demands: no body")
	}
	var fail errs
	for _, d := range *resp.JSON200 {
		if d.Source != pw.DemandSourceHourchd {
			continue
		}
		if _, ok := want[d.Id]; ok {
			continue
		}
		dr, err := p.c.DeleteDemandWithResponse(ctx, d.Id)
		if _, err := check("delete stale demand "+d.Id, dr, err); err != nil {
			fail.add(err)
			continue
		}
		p.log.Info("powerwarden stale demand deleted", "demand", d.Id)
		delete(p.demands, d.Id)
	}
	return fail.err()
}

// meta returns a generated response's status code and body (0, nil when
// there is no response).
func meta(r any) (int, []byte) {
	code := func(h *http.Response) int {
		if h == nil {
			return 0
		}
		return h.StatusCode
	}
	switch v := r.(type) {
	case *pw.PutDemandResponse:
		if v != nil {
			return code(v.HTTPResponse), v.Body
		}
	case *pw.DeleteDemandResponse:
		if v != nil {
			return code(v.HTTPResponse), v.Body
		}
	case *pw.PutWorkerResponse:
		if v != nil {
			return code(v.HTTPResponse), v.Body
		}
	case *pw.DeleteWorkerResponse:
		if v != nil {
			return code(v.HTTPResponse), v.Body
		}
	case *pw.ListDemandsResponse:
		if v != nil {
			return code(v.HTTPResponse), v.Body
		}
	case *pw.ListHostsResponse:
		if v != nil {
			return code(v.HTTPResponse), v.Body
		}
	}
	return 0, nil
}

type hostsCache struct {
	at    time.Time
	hosts []dispatch.Host
	err   error
}

// Hosts returns powerwarden's hosts (GET /v1/hosts) for hourchd's
// submission check. The answer, or the failure, is reused for HostsTTL so a
// burst of submissions makes at most one call; concurrent callers wait for
// the one in flight. A call cut short by the caller's ctx is not cached.
func (p *Publisher) Hosts(ctx context.Context) ([]dispatch.Host, error) {
	p.hmu.Lock()
	defer p.hmu.Unlock()
	if !p.hosts.at.IsZero() && time.Since(p.hosts.at) < p.o.HostsTTL {
		return slices.Clone(p.hosts.hosts), p.hosts.err
	}
	hosts, err := p.listHosts(ctx)
	if err != nil && ctx.Err() != nil {
		return nil, err
	}
	if err != nil {
		p.log.Warn("powerwarden hosts unavailable for the submission check", "error", err)
	}
	p.hosts = hostsCache{at: time.Now(), hosts: hosts, err: err}
	return slices.Clone(hosts), err
}

func (p *Publisher) listHosts(ctx context.Context) ([]dispatch.Host, error) {
	resp, err := p.c.ListHostsWithResponse(ctx, nil)
	if _, err := check("list hosts", resp, err); err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, errors.New("list hosts: no body")
	}
	out := make([]dispatch.Host, 0, len(*resp.JSON200))
	for _, h := range *resp.JSON200 {
		out = append(out, dispatch.Host{Hostname: h.Hostname, Tags: slices.Clone(h.Tags), Locked: h.Locked, Exclude: h.Exclude})
	}
	return out, nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
