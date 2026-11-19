package dispatch

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/store"
)

// ReasonNoWorker fails a ready job that had no connected worker able to run
// it for dispatch.unschedulable_timeout.
const ReasonNoWorker = "no_worker"

// Unsatisfiable is a submission with a job no worker can ever run (422
// unsatisfiable). Msg has one line per failing job.
type Unsatisfiable struct{ Msg string }

func (e *Unsatisfiable) Error() string { return e.Msg }

// Host is a powerwarden host as the submission check sees it.
type Host struct {
	Hostname        string
	Tags            []string
	Locked, Exclude bool
}

// HostLister lists powerwarden's hosts (the publisher, with a short cache).
type HostLister interface {
	Hosts(ctx context.Context) ([]Host, error)
}

// hostsTimeout bounds the one powerwarden call a rejected-looking
// submission may make.
const hostsTimeout = 3 * time.Second

// SetHosts registers powerwarden's host view for the submission check (nil:
// powerwarden is disabled). Call it before serving.
func (e *Engine) SetHosts(h HostLister) {
	e.hostsMu.Lock()
	defer e.hostsMu.Unlock()
	e.hosts = h
}

func (e *Engine) hostLister() HostLister {
	e.hostsMu.Lock()
	defer e.hostsMu.Unlock()
	return e.hosts
}

// requirement is what a job needs of a worker: tags and, for an image, the
// docker capability (the dispatch rule).
func requirement(tags []string, image string) DemandJob {
	return DemandJob{Tags: tags, Docker: image != ""}
}

// prepare runs every submission check: the graph, the secrets, the context
// (when given) and satisfiability. It stores nothing.
func (e *Engine) prepare(ctx context.Context, s graph.Submission) ([][]string, error) {
	deps, err := graph.Validate(s, graph.Limits{MaxJobs: e.o.MaxJobsPerRun})
	if err != nil {
		return nil, err
	}
	if s.ContextRef != "" {
		if err := e.statContext(ctx, s.ContextRef); err != nil {
			return nil, err
		}
	}
	if err := e.checkSatisfiable(ctx, s.Jobs); err != nil {
		return nil, err
	}
	return deps, nil
}

// Validate runs every check Submit runs (POST /v1/runs?validate_only=true)
// without storing anything. Errors are Submit's.
func (e *Engine) Validate(ctx context.Context, s graph.Submission) error {
	_, err := e.prepare(ctx, s)
	return err
}

// checkSatisfiable fails with *Unsatisfiable when some job's requirement is
// covered neither by a worker hourchd has ever seen (connected or not) nor,
// for a job without an image, by an unlocked, non-excluded powerwarden host
// with non-empty tags covering the job's. A never-seen host's capabilities
// are unknown, so hosts never satisfy an image job. Powerwarden is asked
// only when the workers leave some job uncovered.
func (e *Engine) checkSatisfiable(ctx context.Context, jobs []graph.Job) error {
	workers, err := e.o.Store.ListWorkers(time.Time{})
	if err != nil {
		return err
	}
	live := make([]LiveWorker, 0, len(workers))
	for _, w := range workers {
		live = append(live, LiveWorker{ID: w.ID, Tags: w.Tags, Capabilities: w.Capabilities})
	}
	var bad []graph.Job
	for _, j := range jobs {
		need := requirement(j.Tags, j.Image)
		if !slices.ContainsFunc(live, func(w LiveWorker) bool { return Covers(w, need) }) {
			bad = append(bad, j)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	var hosts []Host
	var hostErr error
	lister := e.hostLister()
	if lister != nil {
		hctx, cancel := context.WithTimeout(ctx, hostsTimeout)
		hosts, hostErr = lister.Hosts(hctx)
		cancel()
	}
	var eligible []Host
	for _, h := range hosts {
		if !h.Locked && !h.Exclude && len(h.Tags) > 0 {
			eligible = append(eligible, h)
		}
	}
	var lines []string
	for _, j := range bad {
		if j.Image == "" && hostErr == nil && slices.ContainsFunc(eligible, func(h Host) bool { return subset(j.Tags, h.Tags) }) {
			continue
		}
		lines = append(lines, describeUnsatisfiable(j, live, eligible, lister == nil, hostErr))
	}
	if len(lines) == 0 {
		return nil
	}
	e.log.Warn("submission unsatisfiable", "jobs", len(lines), "detail", strings.Join(lines, " | "))
	return &Unsatisfiable{Msg: strings.Join(lines, "\n")}
}

func fmtTags(tags []string) string {
	t := slices.Clone(tags)
	sort.Strings(t)
	return "[" + strings.Join(t, " ") + "]"
}

// offers lists the distinct tag/capability sets, sorted.
func offers(sets []string) string {
	sort.Strings(sets)
	return strings.Join(slices.Compact(sets), ", ")
}

func describeUnsatisfiable(j graph.Job, workers []LiveWorker, hosts []Host, pwDisabled bool, hostErr error) string {
	var b strings.Builder
	fmt.Fprintf(&b, "job %q needs ", j.Name)
	if len(j.Tags) == 0 {
		b.WriteString("any worker")
	} else {
		b.WriteString("tags " + fmtTags(j.Tags))
	}
	if j.Image != "" {
		b.WriteString(" + " + proto.CapabilityDocker)
	}
	b.WriteString("; ")
	if len(workers) == 0 {
		b.WriteString("no worker has ever connected")
	} else {
		var sets []string
		for _, w := range workers {
			s := fmtTags(w.Tags)
			if caps := slices.Clone(w.Capabilities); len(caps) > 0 {
				sort.Strings(caps)
				s += " + " + strings.Join(caps, " + ")
			}
			sets = append(sets, s)
		}
		b.WriteString("known workers offer " + offers(sets))
	}
	b.WriteString("; ")
	switch {
	case pwDisabled:
		b.WriteString("powered-off hosts could not be checked (powerwarden is disabled)")
	case hostErr != nil:
		fmt.Fprintf(&b, "powered-off hosts could not be checked (powerwarden unreachable: %v)", hostErr)
	case len(hosts) == 0:
		b.WriteString("powerwarden has no unlocked, non-excluded host with tags")
	default:
		var sets []string
		for _, h := range hosts {
			sets = append(sets, fmtTags(h.Tags))
		}
		b.WriteString("powerwarden hosts offer " + offers(sets))
		if j.Image != "" {
			b.WriteString(" (no " + proto.CapabilityDocker + " known)")
		}
	}
	return b.String()
}

// ---- runtime backstop ----

// coverage is what the reaper last knew about a ready job's coverage.
type coverage struct {
	since     time.Time // last time a connected worker covered it, or when it became ready
	uncovered bool      // the last check found no covering connected worker
}

// coveredLocked reports whether a connected session (idle or busy) could
// run j. Caller holds e.mu.
func (e *Engine) coveredLocked(j store.Job) bool {
	need := requirement(j.Tags, j.Image)
	for _, s := range e.sessions {
		if !s.gone && Covers(LiveWorker{Tags: s.Tags, Capabilities: s.Capabilities}, need) {
			return true
		}
	}
	return false
}

// reapUnschedulable tracks ready jobs that no connected worker covers, logs
// a warning when one first becomes uncovered, and fails with no_worker those
// uncovered for longer than dispatch.unschedulable_timeout (0: never).
// Coverage is measured from when the job became ready, when it was last
// seen covered, or when this hourchd started (workers reconnect after a
// restart), whichever is latest. Caller holds e.mu, inside a tx.
func (e *Engine) reapUnschedulable(q store.Q, f *fx, now time.Time) error {
	ready, err := q.ReadyJobs()
	if err != nil {
		return err
	}
	present := make(map[string]bool, len(ready))
	for _, j := range ready {
		present[j.ID] = true
		if e.coveredLocked(j) {
			e.coverage[j.ID] = coverage{since: now}
			continue
		}
		c, ok := e.coverage[j.ID]
		if !ok {
			c.since = e.started
		}
		if j.ReadyAt != nil && j.ReadyAt.After(c.since) {
			c.since = *j.ReadyAt
		}
		if !c.uncovered {
			e.log.Warn("ready job has no connected worker that can run it", "run", j.RunID, "job", j.ID, "name", j.Name,
				"tags", j.Tags, "docker", j.Image != "", "timeout", e.o.UnschedulableTimeout)
			c.uncovered = true
		}
		e.coverage[j.ID] = c
		if e.o.UnschedulableTimeout > 0 && now.Sub(c.since) > e.o.UnschedulableTimeout {
			e.log.Error("job failed: no worker", "run", j.RunID, "job", j.ID, "name", j.Name, "uncovered_since", c.since)
			jj := j
			if err := e.finishUnleased(q, f, &jj, now, ReasonNoWorker, store.JobFailed, ReasonNoWorker); err != nil {
				return err
			}
			delete(e.coverage, j.ID)
		}
	}
	for id := range e.coverage {
		if !present[id] {
			delete(e.coverage, id)
		}
	}
	return nil
}

// UnschedulableReady counts ready jobs that no connected worker (idle or
// busy) can run now.
func (e *Engine) UnschedulableReady() (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ready, err := e.o.Store.Q.ReadyJobs()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, j := range ready {
		if !e.coveredLocked(j) {
			n++
		}
	}
	return n, nil
}
