package dispatch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/store"
)

// fakeHosts is powerwarden's host view for the submission check.
type fakeHosts struct {
	mu    sync.Mutex
	hosts []Host
	err   error
	calls int
}

func (f *fakeHosts) Hosts(context.Context) ([]Host, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.hosts, f.err
}

func image(i string) func(*graph.Job) { return func(j *graph.Job) { j.Image = i } }

// trySubmit submits and returns the error (nil on success).
func (e *env) trySubmit(jobs ...graph.Job) (store.Run, error) {
	return e.eng.Submit(context.Background(), "tester", graph.Submission{Workflow: "wf", Jobs: jobs})
}

func unsatisfiable(t *testing.T, err error, want ...string) {
	t.Helper()
	var us *Unsatisfiable
	if !errors.As(err, &us) {
		t.Fatalf("err = %v, want *Unsatisfiable", err)
	}
	for _, w := range want {
		if !strings.Contains(us.Msg, w) {
			t.Errorf("message lacks %q:\n%s", w, us.Msg)
		}
	}
}

func (e *env) runCount() int {
	e.t.Helper()
	n, err := e.st.CountRuns(store.RunRunning)
	if err != nil {
		e.t.Fatal(err)
	}
	return n
}

// seenWorker records a worker that connected once and left.
func (e *env) seenWorker(id string, tags, caps []string) {
	e.t.Helper()
	s, _ := e.connect(id, tags, caps, "")
	e.eng.Disconnect(s)
}

func TestSubmitUnsatisfiableNoWorkersEverSeen(t *testing.T) {
	e := newEnv(t)
	forgetSeen(t, e.st)
	_, err := e.trySubmit(job("settings"), job("build:hello", tags("kernel"), image("img")))
	unsatisfiable(t, err,
		`job "settings" needs any worker; no worker has ever connected; powered-off hosts could not be checked (powerwarden is disabled)`,
		`job "build:hello" needs tags [kernel] + docker; no worker has ever connected`)
	if n := e.runCount(); n != 0 {
		t.Fatalf("%d runs stored", n)
	}
	if strings.Count(err.Error(), "\n") != 1 {
		t.Fatalf("want one line per job:\n%s", err)
	}
}

func TestSubmitAcceptedWhenDisconnectedWorkerCovers(t *testing.T) {
	e := newEnv(t)
	forgetSeen(t, e.st)
	e.seenWorker("k1", []string{"kernel", "big"}, []string{proto.CapabilityDocker})
	if _, err := e.trySubmit(job("a", tags("kernel")), job("b", tags("kernel", "big"), image("img")), job("c")); err != nil {
		t.Fatal(err)
	}
	_, err := e.trySubmit(job("d", tags("kernel", "gpu")))
	unsatisfiable(t, err, `job "d" needs tags [gpu kernel]; known workers offer [big kernel] + docker`)
}

func TestImageJobNeedsDockerWorker(t *testing.T) {
	e := newEnv(t)
	forgetSeen(t, e.st)
	h := &fakeHosts{hosts: []Host{{Hostname: "veritech01", Tags: []string{"kernel"}}}}
	e.eng.SetHosts(h)
	e.seenWorker("t1", []string{"tiny"}, nil)
	e.seenWorker("k1", []string{"kernel"}, nil)
	_, err := e.trySubmit(job("build:hello", tags("kernel"), image("img")))
	unsatisfiable(t, err, `job "build:hello" needs tags [kernel] + docker; known workers offer [kernel], [tiny]; powerwarden hosts offer [kernel] (no docker known)`)
	// The same job without an image runs on k1.
	if _, err := e.trySubmit(job("build", tags("kernel"))); err != nil {
		t.Fatal(err)
	}
}

func TestPowerwardenHostSatisfiesNonImageJob(t *testing.T) {
	e := newEnv(t)
	forgetSeen(t, e.st)
	h := &fakeHosts{hosts: []Host{{Hostname: "veritech01", Tags: []string{"kernel", "x86"}}}}
	e.eng.SetHosts(h)
	e.seenWorker("t1", []string{"tiny"}, []string{proto.CapabilityDocker})
	if _, err := e.trySubmit(job("tiny", tags("tiny"))); err != nil {
		t.Fatal(err)
	}
	if h.calls != 0 {
		t.Fatalf("powerwarden asked %d times although workers covered every job", h.calls)
	}
	if _, err := e.trySubmit(job("build", tags("kernel")), job("t", tags("tiny"), image("img"))); err != nil {
		t.Fatal(err)
	}
	if h.calls != 1 {
		t.Fatalf("powerwarden asked %d times, want 1", h.calls)
	}
}

func TestLockedExcludedAndUntaggedHostsDoNotCount(t *testing.T) {
	e := newEnv(t)
	forgetSeen(t, e.st)
	e.eng.SetHosts(&fakeHosts{hosts: []Host{
		{Hostname: "v1", Tags: []string{"kernel"}, Locked: true},
		{Hostname: "v2", Tags: []string{"kernel"}, Exclude: true},
		{Hostname: "v3"},
	}})
	_, err := e.trySubmit(job("build", tags("kernel")))
	unsatisfiable(t, err, `job "build" needs tags [kernel]; no worker has ever connected; powerwarden has no unlocked, non-excluded host with tags`)
	// An untagged job is not satisfied by an untagged host either.
	_, err = e.trySubmit(job("any"))
	unsatisfiable(t, err, `job "any" needs any worker`)
}

func TestPowerwardenUnreachableChecksWorkersOnly(t *testing.T) {
	e := newEnv(t)
	forgetSeen(t, e.st)
	e.eng.SetHosts(&fakeHosts{err: errors.New("dial tcp: connection refused")})
	e.seenWorker("t1", []string{"tiny"}, nil)
	if _, err := e.trySubmit(job("t", tags("tiny"))); err != nil {
		t.Fatal(err)
	}
	_, err := e.trySubmit(job("build", tags("kernel")))
	unsatisfiable(t, err, `job "build" needs tags [kernel]; known workers offer [tiny]; powered-off hosts could not be checked (powerwarden unreachable: dial tcp: connection refused)`)
}

func TestRetryIsChecked(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("build", tags("kernel")))
	if err := e.eng.CancelRun(r.ID); err != nil {
		t.Fatal(err)
	}
	forgetSeen(t, e.st)
	_, err := e.eng.Retry(context.Background(), "tester", r.ID, nil)
	unsatisfiable(t, err, `job "build" needs tags [kernel]`)
}

func TestValidateStoresNothing(t *testing.T) {
	e := newEnv(t)
	s := graph.Submission{Workflow: "wf", Secrets: map[string]string{"S": "v"},
		Jobs: []graph.Job{job("a", secretsOf("S"))}}
	if err := e.eng.Validate(context.Background(), s); err != nil {
		t.Fatal(err)
	}
	if n := e.runCount(); n != 0 {
		t.Fatalf("%d runs", n)
	}
	if keys := e.obj.Keys(); len(keys) != 0 {
		t.Fatalf("objects stored: %v", keys)
	}
	var cnt int
	if err := e.st.DB().QueryRow(`SELECT COUNT(*) FROM events`).Scan(&cnt); err != nil || cnt != 0 {
		t.Fatalf("events = %d (%v)", cnt, err)
	}
	// Each failure is Submit's.
	bad := []graph.Submission{
		{Workflow: "wf", Jobs: []graph.Job{job("a", after("nope"))}},
		{Workflow: "wf", Jobs: []graph.Job{job("a", secretsOf("S"))}},
		{Workflow: "wf", ContextRef: "sha256:" + strings.Repeat("a", 64), Jobs: []graph.Job{job("a")}},
		{Workflow: "wf", Jobs: []graph.Job{job("a", tags("gpu-cluster"))}},
	}
	for i, b := range bad {
		verr := e.eng.Validate(context.Background(), b)
		_, serr := e.eng.Submit(context.Background(), "t", b)
		if verr == nil || serr == nil || verr.Error() != serr.Error() {
			t.Errorf("%d: validate %v, submit %v", i, verr, serr)
		}
	}
}

// ---- runtime backstop ----

// tick advances the clock in one-minute steps, keeping the given sessions'
// leases alive, and reaps after each step.
func (e *env) tick(d time.Duration, keep ...*Session) {
	e.t.Helper()
	for step := time.Duration(0); step < d; step += time.Minute {
		e.clk.Advance(time.Minute)
		for _, s := range keep {
			e.send(s, &proto.Heartbeat{JobID: s.job})
		}
		e.eng.Reap()
	}
}

func TestNoWorkerBackstopFailsUncoveredJob(t *testing.T) {
	e := newEnv(t)
	e.eng.o.UnschedulableTimeout = 30 * time.Minute
	r := e.submit(nil, job("build", tags("kernel")), job("record", after("build")))
	e.connect("t1", []string{"tiny"}, nil, "") // connected, but not covering
	e.eng.Reap()
	if n, _ := e.eng.UnschedulableReady(); n != 1 {
		t.Fatalf("unschedulable_ready = %d", n)
	}
	e.tick(30 * time.Minute)
	if st := e.job(r.ID, "build").Status; st != store.JobReady {
		t.Fatalf("failed early: %s", st)
	}
	e.tick(time.Minute)
	b, rec := e.job(r.ID, "build"), e.job(r.ID, "record")
	if b.Status != store.JobFailed || b.Error != ReasonNoWorker {
		t.Fatalf("build = %s %q", b.Status, b.Error)
	}
	if rec.Status != store.JobCancelled || rec.Error != ReasonDependencyFailed {
		t.Fatalf("record = %s %q", rec.Status, rec.Error)
	}
	if e.run(r.ID).Status != store.RunFailed {
		t.Fatal("run not failed")
	}
	if n := strings.Count(e.out.String(), "ready job has no connected worker that can run it"); n != 1 {
		t.Fatalf("uncovered warning logged %d times, want once", n)
	}
}

func TestNoWorkerBackstopSparesBusyFleet(t *testing.T) {
	e := newEnv(t)
	e.eng.o.UnschedulableTimeout = 30 * time.Minute
	s, c := e.connect("k1", []string{"kernel"}, nil, "")
	r1 := e.submit(nil, job("first", tags("kernel")))
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	r2 := e.submit(nil, job("second", tags("kernel")))
	e.tick(45*time.Minute, s)
	if st := e.job(r1.ID, "first").Status; st != store.JobLeased {
		t.Fatalf("first = %s", st)
	}
	if j := e.job(r2.ID, "second"); j.Status != store.JobReady {
		t.Fatalf("second = %s %q: the busy covering worker should spare it", j.Status, j.Error)
	}
	if n, _ := e.eng.UnschedulableReady(); n != 0 {
		t.Fatalf("unschedulable_ready = %d", n)
	}
	if strings.Contains(e.out.String(), "ready job has no connected worker") {
		t.Fatal("warned about a covered job")
	}
}

func TestNoWorkerBackstopResetsWhenCoverageAppears(t *testing.T) {
	e := newEnv(t)
	e.eng.o.UnschedulableTimeout = 30 * time.Minute
	// holder keeps concurrency key k, so the kernel job stays ready even
	// while a kernel worker is connected and idle.
	hs, hc := e.connect("t1", []string{"tiny"}, nil, "")
	e.submit(nil, job("holder", tags("tiny"), key("k")))
	e.send(hs, &proto.Accept{JobID: lastAssign(t, hc).Job.ID})
	r := e.submit(nil, job("build", tags("kernel"), key("k")))
	e.tick(20*time.Minute, hs)
	ks, _ := e.connect("k1", []string{"kernel"}, nil, "")
	e.tick(time.Minute, hs)
	e.eng.Disconnect(ks)
	e.tick(25*time.Minute, hs) // 46m ready, 25m uncovered
	if j := e.job(r.ID, "build"); j.Status != store.JobReady {
		t.Fatalf("build = %s %q: coverage should have reset the clock", j.Status, j.Error)
	}
	if n := strings.Count(e.out.String(), "ready job has no connected worker that can run it"); n != 2 {
		t.Fatalf("uncovered warnings = %d, want 2 (once per uncovered stretch)", n)
	}
	e.tick(6*time.Minute, hs)
	if j := e.job(r.ID, "build"); j.Status != store.JobFailed || j.Error != ReasonNoWorker {
		t.Fatalf("build = %s %q", j.Status, j.Error)
	}
}

func TestNoWorkerBackstopZeroDisables(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("build", tags("kernel")))
	e.eng.Reap()
	e.clk.Advance(48 * time.Hour)
	e.eng.Reap()
	if j := e.job(r.ID, "build"); j.Status != store.JobReady {
		t.Fatalf("build = %s %q with the backstop disabled", j.Status, j.Error)
	}
	if n, _ := e.eng.UnschedulableReady(); n != 1 {
		t.Fatalf("unschedulable_ready = %d", n)
	}
}

// After a restart the clock starts no earlier than the new process: workers
// have not reconnected yet.
func TestNoWorkerBackstopStartsAtRestart(t *testing.T) {
	dir, clk, obj := t.TempDir(), clock.NewFake(t0), objects.NewMem()
	e := openEnv(t, dir, clk, obj)
	r := e.submit(nil, job("build", tags("kernel")))
	clk.Advance(5 * time.Hour)
	e2 := openEnv(t, dir, clk, obj)
	e2.eng.o.UnschedulableTimeout = 30 * time.Minute
	if err := e2.eng.Recover(); err != nil {
		t.Fatal(err)
	}
	e2.eng.Reap()
	if j := e2.job(r.ID, "build"); j.Status != store.JobReady {
		t.Fatalf("build = %s %q right after a restart", j.Status, j.Error)
	}
	e2.tick(31 * time.Minute)
	if j := e2.job(r.ID, "build"); j.Status != store.JobFailed || j.Error != ReasonNoWorker {
		t.Fatalf("build = %s %q", j.Status, j.Error)
	}
}
