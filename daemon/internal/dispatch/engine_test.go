package dispatch

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
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
)

// fakeConn records what the engine sends.
type fakeConn struct {
	mu     sync.Mutex
	msgs   []proto.Message
	closed string
}

func (c *fakeConn) Send(m proto.Message) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed != "" {
		return false
	}
	// Round-trip through the wire encoding: the engine must only send
	// messages that validate.
	b, err := proto.Encode(m)
	if err != nil {
		panic(err)
	}
	dm, err := proto.Decode(b)
	if err != nil {
		panic(err)
	}
	c.msgs = append(c.msgs, dm)
	return true
}

func (c *fakeConn) Close(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed == "" {
		c.closed = reason
	}
}

func (c *fakeConn) all() []proto.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]proto.Message{}, c.msgs...)
}

func (c *fakeConn) isClosed() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func ofType[T proto.Message](c *fakeConn) []T {
	var out []T
	for _, m := range c.all() {
		if v, ok := m.(T); ok {
			out = append(out, v)
		}
	}
	return out
}

type env struct {
	t    *testing.T
	eng  *Engine
	st   *store.Store
	obj  *objects.Mem
	clk  *clock.Fake
	logs *logs.Manager
	box  *secrets.Box
	key  []byte      // the box's key (to prove it never reaches a log)
	out  *syncBuffer // everything the engine logged
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

var t0 = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	return openEnv(t, dir, clock.NewFake(t0), objects.NewMem())
}

func openEnv(t *testing.T, dir string, clk *clock.Fake, obj *objects.Mem) *env {
	t.Helper()
	st, err := store.Open(filepath.Join(dir, "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	seedSeen(t, st)
	lm, err := logs.New(filepath.Join(dir, "logs"))
	if err != nil {
		t.Fatal(err)
	}
	// Every open is a new hourchd process: a new instance id and key.
	k := make([]byte, secrets.KeySize)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	box, err := secrets.NewWithKey(k, ulid.New().Make(clk.Now()))
	if err != nil {
		t.Fatal(err)
	}
	out := &syncBuffer{}
	eng, err := New(Options{
		Store: st, Objects: obj, Box: box, Logs: lm, Hub: events.NewHub(100), Clock: clk,
		Log:       slog.New(slog.NewTextHandler(io.MultiWriter(out, testLogOut()), &slog.HandlerOptions{Level: slog.LevelDebug})),
		Heartbeat: 10 * time.Second, Lease: 45 * time.Second, AcceptTimeout: 10 * time.Second,
		ReaperInterval: 5 * time.Second, DefaultTimeout: 2 * time.Hour, MaxJobsPerRun: 200, OutputValueBytes: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, eng: eng, st: st, obj: obj, clk: clk, logs: lm, box: box, key: k, out: out}
	t.Cleanup(func() {
		eng.WaitUploads()
		st.Close()
	})
	return e
}

func (e *env) submit(secrets map[string]string, jobs ...graph.Job) store.Run {
	e.t.Helper()
	r, err := e.eng.Submit(context.Background(), "tester", graph.Submission{Workflow: "wf", Secrets: secrets, Jobs: jobs})
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *env) connect(id string, tags []string, caps []string, running string) (*Session, *fakeConn) {
	e.t.Helper()
	c := &fakeConn{}
	s, err := e.eng.Hello(c, &proto.Hello{WorkerID: id, Hostname: id + ".lab", Tags: tags, Capabilities: caps, RunningJobID: running, Version: "test"})
	if err != nil {
		e.t.Fatal(err)
	}
	return s, c
}

func (e *env) send(s *Session, m proto.Message) {
	e.t.Helper()
	if err := e.eng.Handle(s, m); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) job(runID, name string) store.Job {
	e.t.Helper()
	jobs, err := e.st.RunJobs(runID)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, j := range jobs {
		if j.Name == name {
			return j
		}
	}
	e.t.Fatalf("no job %s", name)
	return store.Job{}
}

func (e *env) run(id string) store.Run {
	e.t.Helper()
	r, err := e.st.GetRun(id)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func lastAssign(t *testing.T, c *fakeConn) *proto.Assign {
	t.Helper()
	as := ofType[*proto.Assign](c)
	if len(as) == 0 {
		t.Fatalf("no assign; got %v", c.all())
	}
	return as[len(as)-1]
}

// finish runs a job's happy path on s: accept, outputs, result.
func (e *env) finish(s *Session, jobID string, exit int, outputs map[string]string) {
	e.t.Helper()
	e.send(s, &proto.Accept{JobID: jobID})
	for k, v := range outputs {
		e.send(s, &proto.Output{JobID: jobID, Key: k, Value: v})
	}
	e.send(s, &proto.Result{JobID: jobID, ExitCode: exit})
}

func job(name string, opts ...func(*graph.Job)) graph.Job {
	j := graph.Job{Name: name, Argv: []string{"run", name}}
	for _, o := range opts {
		o(&j)
	}
	return j
}

func tags(t ...string) func(*graph.Job)      { return func(j *graph.Job) { j.Tags = t } }
func argv(a ...string) func(*graph.Job)      { return func(j *graph.Job) { j.Argv = a } }
func after(d ...string) func(*graph.Job)     { return func(j *graph.Job) { j.DependsOn = d } }
func secretsOf(s ...string) func(*graph.Job) { return func(j *graph.Job) { j.Secrets = s } }
func attempts(n int) func(*graph.Job)        { return func(j *graph.Job) { j.MaxAttempts = n } }
func key(k string) func(*graph.Job)          { return func(j *graph.Job) { j.ConcurrencyKey = k } }

func TestLifecycleOutputSubstitutionAndRunNumbers(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil,
		job("settings", tags("tiny")),
		job("build", tags("kernel"), argv("just", "package", "--version", "${outputs.settings.version}"),
			func(j *graph.Job) { j.Env = map[string]string{"BN": "n${outputs.settings.build_num}"} }),
	)
	if r.Number != 1 {
		t.Fatalf("number = %d", r.Number)
	}
	if e.job(r.ID, "settings").Status != store.JobReady || e.job(r.ID, "build").Status != store.JobPending {
		t.Fatal("initial statuses")
	}
	s, c := e.connect("w1", []string{"tiny", "kernel"}, nil, "")
	if _, ok := c.all()[0].(*proto.Welcome); !ok {
		t.Fatalf("first message %T", c.all()[0])
	}
	a := lastAssign(t, c)
	if a.Job.Name != "settings" || a.Attempt != 1 || a.JobToken == "" {
		t.Fatalf("assign = %+v", a)
	}
	j := e.job(r.ID, "settings")
	if j.Status != store.JobLeased || j.LeaseOwner != "w1" || j.Attempt != 1 || !j.LeaseUntil.Equal(t0.Add(45*time.Second)) {
		t.Fatalf("leased row = %+v", j)
	}
	if w, _ := e.st.GetWorker("w1"); w.CurrentJob != j.ID || !w.Connected {
		t.Fatalf("worker row = %+v", w)
	}
	e.finish(s, a.Job.ID, 0, map[string]string{"version": "1.2.3", "build_num": "7"})
	b := lastAssign(t, c)
	if b.Job.Name != "build" {
		t.Fatalf("second assign = %s", b.Job.Name)
	}
	if strings.Join(b.Job.Argv, " ") != "just package --version 1.2.3" || b.Job.Env["BN"] != "n7" {
		t.Fatalf("substituted argv/env = %v %v", b.Job.Argv, b.Job.Env)
	}
	bj := e.job(r.ID, "build")
	if bj.SpecArgv[3] != "${outputs.settings.version}" {
		t.Fatal("spec argv lost")
	}
	e.finish(s, b.Job.ID, 0, nil)
	if got := e.run(r.ID); got.Status != store.RunSucceeded || got.FinishedAt == nil {
		t.Fatalf("run = %+v", got)
	}
	if r2 := e.submit(nil, job("x")); r2.Number != 2 {
		t.Fatalf("second run number = %d", r2.Number)
	}
	other, err := e.eng.Submit(context.Background(), "t", graph.Submission{Workflow: "other", Jobs: []graph.Job{job("x")}})
	if err != nil || other.Number != 1 {
		t.Fatalf("other workflow number = %d %v", other.Number, err)
	}
}

func TestTagCapabilityAndConcurrencyMatching(t *testing.T) {
	e := newEnv(t)
	_, tiny := e.connect("tiny1", []string{"tiny"}, nil, "")
	r := e.submit(nil,
		job("k", tags("kernel")),
		job("img", tags("tiny"), func(j *graph.Job) { j.Image = "registry/x:1" }),
	)
	if len(ofType[*proto.Assign](tiny)) != 0 {
		t.Fatal("tiny worker got a kernel or image job")
	}
	_, dock := e.connect("tiny2", []string{"tiny", "extra"}, []string{"docker"}, "")
	if a := lastAssign(t, dock); a.Job.Name != "img" || a.Job.Image != "registry/x:1" {
		t.Fatalf("docker worker got %+v", a.Job)
	}
	_, kern := e.connect("kern", []string{"kernel"}, nil, "")
	if a := lastAssign(t, kern); a.Job.Name != "k" {
		t.Fatalf("kernel worker got %s", a.Job.Name)
	}
	_ = r

	// Concurrency key: one leased job per key across runs.
	e2 := newEnv(t)
	s1, c1 := e2.connect("a", nil, nil, "")
	_, c2 := e2.connect("b", nil, nil, "")
	e2.submit(nil, job("p1", key("aptly")))
	e2.submit(nil, job("p2", key("aptly")))
	n1, n2 := len(ofType[*proto.Assign](c1)), len(ofType[*proto.Assign](c2))
	if n1+n2 != 1 {
		t.Fatalf("assigned %d jobs with one key", n1+n2)
	}
	holder, other := s1, c2
	if n1 == 0 {
		t.Fatal("expected the longest-idle worker (a) to get the first job")
	}
	e2.clk.Advance(time.Second)
	e2.finish(holder, lastAssign(t, c1).Job.ID, 0, nil)
	// The key is free: the idle worker b gets p2 (a also idle now; b idle longer).
	if a := lastAssign(t, other); a.Job.Name != "p2" {
		t.Fatalf("b got %s", a.Job.Name)
	}
}

func TestDispatchTriggers(t *testing.T) {
	e := newEnv(t)
	// 1. Job becomes ready while a worker is idle.
	s, c := e.connect("w", nil, nil, "")
	e.submit(nil, job("a"), job("b"))
	a := lastAssign(t, c)
	if len(ofType[*proto.Assign](c)) != 1 {
		t.Fatal("worker holds two jobs")
	}
	// 2. Worker finishes: the next ready job follows at once.
	e.finish(s, a.Job.ID, 0, nil)
	if len(ofType[*proto.Assign](c)) != 2 {
		t.Fatal("no dispatch on finish")
	}
	// 3. Worker connects idle with work waiting.
	e.submit(nil, job("c"))
	_, c2 := e.connect("w2", nil, nil, "")
	if lastAssign(t, c2).Job.Name != "c" {
		t.Fatal("no dispatch on connect")
	}
	// Latency is recorded.
	if _, _, ok := e.eng.Latency(); !ok {
		t.Fatal("no latency samples")
	}
}

func TestTwoWorkersRaceForOneJob(t *testing.T) {
	for i := 0; i < 20; i++ {
		e := newEnv(t)
		e.submit(nil, job("only"))
		var wg sync.WaitGroup
		conns := make([]*fakeConn, 8)
		for k := range conns {
			wg.Add(1)
			go func(k int) {
				defer wg.Done()
				c := &fakeConn{}
				if _, err := e.eng.Hello(c, &proto.Hello{WorkerID: "w" + string(rune('a'+k)), Hostname: "h", Version: "t"}); err != nil {
					t.Error(err)
				}
				conns[k] = c
			}(k)
		}
		wg.Wait()
		n := 0
		for _, c := range conns {
			n += len(ofType[*proto.Assign](c))
		}
		if n != 1 {
			t.Fatalf("round %d: %d assigns for one job", i, n)
		}
	}
}

func TestLeaseLapseRequeueThenFail(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("j", attempts(2)))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	e.eng.Disconnect(s)
	e.clk.Advance(44 * time.Second)
	e.eng.Reap()
	if e.job(r.ID, "j").Status != store.JobLeased {
		t.Fatal("reaped before the lease ran out")
	}
	e.clk.Advance(2 * time.Second)
	e.eng.Reap()
	j := e.job(r.ID, "j")
	if j.Status != store.JobReady || j.Attempt != 1 || j.Error != ReasonLeaseExpired {
		t.Fatalf("after first lapse: %+v", j)
	}
	s2, c2 := e.connect("w2", nil, nil, "")
	a2 := lastAssign(t, c2)
	if a2.Attempt != 2 {
		t.Fatalf("attempt = %d", a2.Attempt)
	}
	e.send(s2, &proto.Accept{JobID: a2.Job.ID})
	e.eng.Disconnect(s2)
	e.clk.Advance(46 * time.Second)
	e.eng.Reap()
	j = e.job(r.ID, "j")
	if j.Status != store.JobFailed || j.Error != ReasonLeaseExpired {
		t.Fatalf("after second lapse: %+v", j)
	}
	if e.run(r.ID).Status != store.RunFailed {
		t.Fatal("run not failed")
	}
}

func TestAnyMessageRenewsLease(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("j"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	for i := 0; i < 5; i++ {
		e.clk.Advance(30 * time.Second)
		e.send(s, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: int64(i + 1), Data: "x"})
		e.eng.Reap()
	}
	if e.job(r.ID, "j").Status != store.JobLeased {
		t.Fatal("lease lapsed while the worker talked")
	}
}

func TestTimeoutSendsCancelAndFails(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("slow", func(j *graph.Job) { j.TimeoutSeconds = 60 }), job("next"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	if a.Job.Name != "slow" || a.Job.TimeoutSeconds != 60 {
		t.Fatalf("assign = %+v", a.Job)
	}
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	e.clk.Advance(30 * time.Second)
	e.send(s, &proto.Heartbeat{JobID: a.Job.ID})
	e.clk.Advance(31 * time.Second)
	e.eng.Reap()
	cs := ofType[*proto.Cancel](c)
	if len(cs) != 1 || cs[0].Reason != ReasonTimeout || cs[0].JobID != a.Job.ID {
		t.Fatalf("cancel = %+v", cs)
	}
	if j := e.job(r.ID, "slow"); j.Status != store.JobFailed || j.Error != ReasonTimeout {
		t.Fatalf("slow = %+v", j)
	}
	if len(ofType[*proto.Assign](c)) != 1 {
		t.Fatal("worker given a job before reporting the killed one")
	}
	e.send(s, &proto.Result{JobID: a.Job.ID, ExitCode: -1, Error: "killed"})
	if j := e.job(r.ID, "slow"); j.Status != store.JobFailed || j.Error != ReasonTimeout {
		t.Fatal("late result changed a timed-out job")
	}
	if lastAssign(t, c).Job.Name != "next" {
		t.Fatal("worker not idle after result")
	}
}

func TestAcceptTimeoutKeepsAttempt(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("j"))
	_, c := e.connect("w", nil, nil, "")
	lastAssign(t, c)
	e.clk.Advance(11 * time.Second)
	e.eng.Reap()
	j := e.job(r.ID, "j")
	if j.Status != store.JobReady || j.Attempt != 0 || j.Error != ReasonAcceptTimeout {
		t.Fatalf("after accept timeout: %+v", j)
	}
	if c.isClosed() == "" {
		t.Fatal("worker that missed accept not disconnected")
	}
	_, c2 := e.connect("w2", nil, nil, "")
	if a := lastAssign(t, c2); a.Attempt != 1 {
		t.Fatalf("reassigned attempt = %d", a.Attempt)
	}
}

func TestReconnectWithinLeaseKeepsJob(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("j"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	e.send(s, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: "one\n"})
	e.send(s, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 2, Data: "two\n"})
	e.eng.Disconnect(s)
	e.clk.Advance(30 * time.Second)
	s2, c2 := e.connect("w", nil, nil, a.Job.ID)
	w := ofType[*proto.Welcome](c2)[0]
	if w.Abandon != "" || w.LastLogSeq != 2 {
		t.Fatalf("welcome = %+v", w)
	}
	if len(ofType[*proto.Assign](c2)) != 0 {
		t.Fatal("re-attached worker got another job")
	}
	// Resent chunks are dropped; new ones append.
	e.send(s2, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 2, Data: "two\n"})
	e.send(s2, &proto.Log{JobID: a.Job.ID, Stream: "stderr", Seq: 3, Data: "three\n"})
	e.clk.Advance(30 * time.Second) // past the original lease
	e.eng.Reap()
	if j := e.job(r.ID, "j"); j.Status != store.JobLeased || j.LeaseOwner != "w" {
		t.Fatalf("job = %+v", j)
	}
	b, _ := os.ReadFile(e.logs.Path(a.Job.ID, 1))
	if string(b) != "one\ntwo\nthree\n" {
		t.Fatalf("log = %q", b)
	}
	e.send(s2, &proto.Result{JobID: a.Job.ID, ExitCode: 0})
	if e.run(r.ID).Status != store.RunSucceeded {
		t.Fatal("run not succeeded")
	}
	e.eng.WaitUploads()
	got, ok := e.obj.Bytes(LogKey(r.ID, a.Job.ID, 1))
	if !ok || string(got) != "one\ntwo\nthree\n" {
		t.Fatalf("uploaded log = %q %v", got, ok)
	}
	if e.logs.Exists(a.Job.ID, 1) {
		t.Fatal("live log not removed after upload")
	}
}

func TestStaleHelloGetsAbandonAndLostJobRequeues(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("j", attempts(3)))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	e.eng.Disconnect(s)
	e.clk.Advance(50 * time.Second)
	e.eng.Reap() // lease lapsed: requeued
	_, c2 := e.connect("w", nil, nil, a.Job.ID)
	if w := ofType[*proto.Welcome](c2)[0]; w.Abandon != a.Job.ID || w.LastLogSeq != 0 {
		t.Fatalf("welcome = %+v", w)
	}
	// The idle worker immediately gets the requeued job (attempt 2).
	if a2 := lastAssign(t, c2); a2.Job.ID != a.Job.ID || a2.Attempt != 2 {
		t.Fatalf("assign = %+v", a2)
	}

	// A worker that reconnects without reporting its leased job lost it.
	e.eng.Disconnect(e.eng.sessions["w"])
	_, c3 := e.connect("w", nil, nil, "")
	j := e.job(r.ID, "j")
	if j.Attempt != 2 {
		// Lost an accepted? no: attempt 2 was never accepted, so it is an
		// accept miss and keeps the attempt number before being reassigned.
		t.Fatalf("attempt = %d", j.Attempt)
	}
	if a3 := lastAssign(t, c3); a3.Attempt != 2 {
		t.Fatalf("reassigned attempt = %d", a3.Attempt)
	}
}

func TestNewerConnectionClosesOlder(t *testing.T) {
	e := newEnv(t)
	s1, c1 := e.connect("w", nil, nil, "")
	_, c2 := e.connect("w", nil, nil, "")
	if c1.isClosed() == "" || c2.isClosed() != "" {
		t.Fatalf("closed: old=%q new=%q", c1.isClosed(), c2.isClosed())
	}
	e.eng.Disconnect(s1) // the old socket's reader ends
	if w, _ := e.st.GetWorker("w"); !w.Connected {
		t.Fatal("old connection's disconnect marked the worker offline")
	}
	e.submit(nil, job("j"))
	if len(ofType[*proto.Assign](c1)) != 0 || len(ofType[*proto.Assign](c2)) != 1 {
		t.Fatal("job went to the replaced connection")
	}
}

func TestMissingOutputFailsJob(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("settings"), job("build", argv("x", "${outputs.settings.version}")))
	s, c := e.connect("w", nil, nil, "")
	e.finish(s, lastAssign(t, c).Job.ID, 0, nil)
	if j := e.job(r.ID, "build"); j.Status != store.JobFailed || j.Error != ReasonMissingOutput {
		t.Fatalf("build = %+v", j)
	}
	if e.run(r.ID).Status != store.RunFailed {
		t.Fatal("run not failed")
	}
}

func TestDependencyFailureCancelsDownstream(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("a"), job("b", after("a")), job("c", after("b")), job("d"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	if a.Job.Name != "a" {
		t.Fatalf("first = %s", a.Job.Name)
	}
	e.finish(s, a.Job.ID, 2, nil)
	if j := e.job(r.ID, "a"); j.Status != store.JobFailed || j.Error != ReasonExitCode || *j.ExitCode != 2 {
		t.Fatalf("a = %+v", j)
	}
	for _, n := range []string{"b", "c"} {
		if j := e.job(r.ID, n); j.Status != store.JobCancelled || j.Error != ReasonDependencyFailed {
			t.Fatalf("%s = %+v", n, j)
		}
	}
	if e.run(r.ID).Status != store.RunRunning {
		t.Fatal("run ended while d can still run")
	}
	d := lastAssign(t, c)
	if d.Job.Name != "d" {
		t.Fatalf("next = %s", d.Job.Name)
	}
	e.finish(s, d.Job.ID, 0, nil)
	if e.run(r.ID).Status != store.RunFailed {
		t.Fatal("run not failed")
	}
}

func TestRetryOnNonZeroExit(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("j", attempts(2)))
	s, c := e.connect("w", nil, nil, "")
	e.finish(s, lastAssign(t, c).Job.ID, 1, nil)
	if j := e.job(r.ID, "j"); j.Status != store.JobLeased || j.Attempt != 2 {
		t.Fatalf("not retried: %+v", j)
	}
	e.finish(s, lastAssign(t, c).Job.ID, 0, nil)
	if e.run(r.ID).Status != store.RunSucceeded {
		t.Fatal("run")
	}
}

func TestSecrets(t *testing.T) {
	e := newEnv(t)
	sec := map[string]string{"APTLY": "aptly-secret-value", "CF": "cloudflare-secret"}
	r := e.submit(sec, job("pub", secretsOf("APTLY")), job("other", after("pub")))
	// One sealed object per run, recorded in SQLite; no value in either.
	k := e.box.Key(r.ID)
	obj, ok := e.obj.Bytes(k)
	if !ok {
		t.Fatalf("no object %s; keys %v", k, e.obj.Keys())
	}
	for _, v := range sec {
		if bytes.Contains(obj, []byte(v)) {
			t.Fatal("secret stored in plaintext")
		}
	}
	if row, err := e.st.GetSecretObject(r.ID); err != nil || row.Object != k || row.InstanceID != e.box.Instance() {
		t.Fatalf("ownership row = %+v %v", row, err)
	}
	assertNoValueInDB(t, e, "aptly-secret-value", "cloudflare-secret")
	if strings.Join(e.run(r.ID).SecretNames, ",") != "APTLY,CF" {
		t.Fatalf("secret names = %v", e.run(r.ID).SecretNames)
	}
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	if len(a.Secrets) != 1 || a.Secrets["APTLY"] != "aptly-secret-value" {
		t.Fatalf("assign secrets = %v", a.Secrets)
	}
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	e.send(s, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: "token=aptly-secret-value done\n"})
	e.send(s, &proto.Output{JobID: a.Job.ID, Key: "k", Value: "aptly-secret-value"})
	b, _ := os.ReadFile(e.logs.Path(a.Job.ID, 1))
	if string(b) != "token=*** done\n" {
		t.Fatalf("stored log = %q", b)
	}
	if o, _ := e.st.Outputs(a.Job.ID); o["k"] != "***" {
		t.Fatalf("output not redacted: %v", o)
	}
	e.send(s, &proto.Result{JobID: a.Job.ID, ExitCode: 0})
	o := lastAssign(t, c)
	if o.Job.Name != "other" || len(o.Secrets) != 0 {
		t.Fatalf("other got secrets: %+v", o.Secrets)
	}
	if _, ok := e.obj.Bytes(k); ok {
		t.Fatal("object kept after the only secret-using job succeeded")
	}
	e.finish(s, o.Job.ID, 0, nil)
	if _, err := e.st.GetSecretObject(r.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("ownership row kept after the run succeeded")
	}
	if strings.Join(e.run(r.ID).SecretNames, ",") != "APTLY,CF" {
		t.Fatal("secret names lost")
	}
	// No event detail carries a value.
	evs, _ := e.st.DB().Query(`SELECT detail FROM events`)
	for evs.Next() {
		var d string
		_ = evs.Scan(&d)
		if strings.Contains(d, "secret-value") || strings.Contains(d, "cloudflare-secret") {
			t.Fatalf("event detail has a secret: %s", d)
		}
	}
	evs.Close()
}

func TestSecretUnavailableFailsJob(t *testing.T) {
	e := newEnv(t)
	r := e.submit(map[string]string{"S": "v"}, job("j", secretsOf("S")), job("k", after("j")))
	// Simulate a lost key: corrupt the object.
	if err := e.obj.Put(context.Background(), e.box.Key(r.ID), strings.NewReader("garbage"), -1); err != nil {
		t.Fatal(err)
	}
	_, c := e.connect("w", nil, nil, "")
	if len(ofType[*proto.Assign](c)) != 0 {
		t.Fatal("assigned with unreadable secrets")
	}
	if j := e.job(r.ID, "j"); j.Status != store.JobFailed || j.Error != ReasonSecretUnavailable {
		t.Fatalf("job = %+v", j)
	}
	if k := e.job(r.ID, "k"); k.Status != store.JobCancelled || k.Error != ReasonDependencyFailed {
		t.Fatalf("dependent = %+v", k)
	}
}

func TestRetryRebuildsGraph(t *testing.T) {
	e := newEnv(t)
	r := e.submit(map[string]string{"S": "v"}, job("a"), job("b", secretsOf("S"), argv("x", "${outputs.a.v}")))
	if err := e.eng.CancelRun(r.ID); err != nil {
		t.Fatal(err)
	}
	nr, err := e.eng.Retry(context.Background(), "t", r.ID, map[string]string{"S": "v2"})
	if err != nil {
		t.Fatal(err)
	}
	if nr.ID == r.ID || nr.Number != 2 || nr.Workflow != "wf" {
		t.Fatalf("new run = %+v", nr)
	}
	b := e.job(nr.ID, "b")
	if b.Argv[1] != "${outputs.a.v}" || strings.Join(b.DependsOn, ",") != "a" || b.Status != store.JobPending {
		t.Fatalf("retried b = %+v", b)
	}
	if _, err := e.eng.Retry(context.Background(), "t", "01JBF6Q9W2X7K3M4N5P6Q7R8S9", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry unknown run: %v", err)
	}
}

func TestCancelRunAndJob(t *testing.T) {
	e := newEnv(t)
	r := e.submit(map[string]string{"S": "v"}, job("a", secretsOf("S")), job("b", after("a")), job("c"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	if err := e.eng.CancelRun(r.ID); err != nil {
		t.Fatal(err)
	}
	if cs := ofType[*proto.Cancel](c); len(cs) != 1 || cs[0].Reason != ReasonCancelled {
		t.Fatalf("cancel = %+v", cs)
	}
	if got := e.run(r.ID); got.Status != store.RunCancelled {
		t.Fatalf("run = %+v", got)
	}
	if !e.eng.HoldsSecrets(r.ID) {
		t.Fatal("cancel deleted the run's secrets (a retry needs them)")
	}
	for _, n := range []string{"b", "c"} {
		if j := e.job(r.ID, n); j.Status != store.JobCancelled || j.Error != ReasonCancelled {
			t.Fatalf("%s = %+v", n, j)
		}
	}
	e.send(s, &proto.Result{JobID: a.Job.ID, ExitCode: -1, Error: "killed"})
	if j := e.job(r.ID, "a"); j.Status != store.JobCancelled {
		t.Fatalf("a = %+v", j)
	}
	if err := e.eng.CancelRun(r.ID); err != nil {
		t.Fatal("cancel of a terminal run errored")
	}

	r2 := e.submit(nil, job("x", tags("none")), job("y", after("x")))
	if err := e.eng.CancelJob(e.job(r2.ID, "x").ID); err != nil {
		t.Fatal(err)
	}
	if j := e.job(r2.ID, "y"); j.Status != store.JobCancelled || j.Error != ReasonDependencyFailed {
		t.Fatalf("y = %+v", j)
	}
	if e.run(r2.ID).Status != store.RunCancelled {
		t.Fatal("run with only cancelled jobs")
	}
}

func TestOutputLimitsAndProtocolViolations(t *testing.T) {
	e := newEnv(t)
	r := e.submit(nil, job("j"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Output{JobID: a.Job.ID, Key: "big", Value: strings.Repeat("x", 5000)})
	e.send(s, &proto.Output{JobID: a.Job.ID, Key: "ok", Value: "1"})
	if o, _ := e.st.Outputs(a.Job.ID); len(o) != 1 || o["ok"] != "1" {
		t.Fatalf("outputs = %v", o)
	}
	if err := e.eng.Handle(s, &proto.Assign{}); !errors.Is(err, ErrProtocol) {
		t.Fatalf("worker sending assign: %v", err)
	}
	// Messages about another job are ignored.
	e.send(s, &proto.Result{JobID: "01JBF6Q9W2X7K3M4N5P6Q7R8S9", ExitCode: 0})
	if e.job(r.ID, "j").Status != store.JobLeased {
		t.Fatal("foreign result changed the job")
	}
}

func TestRecoverExtendsLeases(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(t0)
	obj := objects.NewMem()
	e := openEnv(t, dir, clk, obj)
	r := e.submit(nil, job("j"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	e.send(s, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: "before\n"})
	e.st.Close()

	clk.Advance(40 * time.Minute) // hourchd was down a long time
	e2 := openEnv(t, dir, clk, obj)
	if err := e2.eng.Recover(); err != nil {
		t.Fatal(err)
	}
	if w, _ := e2.st.GetWorker("w"); w.Connected {
		t.Fatal("worker still connected after restart")
	}
	e2.eng.Reap()
	j := e2.job(r.ID, "j")
	if j.Status != store.JobLeased || !j.LeaseUntil.Equal(clk.Now().Add(45*time.Second)) {
		t.Fatalf("lease not extended: %+v", j)
	}
	s2, c2 := e2.connect("w", nil, nil, a.Job.ID)
	if w := ofType[*proto.Welcome](c2)[0]; w.Abandon != "" || w.LastLogSeq != 1 {
		t.Fatalf("welcome = %+v", w)
	}
	e2.send(s2, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 2, Data: "after\n"})
	e2.send(s2, &proto.Result{JobID: a.Job.ID, ExitCode: 0})
	e2.eng.WaitUploads()
	if got, _ := obj.Bytes(LogKey(r.ID, a.Job.ID, 1)); string(got) != "before\nafter\n" {
		t.Fatalf("log = %q", got)
	}
	if e2.run(r.ID).Status != store.RunSucceeded {
		t.Fatal("run")
	}
}

func TestJobTokenValidOnlyWhileLeased(t *testing.T) {
	e := newEnv(t)
	e.submit(nil, job("j"))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	if j, err := e.eng.JobForToken(a.JobToken); err != nil || j.ID != a.Job.ID {
		t.Fatalf("token lookup = %v", err)
	}
	if _, err := e.eng.JobForToken("hjt_nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("unknown token accepted")
	}
	e.finish(s, a.Job.ID, 0, nil)
	if _, err := e.eng.JobForToken(a.JobToken); !errors.Is(err, ErrNotFound) {
		t.Fatal("token valid after the job ended")
	}
	var hashes int
	_ = e.st.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_token_hash = ?`, a.JobToken).Scan(&hashes)
	if hashes != 0 {
		t.Fatal("token stored in plaintext")
	}
}

func testLogOut() io.Writer {
	if os.Getenv("HOURCHD_TEST_LOG") != "" {
		return os.Stderr
	}
	return io.Discard
}
