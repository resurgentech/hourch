package pwpub

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	pw "github.com/resurgentech/powerwarden/powerwarden/api"

	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/pwfake"
)

type src struct {
	mu   sync.Mutex
	snap dispatch.DemandSnapshot
}

func (s *src) DemandSnapshot() (dispatch.DemandSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snap, nil
}

func (s *src) set(snap dispatch.DemandSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snap = snap
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func setup(t *testing.T) (*Publisher, *src, *pwfake.Fake) {
	t.Helper()
	fake := pwfake.New("pw-token")
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	s := &src{}
	p, err := New(Options{URL: srv.URL, Token: "pw-token", Source: s, Refresh: time.Hour, Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	return p, s, fake
}

func TestWanted(t *testing.T) {
	kernel := dispatch.DemandJob{ID: "J1", RunID: "R", Name: "build", Tags: []string{"kernel"}}
	docker := dispatch.DemandJob{ID: "J2", RunID: "R", Name: "img", Tags: []string{"kernel"}, Docker: true}
	blocked := dispatch.DemandJob{ID: "J3", RunID: "R", Name: "pub", Tags: []string{"tiny"}, Blocked: true}
	anyTag := dispatch.DemandJob{ID: "J4", RunID: "R", Name: "any"}
	cases := []struct {
		name    string
		snap    dispatch.DemandSnapshot
		demands []string
	}{
		{"no workers", dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{kernel, anyTag}}, []string{"hourchd:J1", "hourchd:J4"}},
		{"idle kernel covers", dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{kernel},
			Workers: []dispatch.LiveWorker{{ID: "v1", Tags: []string{"kernel", "x"}}}}, nil},
		{"busy kernel does not cover", dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{kernel},
			Workers: []dispatch.LiveWorker{{ID: "v1", Tags: []string{"kernel"}, Job: "J9"}}}, []string{"hourchd:J1"}},
		{"tiny does not cover kernel", dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{kernel},
			Workers: []dispatch.LiveWorker{{ID: "t1", Tags: []string{"tiny"}}}}, []string{"hourchd:J1"}},
		{"docker needs capability", dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{docker},
			Workers: []dispatch.LiveWorker{{ID: "v1", Tags: []string{"kernel"}}}}, []string{"hourchd:J2"}},
		{"docker covered", dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{docker},
			Workers: []dispatch.LiveWorker{{ID: "v1", Tags: []string{"kernel"}, Capabilities: []string{"docker"}}}}, nil},
		{"blocked on concurrency key", dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{blocked}}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d, _ := Wanted(c.snap)
			var got []string
			for _, id := range sortedKeys(d) {
				got = append(got, id)
			}
			if !reflect.DeepEqual(got, c.demands) {
				t.Fatalf("demands = %v, want %v", got, c.demands)
			}
		})
	}
	_, ws := Wanted(dispatch.DemandSnapshot{Workers: []dispatch.LiveWorker{
		{ID: "v1", Hostname: "veritech01.labtwo", Tags: []string{"kernel"}, Job: "J7"},
		{ID: "t1", Hostname: "sdf1.labtwo", Tags: []string{"tiny"}},
	}})
	if w := ws["v1"]; w.status != pw.JobStatusBusy || w.demandID != "hourchd:J7" || w.hostname != "veritech01.labtwo" {
		t.Fatalf("busy worker = %+v", w)
	}
	if w := ws["t1"]; w.status != pw.JobStatusIdle || w.demandID != "" {
		t.Fatalf("idle worker = %+v", w)
	}
}

func TestWorkerLease(t *testing.T) {
	for refresh, want := range map[time.Duration]int{15 * time.Second: 90, time.Second: 15, 2 * time.Minute: 300} {
		if got := WorkerLeaseSeconds(refresh); got != want {
			t.Errorf("WorkerLeaseSeconds(%v) = %d, want %d", refresh, got, want)
		}
	}
}

func TestSyncPublishesDiffs(t *testing.T) {
	p, s, fake := setup(t)
	ctx := context.Background()
	kernel := dispatch.DemandJob{ID: "J1", RunID: "R1", Name: "build", Tags: []string{"kernel"}}
	s.set(dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{kernel},
		Workers: []dispatch.LiveWorker{{ID: "t1", Hostname: "sdf1.labtwo", Tags: []string{"tiny"}}}})
	if err := p.Sync(ctx, true); err != nil {
		t.Fatal(err)
	}
	d := fake.Demands()["hourchd:J1"]
	if d.Source != pw.DemandSourceHourchd || !reflect.DeepEqual(d.Tags, pw.Tags{"kernel"}) || d.LeaseSeconds != DemandLeaseSeconds ||
		d.ExternalRef != "run R1 job build" {
		t.Fatalf("demand = %+v", d)
	}
	if w := fake.Workers()["t1"]; w.JobStatus != pw.JobStatusIdle || w.Hostname != "sdf1.labtwo" || w.LeaseSeconds != WorkerLeaseSeconds(time.Hour) {
		t.Fatalf("worker = %+v", w)
	}
	// Nothing changed: an incremental pass sends nothing.
	n := len(fake.Ops())
	if err := p.Sync(ctx, false); err != nil {
		t.Fatal(err)
	}
	if got := fake.Ops()[n:]; len(got) != 0 {
		t.Fatalf("idle pass sent %v", got)
	}
	// A kernel worker connects and takes the job: demand deleted, worker busy.
	s.set(dispatch.DemandSnapshot{Workers: []dispatch.LiveWorker{
		{ID: "t1", Hostname: "sdf1.labtwo", Tags: []string{"tiny"}},
		{ID: "v1", Hostname: "veritech01.labtwo", Tags: []string{"kernel"}, Job: "J1"},
	}})
	if err := p.Sync(ctx, false); err != nil {
		t.Fatal(err)
	}
	if ids := fake.DemandIDs(); len(ids) != 0 {
		t.Fatalf("demands after assign = %v", ids)
	}
	ops := fake.Ops()[n:]
	if len(ops) != 2 || ops[0].String() != "DELETE demand hourchd:J1" || ops[1].String() != "PUT worker v1" ||
		ops[1].Worker.JobStatus != pw.JobStatusBusy || *ops[1].Worker.DemandId != "hourchd:J1" {
		t.Fatalf("ops after assign = %v", ops)
	}
	// v1 disconnects while busy: left to expire (busy hold). t1 leaves idle:
	// deleted.
	s.set(dispatch.DemandSnapshot{})
	if err := p.Sync(ctx, false); err != nil {
		t.Fatal(err)
	}
	ws := fake.Workers()
	if _, ok := ws["t1"]; ok {
		t.Fatal("idle worker that left was not deleted")
	}
	if _, ok := ws["v1"]; !ok {
		t.Fatal("busy worker that left was deleted (must expire for busy hold)")
	}
}

func TestRefreshReconciles(t *testing.T) {
	p, s, fake := setup(t)
	ctx := context.Background()
	s.set(dispatch.DemandSnapshot{
		Ready:   []dispatch.DemandJob{{ID: "J1", RunID: "R1", Name: "build", Tags: []string{"kernel"}}},
		Workers: []dispatch.LiveWorker{{ID: "t1", Hostname: "sdf1.labtwo", Tags: []string{"tiny"}}},
	})
	// Rows left by an earlier hourchd process and a manual demand.
	fake.Seed(pw.Demand{Id: "hourchd:OLD", Source: pw.DemandSourceHourchd, Tags: pw.Tags{"kernel"}})
	fake.Seed(pw.Demand{Id: "hold-kernel", Source: pw.DemandSourceManual, Tags: pw.Tags{"kernel"}})
	if err := p.Sync(ctx, true); err != nil {
		t.Fatal(err)
	}
	if got := fake.DemandIDs(); !reflect.DeepEqual(got, []string{"hold-kernel", "hourchd:J1"}) {
		t.Fatalf("after first refresh: %v", got)
	}
	// powerwarden restarts and loses its rows: an incremental pass does not
	// notice, the refresh puts everything back.
	fake.Wipe()
	if err := p.Sync(ctx, false); err != nil {
		t.Fatal(err)
	}
	if got := fake.DemandIDs(); len(got) != 0 {
		t.Fatalf("incremental pass re-sent unchanged rows: %v", got)
	}
	if err := p.Sync(ctx, true); err != nil {
		t.Fatal(err)
	}
	if got := fake.DemandIDs(); !reflect.DeepEqual(got, []string{"hourchd:J1"}) {
		t.Fatalf("after refresh: %v", got)
	}
	if _, ok := fake.Workers()["t1"]; !ok {
		t.Fatal("worker not re-published by refresh")
	}
}

func TestDownThenBack(t *testing.T) {
	p, s, fake := setup(t)
	p.o.RetryMin = 10 * time.Millisecond
	p.o.Refresh = time.Hour
	s.set(dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{{ID: "J1", RunID: "R1", Name: "b", Tags: []string{"kernel"}}}})
	fake.SetDown(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); p.Run(ctx) }()
	defer func() { cancel(); <-done }()
	waitFor(t, "unreachable", func() bool { st, msg, _ := p.State(); return st == Unreachable && msg != "" })
	// Kick never blocks, even while passes fail.
	for i := 0; i < 100; i++ {
		p.Kick()
	}
	fake.SetDown(false)
	waitFor(t, "connected", func() bool { return p.Status() == Connected })
	waitFor(t, "demand", func() bool { return len(fake.DemandIDs()) == 1 })
	// The job is assigned while powerwarden is down: the delete is retried.
	fake.SetDown(true)
	s.set(dispatch.DemandSnapshot{})
	p.Kick()
	waitFor(t, "unreachable again", func() bool { return p.Status() == Unreachable })
	fake.SetDown(false)
	waitFor(t, "demand removed after recovery", func() bool { return len(fake.DemandIDs()) == 0 && p.Status() == Connected })
}

func TestRejectedRowIsNotRetried(t *testing.T) {
	p, s, fake := setup(t)
	// A tag powerwarden rejects (cannot come from hourchd's own validation,
	// but must not wedge the publisher).
	s.set(dispatch.DemandSnapshot{Ready: []dispatch.DemandJob{{ID: "J1", RunID: "R1", Name: "b", Tags: []string{"bad tag"}}}})
	if err := p.Sync(context.Background(), false); err != nil {
		t.Fatalf("400 should be permanent, got %v", err)
	}
	n := len(fake.Ops())
	if err := p.Sync(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if len(fake.Ops()) != n {
		t.Fatal("rejected row resent on an incremental pass")
	}
}

func TestWrongTokenIsUnreachable(t *testing.T) {
	fake := pwfake.New("right")
	srv := httptest.NewServer(fake)
	defer srv.Close()
	p, err := New(Options{URL: srv.URL, Token: "wrong", Source: &src{}, Log: quiet()})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Sync(context.Background(), true); err == nil {
		t.Fatal("401 not reported")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
