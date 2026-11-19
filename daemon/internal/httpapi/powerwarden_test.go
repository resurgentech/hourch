package httpapi_test

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	api "github.com/resurgentech/hourch/api"
	proto "github.com/resurgentech/hourch/protocol"
	pw "github.com/resurgentech/powerwarden/powerwarden/api"

	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/pwfake"
)

// pwEnv is an hourchd with the publisher pointed at a fake powerwarden.
func pwEnv(t *testing.T) (*env, *pwfake.Fake) {
	t.Helper()
	fake := pwfake.New("pw-secret-token")
	pws := httptest.NewServer(fake)
	t.Cleanup(pws.Close)
	tokFile := filepath.Join(t.TempDir(), "powerwarden.token")
	if err := os.WriteFile(tokFile, []byte("pw-secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	e := newEnv(t, func(c *config.Config) {
		c.Powerwarden.URL = pws.URL
		c.Powerwarden.TokenFile = tokFile
		c.Powerwarden.Refresh = config.Duration(time.Hour) // tests trigger passes by change
	})
	ctx, cancel := context.WithCancel(context.Background())
	e.stop = cancel
	e.a.StartPublisher(ctx)
	return e, fake
}

func healthPW(t *testing.T, e *env) api.HealthzPowerwarden {
	t.Helper()
	var h api.Healthz
	e.json("GET", "/v1/healthz", "", nil, 200, &h)
	return h.Powerwarden
}

func TestPowerwardenDemandAndLiveness(t *testing.T) {
	e, fake := pwEnv(t)
	waitFor(t, "connected", func() bool { return healthPW(t, e) == api.HealthzPowerwardenConnected })
	var st api.Stats
	e.json("GET", "/v1/stats", subTok, nil, 200, &st)
	if st.Powerwarden != api.Connected {
		t.Fatalf("stats powerwarden = %s", st.Powerwarden)
	}

	// A kernel job with no worker at all: demand.
	run := submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "build", Argv: []string{"x"}, Tags: ptr([]string{"kernel"})}}})
	jobID := run.Jobs[0].Id
	did := "hourchd:" + jobID
	waitFor(t, "demand", func() bool { _, ok := fake.Demands()[did]; return ok })
	d := fake.Demands()[did]
	if !reflect.DeepEqual(d.Tags, pw.Tags{"kernel"}) || d.Source != pw.DemandSourceHourchd || d.LeaseSeconds != 3600 {
		t.Fatalf("demand = %+v", d)
	}

	// A tiny worker does not cover it: the demand stays; the worker is idle.
	w1, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1", Hostname: "sdf1.labtwo", Tags: []string{"tiny"}})
	waitFor(t, "w1 idle", func() bool { return fake.Workers()["w1"].JobStatus == pw.JobStatusIdle })
	if _, ok := fake.Demands()[did]; !ok {
		t.Fatal("demand removed although no kernel worker connected")
	}

	// A kernel worker connects and takes it: demand deleted, worker busy
	// with the demand id.
	w2, _ := e.worker(w2Tok, proto.Hello{WorkerID: "w2", Hostname: "veritech01.labtwo", Tags: []string{"kernel"}})
	a := w2.assign()
	if a.Job.ID != jobID {
		t.Fatalf("assigned %s", a.Job.ID)
	}
	waitFor(t, "demand deleted on assign", func() bool { _, ok := fake.Demands()[did]; return !ok })
	waitFor(t, "w2 busy", func() bool {
		w := fake.Workers()["w2"]
		return w.JobStatus == pw.JobStatusBusy && w.DemandId == did && w.Hostname == "veritech01.labtwo"
	})

	// It finishes: w2 idle again.
	w2.send(&proto.Accept{JobID: jobID})
	w2.send(&proto.Result{JobID: jobID, ExitCode: 0})
	waitFor(t, "w2 idle", func() bool { return fake.Workers()["w2"].JobStatus == pw.JobStatusIdle })

	// A gpu job: demand; cancelling the run deletes it.
	run2 := submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "train", Argv: []string{"x"}, Tags: ptr([]string{"gpu"})}}})
	did2 := "hourchd:" + run2.Jobs[0].Id
	waitFor(t, "gpu demand", func() bool { _, ok := fake.Demands()[did2]; return ok })
	e.json("POST", "/v1/runs/"+run2.Id+"/cancel", subTok, nil, 200, nil)
	waitFor(t, "demand deleted on cancel", func() bool { _, ok := fake.Demands()[did2]; return !ok })

	// A job that fails (dependency chain cancelled) never shows up as
	// demand for its pending dependents.
	if ids := fake.DemandIDs(); len(ids) != 0 {
		t.Fatalf("left-over demands: %v", ids)
	}

	// An idle worker that disconnects is deleted.
	w1.ws.CloseNow()
	waitFor(t, "w1 deleted", func() bool { _, ok := fake.Workers()["w1"]; return !ok })
}

func TestPowerwardenDownDoesNotStallDispatch(t *testing.T) {
	e, fake := pwEnv(t)
	waitFor(t, "connected", func() bool { return healthPW(t, e) == api.HealthzPowerwardenConnected })
	fake.SetDown(true)
	w1, _ := e.worker(w1Tok, proto.Hello{WorkerID: "w1", Tags: []string{"tiny"}})
	waitFor(t, "unreachable", func() bool { return healthPW(t, e) == api.HealthzPowerwardenUnreachable })
	start := time.Now()
	run := submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "j", Argv: []string{"x"}, Tags: ptr([]string{"tiny"})}}})
	a := w1.assign()
	if a.Job.ID != run.Jobs[0].Id {
		t.Fatalf("assigned %s", a.Job.ID)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("dispatch took %v while powerwarden was down", d)
	}
	// A demand that comes and goes while powerwarden is down is never
	// published; liveness catches up when it is back.
	run2 := submit(t, e, api.RunSubmission{Workflow: "wf", Jobs: []api.JobSpec{{Name: "k", Argv: []string{"x"}, Tags: ptr([]string{"kernel"})}}})
	fake.SetDown(false)
	waitFor(t, "connected again", func() bool { return healthPW(t, e) == api.HealthzPowerwardenConnected })
	did := "hourchd:" + run2.Jobs[0].Id
	waitFor(t, "demand after recovery", func() bool { _, ok := fake.Demands()[did]; return ok })
	waitFor(t, "w1 busy after recovery", func() bool { return fake.Workers()["w1"].JobStatus == pw.JobStatusBusy })
}

func TestPowerwardenDisabled(t *testing.T) {
	e := newEnv(t, nil)
	if got := healthPW(t, e); got != api.HealthzPowerwardenDisabled {
		t.Fatalf("healthz powerwarden = %s", got)
	}
}
