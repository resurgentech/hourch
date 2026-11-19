// Package e2e is hourchd's end-to-end test (docs/DESIGN-hourchd.md, PR plan
// H3/H6): the real daemon (internal/app: SQLite in a temp dir, in-memory
// object store, real clock), two real hourchworker workers in process, a
// fake powerwarden, and runs built and submitted with hourchflow through
// the generated client. Job commands are this test binary in helper mode
// (helper_test.go); container jobs use a fake docker on PATH. Nothing
// leaves loopback.
//
// It lives in the daemon module because it wires the daemon through
// internal/app (Go's internal rule); the worker and flow modules come from
// go.work.
package e2e

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	api "github.com/resurgentech/hourch/api"
	flow "github.com/resurgentech/hourch/flow"
	pw "github.com/resurgentech/powerwarden/powerwarden/api"

	"github.com/resurgentech/hourch/daemon/internal/secrets"
)

const secretValue = "e2e-SECRET-7b1d0c9a"

func helperBin(t *testing.T) string {
	t.Helper()
	p, err := filepath.Abs(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (c *cluster) client() *flowClient {
	c.t.Helper()
	cl, err := flow.NewClient(c.front.URL, opsToken)
	if err != nil {
		c.t.Fatal(err)
	}
	return &flowClient{c: cl}
}

type flowClient struct{ c *api.Client }

func (c *cluster) submit(r *flow.Run) flow.Submitted {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sub, err := r.Submit(ctx, c.client().c)
	if err != nil {
		c.t.Fatalf("submit %s: %v", r.Workflow(), err)
	}
	return sub
}

func (c *cluster) wait(id string) *api.Run {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	run, err := flow.Wait(ctx, c.client().c, id)
	if err != nil {
		c.t.Fatalf("wait %s: %v", id, err)
	}
	return run
}

func (c *cluster) mustSucceed(id string) *api.Run {
	c.t.Helper()
	run := c.wait(id)
	if run.Status != api.RunStatusSucceeded {
		for _, j := range run.Jobs {
			c.t.Logf("job %s: %s error=%q exit=%v", j.Name, j.Status, j.Error, j.ExitCode)
			if j.Attempt > 0 {
				c.t.Logf("log of %s:\n%s", j.Name, c.log(j.Id, j.Attempt))
			}
		}
		c.t.Fatalf("run %s %s", run.Workflow, run.Status)
	}
	return run
}

func TestEndToEnd(t *testing.T) {
	c := newCluster(t)
	c.startWorker("tiny-1", "sdf1.labtwo", []string{"tiny"}, nil)
	c.startWorker("kernel-1", "veritech01.labtwo", []string{"kernel"}, []string{"docker"})
	bin := helperBin(t)
	env := flow.Env("E2E_DIR", c.shared)
	sum := sha256.Sum256([]byte(secretValue))

	// The context: a file the first job reads, and one .gitignore drops.
	ctxDir := filepath.Join(c.dir, "ctx")
	for p, body := range map[string]string{"hello.txt": "hello-ctx\n", ".gitignore": "ignored.txt\n", "ignored.txt": "x"} {
		if err := os.MkdirAll(ctxDir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(ctxDir, p), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	var mainRun *api.Run
	var mainSub flow.Submitted
	c.step("main graph", func(t *testing.T) {
		r := flow.NewRun("e2e-main", map[string]string{"variant": "static"})
		r.Context(ctxDir)
		sec := r.Secret("E2E_SECRET", secretValue)
		prep := r.Job("prep", flow.Tags("tiny"), flow.Cmd(bin, helperArg, "prep"), env,
			flow.Artifacts("out/file.txt", "out/dir/"))
		r.Job("consume", flow.Tags("kernel"), flow.Cmd(bin, helperArg, "consume", prep.Output("greeting")),
			flow.Env("GREETING", prep.Output("greeting")), env,
			flow.Inputs(prep.Artifact("out/file.txt"), prep.Artifact("out/dir/").As("indir/")))
		r.Job("secret", flow.Tags("tiny"), flow.Cmd(bin, helperArg, "secret"), flow.Secrets(sec), env,
			flow.Env("E2E_SECRET_SHA", hex.EncodeToString(sum[:])))
		r.Job("nosecret", flow.Tags("kernel"), flow.Cmd(bin, helperArg, "nosecret"), env)
		r.Job("record", flow.Tags("tiny"), flow.Cmd(bin, helperArg, "record"), env)
		r.Job("container", flow.Tags("kernel"), flow.Image("registry.labtwo/e2e:1"),
			flow.Cmd("sh", "-c", "echo in-container"), flow.Env("CONTAINER_VAR", "cv"))
		mainSub = c.submit(r)
		if mainSub.Context == nil || !mainSub.Context.Created {
			t.Fatalf("first context upload not created: %+v", mainSub.Context)
		}
		mainRun = c.mustSucceed(mainSub.ID)
		// The secret-using job succeeded: hourchd deleted the run's secrets.
		for _, k := range c.obj.Keys() {
			if strings.HasPrefix(k, secrets.Prefix) {
				t.Fatalf("secrets object kept after the run succeeded: %s", k)
			}
		}

		prepJ := c.job(c.jobByName(*mainRun, "prep").Id)
		consJ := c.job(c.jobByName(*mainRun, "consume").Id)
		// Artifact hand-off across workers, outputs and substitution.
		if prepJ.Worker != "tiny-1" || consJ.Worker != "kernel-1" {
			t.Fatalf("prep on %s, consume on %s", prepJ.Worker, consJ.Worker)
		}
		if prepJ.Outputs["greeting"] != "hello-ctx" || prepJ.Outputs["number"] != "42" || consJ.Outputs["consumed"] != "ok" {
			t.Fatalf("outputs prep=%v consume=%v", prepJ.Outputs, consJ.Outputs)
		}
		if strings.Join(consJ.Argv, " ") != bin+" "+helperArg+" consume hello-ctx" || consJ.Env["GREETING"] != "hello-ctx" {
			t.Fatalf("consume argv %q env %v", consJ.Argv, consJ.Env)
		}
		if strings.Join(consJ.DependsOn, ",") != "prep" {
			t.Fatalf("consume depends_on %v", consJ.DependsOn)
		}
		var paths []string
		for _, a := range prepJ.UploadedArtifacts {
			paths = append(paths, a.Path)
		}
		sort.Strings(paths)
		if strings.Join(paths, ",") != "out/dir/a.txt,out/dir/sub/b.txt,out/file.txt" {
			t.Fatalf("prep artifacts %v", paths)
		}
		resp, b := c.do("GET", "/v1/jobs/"+prepJ.Id+"/artifacts/out/file.txt", opsToken, nil)
		if resp.StatusCode != 200 || string(b) != "file:hello-ctx" {
			t.Fatalf("artifact download = %d %q", resp.StatusCode, b)
		}
		resp, b = c.do("GET", "/v1/jobs/"+prepJ.Id+"/artifacts/out/dir/", opsToken, nil)
		if resp.StatusCode != 200 {
			t.Fatalf("dir artifact download = %d %s", resp.StatusCode, b)
		}
		if got := tarNames(t, b); got != "a.txt,sub/b.txt" {
			t.Fatalf("dir artifact tar = %s", got)
		}
		// The same download through the UI (proxied, basic auth).
		if body := c.uiPage("/ui/jobs/" + prepJ.Id + "/artifacts/out/file.txt"); body != "file:hello-ctx" {
			t.Fatalf("UI artifact download %q", body)
		}

		// The secret: delivered (the job checked its hash), only to the job
		// naming it (nosecret checked its env), redacted in the stored log.
		secJ := c.jobByName(*mainRun, "secret")
		lg := c.log(secJ.Id, 1)
		if !strings.Contains(lg, "secret is *** (len 19)") || strings.Contains(lg, secretValue) {
			t.Fatalf("secret log not redacted: %q", lg)
		}
		if strings.Join(c.job(secJ.Id).Secrets, ",") != "E2E_SECRET" || strings.Join(mainRun.SecretNames, ",") != "E2E_SECRET" {
			t.Fatalf("secret names: job %v run %v", c.job(secJ.Id).Secrets, mainRun.SecretNames)
		}

		// Counters and builds through the job token; the token dies with
		// the job.
		recJ := c.job(c.jobByName(*mainRun, "record").Id)
		var builds []api.Build
		c.getJSON("/v1/builds?type=e2e", &builds)
		if len(builds) != 1 || builds[0].BuildNum != 1 || builds[0].RunId != mainRun.Id || builds[0].JobId != recJ.Id ||
			builds[0].Flavour != flow.BuildFlavour("debian", "amd64", "") || recJ.Outputs["build_num"] != "1" {
			t.Fatalf("builds = %+v outputs %v", builds, recJ.Outputs)
		}
		tok := c.waitShared("job.token")
		if !strings.HasPrefix(tok, "hjt_") {
			t.Fatalf("job token %q", tok)
		}
		jc, err := flow.NewJobClient(c.front.URL, tok)
		if err != nil {
			t.Fatal(err)
		}
		_, err = jc.NextCounter(context.Background(), "e2e-extra")
		var ae *flow.APIError
		if !errors.As(err, &ae) || ae.Status != 401 {
			t.Fatalf("finished job's token: %v", err)
		}
		if strings.Contains(c.log(recJ.Id, 1), tok) {
			t.Fatal("job token appears in the job log")
		}

		// The container job went through docker create/start with the
		// env file.
		contJ := c.jobByName(*mainRun, "container")
		if lg := c.log(contJ.Id, 1); !strings.Contains(lg, "fake container ran: hourch-") {
			t.Fatalf("container log %q", lg)
		}
		argv := readLines(t, filepath.Join(c.dockRec, "create.argv"))
		joined := strings.Join(argv, " ")
		for _, want := range []string{"create --rm --name hourch-", "--env-file ", ":/work -w /work registry.labtwo/e2e:1 sh -c echo in-container"} {
			if !strings.Contains(joined, want) {
				t.Fatalf("docker create argv %q lacks %q", joined, want)
			}
		}
		envFile := readLines(t, filepath.Join(c.dockRec, "env-file"))
		envs := strings.Join(envFile, "\n")
		for _, want := range []string{"CONTAINER_VAR=cv", "HOURCH_JOB_TOKEN=hjt_", "HOURCH_OUTPUTS=/work/.hourch/outputs", "HOURCH_JOB_ID=" + contJ.Id} {
			if !strings.Contains(envs, want) {
				t.Fatalf("env file lacks %q:\n%s", want, envs)
			}
		}
		if strings.Contains(envs, "PATH=") {
			t.Fatal("container env file carries the host PATH")
		}
		if m := strings.TrimSpace(strings.Join(readLines(t, filepath.Join(c.dockRec, "env-file.mode")), "")); m != "600" {
			t.Fatalf("env file mode %s", m)
		}
		if left, _ := filepath.Glob(filepath.Join(c.dir, "work-kernel-1", ".hourch-env-*")); len(left) != 0 {
			t.Fatalf("env file left behind: %v", left)
		}

		// No page of the UI shows the secret value.
		pages := []string{"/ui/runs", "/ui/runs/" + mainRun.Id, "/ui/workers"}
		for _, j := range mainRun.Jobs {
			pages = append(pages, "/ui/jobs/"+j.Id, "/ui/jobs/"+j.Id+"/log", "/ui/jobs/"+j.Id+"/log/stream")
		}
		for _, p := range pages {
			if strings.Contains(c.uiPage(p), secretValue) {
				t.Fatalf("UI page %s shows the secret value", p)
			}
		}
	})

	c.step("concurrency key and context dedupe", func(t *testing.T) {
		r := flow.NewRun("e2e-locks", nil)
		r.Context(ctxDir) // unchanged tree: deduped
		r.Job("lock-a", flow.Cmd(bin, helperArg, "lock", "a"), flow.Concurrency("e2e-lock"), env)
		r.Job("lock-b", flow.Cmd(bin, helperArg, "lock", "b"), flow.Concurrency("e2e-lock"), env)
		sub := c.submit(r)
		if sub.Context == nil || sub.Context.Created || sub.Context.Ref != mainSub.Context.Ref {
			t.Fatalf("context not deduped: %+v (first %s)", sub.Context, mainSub.Context.Ref)
		}
		run := c.mustSucceed(sub.ID)
		a, b := c.job(c.jobByName(*run, "lock-a").Id), c.job(c.jobByName(*run, "lock-b").Id)
		first, second := a, b
		if b.StartedAt.Before(*a.StartedAt) {
			first, second = b, a
		}
		// Two idle workers could run both at once; the key serialised them.
		if second.StartedAt.Before(*first.FinishedAt) {
			t.Fatalf("%s started %v before %s finished %v", second.Name, second.StartedAt, first.Name, first.FinishedAt)
		}
	})

	c.step("cancel a running job", func(t *testing.T) {
		r := flow.NewRun("e2e-cancel", nil)
		r.Job("sleeper", flow.Tags("tiny"), flow.Cmd(bin, helperArg, "block", "sleeper"), env)
		sub := c.submit(r)
		jobID := c.waitShared("sleeper.started")
		resp, b := c.do("POST", "/v1/jobs/"+jobID+"/cancel", opsToken, nil)
		if resp.StatusCode != 200 {
			t.Fatalf("cancel = %d %s", resp.StatusCode, b)
		}
		run := c.wait(sub.ID)
		j := c.job(jobID)
		if run.Status != api.RunStatusCancelled || j.Status != api.JobStatusCancelled || j.Error != "cancelled" {
			t.Fatalf("after cancel: run %s job %s %q", run.Status, j.Status, j.Error)
		}
		if j.ExitCode == nil || *j.ExitCode != 143 {
			t.Fatalf("cancelled job exit code %v (want 143, SIGTERM)", j.ExitCode)
		}
	})

	c.step("failure, timeout and dependency cancel", func(t *testing.T) {
		r := flow.NewRun("e2e-fail", nil)
		boom := r.Job("boom", flow.Tags("tiny"), flow.Cmd(bin, helperArg, "fail"), env)
		r.Job("after-boom", flow.Cmd(bin, helperArg, "noop"), flow.After(boom), env)
		r.Job("slow", flow.Tags("kernel"), flow.Cmd(bin, helperArg, "block", "slow"), flow.Timeout(time.Second), env)
		sub := c.submit(r)
		run := c.wait(sub.ID)
		if run.Status != api.RunStatusFailed {
			t.Fatalf("run %s", run.Status)
		}
		b, a, s := c.job(c.jobByName(*run, "boom").Id), c.job(c.jobByName(*run, "after-boom").Id), c.job(c.jobByName(*run, "slow").Id)
		if b.Status != api.JobStatusFailed || b.Error != "exit_code" || b.ExitCode == nil || *b.ExitCode != 3 {
			t.Fatalf("boom %s %q %v", b.Status, b.Error, b.ExitCode)
		}
		if !strings.Contains(c.log(b.Id, 1), "fail: about to exit 3") {
			t.Fatal("failed job's log missing")
		}
		if a.Status != api.JobStatusCancelled || a.Error != "dependency_failed" || a.Attempt != 0 {
			t.Fatalf("after-boom %s %q attempt %d", a.Status, a.Error, a.Attempt)
		}
		if s.Status != api.JobStatusFailed || s.Error != "timeout" {
			t.Fatalf("slow %s %q", s.Status, s.Error)
		}
		// The timed-out job's worker killed it and is free again.
		waitFor(t, "workers idle", 15*time.Second, func() bool {
			var ws []api.Worker
			c.getJSON("/v1/workers", &ws)
			idle := 0
			for _, w := range ws {
				if w.Connected && w.CurrentJob == nil {
					idle++
				}
			}
			return idle == 2
		})
	})

	c.step("lease lapse requeues to the other worker", func(t *testing.T) {
		r := flow.NewRun("e2e-lapse", nil)
		r.Job("victim", flow.Cmd(bin, helperArg, "victim"), flow.Attempts(2), env)
		sub := c.submit(r)
		jobID := c.waitShared("victim.started")
		holder := c.job(jobID).Worker
		c.killWorker(holder) // no result: hourchd only sees the lease lapse
		run := c.mustSucceed(sub.ID)
		j := c.job(jobID)
		if j.Attempt != 2 || len(j.Attempts) != 2 {
			t.Fatalf("attempts %d %+v", j.Attempt, j.Attempts)
		}
		a1, a2 := j.Attempts[0], j.Attempts[1]
		if a1.Worker != holder || a1.Error != "lease_expired" || a2.Worker == holder || a2.ExitCode == nil || *a2.ExitCode != 0 {
			t.Fatalf("attempts: %+v / %+v (killed %s)", a1, a2, holder)
		}
		_ = run
		// Bring the killed worker back for the remaining steps.
		if holder == "tiny-1" {
			c.startWorker("tiny-1", "sdf1.labtwo", []string{"tiny"}, nil)
		} else {
			c.startWorker("kernel-1", "veritech01.labtwo", []string{"kernel"}, []string{"docker"})
		}
	})

	c.step("hourchd restart mid-job", func(t *testing.T) {
		// survivor runs across the restart; later needs a secret and is
		// still pending when hourchd restarts, so its secrets (sealed under
		// the old process's key) are gone and it fails at startup.
		r := flow.NewRun("e2e-restart", nil)
		sec := r.Secret("E2E_SECRET", secretValue)
		surv := r.Job("survivor", flow.Tags("tiny"), flow.Cmd(bin, helperArg, "block", "survivor"), env)
		r.Job("later", flow.Tags("tiny"), flow.Cmd(bin, helperArg, "secret"), flow.Secrets(sec), env,
			flow.Env("E2E_SECRET_SHA", hex.EncodeToString(sum[:])), flow.After(surv))
		sub := c.submit(r)
		oldInstance := c.a.Engine.Instance()
		oldObject := secrets.ObjectKey(oldInstance, sub.ID)
		if _, ok := c.obj.Bytes(oldObject); !ok {
			t.Fatalf("no secrets object %s; keys %v", oldObject, c.obj.Keys())
		}
		jobID := c.waitShared("survivor.started")
		waitFor(t, "first log line stored", 10*time.Second, func() bool {
			return strings.Contains(c.log(jobID, 1), "block: started survivor")
		})
		// A workflow binary waiting on the run and tailing the log while
		// hourchd restarts (flow.Wait and flow.StreamLog must ride it out).
		cl := c.client().c
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		waited := make(chan *api.Run, 1)
		waitErr := make(chan error, 1)
		go func() {
			r, err := flow.Wait(ctx, cl, sub.ID)
			waitErr <- err
			waited <- r
		}()
		var tail syncBuf
		tailEnd := make(chan *api.LogEndEvent, 1)
		tailErr := make(chan error, 1)
		go func() {
			end, err := flow.StreamLog(ctx, cl, jobID, 1, &tail)
			tailErr <- err
			tailEnd <- end
		}()
		waitFor(t, "tail started", 10*time.Second, func() bool { return strings.Contains(tail.String(), "block: started survivor") })
		c.restartHourchd()
		if c.a.Engine.Instance() == oldInstance {
			t.Fatal("instance id unchanged across the restart")
		}
		// The old object is gone at once, and the pending secret-using job
		// failed before survivor finished.
		for _, k := range c.obj.Keys() {
			if strings.HasPrefix(k, secrets.Prefix) {
				t.Fatalf("secrets object survived the restart: %s", k)
			}
		}
		if later := c.jobByName(c.run(sub.ID), "later"); later.Status != api.JobStatusFailed || later.Error != "secret_unavailable" {
			t.Fatalf("later after restart = %s %q", later.Status, later.Error)
		}
		// The worker re-attaches the running job (no requeue).
		waitFor(t, "re-attach", 20*time.Second, func() bool {
			evs, err := c.a.Store.JobEvents(jobID)
			if err != nil {
				return false
			}
			for _, e := range evs {
				if e.Kind == "reattached" {
					return true
				}
			}
			return false
		})
		c.release("survivor")
		run := c.wait(sub.ID)
		if run.Status != api.RunStatusFailed {
			t.Fatalf("restart run = %s, want failed (later lost its secret)", run.Status)
		}
		if s := c.jobByName(*run, "survivor"); s.Status != api.JobStatusSucceeded {
			t.Fatalf("survivor = %s", s.Status)
		}
		j := c.job(jobID)
		if j.Attempt != 1 || len(j.Attempts) != 1 || j.Outputs["survived"] != "yes" {
			t.Fatalf("after restart: attempt %d attempts %+v outputs %v", j.Attempt, j.Attempts, j.Outputs)
		}
		lg := c.log(jobID, 1)
		if !strings.Contains(lg, "block: started survivor") || !strings.Contains(lg, "block: released survivor") {
			t.Fatalf("log across the restart: %q", lg)
		}
		_ = run
		if err := <-waitErr; err != nil {
			t.Fatalf("flow.Wait across the restart: %v", err)
		}
		if r := <-waited; r == nil || r.Status != api.RunStatusFailed {
			t.Fatalf("flow.Wait across the restart returned %+v", r)
		}
		var st api.Stats
		c.getJSON("/v1/stats", &st)
		if st.RunsHoldingSecrets != 0 {
			t.Fatalf("runs holding secrets after restart = %d", st.RunsHoldingSecrets)
		}
		if err := <-tailErr; err != nil {
			t.Fatalf("flow.StreamLog across the restart: %v (got %q)", err, tail.String())
		}
		if end := <-tailEnd; end == nil || end.Status != api.JobStatusSucceeded {
			t.Fatalf("log tail end %+v", end)
		}
		if got := tail.String(); got != lg {
			t.Fatalf("tailed log differs from the stored log:\ntail:   %q\nstored: %q", got, lg)
		}
	})

	c.step("powerwarden demand while no worker covers", func(t *testing.T) {
		c.waitWorker("kernel-1", true)
		waitFor(t, "kernel-1 idle in powerwarden", 10*time.Second, func() bool {
			return c.pw.Workers()["kernel-1"].JobStatus == pw.JobStatusIdle
		})
		c.drainWorker("kernel-1")
		waitFor(t, "kernel-1 removed from powerwarden", 10*time.Second, func() bool {
			_, ok := c.pw.Workers()["kernel-1"]
			return !ok
		})
		r := flow.NewRun("e2e-demand", nil)
		r.Job("needs-kernel", flow.Tags("kernel"), flow.Cmd(bin, helperArg, "noop"), env)
		sub := c.submit(r)
		jobID := sub.Run.Jobs[0].Id
		did := "hourchd:" + jobID
		waitFor(t, "demand published", 10*time.Second, func() bool { _, ok := c.pw.Demands()[did]; return ok })
		if d := c.pw.Demands()[did]; strings.Join(d.Tags, ",") != "kernel" || d.Source != pw.DemandSourceHourchd {
			t.Fatalf("demand %+v", d)
		}
		// The tiny worker is idle but does not cover it.
		if c.pw.Workers()["tiny-1"].JobStatus != pw.JobStatusIdle {
			t.Fatalf("tiny-1 in powerwarden: %+v", c.pw.Workers()["tiny-1"])
		}
		c.startWorker("kernel-1", "veritech01.labtwo", []string{"kernel"}, []string{"docker"})
		c.mustSucceed(sub.ID)
		waitFor(t, "demand removed", 10*time.Second, func() bool { _, ok := c.pw.Demands()[did]; return !ok })
		busy := false
		for _, op := range c.pw.Ops() {
			if op.Kind == "worker" && op.ID == "kernel-1" && op.Worker != nil && op.Worker.JobStatus == pw.JobStatusBusy &&
				op.Worker.DemandId != nil && *op.Worker.DemandId == did && op.Worker.Hostname == "veritech01.labtwo" {
				busy = true
			}
		}
		if !busy {
			t.Fatalf("kernel-1 never reported busy with %s: %v", did, c.pw.Ops())
		}
		var h api.Healthz
		c.getJSON("/v1/healthz", &h)
		if h.Powerwarden != api.HealthzPowerwardenConnected {
			t.Fatalf("healthz powerwarden %s", h.Powerwarden)
		}
		if ids := c.pw.DemandIDs(); len(ids) != 0 {
			t.Fatalf("left-over demands %v", ids)
		}
	})

	c.step("unsatisfiable submission and server-side dry run", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cl := c.client().c
		before := len(c.runsOf("e2e-unsat"))
		// powerwarden knows a gpu host, but it is locked.
		c.pw.SetHosts(pw.HostView{Hostname: "gpu1.labtwo", Tags: []string{"gpu"}, Locked: true})
		bad := flow.NewRun("e2e-unsat", nil)
		bad.Job("train", flow.Tags("gpu"), flow.Cmd(bin, helperArg, "noop"), env)
		for name, err := range map[string]error{
			"check": bad.Check(ctx, cl),
			"submit": func() error {
				_, err := bad.Submit(ctx, cl)
				return err
			}(),
		} {
			var ae *flow.APIError
			if !errors.As(err, &ae) || ae.Status != 422 || ae.Code != string(api.ErrorCodeUnsatisfiable) ||
				ae.Message != `job "train" needs tags [gpu]; known workers offer [kernel] + docker, [tiny]; powerwarden has no unlocked, non-excluded host with tags` {
				t.Fatalf("%s: %v", name, err)
			}
		}
		if n := len(c.runsOf("e2e-unsat")); n != before {
			t.Fatalf("an unsatisfiable run was stored (%d runs)", n)
		}
		// A valid graph (a secret, a container job, a context) passes the
		// check and stores nothing.
		good := flow.NewRun("e2e-check", nil)
		good.Context(ctxDir)
		sec := good.Secret("E2E_SECRET", secretValue)
		good.Job("t", flow.Tags("tiny"), flow.Cmd(bin, helperArg, "noop"), flow.Secrets(sec))
		good.Job("k", flow.Tags("kernel"), flow.Image("registry.labtwo/e2e:1"), flow.Cmd("true"))
		if err := good.Check(ctx, cl); err != nil {
			t.Fatal(err)
		}
		if n := len(c.runsOf("e2e-check")); n != 0 {
			t.Fatalf("Check stored %d runs", n)
		}
	})
}

func (c *cluster) runsOf(workflow string) []api.RunSummary {
	c.t.Helper()
	var runs []api.RunSummary
	c.getJSON("/v1/runs?workflow="+workflow, &runs)
	return runs
}

// syncBuf is a goroutine-safe buffer.
type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func readLines(t *testing.T, p string) []string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

func tarNames(t *testing.T, b []byte) string {
	t.Helper()
	tr := tar.NewReader(strings.NewReader(string(b)))
	var names []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar: %v", err)
		}
		if h.Typeflag == tar.TypeReg {
			names = append(names, h.Name)
		}
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}
