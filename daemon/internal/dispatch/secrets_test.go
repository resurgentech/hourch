package dispatch

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/secrets"
	"github.com/resurgentech/hourch/daemon/internal/store"
)

// Volatile-key secrets.

// assertNoValueInDB fails if any value appears anywhere in SQLite.
func assertNoValueInDB(t *testing.T, e *env, values ...string) {
	t.Helper()
	tables, err := e.st.DB().Query(`SELECT name FROM sqlite_master WHERE type = 'table'`)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for tables.Next() {
		var n string
		_ = tables.Scan(&n)
		names = append(names, n)
	}
	tables.Close()
	for _, tb := range names {
		rows, err := e.st.DB().Query(`SELECT * FROM ` + tb)
		if err != nil {
			t.Fatal(err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				t.Fatal(err)
			}
			for _, v := range vals {
				s := fmt.Sprint(v)
				if b, ok := v.([]byte); ok {
					s = string(b)
				}
				for _, sec := range values {
					if strings.Contains(s, sec) {
						t.Fatalf("table %s holds a secret value", tb)
					}
				}
			}
		}
		rows.Close()
	}
	if _, err := e.st.DB().Exec(`SELECT 1 FROM run_secrets`); err == nil {
		t.Fatal("run_secrets still exists")
	}
}

func holding(t *testing.T, e *env) int {
	t.Helper()
	n, err := e.eng.RunsHoldingSecrets()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAssignGetsOnlyNamedSecrets(t *testing.T) {
	e := newEnv(t)
	r := e.submit(map[string]string{"A": "value-a", "B": "value-b", "C": "value-c"},
		job("one", secretsOf("A")), job("two", secretsOf("B", "C"), after("one")), job("none", after("two")))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	if len(a.Secrets) != 1 || a.Secrets["A"] != "value-a" {
		t.Fatalf("one got %v", a.Secrets)
	}
	e.finish(s, a.Job.ID, 0, nil)
	a = lastAssign(t, c)
	if len(a.Secrets) != 2 || a.Secrets["B"] != "value-b" || a.Secrets["C"] != "value-c" {
		t.Fatalf("two got %v", a.Secrets)
	}
	e.finish(s, a.Job.ID, 0, nil)
	if a = lastAssign(t, c); a.Job.Name != "none" || a.Secrets != nil {
		t.Fatalf("none got %+v", a)
	}
	e.finish(s, a.Job.ID, 0, nil)
	if e.run(r.ID).Status != store.RunSucceeded {
		t.Fatal("run")
	}
}

// The object goes as soon as every secret-using job has succeeded, even
// while the run is still running; a later failure does not bring it back.
func TestSecretsDeletedWhenSecretJobsSucceed(t *testing.T) {
	e := newEnv(t)
	r := e.submit(map[string]string{"S": "s-val", "T": "t-val"},
		job("a", secretsOf("S")), job("b", secretsOf("T"), after("a")), job("c", after("b")))
	k := e.box.Key(r.ID)
	if holding(t, e) != 1 {
		t.Fatal("not holding")
	}
	s, c := e.connect("w", nil, nil, "")
	e.finish(s, lastAssign(t, c).Job.ID, 0, nil)
	if _, ok := e.obj.Bytes(k); !ok || holding(t, e) != 1 {
		t.Fatal("object deleted while b (secret-using) is still pending")
	}
	e.finish(s, lastAssign(t, c).Job.ID, 0, nil)
	if _, ok := e.obj.Bytes(k); ok || holding(t, e) != 0 {
		t.Fatal("object kept after every secret-using job succeeded")
	}
	if e.run(r.ID).Status != store.RunRunning {
		t.Fatal("run should still be running (c)")
	}
	e.finish(s, lastAssign(t, c).Job.ID, 1, nil)
	if e.run(r.ID).Status != store.RunFailed || holding(t, e) != 0 {
		t.Fatal("run")
	}
	// No secrets submitted with no job naming one: nothing is stored.
	r2 := e.submit(map[string]string{"UNUSED": "u"}, job("x"))
	if _, err := e.st.GetSecretObject(r2.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("stored secrets no job names")
	}
}

func TestSecretsKeptOnFailureAndCancel(t *testing.T) {
	e := newEnv(t)
	r := e.submit(map[string]string{"S": "v"}, job("a", secretsOf("S")))
	s, c := e.connect("w", nil, nil, "")
	e.finish(s, lastAssign(t, c).Job.ID, 2, nil)
	if e.run(r.ID).Status != store.RunFailed {
		t.Fatal("run")
	}
	if _, ok := e.obj.Bytes(e.box.Key(r.ID)); !ok || !e.eng.HoldsSecrets(r.ID) {
		t.Fatal("failed run lost its secrets")
	}
	r2 := e.submit(map[string]string{"S": "v"}, job("a", secretsOf("S"), tags("nobody")))
	if err := e.eng.CancelRun(r2.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok := e.obj.Bytes(e.box.Key(r2.ID)); !ok {
		t.Fatal("cancelled run lost its secrets")
	}
	if holding(t, e) != 2 {
		t.Fatalf("holding = %d", holding(t, e))
	}
}

func TestRetryReusesStoredSecrets(t *testing.T) {
	e := newEnv(t)
	r := e.submit(map[string]string{"S": "orig-s", "T": "orig-t"}, job("a", secretsOf("S", "T")))
	s, c := e.connect("w", nil, nil, "")
	e.finish(s, lastAssign(t, c).Job.ID, 1, nil)
	nr, err := e.eng.Retry(context.Background(), "t", r.ID, map[string]string{"T": "new-t"})
	if err != nil {
		t.Fatal(err)
	}
	a := lastAssign(t, c)
	if a.Job.RunID != nr.ID || a.Secrets["S"] != "orig-s" || a.Secrets["T"] != "new-t" || len(a.Secrets) != 2 {
		t.Fatalf("retry assign = %+v", a)
	}
	if strings.Join(e.run(nr.ID).SecretNames, ",") != "S,T" {
		t.Fatalf("names = %v", e.run(nr.ID).SecretNames)
	}
	// The retry sealed its own object; the original keeps its own.
	if _, ok := e.obj.Bytes(e.box.Key(nr.ID)); !ok {
		t.Fatal("retry has no object")
	}
	if !e.eng.HoldsSecrets(r.ID) {
		t.Fatal("original lost its object")
	}
	e.finish(s, a.Job.ID, 0, nil)
	if e.eng.HoldsSecrets(nr.ID) {
		t.Fatal("successful retry kept its secrets")
	}
	// A no-body retry of the original still works (UI path).
	if _, err := e.eng.Retry(context.Background(), "t", r.ID, nil); err != nil {
		t.Fatal(err)
	}
}

func TestRetryAfterSecretsGone(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(t0)
	obj := objects.NewMem()
	e := openEnv(t, dir, clk, obj)
	r := e.submit(map[string]string{"S": "v"}, job("a", secretsOf("S")), job("b", after("a")))
	s, c := e.connect("w", nil, nil, "")
	e.finish(s, lastAssign(t, c).Job.ID, 0, nil) // a succeeded: secrets deleted
	e.finish(s, lastAssign(t, c).Job.ID, 1, nil) // b failed: run failed
	var ms *graph.MissingSecret
	if _, err := e.eng.Retry(context.Background(), "t", r.ID, nil); !errors.As(err, &ms) || !strings.Contains(ms.Msg, "gone") {
		t.Fatalf("retry after deletion: %v", err)
	}

	r2 := e.submit(map[string]string{"S": "v"}, job("a", secretsOf("S"), tags("nobody")))
	if err := e.eng.CancelRun(r2.ID); err != nil {
		t.Fatal(err)
	}
	e.eng.WaitUploads()
	e.st.Close()
	e2 := openEnv(t, dir, clk, obj) // restart: new key, new instance
	if err := e2.eng.Recover(); err != nil {
		t.Fatal(err)
	}
	_, err := e2.eng.Retry(context.Background(), "t", r2.ID, nil)
	if !errors.As(err, &ms) || !strings.Contains(ms.Msg, "hourchd restarted") || !strings.Contains(ms.Msg, `"S"`) {
		t.Fatalf("retry after restart: %v", err)
	}
	if _, err := e2.eng.Retry(context.Background(), "t", r2.ID, map[string]string{"S": "resent"}); err != nil {
		t.Fatalf("retry with the values: %v", err)
	}
}

// A restart: the old instance's objects go, pending/ready secret-using jobs
// fail at once with their dependents cancelled, leased jobs carry on.
func TestRestartScrubsSecrets(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(t0)
	obj := objects.NewMem()
	e := openEnv(t, dir, clk, obj)
	// r1: a runs (no secret); b needs S after a (pending); d after b.
	// c needs S and is ready (no worker free); x is ready without secrets.
	r1 := e.submit(map[string]string{"S": "s1"},
		job("a", tags("busy")), job("b", secretsOf("S"), after("a")), job("d", after("b")),
		job("c", secretsOf("S"), tags("idle")), job("x", tags("idle")))
	// r2: its secret-using job is running.
	r2 := e.submit(map[string]string{"S": "s2"}, job("z", secretsOf("S"), tags("busy2")))
	s1, c1 := e.connect("w1", []string{"busy"}, nil, "")
	aJob := lastAssign(t, c1).Job.ID
	e.send(s1, &proto.Accept{JobID: aJob})
	s2, c2 := e.connect("w2", []string{"busy2"}, nil, "")
	za := lastAssign(t, c2)
	if za.Secrets["S"] != "s2" {
		t.Fatal("z did not get its secret")
	}
	e.send(s2, &proto.Accept{JobID: za.Job.ID})
	oldKeys := []string{e.box.Key(r1.ID), e.box.Key(r2.ID)}
	// An object of some even older instance, never recorded.
	if err := obj.Put(context.Background(), secrets.ObjectKey("01AAAAAAAAAAAAAAAAAAAAAAAA", "01BBBBBBBBBBBBBBBBBBBBBBBB"), strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	if holding(t, e) != 2 {
		t.Fatal("holding")
	}
	e.st.Close()

	clk.Advance(time.Minute)
	e2 := openEnv(t, dir, clk, obj)
	if e2.box.Instance() == e.box.Instance() {
		t.Fatal("same instance id after restart")
	}
	if err := e2.eng.Recover(); err != nil {
		t.Fatal(err)
	}
	for _, k := range obj.Keys() {
		if strings.HasPrefix(k, secrets.Prefix) {
			t.Fatalf("secrets object survived the restart: %s (old %v)", k, oldKeys)
		}
	}
	if holding(t, e2) != 0 {
		t.Fatal("ownership rows survived the restart")
	}
	for name, want := range map[string][2]string{
		"b": {store.JobFailed, ReasonSecretUnavailable},
		"c": {store.JobFailed, ReasonSecretUnavailable},
		"d": {store.JobCancelled, ReasonDependencyFailed},
		"a": {store.JobLeased, ""},
		"x": {store.JobReady, ""},
	} {
		if j := e2.job(r1.ID, name); j.Status != want[0] || j.Error != want[1] {
			t.Errorf("%s = %s/%q, want %v", name, j.Status, j.Error, want)
		}
	}
	if j := e2.job(r2.ID, "z"); j.Status != store.JobLeased {
		t.Fatalf("leased secret job touched: %+v", j)
	}
	// The running jobs re-attach and finish.
	s1b, _ := e2.connect("w1", []string{"busy"}, nil, aJob)
	s2b, _ := e2.connect("w2", []string{"busy2"}, nil, za.Job.ID)
	e2.send(s2b, &proto.Log{JobID: za.Job.ID, Stream: "stdout", Seq: 1, Data: "still here\n"})
	e2.send(s1b, &proto.Result{JobID: aJob, ExitCode: 0})
	e2.send(s2b, &proto.Result{JobID: za.Job.ID, ExitCode: 0})
	if e2.run(r2.ID).Status != store.RunSucceeded {
		t.Fatal("r2 did not finish")
	}
	if j := e2.job(r1.ID, "a"); j.Status != store.JobSucceeded {
		t.Fatalf("a = %+v", j)
	}
	if e2.run(r1.ID).Status != store.RunRunning { // x still waits for an idle worker
		t.Fatal("r1 settled early")
	}
	_, c3 := e2.connect("w3", []string{"idle"}, nil, "")
	e2.send(e2.eng.sessions["w3"], &proto.Accept{JobID: lastAssign(t, c3).Job.ID})
	e2.send(e2.eng.sessions["w3"], &proto.Result{JobID: lastAssign(t, c3).Job.ID, ExitCode: 0})
	if e2.run(r1.ID).Status != store.RunFailed {
		t.Fatalf("r1 = %s", e2.run(r1.ID).Status)
	}
}

// MinIO down when a secret-using job is due: it stays ready (no failure)
// and is assigned once the store is back.
func TestSecretsFetchFailureKeepsJobReady(t *testing.T) {
	e := newEnv(t)
	r := e.submit(map[string]string{"S": "v"}, job("a", secretsOf("S")))
	e.obj.SetFail(errors.New("minio down"))
	_, c := e.connect("w", nil, nil, "")
	if len(ofType[*proto.Assign](c)) != 0 {
		t.Fatal("assigned without secrets")
	}
	if j := e.job(r.ID, "a"); j.Status != store.JobReady {
		t.Fatalf("job = %+v", j)
	}
	e.obj.SetFail(nil)
	e.eng.Reap()
	if a := lastAssign(t, c); a.Secrets["S"] != "v" {
		t.Fatalf("assign = %+v", a)
	}
	// A submission while MinIO is down is 503, and leaves nothing behind.
	e.obj.SetFail(errors.New("minio down"))
	_, err := e.eng.Submit(context.Background(), "t", graph.Submission{Workflow: "wf", Secrets: map[string]string{"S": "v"},
		Jobs: []graph.Job{job("a", secretsOf("S"))}})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("submit with MinIO down: %v", err)
	}
	e.obj.SetFail(nil)
}

// Neither the key nor a value reaches the log, through a whole lifecycle
// including a failed decrypt and a restart.
func TestKeyAndValuesNeverLogged(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(t0)
	obj := objects.NewMem()
	e := openEnv(t, dir, clk, obj)
	val := "logged-secret-value-77"
	r := e.submit(map[string]string{"S": val}, job("a", secretsOf("S")), job("b", secretsOf("S"), after("a")))
	s, c := e.connect("w", nil, nil, "")
	a := lastAssign(t, c)
	e.send(s, &proto.Accept{JobID: a.Job.ID})
	e.send(s, &proto.Log{JobID: a.Job.ID, Stream: "stdout", Seq: 1, Data: val})
	e.send(s, &proto.Result{JobID: a.Job.ID, ExitCode: 1})
	if _, err := e.eng.Retry(context.Background(), "t", r.ID, nil); err != nil {
		t.Fatal(err)
	}
	e.eng.WaitUploads()
	e.st.Close()
	e2 := openEnv(t, dir, clk, obj)
	if err := e2.eng.Recover(); err != nil {
		t.Fatal(err)
	}
	for _, env := range []*env{e, e2} {
		out := env.out.String()
		if out == "" {
			t.Fatal("no log captured")
		}
		for _, enc := range []string{string(env.key), hex.EncodeToString(env.key), base64.StdEncoding.EncodeToString(env.key),
			base64.RawURLEncoding.EncodeToString(env.key), fmt.Sprint(env.key), val} {
			if strings.Contains(out, enc) {
				t.Fatalf("log holds key material or a value:\n%s", out)
			}
		}
	}
	if !strings.Contains(e2.out.String(), "secret") {
		t.Fatal("restart did not log the lost secrets")
	}
}
