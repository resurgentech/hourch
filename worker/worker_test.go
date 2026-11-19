package hourchworker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	hourchdprotocol "github.com/resurgentech/hourch/protocol"
	"github.com/resurgentech/hourch/worker/internal/fakehourchd"
)

func TestHelloFields(t *testing.T) {
	fake := writeFakeDocker(t)
	h := newHarness(t, func(c *Config, o *Options) {
		c.Capabilities = []string{"docker"}
		c.Tags = []string{"tiny", "x86"}
		o.Docker = fake.bin
	})
	h.start()
	ms := h.srv.Messages()
	hello := byType[*hourchdprotocol.Hello](ms)[0]
	want := &hourchdprotocol.Hello{WorkerID: "sdf1-1", Hostname: "sdf1.labtwo", Tags: []string{"tiny", "x86"}, Capabilities: []string{"docker"}, Version: "test-1"}
	if !reflect.DeepEqual(hello, want) {
		t.Fatalf("hello %+v, want %+v", hello, want)
	}
}

func TestNoDockerCapabilityUnlessConfigured(t *testing.T) {
	h := newHarness(t)
	h.start()
	if hello := byType[*hourchdprotocol.Hello](h.srv.Messages())[0]; len(hello.Capabilities) != 0 {
		t.Fatalf("capabilities %v", hello.Capabilities)
	}
	// A container job on this worker never runs.
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Image: "alpine", Argv: []string{"true"}}})
	_, res := h.waitResult(jobA)
	if res.ExitCode != -1 || !strings.Contains(res.Error, "no docker capability") {
		t.Fatalf("result %+v", res)
	}
}

func TestBadTokenIsRejected(t *testing.T) {
	h := newHarness(t)
	h.w.token = "wrong"
	h.w.data.token = "wrong"
	h.startNoWait()
	time.Sleep(50 * time.Millisecond) // several reconnect attempts
	if n := h.srv.Conns(); n != 0 {
		t.Fatalf("%d connections with a bad token", n)
	}
}

func TestHeartbeatCadence(t *testing.T) {
	h := newHarness(t, func(c *Config, o *Options) { o.HeartbeatUnit = 20 * time.Millisecond })
	h.srv.SetWelcome(func(*hourchdprotocol.Hello) *hourchdprotocol.Welcome {
		return &hourchdprotocol.Welcome{HeartbeatSeconds: 1, LeaseSeconds: 3}
	})
	h.start()
	ms := h.waitFor("5 heartbeats", func(ms []fakehourchd.Received) bool {
		return len(byType[*hourchdprotocol.Heartbeat](ms)) >= 5
	})
	var at []time.Time
	for _, r := range ms {
		if hb, ok := r.Msg.(*hourchdprotocol.Heartbeat); ok {
			if hb.JobID != "" {
				t.Fatalf("idle heartbeat with job %s", hb.JobID)
			}
			at = append(at, r.At)
		}
	}
	for i := 1; i < len(at); i++ {
		if d := at[i].Sub(at[i-1]); d < 10*time.Millisecond {
			t.Fatalf("heartbeats %v apart", d)
		}
	}
	if total := at[4].Sub(ms[0].At); total < 80*time.Millisecond {
		t.Fatalf("5 heartbeats in %v, want >= ~100ms", total)
	}
}

func TestJobLifecycle(t *testing.T) {
	h := newHarness(t, func(c *Config, o *Options) { o.HeartbeatUnit = 20 * time.Millisecond })
	h.srv.SetWelcome(func(*hourchdprotocol.Hello) *hourchdprotocol.Welcome {
		return &hourchdprotocol.Welcome{HeartbeatSeconds: 1, LeaseSeconds: 3}
	})
	h.srv.SetContext(jobA, tarball(t, map[string]string{"src/": "", "src/hello.txt": "from context\n", "top.txt": "top\n"}, true))
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{
		ID:      jobA,
		Workdir: "src",
		Argv: helperArgv(t, "cat:hello.txt", "stderr:to stderr", "pwd",
			"outputs:version=1.2.3", "outputs:pair=a=b",
			"write:out/result.bin=artifact body"),
		Artifacts: []string{"src/out/result.bin"},
	}})
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 0 || res.Error != "" {
		t.Fatalf("result %+v\n%s", res, dump(ms))
	}

	// Order: accept first; logs; outputs; result last.
	var order []hourchdprotocol.Type
	for _, r := range ms {
		switch r.Msg.(type) {
		case *hourchdprotocol.Hello, *hourchdprotocol.Heartbeat:
			continue
		}
		if t := r.Msg.MessageType(); len(order) == 0 || order[len(order)-1] != t {
			order = append(order, t)
		}
	}
	want := []hourchdprotocol.Type{hourchdprotocol.TypeAccept, hourchdprotocol.TypeLog, hourchdprotocol.TypeOutput, hourchdprotocol.TypeResult}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("message order %v, want %v\n%s", order, want, dump(ms))
	}
	// Heartbeats during the job name it.
	if hb := byType[*hourchdprotocol.Heartbeat](ms); len(hb) > 0 && hb[len(hb)-1].JobID != jobA && hb[len(hb)-1].JobID != "" {
		t.Fatalf("heartbeat job %q", hb[len(hb)-1].JobID)
	}

	root := filepath.Join(h.cfg.WorkDir, jobA+"-1")
	stdout := logText(ms, jobA, "stdout")
	if stdout != "from context\n"+filepath.Join(root, "src")+"\n" {
		t.Fatalf("stdout %q", stdout)
	}
	if got := logText(ms, jobA, "stderr"); got != "to stderr\n" {
		t.Fatalf("stderr %q", got)
	}
	logs := byType[*hourchdprotocol.Log](ms)
	for i, l := range logs {
		if l.Seq != int64(i+1) {
			t.Fatalf("log %d has seq %d", i, l.Seq)
		}
	}
	outs := byType[*hourchdprotocol.Output](ms)
	if len(outs) != 2 || outs[0].Key != "version" || outs[0].Value != "1.2.3" || outs[1].Value != "a=b" || outs[0].JobID != jobA {
		t.Fatalf("outputs %+v", outs)
	}
	if b, ok := h.srv.Artifact(jobA, "src/out/result.bin"); !ok || string(b) != "artifact body" {
		t.Fatalf("artifact %q %v", b, ok)
	}
	eventually(t, "work dir removed", func() bool {
		_, err := os.Stat(root)
		return os.IsNotExist(err)
	})
	eventually(t, "idle", func() bool { return h.w.Busy() == "" })
}

func TestEnvAllowlistAndJobEnv(t *testing.T) {
	h := newHarness(t, func(c *Config, o *Options) {
		c.EnvAllowlist = []string{"PATH", "HOME", "HOURCH_FOO", "SNEAKY", "MISSING"}
		o.Environ = func() []string {
			return []string{"PATH=" + os.Getenv("PATH"), "HOME=/home/worker", "LANG=C.UTF-8",
				"HOURCH_FOO=inherited", "HOURCH_URL=http://evil", "SNEAKY=" + testToken, "OTHER=nope"}
		}
	})
	h.start()
	h.assign(&hourchdprotocol.Assign{
		Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "env"),
			Env: map[string]string{"JOBVAR": "jv", "HOURCH_RUN_ID": "spoofed"}},
		Secrets:  map[string]string{"API_KEY": "sekrit-value"},
		Attempt:  3,
		JobToken: "jt-123",
	})
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 0 {
		t.Fatalf("result %+v", res)
	}
	env := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(logText(ms, jobA, "stdout")), "\n") {
		k, v, _ := strings.Cut(line, "=")
		env[k] = v
	}
	base := strings.TrimSuffix(strings.TrimPrefix(h.srv.HTTP.URL, ""), "/")
	want := map[string]string{
		"PATH":             os.Getenv("PATH"),
		"HOME":             "/home/worker",
		"JOBVAR":           "jv",
		"API_KEY":          "***", // present, but redacted in the log
		helperEnv:          "1",
		"GORACE":           "atexit_sleep_ms=0",
		"HOURCH_URL":       base,
		"HOURCH_JOB_TOKEN": "***",
		"HOURCH_OUTPUTS":   filepath.Join(h.cfg.WorkDir, jobA+"-3", ".hourch", "outputs"),
		"HOURCH_RUN_ID":    runA,
		"HOURCH_JOB_ID":    jobA,
		"HOURCH_ATTEMPT":   "3",
	}
	if !reflect.DeepEqual(env, want) {
		t.Fatalf("job env\n got %v\nwant %v", env, want)
	}
}

func TestSecretRedaction(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{
		Job:      hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "echo:key=hunter2-secret ok", "stderr:token jt-xyz", "exit:3")},
		Secrets:  map[string]string{"PW": "hunter2-secret"},
		JobToken: "jt-xyz",
	})
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 3 || res.Error != "" {
		t.Fatalf("result %+v", res)
	}
	all := logText(ms, jobA, "")
	if strings.Contains(all, "hunter2") || strings.Contains(all, "jt-xyz") {
		t.Fatalf("secret in logs: %q", all)
	}
	if all != "key=*** ok\ntoken ***\n" && all != "token ***\nkey=*** ok\n" {
		t.Fatalf("logs %q", all)
	}
}

func TestInvalidUTF8Replaced(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "raw:61ff62")}})
	ms, _ := h.waitResult(jobA)
	if got := logText(ms, jobA, "stdout"); got != "a�b" {
		t.Fatalf("%q", got)
	}
}

func TestLogChunksOrderedAndBounded(t *testing.T) {
	h := newHarness(t, func(c *Config, o *Options) { o.ChunkMaxBytes = 1000 })
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "lines:3000")}})
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 0 {
		t.Fatal(res)
	}
	logs := byType[*hourchdprotocol.Log](ms)
	if len(logs) < 30 {
		t.Fatalf("only %d chunks", len(logs))
	}
	var sb strings.Builder
	for i, l := range logs {
		if l.Seq != int64(i+1) {
			t.Fatalf("chunk %d seq %d", i, l.Seq)
		}
		if len(l.Data) > 1000 {
			t.Fatalf("chunk %d is %d bytes", i, len(l.Data))
		}
		sb.WriteString(l.Data)
	}
	var want strings.Builder
	for i := 1; i <= 3000; i++ {
		fmt.Fprintf(&want, "line %06d\n", i)
	}
	if sb.String() != want.String() {
		t.Fatal("log content differs")
	}
}

func TestReconnectResendsAfterLastLogSeq(t *testing.T) {
	dir := t.TempDir()
	trig1, trig2, trig3 := filepath.Join(dir, "t1"), filepath.Join(dir, "t2"), filepath.Join(dir, "t3")
	h := newHarness(t)
	var lastSeq atomic.Int64
	var helloJob atomic.Value
	h.srv.SetWelcome(func(hl *hourchdprotocol.Hello) *hourchdprotocol.Welcome {
		helloJob.Store(hl.RunningJobID)
		return &hourchdprotocol.Welcome{HeartbeatSeconds: 10, LeaseSeconds: 45, LastLogSeq: lastSeq.Load()}
	})
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t,
		"echo:one", "waitfile:"+trig1, "echo:two", "waitfile:"+trig2, "echo:three", "waitfile:"+trig3, "outputs:k=v")}})
	h.waitFor("one", func(ms []fakehourchd.Received) bool { return strings.Contains(logText(ms, jobA, ""), "one") })
	os.WriteFile(trig1, nil, 0o644)
	h.waitFor("two", func(ms []fakehourchd.Received) bool { return strings.Contains(logText(ms, jobA, ""), "two") })

	// hourchd "stored" only seq 1, then the connection drops and stays
	// down while the job prints "three".
	lastSeq.Store(1)
	h.srv.SetReject(true)
	h.srv.DropConn()
	os.WriteFile(trig2, nil, 0o644)
	eventually(t, "three buffered", func() bool {
		h.w.mu.Lock()
		defer h.w.mu.Unlock()
		jr := h.w.cur
		if jr == nil {
			return false
		}
		jr.logs.mu.Lock()
		defer jr.logs.mu.Unlock()
		for _, c := range jr.logs.chunks {
			if strings.Contains(c.data, "three") {
				return true
			}
		}
		return false
	})
	h.srv.SetReject(false)
	h.waitFor("second hello", func(ms []fakehourchd.Received) bool { return len(byType[*hourchdprotocol.Hello](ms)) >= 2 })
	if got := helloJob.Load(); got != jobA {
		t.Fatalf("re-attach hello running_job_id %v", got)
	}
	os.WriteFile(trig3, nil, 0o644)
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 0 {
		t.Fatalf("result %+v", res)
	}
	// On the second connection: logs resume at seq 2 with "two", then
	// "three" at 3; nothing at seq 1 again.
	var second []*hourchdprotocol.Log
	for _, r := range ms {
		if l, ok := r.Msg.(*hourchdprotocol.Log); ok && r.Conn == 2 {
			second = append(second, l)
		}
	}
	if len(second) != 2 || second[0].Seq != 2 || second[0].Data != "two\n" || second[1].Seq != 3 || second[1].Data != "three\n" {
		t.Fatalf("second connection logs: %s", dump(ms))
	}
	if outs := byType[*hourchdprotocol.Output](ms); len(outs) != 1 {
		t.Fatalf("outputs %+v", outs)
	}
	// Accept was not repeated.
	if n := len(byType[*hourchdprotocol.Accept](ms)); n != 1 {
		t.Fatalf("%d accepts", n)
	}
}

func TestResultDeliveredAfterReconnect(t *testing.T) {
	dir := t.TempDir()
	trig := filepath.Join(dir, "t")
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "waitfile:"+trig, "echo:done", "outputs:x=1")}})
	h.waitFor("accept", func(ms []fakehourchd.Received) bool { return len(byType[*hourchdprotocol.Accept](ms)) == 1 })
	h.srv.SetReject(true)
	h.srv.DropConn()
	os.WriteFile(trig, nil, 0o644)
	eventually(t, "job finished offline", func() bool {
		h.w.mu.Lock()
		defer h.w.mu.Unlock()
		return h.w.cur != nil && h.w.cur.finished
	})
	h.srv.SetReject(false)
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 0 || logText(ms, jobA, "") != "done\n" || len(byType[*hourchdprotocol.Output](ms)) != 1 {
		t.Fatalf("after reconnect: %s", dump(ms))
	}
}

func TestAbandonKillsJob(t *testing.T) {
	dir := t.TempDir()
	pidf := filepath.Join(dir, "pid")
	h := newHarness(t, func(c *Config, o *Options) { o.HeartbeatUnit = 10 * time.Millisecond })
	var abandon atomic.Bool
	h.srv.SetWelcome(func(hl *hourchdprotocol.Hello) *hourchdprotocol.Welcome {
		w := &hourchdprotocol.Welcome{HeartbeatSeconds: 1, LeaseSeconds: 45}
		if abandon.Load() {
			w.Abandon = hl.RunningJobID
		}
		return w
	})
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "pidfile:"+pidf, "sleep")}})
	pid := readPid(t, pidf)
	abandon.Store(true)
	h.srv.DropConn()
	waitDead(t, pid)
	eventually(t, "idle", func() bool { return h.w.Busy() == "" })
	ms := h.waitFor("idle heartbeat on conn 2", func(ms []fakehourchd.Received) bool {
		for _, r := range ms {
			if hb, ok := r.Msg.(*hourchdprotocol.Heartbeat); ok && r.Conn == 2 && hb.JobID == "" {
				return true
			}
		}
		return false
	})
	if hl := byType[*hourchdprotocol.Hello](ms); len(hl) < 2 || hl[1].RunningJobID != jobA {
		t.Fatalf("hellos %s", dump(ms))
	}
	if r := byType[*hourchdprotocol.Result](ms); len(r) != 0 {
		t.Fatalf("result sent for an abandoned job: %+v", r)
	}
	// The worker takes new work afterwards.
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobB, Argv: helperArgv(t, "echo:next")}})
	if _, res := h.waitResult(jobB); res.ExitCode != 0 {
		t.Fatal(res)
	}
}

func TestCancelKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	pidf, gpidf := filepath.Join(dir, "pid"), filepath.Join(dir, "gpid")
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA,
		Argv: helperArgv(t, "pidfile:"+pidf, "grandchild:"+gpidf, "echo:ready", "sleep")}})
	pid, gpid := readPid(t, pidf), readPid(t, gpidf)
	h.waitFor("ready", func(ms []fakehourchd.Received) bool { return strings.Contains(logText(ms, jobA, ""), "ready") })
	if err := h.srv.Send(&hourchdprotocol.Cancel{JobID: jobA, Reason: "timeout"}); err != nil {
		t.Fatal(err)
	}
	_, res := h.waitResult(jobA)
	if res.ExitCode != 128+15 || res.Error != "cancelled: timeout" {
		t.Fatalf("result %+v", res)
	}
	waitDead(t, pid)
	waitDead(t, gpid)
}

func TestCancelEscalatesToSIGKILL(t *testing.T) {
	dir := t.TempDir()
	pidf := filepath.Join(dir, "pid")
	h := newHarness(t, func(c *Config, o *Options) { o.KillGrace = 50 * time.Millisecond })
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "ignoreterm", "pidfile:"+pidf, "sleep")}})
	readPid(t, pidf)
	h.srv.Send(&hourchdprotocol.Cancel{JobID: jobA, Reason: "cancelled"})
	_, res := h.waitResult(jobA)
	if res.ExitCode != 128+9 || res.Error != "cancelled: cancelled" {
		t.Fatalf("result %+v", res)
	}
}

func TestArtifactDirectoryUpload(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA,
		Argv:      helperArgv(t, "write:out/a.txt=AAA", "write:out/sub/b.txt=BBB", "write:single.txt=S"),
		Artifacts: []string{"out/", "single.txt"}}})
	_, res := h.waitResult(jobA)
	if res.ExitCode != 0 || res.Error != "" {
		t.Fatal(res)
	}
	for p, want := range map[string]string{"out/a.txt": "AAA", "out/sub/b.txt": "BBB", "single.txt": "S"} {
		if b, ok := h.srv.Artifact(jobA, p); !ok || string(b) != want {
			t.Errorf("%s: %q %v", p, b, ok)
		}
	}
	if h.srv.ArtifactPuts() != 3 {
		t.Fatalf("%d PUTs", h.srv.ArtifactPuts())
	}
}

func TestArtifactUploadSendsSha256(t *testing.T) {
	// The fake rejects a body whose sha256 differs from X-Sha256; check the
	// worker's header against an independent hash too.
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA,
		Argv: helperArgv(t, "write:f.bin=payload"), Artifacts: []string{"f.bin"}}})
	_, res := h.waitResult(jobA)
	if res.ExitCode != 0 || res.Error != "" {
		t.Fatal(res)
	}
	sum := sha256.Sum256([]byte("payload"))
	if got := h.srv.ArtifactSHA(jobA, "f.bin"); got != hex.EncodeToString(sum[:]) {
		t.Fatalf("X-Sha256 %v", got)
	}
}

func TestArtifactUploadFailureFailsJob(t *testing.T) {
	h := newHarness(t)
	h.srv.SetArtifactStatus(func(_, p string) int {
		if p == "f.bin" {
			return http.StatusConflict // not_leased: final
		}
		return 0
	})
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA,
		Argv: helperArgv(t, "write:f.bin=x"), Artifacts: []string{"f.bin"}}})
	_, res := h.waitResult(jobA)
	if res.ExitCode != 0 || !strings.Contains(res.Error, "artifact f.bin") || !strings.Contains(res.Error, "409") {
		t.Fatalf("result %+v", res)
	}
	if h.srv.ArtifactPuts() != 1 {
		t.Fatalf("409 retried: %d PUTs", h.srv.ArtifactPuts())
	}
}

func TestArtifactUploadRetries5xx(t *testing.T) {
	h := newHarness(t)
	var n atomic.Int32
	h.srv.SetArtifactStatus(func(_, _ string) int {
		if n.Add(1) <= 2 {
			return http.StatusServiceUnavailable
		}
		return 0
	})
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA,
		Argv: helperArgv(t, "write:f.bin=x"), Artifacts: []string{"f.bin"}}})
	_, res := h.waitResult(jobA)
	if res.ExitCode != 0 || res.Error != "" {
		t.Fatalf("result %+v", res)
	}
	if b, ok := h.srv.Artifact(jobA, "f.bin"); !ok || string(b) != "x" {
		t.Fatal("not stored")
	}
}

func TestMissingArtifactFailsJob(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "echo:x"), Artifacts: []string{"nope.bin"}}})
	_, res := h.waitResult(jobA)
	if res.ExitCode != 0 || !strings.Contains(res.Error, "artifact nope.bin") {
		t.Fatalf("result %+v", res)
	}
}

func TestNoArtifactsOnFailure(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "write:f=x", "exit:1"), Artifacts: []string{"f"}}})
	_, res := h.waitResult(jobA)
	if res.ExitCode != 1 || res.Error != "" || h.srv.ArtifactPuts() != 0 {
		t.Fatalf("result %+v puts %d", res, h.srv.ArtifactPuts())
	}
}

func TestInputs(t *testing.T) {
	h := newHarness(t)
	h.srv.SetInput(jobA, "build", "out/", tarball(t, map[string]string{"pkg.deb": "DEB", "meta/info.json": "{}"}, false))
	h.srv.SetInput(jobA, "build", "version.txt", []byte("1.0\n"))
	h.srv.SetInput(jobA, "other", "x.bin", []byte("X"))
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA,
		Argv: helperArgv(t, "cat:inputs/build/out/pkg.deb", "cat:inputs/build/out/meta/info.json", "cat:inputs/build/version.txt", "cat:renamed/x"),
		Inputs: []hourchdprotocol.Input{
			{Job: "build", Path: "out/"},
			{Job: "build", Path: "version.txt"},
			{Job: "other", Path: "x.bin", As: "renamed/x"},
		}}})
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 0 {
		t.Fatalf("result %+v\n%s", res, dump(ms))
	}
	if got := logText(ms, jobA, "stdout"); got != "DEB{}1.0\nX" {
		t.Fatalf("stdout %q", got)
	}
}

func TestMissingInputFailsBeforeRunning(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "echo:ran"),
		Inputs: []hourchdprotocol.Input{{Job: "build", Path: "gone"}}}})
	ms, res := h.waitResult(jobA)
	if res.ExitCode != -1 || !strings.Contains(res.Error, "input build/gone") || strings.Contains(logText(ms, jobA, "stdout"), "ran") {
		t.Fatalf("result %+v", res)
	}
}

func TestStartFailure(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: []string{"no-such-binary-hourch"}}})
	_, res := h.waitResult(jobA)
	if res.ExitCode != -1 || !strings.Contains(res.Error, "start:") {
		t.Fatalf("result %+v", res)
	}
}

func TestDrain(t *testing.T) {
	dir := t.TempDir()
	trig := filepath.Join(dir, "t")
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "waitfile:"+trig, "echo:finished")}})
	h.waitFor("accept", func(ms []fakehourchd.Received) bool { return len(byType[*hourchdprotocol.Accept](ms)) == 1 })
	h.w.Drain()
	// Still running: Run has not returned.
	select {
	case <-h.done:
		t.Fatal("Run returned while a job was running")
	case <-time.After(30 * time.Millisecond):
	}
	// A new assignment while draining is not accepted.
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobB, Argv: helperArgv(t, "echo:x")}})
	os.WriteFile(trig, nil, 0o644)
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 0 {
		t.Fatal(res)
	}
	select {
	case err := <-h.done:
		if err != nil {
			t.Fatal(err)
		}
		h.cancel = nil
	case <-time.After(waitTimeout):
		t.Fatal("Run did not return after the job finished")
	}
	for _, a := range byType[*hourchdprotocol.Accept](h.srv.Messages()) {
		if a.JobID == jobB {
			t.Fatalf("accepted a job while draining: %s", dump(ms))
		}
	}
}

func TestStaleWorkDirsRemoved(t *testing.T) {
	h := newHarness(t)
	stale := filepath.Join(h.cfg.WorkDir, jobB+"-4")
	keep := filepath.Join(h.cfg.WorkDir, "keep-me")
	os.MkdirAll(stale, 0o755)
	os.MkdirAll(keep, 0o755)
	h.start()
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale job dir kept")
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatal("unrelated dir removed")
	}
}

// fakeDocker is a docker CLI stand-in that records its calls.
type fakeDocker struct {
	bin, calls, envCopy, envMode string
}

func writeFakeDocker(t *testing.T) fakeDocker {
	t.Helper()
	dir := t.TempDir()
	f := fakeDocker{
		bin:     filepath.Join(dir, "docker"),
		calls:   filepath.Join(dir, "calls"),
		envCopy: filepath.Join(dir, "envfile"),
		envMode: filepath.Join(dir, "envmode"),
	}
	script := `#!/bin/sh
# fake docker for hourchworker tests
printf '%s\n' "$*" >> ` + f.calls + `
case "$1" in
create)
  prev=""
  for a in "$@"; do
    if [ "$prev" = "--env-file" ]; then cp "$a" ` + f.envCopy + `; stat -c %a "$a" > ` + f.envMode + `; fi
    prev="$a"
  done
  echo "pulling image..." >&2
  echo "containerid123"
  ;;
start)
  echo "container says hi"
  echo "MULTI=$MULTI" > /dev/null
  exit 7
  ;;
esac
exit 0
`
	if err := os.WriteFile(f.bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestContainerRunner(t *testing.T) {
	fake := writeFakeDocker(t)
	h := newHarness(t, func(c *Config, o *Options) {
		c.Capabilities = []string{"docker"}
		o.Docker = fake.bin
	})
	h.start()
	h.srv.SetContext(jobB, tarball(t, map[string]string{"src/": ""}, true))
	h.assign(&hourchdprotocol.Assign{
		Job: hourchdprotocol.AssignedJob{ID: jobB, Image: "registry.labtwo/kbuild:24.04", Workdir: "src",
			Argv: []string{"make", "-j8", "all"}, Env: map[string]string{"FLAVOUR": "aws"}},
		Secrets:  map[string]string{"TOKEN": "s3cr3t", "SSH_KEY": "-----BEGIN\nabc\n-----END"},
		JobToken: "jt-c",
		Attempt:  2,
	})
	ms, res := h.waitResult(jobB)
	if res.ExitCode != 7 || res.Error != "" {
		t.Fatalf("result %+v\n%s", res, dump(ms))
	}
	if got := logText(ms, jobB, "stdout"); got != "container says hi\n" {
		t.Fatalf("stdout %q", got)
	}
	if got := logText(ms, jobB, "stderr"); got != "pulling image...\n" {
		t.Fatalf("stderr %q", got)
	}
	callsB, _ := os.ReadFile(fake.calls)
	lines := strings.Split(strings.TrimSpace(string(callsB)), "\n")
	name := "hourch-" + strings.ToLower(jobB) + "-2"
	root := filepath.Join(h.cfg.WorkDir, jobB+"-2")
	if len(lines) != 3 || lines[0] != "rm -f "+name || lines[2] != "start -a "+name {
		t.Fatalf("docker calls %q", lines)
	}
	create := strings.Fields(lines[1])
	envFile := create[indexOf(create, "--env-file")+1]
	want := []string{"create", "--rm", "--name", name, "--label", "hourch.job=" + jobB, "--env-file", envFile,
		"-e", "SSH_KEY", "-v", root + ":/work", "-w", "/work/src", "registry.labtwo/kbuild:24.04", "make", "-j8", "all"}
	if !reflect.DeepEqual(create, want) {
		t.Fatalf("create\n got %q\nwant %q", create, want)
	}
	if filepath.Dir(envFile) != h.cfg.WorkDir || strings.HasPrefix(envFile, root) {
		t.Fatalf("env file %s not beside the work root", envFile)
	}
	if _, err := os.Stat(envFile); !os.IsNotExist(err) {
		t.Fatal("env file not deleted")
	}
	mode, _ := os.ReadFile(fake.envMode)
	if strings.TrimSpace(string(mode)) != "600" {
		t.Fatalf("env file mode %q", mode)
	}
	body, _ := os.ReadFile(fake.envCopy)
	wantEnv := "FLAVOUR=aws\nHOURCH_ATTEMPT=2\nHOURCH_JOB_ID=" + jobB + "\nHOURCH_JOB_TOKEN=jt-c\n" +
		"HOURCH_OUTPUTS=/work/.hourch/outputs\nHOURCH_RUN_ID=" + runA + "\nHOURCH_URL=" + h.srv.HTTP.URL + "\nTOKEN=s3cr3t\n"
	if string(body) != wantEnv {
		t.Fatalf("env file\n got %q\nwant %q", body, wantEnv)
	}
}

func TestContainerCancel(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	bin := filepath.Join(dir, "docker")
	// start blocks until killed; kill/rm are recorded.
	os.WriteFile(bin, []byte(`#!/bin/sh
printf '%s\n' "$*" >> `+calls+`
case "$1" in
start) echo started; exec sleep 600 ;;
esac
exit 0
`), 0o755)
	h := newHarness(t, func(c *Config, o *Options) {
		c.Capabilities = []string{"docker"}
		o.Docker = bin
		o.KillGrace = 20 * time.Millisecond
	})
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Image: "alpine", Argv: []string{"sleep", "1d"}}})
	h.waitFor("started", func(ms []fakehourchd.Received) bool { return strings.Contains(logText(ms, jobA, ""), "started") })
	h.srv.Send(&hourchdprotocol.Cancel{JobID: jobA, Reason: "cancelled"})
	_, res := h.waitResult(jobA)
	if res.Error != "cancelled: cancelled" {
		t.Fatalf("result %+v", res)
	}
	b, _ := os.ReadFile(calls)
	name := "hourch-" + strings.ToLower(jobA) + "-1"
	for _, want := range []string{"kill --signal TERM " + name, "rm -f " + name} {
		if !strings.Contains(string(b), want+"\n") {
			t.Fatalf("calls %q missing %q", b, want)
		}
	}
}

func TestHardStopKillsJob(t *testing.T) {
	dir := t.TempDir()
	pidf := filepath.Join(dir, "pid")
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "pidfile:"+pidf, "sleep")}})
	pid := readPid(t, pidf)
	h.stop() // cancels Run's context
	waitDead(t, pid)
	if r := byType[*hourchdprotocol.Result](h.srv.Messages()); len(r) != 0 {
		t.Fatalf("result sent on hard stop: %+v", r)
	}
}

func TestEscapingContextOrInputFailsJob(t *testing.T) {
	ctxTar := func(link string) []byte {
		var raw bytes.Buffer
		w := tarWriterFull(&raw)
		w.file("ok.txt", "x")
		w.symlink("evil", link)
		w.close()
		var z bytes.Buffer
		zw, _ := zstd.NewWriter(&z)
		zw.Write(raw.Bytes())
		zw.Close()
		return z.Bytes()
	}
	h := newHarness(t)
	h.srv.SetContext(jobA, ctxTar("../../outside"))
	var inTar bytes.Buffer
	w := tarWriterFull(&inTar)
	w.symlink("l", "/etc/passwd")
	w.close()
	h.srv.SetInput(jobB, "build", "out/", inTar.Bytes())
	h.start()

	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA, Argv: helperArgv(t, "echo:ran")}})
	ms, res := h.waitResult(jobA)
	if res.ExitCode != -1 || !strings.HasPrefix(res.Error, "context: ") || !strings.Contains(res.Error, "resolves outside the destination directory") {
		t.Fatalf("result %+v", res)
	}
	if strings.Contains(logText(ms, jobA, "stdout"), "ran") {
		t.Fatal("job ran")
	}

	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobB, Argv: helperArgv(t, "echo:ran"),
		Inputs: []hourchdprotocol.Input{{Job: "build", Path: "out/"}}}})
	_, res = h.waitResult(jobB)
	if res.ExitCode != -1 || !strings.HasPrefix(res.Error, "input build/out/: ") || !strings.Contains(res.Error, "resolves outside") {
		t.Fatalf("result %+v", res)
	}
}

func TestBadOutputFailsJob(t *testing.T) {
	h := newHarness(t)
	h.start()
	h.assign(&hourchdprotocol.Assign{Job: hourchdprotocol.AssignedJob{ID: jobA,
		Argv:      helperArgv(t, "outputs:good=1", "outputs:no equals here", "write:f=x"),
		Artifacts: []string{"f"}}})
	ms, res := h.waitResult(jobA)
	if res.ExitCode != 0 || !strings.HasPrefix(res.Error, "bad_output: line 2") {
		t.Fatalf("result %+v", res)
	}
	if n := len(byType[*hourchdprotocol.Output](ms)); n != 0 || h.srv.ArtifactPuts() != 0 {
		t.Fatalf("%d outputs, %d artifact PUTs after bad_output", n, h.srv.ArtifactPuts())
	}
}
