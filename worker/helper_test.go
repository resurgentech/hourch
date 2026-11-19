package hourchworker

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	hourchdprotocol "github.com/resurgentech/hourch/protocol"
	"github.com/resurgentech/hourch/worker/internal/fakehourchd"
)

// The test binary doubles as the job command: with WORKERTEST_HELPER=1 in
// its environment it runs the ops in its arguments instead of the tests.
// Ops are "name:arg":
//
//	echo:TEXT      stdout line          stderr:TEXT  stderr line
//	env            print the env sorted  pwd          print the cwd
//	cat:FILE       print a file          lines:N      print N numbered lines
//	raw:HEX        write raw bytes to stdout
//	outputs:K=V    append to $HOURCH_OUTPUTS
//	write:PATH=TEXT  write a file (relative to cwd)
//	waitfile:PATH  wait until PATH exists
//	pidfile:PATH   write own pid
//	grandchild:PATH  start a sleeping child in the same group, write its pid
//	ignoreterm     ignore SIGTERM
//	sleep          block until killed
//	exit:N         exit with N
const helperEnv = "WORKERTEST_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) == "1" {
		os.Exit(runHelper(os.Args[1:]))
	}
	os.Exit(m.Run())
}

func runHelper(ops []string) int {
	for _, op := range ops {
		name, arg, _ := strings.Cut(op, ":")
		switch name {
		case "echo":
			fmt.Println(arg)
		case "stderr":
			fmt.Fprintln(os.Stderr, arg)
		case "env":
			env := os.Environ()
			sort.Strings(env)
			for _, kv := range env {
				fmt.Println(kv)
			}
		case "pwd":
			wd, _ := os.Getwd()
			fmt.Println(wd)
		case "cat":
			b, err := os.ReadFile(arg)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 2
			}
			os.Stdout.Write(b)
		case "lines":
			n, _ := strconv.Atoi(arg)
			var buf bytes.Buffer
			for i := 1; i <= n; i++ {
				fmt.Fprintf(&buf, "line %06d\n", i)
			}
			os.Stdout.Write(buf.Bytes())
		case "raw":
			var b []byte
			fmt.Sscanf(arg, "%x", &b)
			os.Stdout.Write(b)
		case "outputs":
			f, err := os.OpenFile(os.Getenv("HOURCH_OUTPUTS"), os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 2
			}
			fmt.Fprintln(f, arg)
			f.Close()
		case "write":
			p, text, _ := strings.Cut(arg, "=")
			os.MkdirAll(filepath.Dir(p), 0o755)
			if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 2
			}
		case "waitfile":
			for {
				if _, err := os.Stat(arg); err == nil {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
		case "pidfile":
			os.WriteFile(arg, []byte(strconv.Itoa(os.Getpid())), 0o644)
		case "grandchild":
			exe, _ := os.Executable()
			cmd := exec.Command(exe, "sleep")
			cmd.Env = append(os.Environ(), helperEnv+"=1", "GORACE=atexit_sleep_ms=0")
			if err := cmd.Start(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 2
			}
			os.WriteFile(arg, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644)
		case "ignoreterm":
			signal.Ignore(syscall.SIGTERM)
		case "sleep":
			select {}
		case "exit":
			n, _ := strconv.Atoi(arg)
			return n
		default:
			fmt.Fprintln(os.Stderr, "unknown helper op", op)
			return 2
		}
	}
	return 0
}

// helperArgv is the argv for a job running the helper ops.
func helperArgv(t *testing.T, ops ...string) []string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return append([]string{exe}, ops...)
}

// ULIDs for test jobs.
const (
	jobA = "01JB0000000000000000000001"
	jobB = "01JB0000000000000000000002"
	runA = "01JB00000000000000000000R1"
)

// harness is a worker connected to a fake hourchd.
type harness struct {
	t      *testing.T
	srv    *fakehourchd.Server
	w      *Worker
	cfg    Config
	dir    string
	cancel context.CancelFunc
	done   chan error
}

const testToken = "worker-token-0123456789"

type harnessOpt func(*Config, *Options)

func newHarness(t *testing.T, opts ...harnessOpt) *harness {
	t.Helper()
	srv := fakehourchd.New(testToken)
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	cfg := DefaultConfig()
	cfg.HourchdURL = srv.URL()
	cfg.WorkerID = "sdf1-1"
	cfg.Hostname = "sdf1.labtwo"
	cfg.Tags = []string{"tiny"}
	cfg.WorkDir = filepath.Join(dir, "work")
	cfg.TokenFile = filepath.Join(dir, "token")
	cfg.Reconnect = ReconnectConfig{Initial: Duration(5 * time.Millisecond), Max: Duration(20 * time.Millisecond)}
	o := Options{
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Version:        "test-1",
		HeartbeatUnit:  time.Second,
		KillGrace:      200 * time.Millisecond,
		ChunkMaxDelay:  5 * time.Millisecond,
		WelcomeTimeout: 2 * time.Second,
		PipeDrain:      time.Second,
		Environ: func() []string {
			return []string{"PATH=" + os.Getenv("PATH"), "HOME=/home/worker", "LANG=C.UTF-8"}
		},
	}
	if testing.Verbose() {
		o.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	for _, f := range opts {
		f(&cfg, &o)
	}
	w, err := New(cfg, testToken, o)
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, srv: srv, w: w, cfg: cfg, dir: dir}
}

func (h *harness) start() {
	h.startNoWait()
	h.waitFor("hello", func(ms []fakehourchd.Received) bool { return len(byType[*hourchdprotocol.Hello](ms)) > 0 })
}

// startNoWait starts the worker without waiting for its hello.
func (h *harness) startNoWait() {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan error, 1)
	go func() { h.done <- h.w.Run(ctx) }()
	h.t.Cleanup(h.stop)
}

func (h *harness) stop() {
	if h.cancel == nil {
		return
	}
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(10 * time.Second):
		h.t.Error("worker did not stop")
	}
	h.cancel = nil
}

const waitTimeout = 15 * time.Second

func (h *harness) waitFor(what string, pred func([]fakehourchd.Received) bool) []fakehourchd.Received {
	h.t.Helper()
	ms, ok := h.srv.Wait(waitTimeout, pred)
	if !ok {
		h.t.Fatalf("timed out waiting for %s; got %s", what, dump(ms))
	}
	return ms
}

func (h *harness) assign(a *hourchdprotocol.Assign) {
	h.t.Helper()
	if a.Attempt == 0 {
		a.Attempt = 1
	}
	if a.JobToken == "" {
		a.JobToken = "job-token-" + a.Job.ID
	}
	if a.Job.RunID == "" {
		a.Job.RunID = runA
	}
	if a.Job.Name == "" {
		a.Job.Name = "test"
	}
	if a.Job.Image == "" {
		if a.Job.Env == nil {
			a.Job.Env = map[string]string{}
		}
		a.Job.Env[helperEnv] = "1"
		// A -race helper otherwise sleeps 1s at exit.
		a.Job.Env["GORACE"] = "atexit_sleep_ms=0"
	}
	if a.Job.TimeoutSeconds == 0 {
		a.Job.TimeoutSeconds = 60
	}
	if err := h.srv.Send(a); err != nil {
		h.t.Fatal(err)
	}
}

// waitResult waits for the result of job id and returns all messages.
func (h *harness) waitResult(id string) ([]fakehourchd.Received, *hourchdprotocol.Result) {
	h.t.Helper()
	ms := h.waitFor("result "+id, func(ms []fakehourchd.Received) bool {
		for _, r := range byType[*hourchdprotocol.Result](ms) {
			if r.JobID == id {
				return true
			}
		}
		return false
	})
	for _, r := range byType[*hourchdprotocol.Result](ms) {
		if r.JobID == id {
			return ms, r
		}
	}
	return ms, nil
}

func byType[T hourchdprotocol.Message](ms []fakehourchd.Received) []T {
	var out []T
	for _, r := range ms {
		if m, ok := r.Msg.(T); ok {
			out = append(out, m)
		}
	}
	return out
}

// logText concatenates one stream's chunks for job id in seq order (the
// last copy of a seq wins).
func logText(ms []fakehourchd.Received, id, stream string) string {
	bySeq := map[int64]*hourchdprotocol.Log{}
	var seqs []int64
	for _, l := range byType[*hourchdprotocol.Log](ms) {
		if l.JobID != id {
			continue
		}
		if _, ok := bySeq[l.Seq]; !ok {
			seqs = append(seqs, l.Seq)
		}
		bySeq[l.Seq] = l
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	var sb strings.Builder
	for _, s := range seqs {
		if stream == "" || bySeq[s].Stream == stream {
			sb.WriteString(bySeq[s].Data)
		}
	}
	return sb.String()
}

func dump(ms []fakehourchd.Received) string {
	var sb strings.Builder
	for _, r := range ms {
		b, _ := hourchdprotocol.Encode(r.Msg)
		if len(b) > 300 {
			b = append(b[:300], "..."...)
		}
		fmt.Fprintf(&sb, "\n  conn%d %s", r.Conn, b)
	}
	return sb.String()
}

// tarball builds a tar (zstd-compressed when zst) from name → content; a
// name ending in "/" is a directory.
func tarball(t *testing.T, files map[string]string, zst bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	var w io.Writer = &buf
	var zw *zstd.Encoder
	if zst {
		var err error
		zw, err = zstd.NewWriter(&buf)
		if err != nil {
			t.Fatal(err)
		}
		w = zw
	}
	tw := tar.NewWriter(w)
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if strings.HasSuffix(n, "/") {
			tw.WriteHeader(&tar.Header{Name: n, Typeflag: tar.TypeDir, Mode: 0o755})
			continue
		}
		body := files[n]
		tw.WriteHeader(&tar.Header{Name: n, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body)), ModTime: time.Unix(1700000000, 0)})
		tw.Write([]byte(body))
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if zw != nil {
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return buf.Bytes()
}

// pidAlive reports whether pid exists (and is not a zombie we can see).
func pidAlive(pid int) bool {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	// state is the field after the ")" of the comm.
	s := string(b)
	if i := strings.LastIndexByte(s, ')'); i >= 0 && i+2 < len(s) {
		return s[i+2] != 'Z'
	}
	return true
}

func readPid(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(path)
		if err == nil && len(b) > 0 {
			n, err := strconv.Atoi(string(b))
			if err == nil {
				return n
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no pid in %s", path)
	return 0
}

func waitDead(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if !pidAlive(pid) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("pid %d still alive", pid)
}

// eventually polls cond.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type tarW struct{ tw *tar.Writer }

func tarWriterFull(w io.Writer) *tarW { return &tarW{tar.NewWriter(w)} }

func (t *tarW) file(name, body string) {
	t.tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(body))})
	t.tw.Write([]byte(body))
}

func (t *tarW) symlink(name, target string) {
	t.tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeSymlink, Linkname: target})
}

func (t *tarW) hardlink(name, target string) {
	t.tw.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeLink, Linkname: target})
}

func (t *tarW) close() { t.tw.Close() }

// tarWriter returns a func that writes one file entry and closes the tar.
func tarWriter(w io.Writer) func(name, body string) {
	return func(name, body string) {
		t := tarWriterFull(w)
		t.file(name, body)
		t.close()
	}
}
