package hourchworker

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func takeAll(b *logBuffer) []*logChunk {
	var out []*logChunk
	for c := b.take(); c != nil; c = b.take() {
		out = append(out, c)
	}
	return out
}

func TestLogBufferOverflowMarker(t *testing.T) {
	b := newLogBuffer(100, nil)
	for i := 0; i < 10; i++ {
		b.add("stdout", strings.Repeat(string(rune('a'+i)), 30))
	}
	b.close()
	cs := takeAll(b)
	// 100 bytes hold 3 chunks of 30; 7 (210 bytes) were dropped.
	if len(cs) != 4 {
		t.Fatalf("got %d chunks: %+v", len(cs), cs)
	}
	if cs[0].stream != "stderr" || !strings.Contains(cs[0].data, "210 bytes of log output dropped") {
		t.Fatalf("first chunk is not the marker: %+v", cs[0])
	}
	for i, c := range cs {
		if c.seq != int64(i+1) {
			t.Fatalf("chunk %d seq %d", i, c.seq)
		}
	}
	if cs[1].data[0] != 'h' || cs[3].data[0] != 'j' {
		t.Fatalf("kept the wrong chunks: %q %q", cs[1].data, cs[3].data)
	}
	if !b.drained() {
		t.Fatal("not drained")
	}
}

func TestLogBufferSentChunksEvictedSilently(t *testing.T) {
	b := newLogBuffer(100, nil)
	b.add("stdout", strings.Repeat("a", 60))
	if c := b.take(); c == nil || c.seq != 1 {
		t.Fatal("take")
	}
	b.add("stdout", strings.Repeat("b", 60)) // evicts the sent "a" chunk
	cs := takeAll(b)
	if len(cs) != 1 || cs[0].data[0] != 'b' || cs[0].seq != 2 {
		t.Fatalf("got %+v", cs)
	}
}

func TestLogBufferResetResend(t *testing.T) {
	b := newLogBuffer(1<<20, nil)
	for _, s := range []string{"one", "two", "three"} {
		b.add("stdout", s)
	}
	cs := takeAll(b) // seq 1..3 sent, connection then drops
	if len(cs) != 3 {
		t.Fatal(cs)
	}
	b.add("stdout", "four") // while disconnected
	b.reset(1)              // hourchd stored seq 1 only
	cs = takeAll(b)
	var got []string
	for i, c := range cs {
		got = append(got, c.data)
		if c.seq != int64(i+2) {
			t.Fatalf("seq %d for %q", c.seq, c.data)
		}
	}
	if !reflect.DeepEqual(got, []string{"two", "three", "four"}) {
		t.Fatalf("resent %v", got)
	}
}

func TestLogBufferResetAfterLostSent(t *testing.T) {
	b := newLogBuffer(100, nil)
	b.add("stdout", strings.Repeat("a", 60))
	b.take()                                 // seq 1 sent
	b.add("stdout", strings.Repeat("b", 60)) // evicts sent seq 1
	b.reset(0)                               // hourchd stored nothing
	cs := takeAll(b)
	if len(cs) != 2 || !strings.Contains(cs[0].data, "dropped") || cs[0].seq != 1 || cs[1].seq != 2 {
		t.Fatalf("got %+v", cs)
	}
}

func TestRedactor(t *testing.T) {
	r := newRedactor([]string{"abc", "abcdef", ""})
	if got := r.apply("x abcdef y abc z"); got != "x *** y *** z" {
		t.Fatal(got)
	}
	if got := (redactor{}).apply("abc"); got != "abc" {
		t.Fatal(got)
	}
}

func TestSanitizeUTF8(t *testing.T) {
	got := sanitizeUTF8("a\xff\xfeb")
	if got != "a��b" || !utf8.ValidString(got) {
		t.Fatalf("%q", got)
	}
}

func TestSplitRunes(t *testing.T) {
	s := strings.Repeat("é", 10) // 20 bytes
	parts := splitRunes(s, 5)
	if strings.Join(parts, "") != s {
		t.Fatal("lost bytes")
	}
	for _, p := range parts {
		if len(p) > 5 || !utf8.ValidString(p) {
			t.Fatalf("bad part %q", p)
		}
	}
}

type chunkRec struct {
	mu sync.Mutex
	cs []string
}

func (r *chunkRec) emit(b []byte) {
	r.mu.Lock()
	r.cs = append(r.cs, string(b))
	r.mu.Unlock()
}

func (r *chunkRec) get() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.cs...)
}

func TestPumpSizeAndNewlineCut(t *testing.T) {
	var rec chunkRec
	in := strings.Repeat("0123456789abcd\n", 10) // 150 bytes, lines of 15
	pump(strings.NewReader(in), 40, time.Hour, rec.emit)
	cs := rec.get()
	if strings.Join(cs, "") != in {
		t.Fatal("lost bytes")
	}
	for _, c := range cs[:len(cs)-1] {
		if len(c) > 40 || !strings.HasSuffix(c, "\n") {
			t.Fatalf("chunk %q", c)
		}
	}
}

func TestPumpDoesNotSplitRunes(t *testing.T) {
	var rec chunkRec
	in := strings.Repeat("€", 100) // 3 bytes each, no newlines
	pump(strings.NewReader(in), 10, time.Hour, rec.emit)
	for _, c := range rec.get() {
		if len(c) > 10 || !utf8.ValidString(c) {
			t.Fatalf("chunk %q", c)
		}
	}
}

func TestPumpDelayFlush(t *testing.T) {
	var rec chunkRec
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() { pump(pr, 1<<16, 5*time.Millisecond, rec.emit); close(done) }()
	pw.Write([]byte("partial"))
	eventually(t, "delay flush", func() bool { return len(rec.get()) == 1 })
	pw.Close()
	<-done
	if got := rec.get(); !reflect.DeepEqual(got, []string{"partial"}) {
		t.Fatal(got)
	}
}

func TestExtractTarRejectsEscape(t *testing.T) {
	for _, name := range []string{"../evil", "/abs", "a/../../evil"} {
		var buf bytes.Buffer
		tw := tarWriter(&buf)
		tw(name, "x")
		dir := t.TempDir()
		if err := extractTar(&buf, filepath.Join(dir, "root")); err == nil {
			t.Errorf("%s: extracted", name)
		}
		if _, err := os.Stat(filepath.Join(dir, "evil")); err == nil {
			t.Errorf("%s: wrote outside", name)
		}
	}
}

func TestExtractTarNoWriteThroughSymlink(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(dir, "outside")
	os.Mkdir(outside, 0o755)
	var buf bytes.Buffer
	tw := tarWriterFull(&buf)
	tw.symlink("link", outside)
	tw.file("link/pwned", "x")
	tw.close()
	if err := extractTar(&buf, filepath.Join(dir, "root")); err == nil {
		t.Fatal("wrote through a symlink")
	}
	if _, err := os.Stat(filepath.Join(outside, "pwned")); err == nil {
		t.Fatal("file written outside the root")
	}
}

func TestContainerArgs(t *testing.T) {
	s := runSpec{
		jobID: jobA, attempt: 2, argv: []string{"make", "-j8"},
		workRoot: "/w/" + jobA + "-2", workdir: "src/kernel",
		env:   map[string]string{"A": "1", "KEY": "line1\nline2", "HOURCH_JOB_ID": jobA},
		image: "registry.labtwo/kbuild:24.04",
	}
	args, body, pass := containerArgs(s, "/w/.hourch-env-1")
	want := []string{"create", "--rm", "--name", "hourch-01jb0000000000000000000001-2", "--label", "hourch.job=" + jobA,
		"--env-file", "/w/.hourch-env-1", "-e", "KEY",
		"-v", "/w/" + jobA + "-2:/work", "-w", "/work/src/kernel", "registry.labtwo/kbuild:24.04", "make", "-j8"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("args\n got %q\nwant %q", args, want)
	}
	if string(body) != "A=1\nHOURCH_JOB_ID="+jobA+"\n" {
		t.Fatalf("env file %q", body)
	}
	if !reflect.DeepEqual(pass, []string{"KEY"}) {
		t.Fatal(pass)
	}
	s.workdir = "."
	args, _, _ = containerArgs(s, "f")
	if i := indexOf(args, "-w"); args[i+1] != "/work" {
		t.Fatalf("root workdir: %q", args[i+1])
	}
}

func indexOf(xs []string, x string) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

func TestParseOutputs(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "outputs")
	os.WriteFile(p, []byte("a=1\n\nb=x=y\nempty=\na=3\r\n"), 0o644)
	outs, err := parseOutputs(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(outs) != 3 || outs[0].Key != "a" || outs[0].Value != "3" || outs[1].Value != "x=y" || outs[2].Value != "" {
		t.Fatalf("%+v", outs)
	}
	for body, want := range map[string]string{
		"a=1\nnoequals\n": "line 2: no '='",
		"bad key=2\n":     "line 1: bad key",
		"big=" + strings.Repeat("v", 4097) + "\n": "line 1: value of big is 4097 bytes",
	} {
		os.WriteFile(p, []byte(body), 0o644)
		if _, err := parseOutputs(p); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q: %v, want %q", body[:min(len(body), 20)], err, want)
		}
	}
	os.WriteFile(p, []byte("ok="+strings.Repeat("v", 4096)+"\n"), 0o644)
	if _, err := parseOutputs(p); err != nil {
		t.Fatalf("4096-byte value: %v", err)
	}
	if outs, err := parseOutputs(filepath.Join(dir, "missing")); err != nil || outs != nil {
		t.Fatal("missing file")
	}
}

func TestExtractTarRejectsEscapingLinks(t *testing.T) {
	for name, build := range map[string]func(*tarW){
		"absolute symlink": func(w *tarW) { w.symlink("l", "/etc") },
		"relative escape":  func(w *tarW) { w.symlink("a/l", "../../x") },
		"chained escape":   func(w *tarW) { w.symlink("s", "l/.."); w.symlink("l", ".") },
		"hardlink escape":  func(w *tarW) { w.hardlink("h", "../x") },
		"dot-dot file":     func(w *tarW) { w.file("a/../../x", "x") },
	} {
		var buf bytes.Buffer
		w := tarWriterFull(&buf)
		build(w)
		w.close()
		err := extractTar(&buf, filepath.Join(t.TempDir(), "root"))
		if err == nil {
			t.Errorf("%s: extracted", name)
		}
	}
	// Links that stay inside are fine.
	var buf bytes.Buffer
	w := tarWriterFull(&buf)
	w.file("d/f", "x")
	w.symlink("l", "d/f")
	w.symlink("d/up", "../d")
	w.hardlink("h", "d/f")
	w.close()
	dir := filepath.Join(t.TempDir(), "root")
	if err := extractTar(&buf, dir); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "h")); err != nil || string(b) != "x" {
		t.Fatal("hardlink", err)
	}
}

func TestBackoff(t *testing.T) {
	w := &Worker{cfg: Config{Reconnect: ReconnectConfig{Initial: Duration(500 * time.Millisecond), Max: Duration(10 * time.Second)}}}
	w.opts.Jitter = func() float64 { return 0.999999 }
	var got []time.Duration
	for n := 1; n <= 7; n++ {
		got = append(got, w.backoff(n).Round(time.Millisecond))
	}
	want := []time.Duration{500, 1000, 2000, 4000, 8000, 10000, 10000}
	for i := range want {
		if got[i] != want[i]*time.Millisecond {
			t.Fatalf("backoff %v", got)
		}
	}
	w.opts.Jitter = func() float64 { return 0 }
	if d := w.backoff(1); d != 250*time.Millisecond {
		t.Fatalf("min jitter %v", d)
	}
}
