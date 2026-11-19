package hourchflow

import (
	"archive/tar"
	"bytes"
	"errors"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

type tarEntry struct {
	typ      byte
	mode     int64
	body     string
	linkname string
}

// untar decodes a .tar.zst and checks the normalised headers.
func untar(t *testing.T, data []byte) (map[string]tarEntry, []string) {
	t.Helper()
	zr, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	out := map[string]tarEntry{}
	var order []string
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.ModTime.Unix() != 0 || h.Uid != 0 || h.Gid != 0 || h.Uname != "" || h.Gname != "" {
			t.Errorf("%s: header not normalised: mtime %v uid %d gid %d %q %q", h.Name, h.ModTime, h.Uid, h.Gid, h.Uname, h.Gname)
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		out[h.Name] = tarEntry{typ: h.Typeflag, mode: h.Mode, body: string(b), linkname: h.Linkname}
		order = append(order, h.Name)
	}
	return out, order
}

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for p, body := range files {
		full := filepath.Join(root, filepath.FromSlash(p))
		if strings.HasSuffix(p, "/") {
			if err := os.MkdirAll(full, 0o755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func buildContext(t *testing.T, dir string, excludes ...string) ([]byte, ContextStats) {
	t.Helper()
	var buf bytes.Buffer
	st, err := WriteContext(&buf, dir, excludes...)
	if err != nil {
		t.Fatal(err)
	}
	if st.Compressed != int64(buf.Len()) {
		t.Errorf("Compressed = %d, wrote %d", st.Compressed, buf.Len())
	}
	return buf.Bytes(), st
}

func TestContextRoundTrip(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		"README.md":                  "hello",
		"apps/hello/src/main.rs":     "fn main() {}",
		"apps/hello/output/x.deb":    "deb",       // excluded by apps/*/output
		"apps/kernel/output/y.deb":   "deb",       // excluded by apps/*/output
		"libs/rust/a/target/debug/x": "obj",       // excluded by **/target
		"target/top":                 "obj",       // excluded by **/target (zero dirs)
		".git/HEAD":                  "ref",       // always excluded
		"sub/.git":                   "gitdir: x", // .git file of a submodule, excluded
		"empty/":                     "",
		"tools/run.sh":               "#!/bin/sh\n",
	})
	if err := os.Chmod(filepath.Join(dir, "tools/run.sh"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../README.md", filepath.Join(dir, "tools/readme")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/etc", filepath.Join(dir, "etc-link")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("apps", filepath.Join(dir, "apps-link")); err != nil { // dir link: not followed
		t.Fatal(err)
	}

	data, st := buildContext(t, dir, "apps/*/output", "**/target")
	got, order := untar(t, data)

	wantNames := []string{
		"README.md",
		"apps/",
		"apps/hello/",
		"apps/hello/src/",
		"apps/hello/src/main.rs",
		"apps/kernel/",
		"apps-link",
		"empty/",
		"etc-link",
		"libs/",
		"libs/rust/",
		"libs/rust/a/",
		"sub/",
		"tools/",
		"tools/readme",
		"tools/run.sh",
	}
	if !reflect.DeepEqual(order, wantNames) {
		t.Fatalf("entries:\n got %q\nwant %q", order, wantNames)
	}
	if e := got["README.md"]; e.typ != tar.TypeReg || e.body != "hello" || e.mode != 0o644 {
		t.Errorf("README.md = %+v", e)
	}
	if e := got["tools/run.sh"]; e.mode != 0o755 {
		t.Errorf("run.sh mode = %o, want 755", e.mode)
	}
	if e := got["tools/readme"]; e.typ != tar.TypeSymlink || e.linkname != "../README.md" {
		t.Errorf("tools/readme = %+v", e)
	}
	if e := got["etc-link"]; e.typ != tar.TypeSymlink || e.linkname != "/etc" {
		t.Errorf("etc-link = %+v", e)
	}
	if e := got["apps-link"]; e.typ != tar.TypeSymlink || e.linkname != "apps" {
		t.Errorf("apps-link = %+v (directory links must not be followed)", e)
	}
	if e := got["empty/"]; e.typ != tar.TypeDir {
		t.Errorf("empty/ = %+v", e)
	}
	want := ContextStats{Files: 3, Dirs: 10, Symlinks: 3, Bytes: int64(len("hello") + len("fn main() {}") + len("#!/bin/sh\n")), Compressed: st.Compressed}
	if st != want {
		t.Errorf("stats = %+v, want %+v", st, want)
	}
}

func TestContextGitignore(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		".gitignore": strings.Join([]string{
			"# comment",
			"*.log",
			"build/",        // directories named build, any depth
			"/rootonly.txt", // anchored
			"docs/*.tmp",    // anchored (contains a slash)
			"bin",           // name at any depth
			"",
		}, "\n"),
		"a.log":                   "x",
		"rootonly.txt":            "x",
		"nested/rootonly.txt":     "kept: anchored pattern only matches at root",
		"build/out":               "x",
		"src/build/out":           "x",
		"src/build.txt":           "kept",
		"docs/a.tmp":              "x",
		"docs/sub/b.tmp":          "kept: * does not cross /",
		"keep/.gitignore":         "!important.log\n",
		"keep/important.log":      "kept by negation in a deeper file",
		"keep/other.log":          "x",
		"workflows/wf/.gitignore": "wf\n# tarballer_override=wf\n",
		"workflows/wf/wf":         "x (ignored; the old override comment is just a comment)",
		"workflows/wf/bin/tool":   "x (bin dir ignored by root)",
	})
	data, _ := buildContext(t, dir)
	got, _ := untar(t, data)
	files := []string{}
	for name, e := range got {
		if e.typ == tar.TypeReg {
			files = append(files, name)
		}
	}
	sort.Strings(files)
	want := []string{
		".gitignore",
		"docs/sub/b.tmp",
		"keep/.gitignore",
		"keep/important.log",
		"nested/rootonly.txt",
		"src/build.txt",
		"workflows/wf/.gitignore",
	}
	if !reflect.DeepEqual(files, want) {
		t.Errorf("files:\n got %q\nwant %q", files, want)
	}
	if _, ok := got["build/"]; ok {
		t.Error("ignored dir build/ has an entry")
	}
}

func TestContextIgnoredDirCannotBeReincluded(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		".gitignore": "out/\n!out/keep\n",
		"out/keep":   "x",
		"out/drop":   "x",
	})
	data, _ := buildContext(t, dir)
	got, _ := untar(t, data)
	if _, ok := got["out/keep"]; ok {
		t.Error("file under an ignored directory was included (git does not do this)")
	}
}

func TestContextDeterministic(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(1))
	files := map[string]string{}
	for i := 0; i < 20; i++ {
		b := make([]byte, 200<<10)
		for j := range b {
			b[j] = byte('a' + rng.Intn(4)) // compressible, multi-block
		}
		files[filepath.Join("d", string(rune('a'+i)))] = string(b)
	}
	writeTree(t, dir, files)
	var outs [][]byte
	for _, conc := range []int{1, 4, 0} {
		var buf bytes.Buffer
		if _, err := WriteContextOpts(&buf, dir, ContextOptions{EncoderConcurrency: conc}); err != nil {
			t.Fatal(err)
		}
		outs = append(outs, buf.Bytes())
	}
	// Touching a file (mtime only) must not change the bytes either.
	if err := os.Chtimes(filepath.Join(dir, "d", "a"), time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if _, err := WriteContext(&buf, dir); err != nil {
		t.Fatal(err)
	}
	outs = append(outs, buf.Bytes())
	for i := 1; i < len(outs); i++ {
		if !bytes.Equal(outs[0], outs[i]) {
			t.Fatalf("output %d differs from output 0", i)
		}
	}
}

func TestContextTooLarge(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(2))
	b := make([]byte, 1<<20)
	rng.Read(b) // incompressible
	writeTree(t, dir, map[string]string{"big": string(b)})
	_, err := WriteContextOpts(io.Discard, dir, ContextOptions{MaxBytes: 64 << 10})
	if !errors.Is(err, ErrContextTooLarge) {
		t.Fatalf("err = %v, want ErrContextTooLarge", err)
	}
}

func TestContextErrors(t *testing.T) {
	if _, err := WriteContext(io.Discard, filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("missing dir accepted")
	}
	f := filepath.Join(t.TempDir(), "file")
	writeTree(t, filepath.Dir(f), map[string]string{"file": "x"})
	if _, err := WriteContext(io.Discard, f); err == nil {
		t.Error("file accepted as context dir")
	}
	if _, err := WriteContext(io.Discard, t.TempDir(), "!x"); err == nil {
		t.Error("negated exclude accepted")
	}
}

func TestContextUnreadableFileIsError(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads everything")
	}
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"secret": "x"})
	p := filepath.Join(dir, "secret")
	if err := os.Chmod(p, 0); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(p, 0o644)
	if _, err := WriteContext(io.Discard, dir); err == nil {
		t.Error("unreadable file silently skipped")
	}
}

func TestContextSkipsFIFO(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"a": "x"})
	if err := syscall.Mkfifo(filepath.Join(dir, "pipe"), 0o644); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	data, st := buildContext(t, dir)
	got, _ := untar(t, data)
	if _, ok := got["pipe"]; ok || st.Skipped != 1 {
		t.Errorf("fifo included or not counted: skipped=%d", st.Skipped)
	}
}

func TestMatchSegs(t *testing.T) {
	cases := []struct {
		pat, path string
		want      bool
	}{
		{"**/target", "target", true},
		{"**/target", "a/b/target", true},
		{"a/**/b", "a/b", true},
		{"a/**/b", "a/x/y/b", true},
		{"a/**", "a/x/y", true},
		{"apps/*/output", "apps/k/output", true},
		{"apps/*/output", "apps/k/l/output", false},
		{"a/b", "a/b/c", false},
	}
	for _, c := range cases {
		if got := matchSegs(strings.Split(c.pat, "/"), strings.Split(c.path, "/")); got != c.want {
			t.Errorf("matchSegs(%q, %q) = %v", c.pat, c.path, got)
		}
	}
}

// Include packs named files that .gitignore or excludes would drop, and
// fails when one is missing or its directory is excluded.
func TestContextInclude(t *testing.T) {
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		"workflows/wf/.gitignore": "wf\n",
		"workflows/wf/wf":         "ELF",
		"workflows/wf/other":      "kept",
		"target/bin/tool":         "x",
	})
	var buf bytes.Buffer
	if _, err := WriteContextOpts(&buf, dir, ContextOptions{
		Excludes: []string{"target"},
		Include:  []string{"workflows/wf/wf"},
	}); err != nil {
		t.Fatal(err)
	}
	got, _ := untar(t, buf.Bytes())
	if _, ok := got["workflows/wf/wf"]; !ok {
		t.Fatalf("include not packed: %v", got)
	}
	if _, ok := got["target/bin/tool"]; ok {
		t.Fatal("excluded file packed")
	}
	for _, inc := range []string{"workflows/wf/missing", "target/bin/tool"} {
		buf.Reset()
		_, err := WriteContextOpts(&buf, dir, ContextOptions{Excludes: []string{"target"}, Include: []string{inc}})
		if err == nil || !strings.Contains(err.Error(), inc) {
			t.Fatalf("include %s: err = %v", inc, err)
		}
	}
}
