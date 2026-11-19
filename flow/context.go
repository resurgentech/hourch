package hourchflow

import (
	"archive/tar"
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// ContextMaxBytes is the spec's limits.context_max_bytes default (2 GiB).
// WriteContext stops with ErrContextTooLarge once the compressed stream
// passes it rather than uploading bytes the server will refuse.
const ContextMaxBytes int64 = 2 << 30

// ErrContextTooLarge is returned when the compressed context exceeds the
// limit.
var ErrContextTooLarge = errors.New("context tarball exceeds limit")

// DefaultExcludes are always applied: `.git` at any depth.
var DefaultExcludes = []string{".git"}

// ContextStats describes a written context.
type ContextStats struct {
	Files      int   // regular files
	Dirs       int   // directories (the root is not an entry)
	Symlinks   int   // symlinks, stored as links
	Skipped    int   // sockets, FIFOs and devices, which are left out
	Bytes      int64 // regular-file content, uncompressed
	Compressed int64 // bytes written to w
}

// ContextOptions tunes WriteContextOpts.
type ContextOptions struct {
	// Excludes are gitignore-syntax patterns rooted at the context dir,
	// applied after .gitignore files and winning over them.
	Excludes []string
	// Include are files (slash-separated paths relative to the context dir)
	// packed even when .gitignore or Excludes would drop them, e.g. a
	// workflow's own compiled binary, which its jobs run. Their directories
	// must not be excluded. A missing include is an error.
	Include []string
	// MaxBytes caps the compressed size; 0 means ContextMaxBytes, <0 none.
	MaxBytes int64
	// EncoderConcurrency is passed to zstd; 0 means the zstd default.
	EncoderConcurrency int
}

// WriteContext streams a zstd-compressed tar of dir to w, holding at most
// one file's read buffer in memory. Rules:
//
//   - Entries are written depth first with each directory's names in byte
//     order (a directory's entry, then its contents), with forward slashes
//     and no leading "./". Directories are entries (so empty directories
//     survive).
//   - Headers are normalised so the same tree gives the same bytes (and the
//     same content-addressed context_ref): mtime is the Unix epoch (as
//     tarballer does today), uid/gid 0, no user/group names; the permission
//     bits are kept, so executables stay executable.
//   - .gitignore files are honoured as git does for the common cases: `#`
//     comments, `!` negation, a trailing `/` matches only directories, a
//     pattern with a `/` (other than trailing) is anchored at its
//     .gitignore's directory, otherwise it matches a name at any depth below
//     it; `*`, `?`, `[...]` and `**` segments. A later line and a deeper file
//     win. An excluded directory is not entered, so nothing below it can be
//     re-included. Global git excludes and .git/info/exclude are not read.
//   - DefaultExcludes and excludes (same syntax, rooted at dir) are applied
//     after .gitignore and cannot be negated.
//   - ContextOptions.Include files are packed regardless of .gitignore and
//     excludes (their directories must still be walked); a missing one is an
//     error.
//   - Symlinks are stored as symlinks with their target verbatim and are
//     never followed, including links to directories; whoever unpacks must
//     not follow links that leave the tree. Hard links are stored as
//     separate regular files. Sockets, FIFOs and devices are skipped.
//   - An unreadable file or directory is an error (a silently partial
//     context builds the wrong thing).
func WriteContext(w io.Writer, dir string, excludes ...string) (ContextStats, error) {
	return WriteContextOpts(w, dir, ContextOptions{Excludes: excludes})
}

// WriteContextOpts is WriteContext with options.
func WriteContextOpts(w io.Writer, dir string, opts ContextOptions) (ContextStats, error) {
	var st ContextStats
	info, err := os.Stat(dir)
	if err != nil {
		return st, fmt.Errorf("context dir: %w", err)
	}
	if !info.IsDir() {
		return st, fmt.Errorf("context dir %s is not a directory", dir)
	}

	var user []ignorePattern
	for _, ex := range append(append([]string(nil), DefaultExcludes...), opts.Excludes...) {
		p, ok := parseIgnoreLine(ex, "")
		if !ok {
			continue
		}
		if p.negate {
			return st, fmt.Errorf("exclude %q: negation is not supported in excludes", ex)
		}
		user = append(user, p)
	}

	limit := opts.MaxBytes
	if limit == 0 {
		limit = ContextMaxBytes
	}
	cw := &countingWriter{w: w, limit: limit}
	zopts := []zstd.EOption{}
	if opts.EncoderConcurrency > 0 {
		zopts = append(zopts, zstd.WithEncoderConcurrency(opts.EncoderConcurrency))
	}
	zw, err := zstd.NewWriter(cw, zopts...)
	if err != nil {
		return st, err
	}
	tw := tar.NewWriter(zw)

	cb := &contextBuilder{tw: tw, user: user, st: &st, buf: make([]byte, 256<<10), include: map[string]bool{}}
	for _, inc := range opts.Include {
		cb.include[strings.Trim(filepath.ToSlash(inc), "/")] = false
	}
	if err := cb.walk(dir, "", nil); err != nil {
		zw.Close()
		return st, err
	}
	var missing []string
	for inc, written := range cb.include {
		if !written {
			missing = append(missing, inc)
		}
	}
	if len(missing) > 0 {
		zw.Close()
		sort.Strings(missing)
		return st, fmt.Errorf("context: include %s: not a regular file in %s, or its directory is excluded", strings.Join(missing, ", "), dir)
	}
	if err := tw.Close(); err != nil {
		zw.Close()
		return st, err
	}
	if err := zw.Close(); err != nil {
		return st, err
	}
	st.Compressed = cw.n
	return st, nil
}

type countingWriter struct {
	w     io.Writer
	n     int64
	limit int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	if c.limit > 0 && c.n+int64(len(p)) > c.limit {
		return 0, fmt.Errorf("%w (%d bytes)", ErrContextTooLarge, c.limit)
	}
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

type contextBuilder struct {
	tw      *tar.Writer
	user    []ignorePattern
	st      *ContextStats
	buf     []byte
	include map[string]bool // Include paths -> written
}

var epoch = time.Unix(0, 0).UTC()

// walk writes the entries below absDir (whose context-relative path is rel,
// "" for the root). inherited holds the .gitignore patterns of its parents.
func (cb *contextBuilder) walk(absDir, rel string, inherited []ignorePattern) error {
	patterns := inherited
	gi := filepath.Join(absDir, ".gitignore")
	if fi, err := os.Lstat(gi); err == nil && fi.Mode().IsRegular() {
		local, err := readGitignore(gi, rel)
		if err != nil {
			return err
		}
		patterns = append(append([]ignorePattern(nil), inherited...), local...)
	}

	entries, err := os.ReadDir(absDir) // sorted by name
	if err != nil {
		return fmt.Errorf("context: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		childRel := name
		if rel != "" {
			childRel = rel + "/" + name
		}
		abs := filepath.Join(absDir, name)
		fi, err := os.Lstat(abs)
		if err != nil {
			return fmt.Errorf("context: %w", err)
		}
		isDir := fi.IsDir()
		_, included := cb.include[childRel]
		included = included && fi.Mode().IsRegular()
		if !included && (ignored(patterns, childRel, isDir) || excluded(cb.user, childRel, isDir)) {
			continue
		}
		if included {
			cb.include[childRel] = true
		}
		mode := fi.Mode()
		hdr := &tar.Header{
			Name:    childRel,
			Mode:    int64(mode.Perm()),
			ModTime: epoch,
			Format:  tar.FormatPAX,
		}
		switch {
		case isDir:
			hdr.Typeflag = tar.TypeDir
			hdr.Name += "/"
			if err := cb.tw.WriteHeader(hdr); err != nil {
				return err
			}
			cb.st.Dirs++
			if err := cb.walk(abs, childRel, patterns); err != nil {
				return err
			}
		case mode&fs.ModeSymlink != 0:
			target, err := os.Readlink(abs)
			if err != nil {
				return fmt.Errorf("context: %w", err)
			}
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = target
			if err := cb.tw.WriteHeader(hdr); err != nil {
				return err
			}
			cb.st.Symlinks++
		case mode.IsRegular():
			if err := cb.writeFile(abs, hdr, fi.Size()); err != nil {
				return err
			}
		default:
			cb.st.Skipped++
		}
	}
	return nil
}

func (cb *contextBuilder) writeFile(abs string, hdr *tar.Header, size int64) error {
	f, err := os.Open(abs)
	if err != nil {
		return fmt.Errorf("context: %w", err)
	}
	defer f.Close()
	hdr.Typeflag = tar.TypeReg
	hdr.Size = size
	if err := cb.tw.WriteHeader(hdr); err != nil {
		return err
	}
	n, err := io.CopyBuffer(cb.tw, io.LimitReader(f, size), cb.buf)
	if err != nil {
		return fmt.Errorf("context: %s: %w", hdr.Name, err)
	}
	if n != size {
		return fmt.Errorf("context: %s: file shrank while reading (%d of %d bytes)", hdr.Name, n, size)
	}
	cb.st.Files++
	cb.st.Bytes += n
	return nil
}

// ignorePattern is one parsed gitignore line.
type ignorePattern struct {
	base     string   // context-relative dir of the .gitignore ("" = root)
	negate   bool     // "!" prefix
	dirOnly  bool     // trailing "/"
	anchored bool     // contains "/" (other than trailing): match from base
	segs     []string // anchored: pattern split on "/"
	glob     string   // unanchored: matched against the last path element
}

func readGitignore(file, base string) ([]ignorePattern, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("context: %w", err)
	}
	defer f.Close()
	var out []ignorePattern
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		if p, ok := parseIgnoreLine(sc.Text(), base); ok {
			out = append(out, p)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("context: %s: %w", file, err)
	}
	return out, nil
}

func parseIgnoreLine(line, base string) (ignorePattern, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return ignorePattern{}, false
	}
	if strings.HasPrefix(line, "#") {
		return ignorePattern{}, false
	}
	p := ignorePattern{base: base}
	if strings.HasPrefix(line, "!") {
		p.negate = true
		line = line[1:]
	} else if strings.HasPrefix(line, `\!`) || strings.HasPrefix(line, `\#`) {
		line = line[1:]
	}
	if strings.HasSuffix(line, "/") {
		p.dirOnly = true
		line = strings.TrimRight(line, "/")
	}
	if line == "" {
		return ignorePattern{}, false
	}
	if strings.Contains(line, "/") {
		p.anchored = true
		p.segs = strings.Split(strings.TrimPrefix(line, "/"), "/")
	} else {
		p.glob = line
	}
	return p, true
}

// matches reports whether p matches rel (context-relative, slash form).
func (p ignorePattern) matches(rel string, isDir bool) bool {
	if p.dirOnly && !isDir {
		return false
	}
	sub := rel
	if p.base != "" {
		var ok bool
		sub, ok = strings.CutPrefix(rel, p.base+"/")
		if !ok {
			return false
		}
	}
	if !p.anchored {
		g := p.glob
		if g == "**" {
			return true
		}
		ok, _ := path.Match(strings.ReplaceAll(g, "**", "*"), path.Base(sub))
		return ok
	}
	return matchSegs(p.segs, strings.Split(sub, "/"))
}

// matchSegs matches pattern segments against path segments; a "**" segment
// matches zero or more path segments.
func matchSegs(pat, segs []string) bool {
	for len(pat) > 0 {
		if pat[0] == "**" {
			rest := pat[1:]
			if len(rest) == 0 {
				return true
			}
			for i := 0; i <= len(segs); i++ {
				if matchSegs(rest, segs[i:]) {
					return true
				}
			}
			return false
		}
		if len(segs) == 0 {
			return false
		}
		if ok, _ := path.Match(pat[0], segs[0]); !ok {
			return false
		}
		pat, segs = pat[1:], segs[1:]
	}
	return len(segs) == 0
}

// ignored applies gitignore patterns in order; the last match wins.
func ignored(patterns []ignorePattern, rel string, isDir bool) bool {
	out := false
	for _, p := range patterns {
		if p.matches(rel, isDir) {
			out = !p.negate
		}
	}
	return out
}

// excluded applies the always-on excludes; any match excludes.
func excluded(patterns []ignorePattern, rel string, isDir bool) bool {
	for _, p := range patterns {
		if p.matches(rel, isDir) {
			return true
		}
	}
	return false
}
