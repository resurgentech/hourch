package hourchworker

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// cleanRel checks a slash-separated relative path from the job or a tar
// header and returns it cleaned. Absolute paths and paths that leave the
// root are errors.
func cleanRel(p string) (string, error) {
	if p == "" {
		return "", errors.New("empty path")
	}
	if strings.HasPrefix(p, "/") || strings.Contains(p, "\x00") {
		return "", fmt.Errorf("path %q: must be relative", p)
	}
	c := path.Clean(p)
	if c == ".." || strings.HasPrefix(c, "../") {
		return "", fmt.Errorf("path %q: leaves the work root", p)
	}
	return c, nil
}

// mkdirAllIn creates rel (and parents) under root without following
// symlinks out of it. os.Root (Go 1.24) has no MkdirAll.
func mkdirAllIn(root *os.Root, rel string) error {
	if rel == "." || rel == "" {
		return nil
	}
	parts := strings.Split(rel, "/")
	for i := range parts {
		p := strings.Join(parts[:i+1], "/")
		st, err := root.Lstat(p)
		if err == nil {
			if !st.IsDir() {
				return fmt.Errorf("%s: exists and is not a directory", p)
			}
			continue
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := root.Mkdir(p, 0o755); err != nil && !errors.Is(err, fs.ErrExist) {
			return err
		}
	}
	return nil
}

// writeFileIn streams r into rel under root (parents created) and returns
// the hex SHA-256 of what it wrote.
func writeFileIn(root *os.Root, rel string, mode fs.FileMode, r io.Reader) (string, error) {
	if err := mkdirAllIn(root, path.Dir(rel)); err != nil {
		return "", err
	}
	f, err := root.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), r); err != nil {
		f.Close()
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// extractTar unpacks an uncompressed tar stream into dir. Entries may not
// escape dir (absolute names, "..", or writing through a symlink). Regular
// files, directories, symlinks and hard links are supported; other types
// are skipped.
func extractTar(r io.Reader, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	tr := tar.NewReader(r)
	var links []string
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			// Every link checked again now that all exist: a link can be
			// made to escape by a link created after it (l -> ".", then
			// s -> "l/.." resolves above dir).
			for _, l := range links {
				if err := resolveInside(dir, l); err != nil {
					return fmt.Errorf("tar: symlink %s: %w", l, err)
				}
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("tar: %w", err)
		}
		name := strings.TrimPrefix(hdr.Name, "./")
		if name == "" || name == "." {
			continue
		}
		rel, err := cleanRel(name)
		if err != nil {
			return fmt.Errorf("tar: %w", err)
		}
		mode := fs.FileMode(hdr.Mode).Perm()
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := mkdirAllIn(root, rel); err != nil {
				return fmt.Errorf("tar: %s: %w", rel, err)
			}
		case tar.TypeReg, tar.TypeRegA:
			_ = root.Remove(rel)
			if _, err := writeFileIn(root, rel, mode|0o200, tr); err != nil {
				return fmt.Errorf("tar: %s: %w", rel, err)
			}
			if mode&0o200 == 0 {
				_ = os.Chmod(filepath.Join(dir, filepath.FromSlash(rel)), mode)
			}
			if !hdr.ModTime.IsZero() {
				_ = os.Chtimes(filepath.Join(dir, filepath.FromSlash(rel)), time.Now(), hdr.ModTime)
			}
		case tar.TypeSymlink:
			if hdr.Linkname == "" || strings.HasPrefix(hdr.Linkname, "/") {
				return fmt.Errorf("tar: symlink %s -> %q: %w", rel, hdr.Linkname, errEscape)
			}
			if err := resolveInside(dir, path.Dir(rel)+"/"+hdr.Linkname); err != nil {
				return fmt.Errorf("tar: symlink %s -> %q: %w", rel, hdr.Linkname, err)
			}
			links = append(links, rel)
			parent := path.Dir(rel)
			if err := mkdirAllIn(root, parent); err != nil {
				return fmt.Errorf("tar: %s: %w", rel, err)
			}
			// mkdirAllIn verified every parent component is a real
			// directory inside root, so this does not write outside it.
			_ = root.Remove(rel)
			if err := os.Symlink(hdr.Linkname, filepath.Join(dir, filepath.FromSlash(rel))); err != nil {
				return fmt.Errorf("tar: %s: %w", rel, err)
			}
		case tar.TypeLink:
			target, err := cleanRel(hdr.Linkname)
			if err != nil {
				return fmt.Errorf("tar: %s: link: %w", rel, err)
			}
			src, err := root.Open(target)
			if err != nil {
				return fmt.Errorf("tar: %s: link: %w", rel, err)
			}
			_ = root.Remove(rel)
			if st, serr := src.Stat(); serr == nil {
				mode = st.Mode().Perm() // a link has its target's mode
			}
			_, err = writeFileIn(root, rel, mode|0o200, src)
			src.Close()
			if err != nil {
				return fmt.Errorf("tar: %s: %w", rel, err)
			}
		default:
			// Devices, fifos and the like have no place in a context.
		}
	}
}

// errEscape marks an archive entry that would resolve outside its
// destination directory.
var errEscape = errors.New("resolves outside the destination directory")

// resolveInside resolves rel (slash-separated, relative to dir) the way the
// kernel would, following symlinks that already exist under dir, and fails
// if any step leaves dir or a link is absolute. Components that do not
// exist yet are taken lexically.
func resolveInside(dir, rel string) error {
	var stack []string
	todo := strings.Split(rel, "/")
	hops := 0
	for len(todo) > 0 {
		c := todo[0]
		todo = todo[1:]
		switch c {
		case "", ".":
			continue
		case "..":
			if len(stack) == 0 {
				return errEscape
			}
			stack = stack[:len(stack)-1]
			continue
		}
		stack = append(stack, c)
		p := filepath.Join(append([]string{dir}, stack...)...)
		fi, err := os.Lstat(p)
		if err != nil || fi.Mode()&fs.ModeSymlink == 0 {
			continue
		}
		if hops++; hops > 40 {
			return errors.New("too many levels of symbolic links")
		}
		t, err := os.Readlink(p)
		if err != nil {
			return err
		}
		if strings.HasPrefix(t, "/") {
			return errEscape
		}
		stack = stack[:len(stack)-1]
		todo = append(strings.Split(t, "/"), todo...)
	}
	return nil
}

// fileSHA256 returns the hex SHA-256 and size of a file.
func fileSHA256(p string) (string, int64, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// artifactFiles expands a declared artifact path into the files to upload,
// as (artifact path, local path) pairs. A path ending in "/" is a directory:
// every regular file under it (symlinked directories are not followed) is
// one upload named <dir>/<relative path> (assumption 8).
func artifactFiles(workRoot, decl string) ([][2]string, error) {
	isDir := strings.HasSuffix(decl, "/")
	rel, err := cleanRel(decl)
	if err != nil {
		return nil, err
	}
	local := filepath.Join(workRoot, filepath.FromSlash(rel))
	if !isDir {
		st, err := os.Stat(local)
		if err != nil {
			return nil, err
		}
		if !st.Mode().IsRegular() {
			return nil, fmt.Errorf("%s: not a regular file", decl)
		}
		return [][2]string{{rel, local}}, nil
	}
	st, err := os.Stat(local)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("%s: not a directory", decl)
	}
	var out [][2]string
	err = filepath.WalkDir(local, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := os.Stat(p) // follow file symlinks
		if err != nil || !info.Mode().IsRegular() {
			return nil
		}
		sub, err := filepath.Rel(local, p)
		if err != nil {
			return err
		}
		out = append(out, [2]string{rel + "/" + filepath.ToSlash(sub), p})
		return nil
	})
	return out, err
}
