// Package logs keeps the live log of each leased job attempt: an append-only
// file under <state_dir>/logs/<job>/<attempt>.log, and in-process tail
// subscriptions for GET /v1/jobs/{id}/log/stream. When the attempt ends the
// engine uploads the file to the object store and removes it.
package logs

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Chunk is one appended piece. Offset is the log length after it.
type Chunk struct {
	Offset int64
	Stream string
	Data   string
}

// End closes an attempt's tail.
type End struct {
	Offset   int64
	Status   string
	ExitCode *int
}

// Sub is a tail subscription. C carries Chunks; it is closed after End is
// set (attempt over) or when the subscriber lagged.
type Sub struct {
	C      <-chan Chunk
	ch     chan Chunk
	m      *Manager
	key    string
	mu     sync.Mutex
	end    *End
	closed bool
	lagged bool
}

// Ended returns the end record once C is closed (nil if lagged or closed by
// the subscriber).
func (s *Sub) Ended() *End {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.end
}

// Lagged reports whether the subscriber fell too far behind.
func (s *Sub) Lagged() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lagged
}

// Close unsubscribes.
func (s *Sub) Close() {
	s.m.mu.Lock()
	defer s.m.mu.Unlock()
	s.m.dropLocked(s)
}

// Manager owns the live log files.
type Manager struct {
	dir  string
	mu   sync.Mutex
	subs map[string]map[*Sub]struct{}
}

// New returns a manager rooted at dir (created if missing).
func New(dir string) (*Manager, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &Manager{dir: dir, subs: map[string]map[*Sub]struct{}{}}, nil
}

func key(jobID string, attempt int) string { return jobID + "/" + strconv.Itoa(attempt) }

// Path is the live file of an attempt.
func (m *Manager) Path(jobID string, attempt int) string {
	return filepath.Join(m.dir, jobID, strconv.Itoa(attempt)+".log")
}

// Reset truncates an attempt's file (a fresh assignment of that attempt).
func (m *Manager) Reset(jobID string, attempt int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.Path(jobID, attempt)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return err
	}
	return os.WriteFile(p, nil, 0o600)
}

// Append adds data to an attempt's log and returns the new length.
func (m *Manager) Append(jobID string, attempt int, stream, data string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.Path(jobID, attempt)
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return 0, err
	}
	_, werr := f.WriteString(data)
	fi, serr := f.Stat()
	cerr := f.Close()
	if werr != nil {
		return 0, werr
	}
	if serr != nil {
		return 0, serr
	}
	if cerr != nil {
		return 0, cerr
	}
	off := fi.Size()
	for s := range m.subs[key(jobID, attempt)] {
		select {
		case s.ch <- Chunk{Offset: off, Stream: stream, Data: data}:
		default:
			s.mu.Lock()
			s.lagged = true
			s.mu.Unlock()
			m.dropLocked(s)
		}
	}
	return off, nil
}

// Size returns an attempt's live length (0 when no file).
func (m *Manager) Size(jobID string, attempt int) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	fi, err := os.Stat(m.Path(jobID, attempt))
	if err != nil {
		return 0
	}
	return fi.Size()
}

// Exists reports whether an attempt has a live file.
func (m *Manager) Exists(jobID string, attempt int) bool {
	_, err := os.Stat(m.Path(jobID, attempt))
	return err == nil
}

// Subscribe registers a tail and returns the length at subscription: chunks
// on C start after it.
func (m *Manager) Subscribe(jobID string, attempt int) (*Sub, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan Chunk, 1024)
	s := &Sub{C: ch, ch: ch, m: m, key: key(jobID, attempt)}
	if m.subs[s.key] == nil {
		m.subs[s.key] = map[*Sub]struct{}{}
	}
	m.subs[s.key][s] = struct{}{}
	var size int64
	if fi, err := os.Stat(m.Path(jobID, attempt)); err == nil {
		size = fi.Size()
	}
	return s, size
}

// Finish ends every tail of an attempt with e.
func (m *Manager) Finish(jobID string, attempt int, e End) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for s := range m.subs[key(jobID, attempt)] {
		s.mu.Lock()
		ec := e
		s.end = &ec
		s.mu.Unlock()
		m.dropLocked(s)
	}
}

func (m *Manager) dropLocked(s *Sub) {
	set := m.subs[s.key]
	if _, ok := set[s]; !ok {
		return
	}
	delete(set, s)
	if len(set) == 0 {
		delete(m.subs, s.key)
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.ch)
	}
	s.mu.Unlock()
}

// Remove deletes an attempt's file (after upload).
func (m *Manager) Remove(jobID string, attempt int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.Path(jobID, attempt)
	err := os.Remove(p)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	_ = os.Remove(filepath.Dir(p)) // only succeeds when empty
	return nil
}

// File is one live file on disk.
type File struct {
	JobID   string
	Attempt int
}

// Files lists every live file (for re-uploading after a restart).
func (m *Manager) Files() ([]File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dirs, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, err
	}
	var out []File
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		ents, err := os.ReadDir(filepath.Join(m.dir, d.Name()))
		if err != nil {
			return nil, err
		}
		for _, e := range ents {
			name := e.Name()
			if !strings.HasSuffix(name, ".log") {
				continue
			}
			n, err := strconv.Atoi(strings.TrimSuffix(name, ".log"))
			if err != nil {
				return nil, fmt.Errorf("unexpected log file %s", filepath.Join(d.Name(), name))
			}
			out = append(out, File{JobID: d.Name(), Attempt: n})
		}
	}
	return out, nil
}
