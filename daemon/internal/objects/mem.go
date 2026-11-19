package objects

import (
	"bytes"
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Mem is an in-memory Store for tests. Set Fail to make every call return
// that error (simulating MinIO down).
type Mem struct {
	mu   sync.Mutex
	objs map[string]memObj
	fail error
	// Now stamps LastModified; nil means time.Now.
	Now func() time.Time
}

type memObj struct {
	data []byte
	mod  time.Time
}

// NewMem returns an empty store.
func NewMem() *Mem { return &Mem{objs: map[string]memObj{}} }

// SetFail makes every later call fail with err (nil clears it).
func (m *Mem) SetFail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = err
}

func (m *Mem) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now().UTC()
}

func (m *Mem) Put(ctx context.Context, key string, r io.Reader, size int64) error {
	m.mu.Lock()
	fail := m.fail
	m.mu.Unlock()
	if fail != nil {
		return fail
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return err
	}
	if size >= 0 && int64(buf.Len()) != size {
		return io.ErrUnexpectedEOF
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objs[key] = memObj{data: buf.Bytes(), mod: m.now()}
	return nil
}

func (m *Mem) Get(ctx context.Context, key string) (io.ReadCloser, Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return nil, Info{}, m.fail
	}
	o, ok := m.objs[key]
	if !ok {
		return nil, Info{}, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(o.data)), Info{Key: key, Size: int64(len(o.data)), LastModified: o.mod}, nil
}

func (m *Mem) Stat(ctx context.Context, key string) (Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return Info{}, m.fail
	}
	o, ok := m.objs[key]
	if !ok {
		return Info{}, ErrNotFound
	}
	return Info{Key: key, Size: int64(len(o.data)), LastModified: o.mod}, nil
}

func (m *Mem) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	delete(m.objs, key)
	return nil
}

func (m *Mem) List(ctx context.Context, prefix string) ([]Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return nil, m.fail
	}
	var out []Info
	for k, o := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, Info{Key: k, Size: int64(len(o.data)), LastModified: o.mod})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *Mem) Copy(ctx context.Context, src, dst string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail != nil {
		return m.fail
	}
	o, ok := m.objs[src]
	if !ok {
		return ErrNotFound
	}
	m.objs[dst] = memObj{data: o.data, mod: m.now()}
	return nil
}

func (m *Mem) Ping(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fail
}

// Keys returns every key (for tests).
func (m *Mem) Keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.objs))
	for k := range m.objs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Bytes returns an object's content (for tests).
func (m *Mem) Bytes(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[key]
	return o.data, ok
}

// SetModified backdates an object (for prune tests).
func (m *Mem) SetModified(key string, t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if o, ok := m.objs[key]; ok {
		o.mod = t
		m.objs[key] = o
	}
}
