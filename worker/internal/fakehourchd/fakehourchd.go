// Package fakehourchd is an in-process stand-in for hourchd in worker
// tests: an httptest server that speaks the worker protocol with
// hourchdprotocol and serves the worker data routes from memory.
package fakehourchd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	hourchdprotocol "github.com/resurgentech/hourch/protocol"
)

// Received is one message from a worker.
type Received struct {
	Conn int // 1 for the first connection, 2 for the next, ...
	Msg  hourchdprotocol.Message
	At   time.Time
}

// Server is the fake hourchd.
type Server struct {
	HTTP  *httptest.Server
	Token string

	mu      sync.Mutex
	changed chan struct{}
	msgs    []Received
	nconn   int
	conn    *websocket.Conn
	connCtx context.CancelFunc
	reject  bool

	welcome        func(h *hourchdprotocol.Hello) *hourchdprotocol.Welcome
	contexts       map[string][]byte
	inputs         map[string][]byte
	artifactStatus func(jobID, path string) int
	artifacts      map[string][]byte
	artifactSHA    map[string]string
	artifactPuts   int
}

// SetWelcome sets the welcome builder (default: heartbeat 10, lease 45).
func (s *Server) SetWelcome(f func(h *hourchdprotocol.Hello) *hourchdprotocol.Welcome) {
	s.mu.Lock()
	s.welcome = f
	s.mu.Unlock()
}

// SetContext stores a job's context tarball (.tar.zst).
func (s *Server) SetContext(jobID string, b []byte) {
	s.mu.Lock()
	s.contexts[jobID] = b
	s.mu.Unlock()
}

// SetInput stores an input body for job jobID, upstream job and path; a
// path ending in "/" should hold a tar.
func (s *Server) SetInput(jobID, upstream, path string, b []byte) {
	s.mu.Lock()
	s.inputs[jobID+"/"+upstream+"/"+path] = b
	s.mu.Unlock()
}

// SetArtifactStatus lets a test fail artifact PUTs (return 0 to accept).
func (s *Server) SetArtifactStatus(f func(jobID, path string) int) {
	s.mu.Lock()
	s.artifactStatus = f
	s.mu.Unlock()
}

// ArtifactPuts counts PUT requests, failed ones included.
func (s *Server) ArtifactPuts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.artifactPuts
}

// ArtifactSHA returns the X-Sha256 header of a stored artifact.
func (s *Server) ArtifactSHA(jobID, path string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.artifactSHA[jobID+"/"+path]
}

// New starts a fake hourchd that accepts token.
func New(token string) *Server {
	s := &Server{
		Token:       token,
		changed:     make(chan struct{}),
		contexts:    map[string][]byte{},
		inputs:      map[string][]byte{},
		artifacts:   map[string][]byte{},
		artifactSHA: map[string]string{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/worker/connect", s.connect)
	mux.HandleFunc("GET /v1/worker/jobs/{id}/context", s.context)
	mux.HandleFunc("GET /v1/worker/jobs/{id}/inputs/{upstream}/{path...}", s.input)
	mux.HandleFunc("PUT /v1/worker/jobs/{id}/artifacts/{path...}", s.artifact)
	s.HTTP = httptest.NewServer(s.auth(mux))
	return s
}

// Close stops the server.
func (s *Server) Close() {
	s.DropConn()
	s.HTTP.Close()
}

// URL is the websocket URL for worker.yaml's hourchd_url.
func (s *Server) URL() string {
	return "ws" + strings.TrimPrefix(s.HTTP.URL, "http") + "/v1/worker/connect"
}

// SetReject makes connects fail with 503 while on.
func (s *Server) SetReject(on bool) {
	s.mu.Lock()
	s.reject = on
	s.mu.Unlock()
}

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+s.Token {
			http.Error(w, `{"code":"unauthorized","message":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) record(conn int, m hourchdprotocol.Message) {
	s.mu.Lock()
	s.msgs = append(s.msgs, Received{Conn: conn, Msg: m, At: time.Now()})
	close(s.changed)
	s.changed = make(chan struct{})
	s.mu.Unlock()
}

func (s *Server) connect(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	reject := s.reject
	s.mu.Unlock()
	if reject {
		http.Error(w, `{"code":"unavailable","message":"rejecting"}`, http.StatusServiceUnavailable)
		return
	}
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(16 << 20)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	_, data, err := c.Read(ctx)
	if err != nil {
		c.CloseNow()
		return
	}
	m, err := hourchdprotocol.Decode(data)
	hello, ok := m.(*hourchdprotocol.Hello)
	if err != nil || !ok {
		c.Close(websocket.StatusPolicyViolation, "first message must be hello")
		return
	}
	s.mu.Lock()
	// A newer connection closes the older one.
	if s.conn != nil {
		s.connCtx()
		s.conn.CloseNow()
	}
	s.nconn++
	id := s.nconn
	s.conn, s.connCtx = c, cancel
	s.mu.Unlock()
	s.mu.Lock()
	wf := s.welcome
	s.mu.Unlock()
	wel := &hourchdprotocol.Welcome{HeartbeatSeconds: 10, LeaseSeconds: 45}
	if wf != nil {
		wel = wf(hello)
	}
	if err := s.write(ctx, c, wel); err != nil {
		c.CloseNow()
		return
	}
	// Recorded after welcome went out, so a test that saw the hello may
	// Send without racing the welcome.
	s.record(id, hello)
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			c.CloseNow()
			return
		}
		m, err := hourchdprotocol.Decode(data)
		if err != nil {
			c.Close(websocket.StatusPolicyViolation, err.Error())
			return
		}
		s.record(id, m)
	}
}

func (s *Server) write(ctx context.Context, c *websocket.Conn, m hourchdprotocol.Message) error {
	b, err := hourchdprotocol.Encode(m)
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, b)
}

// Send writes m on the current connection.
func (s *Server) Send(m hourchdprotocol.Message) error {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c == nil {
		return errors.New("fakehourchd: no connection")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.write(ctx, c, m)
}

// DropConn closes the current connection abruptly.
func (s *Server) DropConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		s.connCtx()
		s.conn.CloseNow()
		s.conn = nil
	}
}

// Conns is the number of accepted connections so far.
func (s *Server) Conns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nconn
}

// Messages returns a copy of everything received so far.
func (s *Server) Messages() []Received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Received(nil), s.msgs...)
}

// Wait blocks until pred holds for the received messages or timeout.
func (s *Server) Wait(timeout time.Duration, pred func([]Received) bool) ([]Received, bool) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		s.mu.Lock()
		msgs := append([]Received(nil), s.msgs...)
		ch := s.changed
		s.mu.Unlock()
		if pred(msgs) {
			return msgs, true
		}
		select {
		case <-ch:
		case <-deadline.C:
			return msgs, false
		}
	}
}

// Artifact returns an uploaded artifact.
func (s *Server) Artifact(jobID, path string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.artifacts[jobID+"/"+path]
	return b, ok
}

func (s *Server) context(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	b, ok := s.contexts[r.PathValue("id")]
	s.mu.Unlock()
	if !ok {
		http.Error(w, `{"code":"not_found","message":"run has no context"}`, http.StatusNotFound)
		return
	}
	sum := sha256.Sum256(b)
	w.Header().Set("X-Sha256", hex.EncodeToString(sum[:]))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(b)
}

func (s *Server) input(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("id") + "/" + r.PathValue("upstream") + "/" + r.PathValue("path")
	s.mu.Lock()
	b, ok := s.inputs[key]
	s.mu.Unlock()
	if !ok {
		http.Error(w, fmt.Sprintf(`{"code":"not_found","message":"no input %s"}`, key), http.StatusNotFound)
		return
	}
	if strings.HasSuffix(key, "/") {
		w.Header().Set("Content-Type", "application/x-tar")
	} else {
		sum := sha256.Sum256(b)
		w.Header().Set("X-Sha256", hex.EncodeToString(sum[:]))
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Write(b)
}

func (s *Server) artifact(w http.ResponseWriter, r *http.Request) {
	id, p := r.PathValue("id"), r.PathValue("path")
	s.mu.Lock()
	s.artifactPuts++
	st := s.artifactStatus
	s.mu.Unlock()
	if st != nil {
		if code := st(id, p); code != 0 {
			http.Error(w, `{"code":"injected","message":"injected failure"}`, code)
			return
		}
	}
	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sum := sha256.Sum256(b)
	if got := hex.EncodeToString(sum[:]); got != r.Header.Get("X-Sha256") {
		http.Error(w, `{"code":"checksum_mismatch","message":"sha256 mismatch"}`, http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.artifacts[id+"/"+p] = b
	s.artifactSHA[id+"/"+p] = r.Header.Get("X-Sha256")
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"path":%q,"size":%d,"sha256":%q}`, p, len(b), hex.EncodeToString(sum[:]))
}
