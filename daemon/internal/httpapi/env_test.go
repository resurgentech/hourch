package httpapi_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	api "github.com/resurgentech/hourch/api"
	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/app"
	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/objects"
)

const (
	subTok = "tok-submitter-jared"
	w1Tok  = "tok-worker-w1"
	w2Tok  = "tok-worker-w2"
)

var t0 = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

type env struct {
	t    *testing.T
	dir  string
	cfg  config.Config
	clk  *clock.Fake
	obj  *objects.Mem
	a    *app.App
	srv  *httptest.Server
	stop func()
}

func testLog() *slog.Logger {
	if os.Getenv("HOURCHD_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newEnv(t *testing.T, tweak func(*config.Config)) *env {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StateDir = filepath.Join(dir, "state")
	cfg.TokensFile = filepath.Join(dir, "tokens.yaml")
	cfg.Events.Heartbeat = config.Duration(50 * time.Millisecond)
	cfg.Powerwarden.URL, cfg.UI.Enabled = "", false
	if tweak != nil {
		tweak(&cfg)
	}
	tokens := "submitters:\n  jared: " + subTok + "\nworkers:\n  w1: " + w1Tok + "\n  w2: " + w2Tok + "\n"
	if err := os.WriteFile(cfg.TokensFile, []byte(tokens), 0o600); err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(t0)
	obj := objects.NewMem()
	obj.Now = clk.Now
	e := &env{t: t, dir: dir, cfg: cfg, clk: clk, obj: obj}
	e.start()
	t.Cleanup(e.close)
	return e
}

func (e *env) start() {
	e.t.Helper()
	a, err := app.Build(e.cfg, app.Deps{Clock: e.clk, Objects: e.obj, Log: testLog()})
	if err != nil {
		e.t.Fatal(err)
	}
	seedSeen(e.t, a.Store)
	// Recover without the reaper loop: tests call Reap after moving the
	// fake clock.
	if err := a.Engine.Recover(); err != nil {
		e.t.Fatal(err)
	}
	e.a, e.stop = a, func() {}
	e.srv = httptest.NewServer(a.Handler())
}

// close stops the server and the app (a restart is close + start).
func (e *env) close() {
	if e.a == nil {
		return
	}
	e.a.Engine.CloseAll("test shutdown")
	e.srv.CloseClientConnections()
	e.srv.Close()
	e.stop()
	_ = e.a.Close()
	e.a = nil
}

func (e *env) do(method, path, token string, body io.Reader, hdr map[string]string) (*http.Response, []byte) {
	e.t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, body)
	if err != nil {
		e.t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func (e *env) json(method, path, token string, in any, want int, out any) []byte {
	e.t.Helper()
	var body io.Reader
	if in != nil {
		b, _ := json.Marshal(in)
		body = bytes.NewReader(b)
	}
	resp, b := e.do(method, path, token, body, nil)
	if resp.StatusCode != want {
		e.t.Fatalf("%s %s = %d, want %d: %s", method, path, resp.StatusCode, want, b)
	}
	if out != nil {
		if err := json.Unmarshal(b, out); err != nil {
			e.t.Fatalf("decode %s: %v", b, err)
		}
	}
	return b
}

func errCode(t *testing.T, b []byte) api.ErrorCode {
	t.Helper()
	var e api.Error
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("not an Error body: %s", b)
	}
	return e.Error
}

func sha(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// waitFor polls cond (the server processes websocket messages
// asynchronously); it does not depend on timing beyond the generous bound.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// --- a websocket worker ---

type worker struct {
	t    *testing.T
	ws   *websocket.Conn
	msgs chan proto.Message
	done chan struct{}
}

func (e *env) dial(token string) (*websocket.Conn, *http.Response, error) {
	u := "ws" + strings.TrimPrefix(e.srv.URL, "http") + "/v1/worker/connect"
	return websocket.Dial(context.Background(), u, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}})
}

func (e *env) worker(token string, h proto.Hello) (*worker, *proto.Welcome) {
	e.t.Helper()
	ws, _, err := e.dial(token)
	if err != nil {
		e.t.Fatal(err)
	}
	w := &worker{t: e.t, ws: ws, msgs: make(chan proto.Message, 100), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			_, b, err := ws.Read(context.Background())
			if err != nil {
				return
			}
			m, err := proto.Decode(b)
			if err != nil {
				panic(err)
			}
			w.msgs <- m
		}
	}()
	if h.Hostname == "" {
		h.Hostname = h.WorkerID + ".lab"
	}
	if h.Version == "" {
		h.Version = "test"
	}
	w.send(&h)
	wel, ok := w.next().(*proto.Welcome)
	if !ok {
		e.t.Fatal("first message is not welcome")
	}
	e.t.Cleanup(func() { ws.CloseNow() })
	return w, wel
}

func (w *worker) send(m proto.Message) {
	w.t.Helper()
	b, err := proto.Encode(m)
	if err != nil {
		w.t.Fatal(err)
	}
	if err := w.ws.Write(context.Background(), websocket.MessageText, b); err != nil {
		w.t.Fatal(err)
	}
}

func (w *worker) next() proto.Message {
	w.t.Helper()
	select {
	case m := <-w.msgs:
		return m
	case <-w.done:
		w.t.Fatal("connection closed")
	case <-time.After(10 * time.Second):
		w.t.Fatal("no message within 10s")
	}
	return nil
}

func (w *worker) assign() *proto.Assign {
	w.t.Helper()
	a, ok := w.next().(*proto.Assign)
	if !ok {
		w.t.Fatal("expected assign")
	}
	return a
}

// closed waits for the server to close the connection.
func (w *worker) closed() {
	w.t.Helper()
	select {
	case <-w.done:
	case <-time.After(10 * time.Second):
		w.t.Fatal("connection not closed")
	}
}

// --- SSE ---

type sseEvent struct {
	id, event, data string
}

func readSSE(t *testing.T, r *bufio.Reader, n int) []sseEvent {
	t.Helper()
	var out []sseEvent
	var cur sseEvent
	for len(out) < n {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("sse read after %d events: %v", len(out), err)
		}
		line = strings.TrimRight(line, "\n")
		switch {
		case line == "":
			if cur.event != "" {
				out = append(out, cur)
			}
			cur = sseEvent{}
		case strings.HasPrefix(line, "id: "):
			cur.id = line[4:]
		case strings.HasPrefix(line, "event: "):
			cur.event = line[7:]
		case strings.HasPrefix(line, "data: "):
			cur.data = line[6:]
		}
	}
	return out
}

func (e *env) sse(path string, lastID string) (*http.Response, *bufio.Reader) {
	e.t.Helper()
	req, _ := http.NewRequest("GET", e.srv.URL+path, nil)
	req.Header.Set("Authorization", "Bearer "+subTok)
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		e.t.Fatalf("sse %s = %d %s", path, resp.StatusCode, b)
	}
	return resp, bufio.NewReader(resp.Body)
}
