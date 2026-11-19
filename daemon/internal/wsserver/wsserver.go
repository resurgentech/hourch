// Package wsserver serves GET /v1/worker/connect: one websocket per worker,
// every frame a JSON message from specs/hourchd/worker-protocol.schema.json
// (decoded and encoded with hourchdprotocol). The HTTP layer has already
// authenticated the worker token; its name is the worker id.
package wsserver

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"

	proto "github.com/resurgentech/hourch/protocol"

	"github.com/resurgentech/hourch/daemon/internal/dispatch"
)

// Options configure the server.
type Options struct {
	Engine *dispatch.Engine
	Log    *slog.Logger
	// ChunkMaxBytes is logs.chunk_max_bytes; the read limit allows a log
	// message of that size with worst-case JSON escaping.
	ChunkMaxBytes int
	// HelloTimeout bounds the wait for the first message (default 30s).
	HelloTimeout time.Duration
	// WriteTimeout bounds one frame write (default 30s).
	WriteTimeout time.Duration
}

// Server handles worker connections.
type Server struct {
	o Options
}

// New returns a server.
func New(o Options) *Server {
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.ChunkMaxBytes <= 0 {
		o.ChunkMaxBytes = 65536
	}
	if o.HelloTimeout <= 0 {
		o.HelloTimeout = 30 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 30 * time.Second
	}
	return &Server{o: o}
}

// conn adapts a websocket to dispatch.Conn: a bounded send queue drained by
// one writer goroutine.
type conn struct {
	ws      *websocket.Conn
	out     chan []byte
	closeCh chan string
	once    sync.Once
	log     *slog.Logger
	worker  string
}

const sendQueue = 256

func (c *conn) Send(m proto.Message) bool {
	b, err := proto.Encode(m)
	if err != nil {
		c.log.Error("encode worker message", "worker", c.worker, "type", m.MessageType(), "error", err)
		return false
	}
	select {
	case c.out <- b:
		return true
	default:
		return false
	}
}

func (c *conn) Close(reason string) {
	c.once.Do(func() { c.closeCh <- reason })
}

func (c *conn) writer(ctx context.Context, timeout time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case reason := <-c.closeCh:
			// Flush what is queued (a final cancel or welcome), then close.
			for {
				select {
				case b := <-c.out:
					wctx, cancel := context.WithTimeout(ctx, timeout)
					_ = c.ws.Write(wctx, websocket.MessageText, b)
					cancel()
					continue
				default:
				}
				break
			}
			_ = c.ws.Close(websocket.StatusPolicyViolation, reason)
			return
		case b := <-c.out:
			wctx, cancel := context.WithTimeout(ctx, timeout)
			err := c.ws.Write(wctx, websocket.MessageText, b)
			cancel()
			if err != nil {
				c.log.Info("worker write failed", "worker", c.worker, "error", err)
				c.ws.CloseNow()
				return
			}
		}
	}
}

// Serve upgrades the request and runs the connection until it closes.
// workerID is the authenticated worker token's name.
func (s *Server) Serve(w http.ResponseWriter, r *http.Request, workerID string) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.o.Log.Info("websocket accept failed", "worker", workerID, "error", err)
		return
	}
	// A log frame carries up to chunk_max_bytes of text; JSON escaping can
	// grow it up to 6x.
	ws.SetReadLimit(int64(s.o.ChunkMaxBytes)*6 + 64*1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hctx, hcancel := context.WithTimeout(ctx, s.o.HelloTimeout)
	typ, data, err := ws.Read(hctx)
	hcancel()
	if err != nil {
		ws.CloseNow()
		return
	}
	var hello *proto.Hello
	if typ == websocket.MessageText {
		if m, derr := proto.Decode(data); derr == nil {
			hello, _ = m.(*proto.Hello)
		} else {
			err = derr
		}
	}
	if hello == nil {
		s.o.Log.Warn("worker first message is not a valid hello", "worker", workerID, "error", err)
		_ = ws.Close(websocket.StatusPolicyViolation, "first message must be hello")
		return
	}
	if hello.WorkerID != workerID {
		s.o.Log.Warn("hello worker_id does not match token", "token", workerID, "hello", hello.WorkerID)
		_ = ws.Close(websocket.StatusPolicyViolation, "worker_id does not match the worker token")
		return
	}
	c := &conn{ws: ws, out: make(chan []byte, sendQueue), closeCh: make(chan string, 1), log: s.o.Log, worker: workerID}
	wdone := make(chan struct{})
	go func() {
		defer close(wdone)
		c.writer(ctx, s.o.WriteTimeout)
	}()
	sess, err := s.o.Engine.Hello(c, hello)
	if err != nil {
		s.o.Log.Error("worker hello failed", "worker", workerID, "error", err)
		c.Close("internal error")
		<-wdone
		return
	}
	for {
		typ, data, err := ws.Read(ctx)
		if err != nil {
			break
		}
		if typ != websocket.MessageText {
			c.Close("binary frames are not part of the protocol")
			break
		}
		m, err := proto.Decode(data)
		if err != nil {
			s.o.Log.Warn("bad worker message", "worker", workerID, "error", err)
			c.Close("invalid message: " + truncate(err.Error(), 100))
			break
		}
		if err := s.o.Engine.Handle(sess, m); err != nil {
			if errors.Is(err, dispatch.ErrProtocol) {
				s.o.Log.Warn("worker protocol violation", "worker", workerID, "error", err)
			}
			c.Close(truncate(err.Error(), 100))
			break
		}
	}
	s.o.Engine.Disconnect(sess)
	c.Close("bye")
	// Give the writer a moment to flush the close, then stop it.
	select {
	case <-wdone:
	case <-time.After(5 * time.Second):
		cancel()
		<-wdone
	}
	ws.CloseNow()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
