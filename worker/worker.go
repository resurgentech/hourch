package hourchworker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	hourchdprotocol "github.com/resurgentech/hourch/protocol"
)

// Version is reported in hello; set with
// -ldflags "-X github.com/resurgentech/hourch/worker.Version=...".
var Version = "dev"

// Options are the knobs that are not in worker.yaml. Zero values take the
// defaults; tests shorten the timings.
type Options struct {
	Logger *slog.Logger
	// Version overrides the package Version in hello.
	Version string
	// HeartbeatUnit is the length of one welcome.heartbeat_seconds second
	// (default 1s; tests use milliseconds).
	HeartbeatUnit time.Duration
	// KillGrace is the wait between SIGTERM and SIGKILL (default 10s).
	KillGrace time.Duration
	// ChunkMaxBytes and ChunkMaxDelay bound a log chunk (64 KiB, 250ms).
	ChunkMaxBytes int
	ChunkMaxDelay time.Duration
	// WelcomeTimeout bounds the wait for welcome after hello (10s).
	WelcomeTimeout time.Duration
	// WriteTimeout bounds one websocket write (30s).
	WriteTimeout time.Duration
	// PipeDrain is how long output is read after the process exits before
	// the pipes are closed (5s).
	PipeDrain time.Duration
	// TransferAttempts is the number of tries for one context, input or
	// artifact transfer (5); the backoff is the reconnect backoff.
	TransferAttempts int
	// HTTPClient is used for data routes and the websocket dial.
	HTTPClient *http.Client
	// Environ is the worker environment the allowlist reads (os.Environ).
	Environ func() []string
	// Docker is the docker CLI (default: "docker" on the worker's PATH).
	Docker string
	// Jitter returns a value in [0,1) for backoff jitter (math/rand).
	Jitter func() float64
}

func (o *Options) defaults() {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Version == "" {
		o.Version = Version
	}
	if o.HeartbeatUnit <= 0 {
		o.HeartbeatUnit = time.Second
	}
	if o.KillGrace <= 0 {
		o.KillGrace = 10 * time.Second
	}
	if o.ChunkMaxBytes <= 0 {
		o.ChunkMaxBytes = ChunkMaxBytes
	}
	if o.ChunkMaxDelay <= 0 {
		o.ChunkMaxDelay = ChunkMaxDelay
	}
	if o.WelcomeTimeout <= 0 {
		o.WelcomeTimeout = 10 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 30 * time.Second
	}
	if o.PipeDrain <= 0 {
		o.PipeDrain = 5 * time.Second
	}
	if o.TransferAttempts <= 0 {
		o.TransferAttempts = 5
	}
	if o.HTTPClient == nil {
		o.HTTPClient = http.DefaultClient
	}
	if o.Environ == nil {
		o.Environ = os.Environ
	}
	if o.Docker == "" {
		o.Docker = "docker"
	}
	if o.Jitter == nil {
		o.Jitter = rand.Float64
	}
}

// Worker is one hourch worker: one websocket, at most one job.
type Worker struct {
	cfg       Config
	token     string
	opts      Options
	log       *slog.Logger
	httpBase  string
	data      *dataClient
	container *containerRunner

	wake chan struct{}

	mu       sync.Mutex
	cur      *jobRun
	draining bool
	jobs     sync.WaitGroup
}

// New returns a worker. token is the worker token (ReadTokenFile).
func New(cfg Config, token string, opts Options) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if token == "" {
		return nil, errors.New("empty worker token")
	}
	opts.defaults()
	base, err := cfg.HTTPBase()
	if err != nil {
		return nil, err
	}
	w := &Worker{
		cfg:      cfg,
		token:    token,
		opts:     opts,
		log:      opts.Logger,
		httpBase: base,
		wake:     make(chan struct{}, 1),
	}
	w.data = &dataClient{base: base, token: token, hc: opts.HTTPClient, attempts: opts.TransferAttempts, backoff: w.backoff}
	if cfg.HasDocker() {
		docker := opts.Docker
		if !strings.Contains(docker, "/") {
			p, err := exec.LookPath(docker)
			if err != nil {
				return nil, fmt.Errorf("capabilities: docker: %w", err)
			}
			docker = p
		}
		w.container = &containerRunner{docker: docker, envDir: cfg.WorkDir, cliEnv: cliEnv(opts.Environ())}
	}
	return w, nil
}

// cliEnv is the docker CLI's environment: the worker's, minus HOURCH_*.
func cliEnv(environ []string) []string {
	var out []string
	for _, kv := range environ {
		if !strings.HasPrefix(kv, "HOURCH_") {
			out = append(out, kv)
		}
	}
	return out
}

// backoff is the delay before reconnect (or transfer retry) n (n >= 1):
// initial doubling to max, then jittered into [d/2, d).
func (w *Worker) backoff(n int) time.Duration {
	d := w.cfg.Reconnect.Initial.D()
	for i := 1; i < n && d < w.cfg.Reconnect.Max.D(); i++ {
		d *= 2
	}
	if d > w.cfg.Reconnect.Max.D() {
		d = w.cfg.Reconnect.Max.D()
	}
	return d/2 + time.Duration(w.opts.Jitter()*float64(d/2))
}

// poke wakes the sender (and Run while it waits to reconnect).
func (w *Worker) poke() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Drain stops taking new jobs. Run returns once the running job (if any)
// has ended and its result was delivered; an idle worker returns at once.
// The running job is not killed.
func (w *Worker) Drain() {
	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()
	w.poke()
}

func (w *Worker) drainedIdle() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.draining && w.cur == nil
}

// Busy reports the running job id, or "".
func (w *Worker) Busy() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == nil {
		return ""
	}
	return w.cur.id
}

var workDirEntryRE = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}-[0-9]+$`)

// cleanWorkDir removes job directories a previous worker process left.
// Only names shaped <ULID>-<attempt> are touched.
func (w *Worker) cleanWorkDir() error {
	if err := os.MkdirAll(w.cfg.WorkDir, 0o755); err != nil {
		return err
	}
	ents, err := os.ReadDir(w.cfg.WorkDir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if workDirEntryRE.MatchString(e.Name()) || strings.HasPrefix(e.Name(), ".hourch-env-") {
			if err := os.RemoveAll(filepath.Join(w.cfg.WorkDir, e.Name())); err != nil {
				w.log.Warn("stale work dir not removed", "dir", e.Name(), "err", err)
			}
		}
	}
	return nil
}

// Run connects to hourchd and serves until ctx is done (the running job is
// killed, no result is sent: its lease lapses) or, after Drain, until the
// worker is idle.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.cleanWorkDir(); err != nil {
		return fmt.Errorf("work_dir: %w", err)
	}
	defer func() {
		w.mu.Lock()
		if jr := w.cur; jr != nil {
			jr.cancel(errShutdown)
		}
		w.mu.Unlock()
		w.jobs.Wait()
	}()
	fails := 0
	for {
		if w.drainedIdle() {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		welcomed, err := w.session(ctx)
		if w.drainedIdle() || ctx.Err() != nil {
			return nil
		}
		if welcomed {
			fails = 0
		}
		fails++
		d := w.backoff(fails)
		w.log.Warn("disconnected from hourchd", "err", err, "retry_in", d)
		t := time.NewTimer(d)
	wait:
		for {
			select {
			case <-ctx.Done():
				t.Stop()
				return nil
			case <-t.C:
				break wait
			case <-w.wake:
				if w.drainedIdle() {
					t.Stop()
					return nil
				}
			}
		}
	}
}

// errDrained ends a session when a draining worker is idle.
var errDrained = errors.New("drained")

// session runs one websocket connection. welcomed reports that hourchd
// accepted the hello.
func (w *Worker) session(ctx context.Context) (welcomed bool, err error) {
	dialCtx, cancelDial := context.WithTimeout(ctx, w.opts.WelcomeTimeout)
	defer cancelDial()
	conn, _, err := websocket.Dial(dialCtx, w.cfg.HourchdURL, &websocket.DialOptions{
		HTTPClient: w.opts.HTTPClient,
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + w.token}},
	})
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(16 << 20)

	sctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	send := func(m hourchdprotocol.Message) error {
		b, err := hourchdprotocol.Encode(m)
		if err != nil {
			return err
		}
		wctx, c := context.WithTimeout(sctx, w.opts.WriteTimeout)
		defer c()
		return conn.Write(wctx, websocket.MessageText, b)
	}

	hello := &hourchdprotocol.Hello{
		WorkerID:     w.cfg.WorkerID,
		Hostname:     w.cfg.Hostname,
		Tags:         w.cfg.Tags,
		Capabilities: w.cfg.Capabilities,
		Version:      w.opts.Version,
	}
	w.mu.Lock()
	reported := w.cur
	if reported != nil && reported.abandoned {
		reported = nil
	}
	if reported != nil {
		hello.RunningJobID = reported.id
	}
	w.mu.Unlock()
	if err := send(hello); err != nil {
		return false, fmt.Errorf("hello: %w", err)
	}

	rctx, cancelRead := context.WithTimeout(sctx, w.opts.WelcomeTimeout)
	_, data, err := conn.Read(rctx)
	cancelRead()
	if err != nil {
		return false, fmt.Errorf("waiting for welcome: %w", err)
	}
	m, err := hourchdprotocol.Decode(data)
	if err != nil {
		return false, fmt.Errorf("waiting for welcome: %w", err)
	}
	welcome, ok := m.(*hourchdprotocol.Welcome)
	if !ok {
		return false, fmt.Errorf("waiting for welcome: got %s", m.MessageType())
	}
	w.mu.Lock()
	if reported != nil && w.cur == reported {
		if welcome.Abandon == reported.id {
			reported.abandoned = true
			reported.cancel(errAbandoned)
			if reported.finished {
				w.cur = nil
			}
			w.log.Warn("job abandoned by hourchd", "job", reported.id)
		} else {
			reported.logs.reset(welcome.LastLogSeq)
			reported.outSent = 0
			w.log.Info("job re-attached", "job", reported.id, "last_log_seq", welcome.LastLogSeq)
		}
	}
	w.mu.Unlock()
	w.log.Info("connected to hourchd", "heartbeat_seconds", welcome.HeartbeatSeconds, "lease_seconds", welcome.LeaseSeconds)

	hb := time.Duration(welcome.HeartbeatSeconds) * w.opts.HeartbeatUnit
	errs := make(chan error, 2)
	go func() { errs <- w.reader(sctx, conn) }()
	go func() { errs <- w.writer(sctx, hb, send) }()
	err = <-errs
	cancel(err)
	conn.CloseNow()
	<-errs
	return true, err
}

func (w *Worker) reader(ctx context.Context, conn *websocket.Conn) error {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		m, err := hourchdprotocol.Decode(data)
		if err != nil {
			// A message this worker cannot decode is a protocol mismatch;
			// drop the connection rather than guess.
			return fmt.Errorf("read: %w", err)
		}
		switch m := m.(type) {
		case *hourchdprotocol.Assign:
			w.onAssign(m)
		case *hourchdprotocol.Cancel:
			w.onCancel(m)
		default:
			w.log.Warn("unexpected message from hourchd", "type", m.MessageType())
		}
	}
}

// nextOutbound picks the next message to send and a commit to run after a
// successful write. Order per job: accept, log chunks, then (after the
// process ended and every chunk went out) outputs, then result.
// Called with w.mu held.
func (w *Worker) nextOutbound() (hourchdprotocol.Message, func()) {
	jr := w.cur
	if jr == nil || jr.abandoned {
		return nil, nil
	}
	if !jr.acceptSent {
		return &hourchdprotocol.Accept{JobID: jr.id}, func() { jr.acceptSent = true }
	}
	if c := jr.logs.take(); c != nil {
		return &hourchdprotocol.Log{JobID: jr.id, Stream: c.stream, Seq: c.seq, Data: c.data}, nil
	}
	if !jr.finished || !jr.logs.drained() {
		return nil, nil
	}
	if jr.outSent < len(jr.outputs) {
		o := jr.outputs[jr.outSent]
		return &o, func() { jr.outSent++ }
	}
	res := *jr.result
	return &res, func() {
		if w.cur == jr {
			w.cur = nil
		}
	}
}

func (w *Worker) writer(ctx context.Context, hb time.Duration, send func(hourchdprotocol.Message) error) error {
	ticker := time.NewTicker(hb)
	defer ticker.Stop()
	heartbeat := func() error {
		w.mu.Lock()
		m := &hourchdprotocol.Heartbeat{}
		if w.cur != nil && !w.cur.abandoned {
			m.JobID = w.cur.id
		}
		w.mu.Unlock()
		return send(m)
	}
	for {
		select {
		case <-ticker.C:
			if err := heartbeat(); err != nil {
				return fmt.Errorf("write: %w", err)
			}
		default:
		}
		w.mu.Lock()
		m, commit := w.nextOutbound()
		w.mu.Unlock()
		if m != nil {
			if err := send(m); err != nil {
				return fmt.Errorf("write %s: %w", m.MessageType(), err)
			}
			if commit != nil {
				w.mu.Lock()
				commit()
				w.mu.Unlock()
			}
			continue
		}
		if w.drainedIdle() {
			return errDrained
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-w.wake:
		case <-ticker.C:
			if err := heartbeat(); err != nil {
				return fmt.Errorf("write: %w", err)
			}
		}
	}
}

func (w *Worker) onAssign(a *hourchdprotocol.Assign) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case w.draining:
		w.log.Warn("assign ignored: worker is stopping", "job", a.Job.ID)
		return
	case w.cur != nil:
		w.log.Error("assign ignored: already running a job", "job", a.Job.ID, "running", w.cur.id)
		return
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	secrets := make([]string, 0, len(a.Secrets)+1)
	for _, v := range a.Secrets {
		secrets = append(secrets, v)
	}
	secrets = append(secrets, a.JobToken)
	jr := &jobRun{
		a:      a,
		id:     a.Job.ID,
		root:   filepath.Join(w.cfg.WorkDir, a.Job.ID+"-"+strconv.Itoa(a.Attempt)),
		logs:   newLogBuffer(int(w.cfg.LogBuffer), w.poke),
		redact: newRedactor(secrets),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	w.cur = jr
	w.log.Info("job assigned", "job", jr.id, "run", a.Job.RunID, "name", a.Job.Name, "attempt", a.Attempt, "image", a.Job.Image)
	w.jobs.Add(1)
	go func() {
		defer w.jobs.Done()
		w.runJob(jr)
	}()
	w.poke()
}

func (w *Worker) onCancel(c *hourchdprotocol.Cancel) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cur == nil || w.cur.id != c.JobID {
		w.log.Warn("cancel for a job this worker is not running", "job", c.JobID)
		return
	}
	w.log.Info("job cancel", "job", c.JobID, "reason", c.Reason)
	w.cur.cancel(cancelled{reason: c.Reason})
}
