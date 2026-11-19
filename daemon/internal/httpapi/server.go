// Package httpapi is hourchd's hand-written net/http server for
// specs/openapi/hourchd.yml (everything except the web UI), using the
// generated types in hourchdapi.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	api "github.com/resurgentech/hourch/api"

	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/wsserver"
)

// Options configure a Server.
type Options struct {
	Engine *dispatch.Engine
	Tokens *Tokens
	WS     *wsserver.Server
	Log    *slog.Logger

	Heartbeat        time.Duration // events.heartbeat
	ContextMaxBytes  int64
	ArtifactMaxBytes int64
	WorkerRetention  time.Duration // retention.rows: how long disconnected workers are listed
	// Powerwarden reports the publisher state: "connected", "unreachable"
	// or "disabled" (nil means disabled).
	Powerwarden func() string
}

// Server serves the API.
type Server struct {
	o   Options
	eng *dispatch.Engine
	log *slog.Logger
	mux *http.ServeMux

	done      chan struct{} // closed by CloseStreams
	closeOnce sync.Once
}

// caller is the authenticated principal.
type caller struct {
	kind string
	name string    // submitter name or worker id
	job  store.Job // for job tokens
}

type route struct {
	method, path string
	kinds        []string // nil: no authentication
	h            func(http.ResponseWriter, *http.Request, caller)
}

// New builds the server.
func New(o Options) (*Server, error) {
	if o.Engine == nil || o.Tokens == nil || o.WS == nil {
		return nil, errors.New("httpapi: engine, tokens and websocket server are required")
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	if o.Heartbeat <= 0 {
		o.Heartbeat = 15 * time.Second
	}
	if o.ContextMaxBytes <= 0 {
		o.ContextMaxBytes = 2 << 30
	}
	if o.ArtifactMaxBytes <= 0 {
		o.ArtifactMaxBytes = 4 << 30
	}
	if o.WorkerRetention <= 0 {
		o.WorkerRetention = 90 * 24 * time.Hour
	}
	s := &Server{o: o, eng: o.Engine, log: o.Log, mux: http.NewServeMux(), done: make(chan struct{})}
	for _, rt := range s.routes() {
		rt := rt
		s.mux.HandleFunc(rt.method+" "+rt.path, func(w http.ResponseWriter, r *http.Request) {
			var c caller
			if name, ok := r.Context().Value(uiUserKey{}).(string); ok && slices.Contains(rt.kinds, KindSubmitter) {
				// The web UI authenticated this request (basic auth) and
				// forwards it as an operator.
				c = caller{kind: KindSubmitter, name: "ui:" + name}
			} else if rt.kinds != nil {
				var status int
				c, status = s.authenticate(r, rt.kinds)
				switch status {
				case http.StatusUnauthorized:
					writeError(w, status, api.ErrorCodeUnauthorized, "missing, wrong or expired token")
					return
				case http.StatusForbidden:
					writeError(w, status, api.ErrorCodeForbidden, "this token kind is not accepted on this route")
					return
				case http.StatusInternalServerError:
					s.internal(w, errors.New("token lookup failed"))
					return
				}
			}
			rt.h(w, r, c)
		})
	}
	s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, api.ErrorCodeNotFound, "no such route")
	})
	return s, nil
}

var (
	sub      = []string{KindSubmitter}
	wrk      = []string{KindWorker}
	subOrJob = []string{KindSubmitter, KindJob}
	jobOnly  = []string{KindJob}
)

func (s *Server) routes() []route {
	return []route{
		{"POST", "/v1/contexts", sub, s.uploadContext},
		{"GET", "/v1/runs", sub, s.listRuns},
		{"POST", "/v1/runs", sub, s.submitRun},
		{"GET", "/v1/runs/{id}", sub, s.getRun},
		{"GET", "/v1/workflows/{workflow}/runs/{number}", sub, s.getRunByNumber},
		{"POST", "/v1/runs/{id}/cancel", sub, s.cancelRun},
		{"POST", "/v1/runs/{id}/retry", sub, s.retryRun},
		{"GET", "/v1/jobs/{id}", sub, s.getJob},
		{"POST", "/v1/jobs/{id}/cancel", sub, s.cancelJob},
		{"GET", "/v1/jobs/{id}/log", sub, s.getJobLog},
		{"GET", "/v1/jobs/{id}/log/stream", sub, s.streamJobLog},
		{"GET", "/v1/jobs/{id}/artifacts", sub, s.listJobArtifacts},
		{"GET", "/v1/jobs/{id}/artifacts/{path...}", sub, s.getJobArtifact},
		{"GET", "/v1/workers", sub, s.listWorkers},
		{"POST", "/v1/counters/{name}/next", subOrJob, s.nextCounter},
		{"GET", "/v1/builds", sub, s.listBuilds},
		{"PUT", "/v1/builds/{type}/{flavour}/{num}", jobOnly, s.putBuild},
		{"GET", "/v1/events", sub, s.streamEvents},
		{"GET", "/v1/healthz", nil, s.healthz},
		{"GET", "/v1/stats", sub, s.stats},
		{"GET", "/v1/worker/connect", wrk, s.connectWorker},
		{"GET", "/v1/worker/jobs/{id}/context", wrk, s.workerContext},
		{"GET", "/v1/worker/jobs/{id}/inputs/{upstream_job}/{path...}", wrk, s.workerInput},
		{"PUT", "/v1/worker/jobs/{id}/artifacts/{path...}", wrk, s.workerPutArtifact},
	}
}

// Routes lists "METHOD /path" for every route, written as in the spec
// ({path...} as {path}), for the coverage test.
func (s *Server) Routes() []string {
	var out []string
	for _, r := range s.routes() {
		out = append(out, r.method+" "+strings.ReplaceAll(r.path, "...}", "}"))
	}
	return out
}

// CloseStreams ends every open SSE stream (/v1/events and live log tails)
// and makes new ones end at once. Call it when shutting down: those streams
// never go idle, so http.Server.Shutdown would otherwise wait out its
// timeout while clients stay attached to a daemon that is going away;
// ended streams make them reconnect (to the next process).
func (s *Server) CloseStreams() { s.closeOnce.Do(func() { close(s.done) }) }

type uiUserKey struct{}

// AsUIUser returns r carrying an operator principal authenticated by the web
// UI (htpasswd basic auth). Only in-process code can set it; routes that
// accept submitter tokens treat it as the submitter "ui:<user>". Worker and
// job-token-only routes ignore it.
func AsUIUser(r *http.Request, user string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), uiUserKey{}, user))
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}

// authenticate resolves the token and checks its kind: 0 when allowed, else
// 401 (unknown, missing or expired) or 403 (valid, wrong kind).
func (s *Server) authenticate(r *http.Request, kinds []string) (caller, int) {
	tok := bearer(r)
	if tok == "" {
		return caller{}, http.StatusUnauthorized
	}
	var c caller
	if name, kind, ok := s.o.Tokens.Lookup(tok); ok {
		c = caller{kind: kind, name: name}
	} else {
		j, err := s.eng.JobForToken(tok)
		if errors.Is(err, dispatch.ErrNotFound) {
			return caller{}, http.StatusUnauthorized
		}
		if err != nil {
			s.log.Error("job token lookup", "error", err)
			return caller{}, http.StatusInternalServerError
		}
		c = caller{kind: KindJob, name: "job:" + j.ID, job: j}
	}
	for _, k := range kinds {
		if k == c.kind {
			return c, 0
		}
	}
	return caller{}, http.StatusForbidden
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code api.ErrorCode, msg string) {
	writeJSON(w, status, api.Error{Error: code, Message: msg})
}

func badRequest(w http.ResponseWriter, f string, a ...any) {
	writeError(w, http.StatusBadRequest, api.ErrorCodeBadRequest, fmt.Sprintf(f, a...))
}

func notFound(w http.ResponseWriter, f string, a ...any) {
	writeError(w, http.StatusNotFound, api.ErrorCodeNotFound, fmt.Sprintf(f, a...))
}

func (s *Server) internal(w http.ResponseWriter, err error) {
	s.log.Error("api internal error", "error", err)
	writeError(w, http.StatusInternalServerError, api.ErrorCodeInternal, "internal error")
}

func (s *Server) unavailable(w http.ResponseWriter, err error) {
	s.log.Error("object store unavailable", "error", err)
	writeError(w, http.StatusServiceUnavailable, api.ErrorCodeUnavailable, "object store unavailable")
}

// decodeBody decodes a JSON body strictly: unknown fields and trailing data
// are errors. It writes the error response and returns false on failure.
func decodeBody(w http.ResponseWriter, r *http.Request, limit int64, v any) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		badRequest(w, "cannot read body")
		return false
	}
	if int64(len(body)) > limit {
		writeError(w, http.StatusRequestEntityTooLarge, api.ErrorCodeTooLarge, fmt.Sprintf("body over %d bytes", limit))
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		badRequest(w, "invalid body: %v", err)
		return false
	}
	if dec.More() {
		badRequest(w, "trailing data after body")
		return false
	}
	return true
}

func parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 50, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 500 {
		badRequest(w, "limit must be 1 to 500")
		return 0, false
	}
	return n, true
}
