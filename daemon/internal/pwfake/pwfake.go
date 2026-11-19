// Package pwfake is an in-memory powerwarden for tests: the demand and
// worker routes of specs/openapi/powerwarden.yml that hourchd's publisher
// uses, with bearer auth, strict bodies and the documented bounds. It
// records every write so tests can assert the sequence.
package pwfake

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	pw "github.com/resurgentech/powerwarden/powerwarden/api"
)

var (
	demandIDRe = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,160}$`)
	workerIDRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	tagRe      = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
)

// Op is one recorded write.
type Op struct {
	Method, Kind, ID string // Kind: "demand" or "worker"
	Demand           *pw.DemandPut
	Worker           *pw.WorkerPut
}

func (o Op) String() string { return o.Method + " " + o.Kind + " " + o.ID }

// Fake is the fake powerwarden. Zero value is not usable; call New.
type Fake struct {
	Token string

	mu       sync.Mutex
	demands  map[string]pw.Demand
	workers  map[string]pw.Worker
	ops      []Op
	down     bool
	changed  chan struct{}
	hosts    []pw.HostView
	hostGets int
}

// New returns a fake accepting token.
func New(token string) *Fake {
	return &Fake{Token: token, demands: map[string]pw.Demand{}, workers: map[string]pw.Worker{}, changed: make(chan struct{})}
}

// SetDown makes every request fail with 503 (powerwarden unreachable).
func (f *Fake) SetDown(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
}

// Wipe forgets every row (a powerwarden restart that lost its state, or a
// sweep).
func (f *Fake) Wipe() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.demands = map[string]pw.Demand{}
	f.workers = map[string]pw.Worker{}
	f.notify()
}

// Seed stores a demand as if some earlier writer had PUT it.
func (f *Fake) Seed(d pw.Demand) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.demands[d.Id] = d
	f.notify()
}

// Demands returns the stored demands.
func (f *Fake) Demands() map[string]pw.Demand {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]pw.Demand{}
	for k, v := range f.demands {
		out[k] = v
	}
	return out
}

// DemandIDs returns the stored demand ids, sorted.
func (f *Fake) DemandIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for k := range f.demands {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Workers returns the stored worker rows.
func (f *Fake) Workers() map[string]pw.Worker {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]pw.Worker{}
	for k, v := range f.workers {
		out[k] = v
	}
	return out
}

// SetHosts sets what GET /v1/hosts returns (only hostname, tags, locked
// and exclude are meaningful; the rest is filled with zero values).
func (f *Fake) SetHosts(hosts ...pw.HostView) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hosts = nil
	for _, h := range hosts {
		if h.Tags == nil {
			h.Tags = []string{}
		}
		if h.Workers == nil {
			h.Workers = []pw.WorkerId{}
		}
		if h.State == "" {
			h.State = pw.SoftOff
		}
		f.hosts = append(f.hosts, h)
	}
}

// HostGets counts GET /v1/hosts requests.
func (f *Fake) HostGets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hostGets
}

// Ops returns every recorded write.
func (f *Fake) Ops() []Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Op(nil), f.ops...)
}

// Changed returns a channel closed at the next write.
func (f *Fake) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

func (f *Fake) notify() {
	close(f.changed)
	f.changed = make(chan struct{})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, code pw.ErrorCode, msg string) {
	writeJSON(w, status, pw.Error{Error: code, Message: msg})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		fail(w, 400, pw.ErrorCodeBadRequest, "read")
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		fail(w, 400, pw.ErrorCodeBadRequest, err.Error())
		return false
	}
	return true
}

func tagsOK(tags []string) bool {
	for _, t := range tags {
		if !tagRe.MatchString(t) {
			return false
		}
	}
	return true
}

// ServeHTTP implements the routes.
func (f *Fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	down := f.down
	f.mu.Unlock()
	if down {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+f.Token {
		fail(w, 401, pw.ErrorCodeUnauthorized, "bad token")
		return
	}
	now := time.Now().UTC()
	path := r.URL.Path
	switch {
	case path == "/v1/demands" && r.Method == http.MethodGet:
		f.mu.Lock()
		out := []pw.Demand{}
		for _, d := range f.demands {
			out = append(out, d)
		}
		f.mu.Unlock()
		sort.Slice(out, func(i, k int) bool { return out[i].Id < out[k].Id })
		writeJSON(w, 200, out)
	case path == "/v1/hosts" && r.Method == http.MethodGet:
		f.mu.Lock()
		f.hostGets++
		out := append([]pw.HostView{}, f.hosts...)
		f.mu.Unlock()
		writeJSON(w, 200, out)
	case strings.HasPrefix(path, "/v1/demands/"):
		id := strings.TrimPrefix(path, "/v1/demands/")
		if !demandIDRe.MatchString(id) {
			fail(w, 400, pw.ErrorCodeBadRequest, "bad id")
			return
		}
		switch r.Method {
		case http.MethodPut:
			var in pw.DemandPut
			if !decode(w, r, &in) {
				return
			}
			if (in.Id != nil && *in.Id != id) || !tagsOK(in.Tags) ||
				(in.Source != pw.DemandPutSourceHourchd && in.Source != pw.DemandPutSourceManual) ||
				(in.LeaseSeconds != nil && (*in.LeaseSeconds < 30 || *in.LeaseSeconds > 3600)) {
				fail(w, 400, pw.ErrorCodeBadRequest, "invalid demand")
				return
			}
			f.mu.Lock()
			d, ok := f.demands[id]
			if !ok {
				d = pw.Demand{Id: id, CreatedAt: now, LeaseSeconds: 3600}
			}
			if in.LeaseSeconds != nil {
				d.LeaseSeconds = *in.LeaseSeconds
			}
			d.Tags, d.Source, d.UpdatedAt = append(pw.Tags{}, in.Tags...), pw.DemandSource(in.Source), now
			d.LeaseUntil = now.Add(time.Duration(d.LeaseSeconds) * time.Second)
			if in.ExternalRef != nil {
				d.ExternalRef = *in.ExternalRef
			}
			f.demands[id] = d
			f.ops = append(f.ops, Op{Method: "PUT", Kind: "demand", ID: id, Demand: &in})
			f.notify()
			f.mu.Unlock()
			writeJSON(w, 200, d)
		case http.MethodDelete:
			f.mu.Lock()
			delete(f.demands, id)
			f.ops = append(f.ops, Op{Method: "DELETE", Kind: "demand", ID: id})
			f.notify()
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			fail(w, 405, pw.ErrorCodeBadRequest, "method")
		}
	case strings.HasPrefix(path, "/v1/workers/"):
		id := strings.TrimPrefix(path, "/v1/workers/")
		if !workerIDRe.MatchString(id) {
			fail(w, 400, pw.ErrorCodeBadRequest, "bad id")
			return
		}
		switch r.Method {
		case http.MethodPut:
			var in pw.WorkerPut
			if !decode(w, r, &in) {
				return
			}
			switch in.JobStatus {
			case pw.JobStatusIdle, pw.JobStatusBusy, pw.JobStatusError, pw.JobStatusMaintenance:
			default:
				fail(w, 400, pw.ErrorCodeBadRequest, "job_status")
				return
			}
			if (in.WorkerId != nil && *in.WorkerId != id) || !tagsOK(in.Tags) || in.Hostname == "" ||
				(in.LeaseSeconds != nil && (*in.LeaseSeconds < 15 || *in.LeaseSeconds > 300)) {
				fail(w, 400, pw.ErrorCodeBadRequest, "invalid worker")
				return
			}
			lease := 90
			if in.LeaseSeconds != nil {
				lease = *in.LeaseSeconds
			}
			wk := pw.Worker{WorkerId: id, Hostname: in.Hostname, Tags: append(pw.Tags{}, in.Tags...), JobStatus: in.JobStatus,
				LeaseSeconds: lease, LeaseUntil: now.Add(time.Duration(lease) * time.Second), UpdatedAt: now}
			if in.DemandId != nil {
				wk.DemandId = *in.DemandId
			}
			f.mu.Lock()
			f.workers[id] = wk
			f.ops = append(f.ops, Op{Method: "PUT", Kind: "worker", ID: id, Worker: &in})
			f.notify()
			f.mu.Unlock()
			writeJSON(w, 200, wk)
		case http.MethodDelete:
			f.mu.Lock()
			delete(f.workers, id)
			f.ops = append(f.ops, Op{Method: "DELETE", Kind: "worker", ID: id})
			f.notify()
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			fail(w, 405, pw.ErrorCodeBadRequest, "method")
		}
	default:
		fail(w, 404, pw.ErrorCodeNotFound, "no route")
	}
}
