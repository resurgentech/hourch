package httpapi

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	api "github.com/resurgentech/hourch/api"

	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/view"
)

var errTooLarge = errors.New("body over the size limit")

// limitedHasher counts and hashes what it reads and fails once more than
// max bytes arrive.
type limitedHasher struct {
	r   io.Reader
	h   hash.Hash
	n   int64
	max int64
	err error
}

func newLimitedHasher(r io.Reader, max int64) *limitedHasher {
	return &limitedHasher{r: r, h: sha256.New(), max: max}
}

func (l *limitedHasher) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	l.n += int64(n)
	l.h.Write(p[:n])
	if l.n > l.max {
		l.err = errTooLarge
		return n, errTooLarge
	}
	if err != nil && err != io.EOF {
		l.err = err
	}
	return n, err
}

func (l *limitedHasher) sum() string { return hex.EncodeToString(l.h.Sum(nil)) }

func (s *Server) tooLarge(w http.ResponseWriter, max int64) {
	writeError(w, http.StatusRequestEntityTooLarge, api.ErrorCodeTooLarge, fmt.Sprintf("body over %d bytes", max))
}

// putStream streams r.Body to key, hashing it. It writes the error response
// itself and returns ok=false on failure.
func (s *Server) putStream(w http.ResponseWriter, r *http.Request, key string, max int64) (sum string, size int64, ok bool) {
	if r.ContentLength > max {
		s.tooLarge(w, max)
		return "", 0, false
	}
	lh := newLimitedHasher(r.Body, max)
	err := s.eng.Objects().Put(r.Context(), key, lh, r.ContentLength)
	if err != nil || lh.err != nil {
		_ = s.eng.Objects().Delete(context.Background(), key)
		switch {
		case errors.Is(lh.err, errTooLarge):
			s.tooLarge(w, max)
		case lh.err != nil:
			badRequest(w, "reading body: %v", lh.err)
		default:
			s.unavailable(w, err)
		}
		return "", 0, false
	}
	return lh.sum(), lh.n, true
}

func (s *Server) uploadContext(w http.ResponseWriter, r *http.Request, c caller) {
	want := r.Header.Get("X-Sha256")
	if want != "" && !graph.Sha256Re.MatchString(want) {
		badRequest(w, "X-Sha256 must be 64 lowercase hex characters")
		return
	}
	ctx := r.Context()
	obj := s.eng.Objects()
	tmp := "tmp/contexts/" + s.eng.NewID()
	sum, size, ok := s.putStream(w, r, tmp, s.o.ContextMaxBytes)
	if !ok {
		return
	}
	defer obj.Delete(context.Background(), tmp)
	if want != "" && want != sum {
		writeError(w, http.StatusBadRequest, api.ErrorCodeChecksumMismatch, "body does not match X-Sha256")
		return
	}
	ref := "sha256:" + sum
	key := dispatch.ContextKey(ref)
	out := api.ContextUpload{ContextRef: ref, Sha256: sum, Size: size}
	if _, err := obj.Stat(ctx, key); err == nil {
		writeJSON(w, http.StatusOK, out)
		return
	} else if !errors.Is(err, objects.ErrNotFound) {
		s.unavailable(w, err)
		return
	}
	if err := obj.Copy(ctx, tmp, key); err != nil {
		s.unavailable(w, err)
		return
	}
	out.Created = true
	s.log.Info("context stored", "ref", ref, "size", size, "by", c.name)
	writeJSON(w, http.StatusCreated, out)
}

// leasedJob resolves {id} for a worker data route: 404 unknown, 409 not
// leased to the caller.
func (s *Server) leasedJob(w http.ResponseWriter, r *http.Request, c caller) (store.Job, bool) {
	id := r.PathValue("id")
	j, err := s.eng.LeasedTo(id, c.name)
	switch {
	case errors.Is(err, dispatch.ErrNotFound):
		notFound(w, "job %s not found", id)
		return j, false
	case errors.Is(err, dispatch.ErrNotLeased):
		writeError(w, http.StatusConflict, api.ErrorCodeNotLeased, fmt.Sprintf("job %s is not leased to %s", id, c.name))
		return j, false
	case err != nil:
		s.internal(w, err)
		return j, false
	}
	return j, true
}

func (s *Server) streamObject(w http.ResponseWriter, r *http.Request, key, sha, contentType string) {
	rc, info, err := s.eng.Objects().Get(r.Context(), key)
	if errors.Is(err, objects.ErrNotFound) {
		notFound(w, "object not found")
		return
	} else if err != nil {
		s.unavailable(w, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	if sha != "" {
		w.Header().Set("X-Sha256", sha)
	}
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, rc); err != nil {
		s.log.Info("object stream interrupted", "key", key, "error", err)
	}
}

func (s *Server) workerContext(w http.ResponseWriter, r *http.Request, c caller) {
	j, ok := s.leasedJob(w, r, c)
	if !ok {
		return
	}
	run, err := s.eng.Store().GetRun(j.RunID)
	if err != nil {
		s.internal(w, err)
		return
	}
	if run.ContextRef == "" {
		notFound(w, "run %s has no context", run.ID)
		return
	}
	s.streamObject(w, r, dispatch.ContextKey(run.ContextRef), strings.TrimPrefix(run.ContextRef, "sha256:"), "application/octet-stream")
}

// serveArtifact streams one artifact of job, or a tar of every artifact
// under a directory path.
func (s *Server) serveArtifact(w http.ResponseWriter, r *http.Request, job store.Job, path string) {
	if err := graph.ValidPath(path); err != nil {
		badRequest(w, "%v", err)
		return
	}
	arts, err := s.eng.Store().Artifacts(job.ID)
	if err != nil {
		s.internal(w, err)
		return
	}
	if !graph.IsDir(path) {
		for _, a := range arts {
			if a.Path == path {
				s.streamObject(w, r, a.Object, a.Sha256, "application/octet-stream")
				return
			}
		}
		notFound(w, "artifact %s not found", path)
		return
	}
	var under []store.Artifact
	for _, a := range arts {
		if strings.HasPrefix(a.Path, path) {
			under = append(under, a)
		}
	}
	if len(under) == 0 {
		notFound(w, "no artifacts under %s", path)
		return
	}
	// Check the store is reachable before committing to a 200.
	if err := s.eng.Objects().Ping(r.Context()); err != nil {
		s.unavailable(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/x-tar")
	w.WriteHeader(http.StatusOK)
	tw := tar.NewWriter(w)
	mod := s.eng.Clock().Now()
	if job.FinishedAt != nil {
		mod = *job.FinishedAt
	}
	for _, a := range under {
		rc, info, err := s.eng.Objects().Get(r.Context(), a.Object)
		if err != nil {
			s.log.Error("tar artifact unavailable; aborting stream", "key", a.Object, "error", err)
			return // the client sees a truncated tar
		}
		hdr := &tar.Header{Name: strings.TrimPrefix(a.Path, path), Mode: 0o644, Size: info.Size, ModTime: mod, Typeflag: tar.TypeReg}
		if err := tw.WriteHeader(hdr); err != nil {
			rc.Close()
			return
		}
		_, err = io.Copy(tw, rc)
		rc.Close()
		if err != nil {
			return
		}
	}
	_ = tw.Close()
}

func (s *Server) workerInput(w http.ResponseWriter, r *http.Request, c caller) {
	j, ok := s.leasedJob(w, r, c)
	if !ok {
		return
	}
	upstream, path := r.PathValue("upstream_job"), r.PathValue("path")
	declared := false
	for _, in := range j.Inputs {
		if in.Job == upstream && graph.Covers(in.Path, path) {
			declared = true
			break
		}
	}
	if !declared {
		notFound(w, "%s/%s is not an input of job %s", upstream, path, j.Name)
		return
	}
	jobs, err := s.eng.Store().RunJobs(j.RunID)
	if err != nil {
		s.internal(w, err)
		return
	}
	for _, up := range jobs {
		if up.Name == upstream {
			s.serveArtifact(w, r, up, path)
			return
		}
	}
	notFound(w, "job %s not found in run", upstream)
}

func (s *Server) workerPutArtifact(w http.ResponseWriter, r *http.Request, c caller) {
	j, ok := s.leasedJob(w, r, c)
	if !ok {
		return
	}
	path := r.PathValue("path")
	if err := graph.ValidPath(path); err != nil || graph.IsDir(path) {
		badRequest(w, "artifact path must be a relative file path")
		return
	}
	declared := false
	for _, a := range j.Artifacts {
		if graph.Covers(a, path) {
			declared = true
			break
		}
	}
	if !declared {
		badRequest(w, "%s is not under a declared artifact of job %s", path, j.Name)
		return
	}
	want := r.Header.Get("X-Sha256")
	if !graph.Sha256Re.MatchString(want) {
		badRequest(w, "X-Sha256 (64 lowercase hex characters) is required")
		return
	}
	key := dispatch.ArtifactKey(j.RunID, j.ID, path)
	sum, size, ok := s.putStream(w, r, key, s.o.ArtifactMaxBytes)
	if !ok {
		_ = s.eng.Store().DeleteArtifact(j.ID, path)
		return
	}
	if sum != want {
		_ = s.eng.Objects().Delete(context.Background(), key)
		_ = s.eng.Store().DeleteArtifact(j.ID, path)
		writeError(w, http.StatusBadRequest, api.ErrorCodeChecksumMismatch, "body does not match X-Sha256")
		return
	}
	// The lease may have lapsed during a long upload.
	if _, err := s.eng.LeasedTo(j.ID, c.name); err != nil {
		_ = s.eng.Objects().Delete(context.Background(), key)
		writeError(w, http.StatusConflict, api.ErrorCodeNotLeased, fmt.Sprintf("job %s is no longer leased to %s", j.ID, c.name))
		return
	}
	a := store.Artifact{JobID: j.ID, Path: path, Object: key, Size: size, Sha256: sum}
	if err := s.eng.Store().PutArtifact(a); err != nil {
		s.internal(w, err)
		return
	}
	s.log.Info("artifact stored", "run", j.RunID, "job", j.ID, "path", path, "size", size, "worker", c.name)
	writeJSON(w, http.StatusOK, view.Artifacts([]store.Artifact{a})[0])
}

func (s *Server) listJobArtifacts(w http.ResponseWriter, r *http.Request, _ caller) {
	id := r.PathValue("id")
	if _, err := s.eng.Store().GetJob(id); errors.Is(err, store.ErrNotFound) {
		notFound(w, "job %s not found", id)
		return
	} else if err != nil {
		s.internal(w, err)
		return
	}
	arts, err := s.eng.Store().Artifacts(id)
	if err != nil {
		s.internal(w, err)
		return
	}
	writeJSON(w, http.StatusOK, view.Artifacts(arts))
}

func (s *Server) getJobArtifact(w http.ResponseWriter, r *http.Request, _ caller) {
	id := r.PathValue("id")
	j, err := s.eng.Store().GetJob(id)
	if errors.Is(err, store.ErrNotFound) {
		notFound(w, "job %s not found", id)
		return
	} else if err != nil {
		s.internal(w, err)
		return
	}
	s.serveArtifact(w, r, j, r.PathValue("path"))
}

// --- logs ---

// logAttempt resolves the attempt query parameter: 404 when the job is
// unknown or the attempt never started.
func (s *Server) logAttempt(w http.ResponseWriter, r *http.Request) (store.Job, int, bool) {
	id := r.PathValue("id")
	j, err := s.eng.Store().GetJob(id)
	if errors.Is(err, store.ErrNotFound) {
		notFound(w, "job %s not found", id)
		return j, 0, false
	} else if err != nil {
		s.internal(w, err)
		return j, 0, false
	}
	attempt := j.Attempt
	if v := r.URL.Query().Get("attempt"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			badRequest(w, "attempt must be an integer >= 1")
			return j, 0, false
		}
		attempt = n
	}
	if attempt < 1 || attempt > j.Attempt {
		notFound(w, "job %s attempt %d never started", id, attempt)
		return j, 0, false
	}
	return j, attempt, true
}

// openLog opens an attempt's log: the live file while it exists (leased, or
// not yet uploaded), else the object store.
func (s *Server) openLog(ctx context.Context, j store.Job, attempt int) (io.ReadCloser, int64, error) {
	lm := s.eng.Logs()
	if f, err := os.Open(lm.Path(j.ID, attempt)); err == nil {
		fi, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, 0, err
		}
		return f, fi.Size(), nil
	}
	rc, info, err := s.eng.Objects().Get(ctx, dispatch.LogKey(j.RunID, j.ID, attempt))
	if err != nil {
		return nil, 0, err
	}
	return rc, info.Size, nil
}

func (s *Server) getJobLog(w http.ResponseWriter, r *http.Request, _ caller) {
	j, attempt, ok := s.logAttempt(w, r)
	if !ok {
		return
	}
	rc, size, err := s.openLog(r.Context(), j, attempt)
	if errors.Is(err, objects.ErrNotFound) {
		notFound(w, "no log for job %s attempt %d", j.ID, attempt)
		return
	} else if err != nil {
		s.unavailable(w, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.CopyN(w, rc, size)
}

// endStatus is the job status reported in a log `end` event: a non-final
// attempt (or one that was requeued) ended `failed`.
func endStatus(j store.Job, attempt int) string {
	if attempt < j.Attempt || j.Status == store.JobReady || j.Status == store.JobPending {
		return store.JobFailed
	}
	return j.Status
}

const replayChunk = 64 << 10

func writeSSE(w io.Writer, id int64, event string, data any) error {
	b, err := jsonMarshal(data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", id, event, b)
	return err
}

// replay sends rc's bytes from offset from to upto as log events.
func replay(w io.Writer, rc io.Reader, from, upto int64) (int64, error) {
	if from > 0 {
		if _, err := io.CopyN(io.Discard, rc, from); err != nil {
			return from, err
		}
	}
	off := from
	buf := make([]byte, replayChunk)
	carry := 0
	for off < upto {
		want := int64(len(buf) - carry)
		if upto-off-int64(carry) < want {
			want = upto - off - int64(carry)
		}
		n, err := io.ReadFull(rc, buf[carry:carry+int(want)])
		total := carry + n
		if total == 0 {
			if err != nil {
				return off, err
			}
			break
		}
		// Do not split a UTF-8 sequence across events.
		cut := total
		if off+int64(total) < upto {
			for k := 0; k < 3 && cut > 0 && !utf8.FullRune(buf[lastRuneStart(buf[:cut]):cut]); k++ {
				cut = lastRuneStart(buf[:cut])
			}
			if cut == 0 {
				cut = total
			}
		}
		off += int64(cut)
		if err := writeSSE(w, off, "log", api.LogChunkEvent{Offset: off, Stream: api.Stdout, Data: string(buf[:cut])}); err != nil {
			return off, err
		}
		carry = copy(buf, buf[cut:total])
		if err != nil && carry == 0 {
			break
		}
	}
	return off, nil
}

func lastRuneStart(b []byte) int {
	for i := len(b) - 1; i >= 0 && i >= len(b)-4; i-- {
		if utf8.RuneStart(b[i]) {
			return i
		}
	}
	return len(b)
}

func (s *Server) streamJobLog(w http.ResponseWriter, r *http.Request, _ caller) {
	j, attempt, ok := s.logAttempt(w, r)
	if !ok {
		return
	}
	var from int64
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if err != nil || n < 0 {
			badRequest(w, "Last-Event-ID must be an integer >= 0")
			return
		}
		from = n
	}
	flusher, fok := w.(http.Flusher)
	if !fok {
		s.internal(w, errors.New("response writer cannot flush"))
		return
	}
	lm := s.eng.Logs()
	live := j.Status == store.JobLeased && attempt == j.Attempt
	if live {
		ls, size := lm.Subscribe(j.ID, attempt)
		defer ls.Close()
		// Re-check: the attempt may have ended before the subscription.
		cur, err := s.eng.Store().GetJob(j.ID)
		if err != nil {
			s.internal(w, err)
			return
		}
		if cur.Status != store.JobLeased || cur.Attempt != attempt {
			live, j = false, cur
		}
		if live {
			sseHeaders(w)
			pos := from
			if from < size {
				f, err := os.Open(lm.Path(j.ID, attempt))
				if err == nil {
					pos, err = replay(w, f, from, size)
					f.Close()
				}
				if err != nil {
					return
				}
			}
			flusher.Flush()
			s.followLog(w, r, flusher, ls, pos)
			return
		}
	}
	// Finished attempt: replay everything, then end.
	rc, total, err := s.openLog(r.Context(), j, attempt)
	if err != nil && !errors.Is(err, objects.ErrNotFound) {
		s.unavailable(w, err)
		return
	}
	sseHeaders(w)
	pos := total
	if err == nil {
		pos, err = replay(w, rc, min(from, total), total)
		rc.Close()
		if err != nil {
			return
		}
	} else {
		pos = 0
	}
	var exit *int
	evs, _ := s.eng.Store().JobEvents(j.ID)
	for _, a := range view.Attempts(evs) {
		if a.Attempt == attempt {
			exit = a.ExitCode
		}
	}
	_ = writeSSE(w, pos, "end", api.LogEndEvent{Offset: pos, Status: api.JobStatus(endStatus(j, attempt)), ExitCode: exit})
	flusher.Flush()
}
