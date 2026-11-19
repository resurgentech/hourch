package hourchworker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	hourchdprotocol "github.com/resurgentech/hourch/protocol"
)

// Job environment variables (assumption 16).
const (
	EnvURL      = "HOURCH_URL"
	EnvJobToken = "HOURCH_JOB_TOKEN"
	EnvOutputs  = "HOURCH_OUTPUTS"
	EnvRunID    = "HOURCH_RUN_ID"
	EnvJobID    = "HOURCH_JOB_ID"
	EnvAttempt  = "HOURCH_ATTEMPT"
)

// outputsRel is the $HOURCH_OUTPUTS file relative to the work root.
const outputsRel = ".hourch/outputs"

// Errors used as cancel causes.
var (
	errAbandoned = errors.New("abandoned")
	errShutdown  = errors.New("worker shutdown")
)

// cancelled is the cause for a cancel message.
type cancelled struct{ reason string }

func (c cancelled) Error() string { return "cancelled: " + c.reason }

// jobRun is one attempt of one job on this worker. Fields after mu-comment
// are guarded by Worker.mu.
type jobRun struct {
	a      *hourchdprotocol.Assign
	id     string
	root   string // work_dir/<job_id>-<attempt>
	logs   *logBuffer
	redact redactor
	ctx    context.Context
	cancel context.CancelCauseFunc
	done   chan struct{}

	// guarded by Worker.mu
	acceptSent bool
	finished   bool
	abandoned  bool
	outputs    []hourchdprotocol.Output
	outSent    int
	result     *hourchdprotocol.Result
}

// note writes a worker line into the job's stderr log.
func (jr *jobRun) note(format string, args ...any) {
	jr.logs.add(hourchdprotocol.StreamStderr, jr.redact.apply(sanitizeUTF8("[hourch-worker] "+fmt.Sprintf(format, args...)+"\n")))
}

var outputKeyRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// OutputValueMax is the largest output value (hourchd's
// limits.output_value_bytes default).
const OutputValueMax = 4096

// errBadOutput prefixes the result error for a malformed outputs file.
const errBadOutput = "bad_output"

// parseOutputs reads key=value lines (the value is everything after the
// first '='; a newline ends it). Blank lines are skipped. A line without
// '=', a key outside [A-Za-z0-9_.-]{1,128}, or a value over
// OutputValueMax bytes is an error naming the line. A repeated key keeps
// the last value.
func parseOutputs(p string) ([]hourchdprotocol.Output, error) {
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []hourchdprotocol.Output
	idx := map[string]int{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSuffix(sc.Text(), "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		switch {
		case !ok:
			return nil, fmt.Errorf("line %d: no '='", n)
		case !outputKeyRE.MatchString(k):
			return nil, fmt.Errorf("line %d: bad key %q", n, k)
		case len(v) > OutputValueMax:
			return nil, fmt.Errorf("line %d: value of %s is %d bytes, max %d", n, k, len(v), OutputValueMax)
		}
		if i, seen := idx[k]; seen {
			out[i].Value = v
			continue
		}
		idx[k] = len(out)
		out = append(out, hourchdprotocol.Output{Key: k, Value: v})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("line %d: %w", n+1, err)
	}
	return out, nil
}

// buildEnv is the job's environment (Key Decision 6, assumption 16):
// allowlisted worker variables (process jobs only), then the job's env,
// then secrets, then the HOURCH_* variables. Inherited HOURCH_* variables
// and anything equal to the worker token never pass.
func (w *Worker) buildEnv(a *hourchdprotocol.Assign, container bool, outputsPath string) map[string]string {
	env := map[string]string{}
	if !container {
		inherited := map[string]string{}
		for _, kv := range w.opts.Environ() {
			k, v, ok := strings.Cut(kv, "=")
			if ok {
				inherited[k] = v
			}
		}
		for _, k := range w.cfg.EnvAllowlist {
			v, ok := inherited[k]
			if !ok || strings.HasPrefix(k, "HOURCH_") || v == w.token {
				continue
			}
			env[k] = v
		}
	}
	for k, v := range a.Job.Env {
		env[k] = v
	}
	for k, v := range a.Secrets {
		env[k] = v
	}
	for k := range env {
		if strings.HasPrefix(k, "HOURCH_") {
			delete(env, k)
		}
	}
	env[EnvURL] = w.httpBase
	env[EnvJobToken] = a.JobToken
	env[EnvOutputs] = outputsPath
	env[EnvRunID] = a.Job.RunID
	env[EnvJobID] = a.Job.ID
	env[EnvAttempt] = strconv.Itoa(a.Attempt)
	return env
}

// execute runs one assignment to the end and returns its result and
// outputs. It never returns early on cancel: the process is killed and
// waited for first.
func (w *Worker) execute(jr *jobRun) (*hourchdprotocol.Result, []hourchdprotocol.Output) {
	a := jr.a
	ctx := jr.ctx
	res := &hourchdprotocol.Result{JobID: jr.id, ExitCode: -1}
	fail := func(format string, args ...any) (*hourchdprotocol.Result, []hourchdprotocol.Output) {
		if cause := context.Cause(ctx); cause != nil {
			res.Error = cause.Error()
		} else {
			res.Error = jr.redact.apply(fmt.Sprintf(format, args...))
		}
		jr.note("%s", res.Error)
		return res, nil
	}

	container := a.Job.Image != ""
	if container && !w.cfg.HasDocker() {
		return fail("job needs a container (image %s) but this worker has no docker capability", a.Job.Image)
	}
	workdir := "."
	if a.Job.Workdir != "" {
		wd, err := cleanRel(a.Job.Workdir)
		if err != nil {
			return fail("workdir: %v", err)
		}
		workdir = wd
	}

	if err := w.data.fetchContext(ctx, jr.id, jr.root); err != nil {
		return fail("context: %v", err)
	}
	for _, in := range a.Job.Inputs {
		dest := in.As
		if dest == "" {
			dest = path.Join("inputs", in.Job, in.Path)
		}
		dest, err := cleanRel(dest)
		if err == nil && dest == "." {
			err = errors.New("input destination is the work root")
		}
		if err != nil {
			return fail("input %s/%s: %v", in.Job, in.Path, err)
		}
		if err := w.data.fetchInput(ctx, jr.id, in.Job, in.Path, jr.root, dest); err != nil {
			return fail("input %s/%s: %v", in.Job, in.Path, err)
		}
	}

	if st, err := os.Stat(filepath.Join(jr.root, filepath.FromSlash(workdir))); err != nil || !st.IsDir() {
		return fail("workdir %s: not a directory in the context", workdir)
	}
	outputsHost := filepath.Join(jr.root, filepath.FromSlash(outputsRel))
	if err := os.MkdirAll(filepath.Dir(outputsHost), 0o755); err != nil {
		return fail("outputs file: %v", err)
	}
	if err := os.WriteFile(outputsHost, nil, 0o666); err != nil {
		return fail("outputs file: %v", err)
	}
	_ = os.Chmod(outputsHost, 0o666) // writable by a container's non-root user
	outputsEnv := outputsHost
	if container {
		outputsEnv = "/work/" + outputsRel
	}
	if err := context.Cause(ctx); err != nil {
		return fail("%v", err)
	}

	outR, outW, err := os.Pipe()
	if err != nil {
		return fail("pipe: %v", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		outR.Close()
		outW.Close()
		return fail("pipe: %v", err)
	}
	var pumps sync.WaitGroup
	for _, p := range []struct {
		r      *os.File
		stream string
	}{{outR, hourchdprotocol.StreamStdout}, {errR, hourchdprotocol.StreamStderr}} {
		pumps.Add(1)
		go func() {
			defer pumps.Done()
			pump(p.r, w.opts.ChunkMaxBytes, w.opts.ChunkMaxDelay, func(b []byte) {
				s := sanitizeUTF8(jr.redact.apply(string(b)))
				for _, piece := range splitRunes(s, w.opts.ChunkMaxBytes) {
					jr.logs.add(p.stream, piece)
				}
			})
		}()
	}
	closeReaders := func() {
		done := make(chan struct{})
		go func() { pumps.Wait(); close(done) }()
		t := time.NewTimer(w.opts.PipeDrain)
		defer t.Stop()
		select {
		case <-done:
		case <-t.C:
			// Something outside the process group still holds the pipes.
			outR.Close()
			errR.Close()
			<-done
		}
		outR.Close()
		errR.Close()
	}

	spec := runSpec{
		jobID: jr.id, attempt: a.Attempt, argv: a.Job.Argv,
		workRoot: jr.root, workdir: workdir,
		env:    w.buildEnv(a, container, outputsEnv),
		image:  a.Job.Image,
		stdout: outW, stderr: errW,
	}
	var r interface {
		start(context.Context, runSpec) (running, error)
	} = processRunner{}
	if container {
		r = w.container
	}
	proc, err := r.start(ctx, spec)
	outW.Close()
	errW.Close()
	if err != nil {
		closeReaders()
		return fail("start: %v", err)
	}

	exited := make(chan int, 1)
	go func() { exited <- proc.wait() }()
	select {
	case code := <-exited:
		res.ExitCode = code
	case <-ctx.Done():
		proc.kill(w.opts.KillGrace)
		res.ExitCode = <-exited
	}
	closeReaders()

	outs, outErr := parseOutputs(outputsHost)
	for i := range outs {
		outs[i].JobID = jr.id
	}

	if cause := context.Cause(ctx); cause != nil {
		res.Error = cause.Error()
		return res, outs
	}
	if res.ExitCode != 0 {
		if outErr != nil {
			jr.note("outputs ignored: %v", outErr)
		}
		return res, outs
	}
	if outErr != nil {
		// A malformed outputs file fails an otherwise successful job;
		// no outputs are sent and no artifacts uploaded.
		res.Error = jr.redact.apply(fmt.Sprintf("%s: %v", errBadOutput, outErr))
		jr.note("%s", res.Error)
		return res, nil
	}
	for _, decl := range a.Job.Artifacts {
		files, err := artifactFiles(jr.root, decl)
		if err == nil {
			for _, f := range files {
				if err = w.data.putArtifact(ctx, jr.id, f[0], f[1]); err != nil {
					err = fmt.Errorf("%s: %w", f[0], err)
					break
				}
			}
		}
		if err != nil {
			res.Error = jr.redact.apply(fmt.Sprintf("artifact %s: %v", decl, err))
			if cause := context.Cause(ctx); cause != nil {
				res.Error = cause.Error()
			}
			jr.note("%s", res.Error)
			return res, outs
		}
	}
	return res, outs
}

// runJob executes jr and hands its outputs and result to the sender.
func (w *Worker) runJob(jr *jobRun) {
	defer close(jr.done)
	res, outs := w.execute(jr)
	jr.logs.close()
	if err := os.RemoveAll(jr.root); err != nil {
		w.log.Warn("work dir cleanup failed", "job", jr.id, "dir", jr.root, "err", err)
	}
	w.mu.Lock()
	jr.result = res
	jr.outputs = outs
	jr.finished = true
	if jr.abandoned && w.cur == jr {
		w.cur = nil
	}
	w.mu.Unlock()
	w.log.Info("job finished", "job", jr.id, "attempt", jr.a.Attempt, "exit_code", res.ExitCode, "error", res.Error)
	w.poke()
}
