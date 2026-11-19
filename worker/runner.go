package hourchworker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// runSpec is what a runner needs to start a job.
type runSpec struct {
	jobID   string
	attempt int
	argv    []string
	// workRoot is the host directory holding the unpacked context;
	// workdir is the job's workdir relative to it ("." for the root).
	workRoot string
	workdir  string
	// env is the job's environment, name → value (already filtered).
	env map[string]string
	// image selects the container runner.
	image  string
	stdout io.Writer
	stderr io.Writer
}

// running is a started job.
type running interface {
	// wait blocks until the job ends and returns its exit code. A process
	// killed by a signal reports 128+signal, as a shell would.
	wait() int
	// kill stops the job: SIGTERM, then SIGKILL after the grace period
	// (process group, or docker kill / docker rm -f).
	kill(grace time.Duration)
}

func envList(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, k+"="+env[k])
	}
	return out
}

// lookPath resolves argv[0] against the job's PATH (not the worker's).
// A name containing "/" is used as is (relative to the job's cwd).
func lookPath(name, pathEnv, dir string) (string, error) {
	if strings.Contains(name, "/") {
		return name, nil
	}
	for _, d := range filepath.SplitList(pathEnv) {
		if d == "" {
			d = "."
		}
		p := filepath.Join(d, name)
		probe := p
		if !filepath.IsAbs(probe) {
			probe = filepath.Join(dir, probe)
		}
		if st, err := os.Stat(probe); err == nil && st.Mode().IsRegular() && st.Mode().Perm()&0o111 != 0 {
			return p, nil
		}
	}
	return "", fmt.Errorf("%s: executable file not found in the job's PATH", name)
}

func exitCode(st *os.ProcessState) int {
	if st == nil {
		return -1
	}
	if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return st.ExitCode()
}

// procGroup is a child started in its own process group.
type procGroup struct {
	cmd  *exec.Cmd
	done chan struct{}
	code int
}

func startGroup(argv []string, dir string, env []string, stdout, stderr io.Writer, pathEnv string) (*procGroup, error) {
	bin, err := lookPath(argv[0], pathEnv, dir)
	if err != nil {
		return nil, err
	}
	cmd := &exec.Cmd{
		Path:        bin,
		Args:        argv,
		Dir:         dir,
		Env:         env,
		Stdout:      stdout,
		Stderr:      stderr,
		SysProcAttr: &syscall.SysProcAttr{Setpgid: true},
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	p := &procGroup{cmd: cmd, done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		p.code = exitCode(cmd.ProcessState)
		// The job is over: anything it left behind in its group goes too.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		close(p.done)
	}()
	return p, nil
}

func (p *procGroup) wait() int {
	<-p.done
	return p.code
}

func (p *procGroup) kill(grace time.Duration) {
	pgid := p.cmd.Process.Pid
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-p.done:
	case <-t.C:
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
}

// processRunner runs argv directly, in its own process group, no shell.
type processRunner struct{}

func (processRunner) start(_ context.Context, s runSpec) (running, error) {
	dir := filepath.Join(s.workRoot, filepath.FromSlash(s.workdir))
	return startGroup(s.argv, dir, envList(s.env), s.stdout, s.stderr, s.env["PATH"])
}

// containerRunner runs the job with the docker CLI:
//
//	docker create --rm --name <name> --env-file <tmp> [-e NAME...] \
//	    -v <workRoot>:/work -w /work/<workdir> <image> <argv...>
//	rm <tmp>
//	docker start -a <name>
//
// create reads the env file, so it is deleted before the container starts.
// Values containing a newline cannot be written to an env file; those are
// passed as "-e NAME" and taken from the docker CLI's own environment, so no
// value is ever on a command line.
type containerRunner struct {
	docker string   // docker binary
	envDir string   // where the 0600 env file is written (outside workRoot)
	cliEnv []string // the docker CLI's base environment
}

// containerName is the generated name used to start and kill a job's
// container.
func containerName(jobID string, attempt int) string {
	return fmt.Sprintf("hourch-%s-%d", strings.ToLower(jobID), attempt)
}

// containerArgs builds the docker create argv (without the binary) and the
// env-file body. Exposed for tests.
func containerArgs(s runSpec, envFile string) (args []string, envFileBody []byte, passthrough []string) {
	name := containerName(s.jobID, s.attempt)
	var buf bytes.Buffer
	for _, kv := range envList(s.env) {
		k, v, _ := strings.Cut(kv, "=")
		if strings.ContainsAny(v, "\n\r") {
			passthrough = append(passthrough, k)
			continue
		}
		buf.WriteString(k + "=" + v + "\n")
	}
	wd := "/work"
	if s.workdir != "" && s.workdir != "." {
		wd = path.Join("/work", s.workdir)
	}
	args = []string{"create", "--rm", "--name", name, "--label", "hourch.job=" + s.jobID,
		"--env-file", envFile}
	for _, k := range passthrough {
		args = append(args, "-e", k)
	}
	args = append(args, "-v", s.workRoot+":/work", "-w", wd, s.image)
	args = append(args, s.argv...)
	return args, buf.Bytes(), passthrough
}

type containerJob struct {
	r    *containerRunner
	name string
	grp  *procGroup
}

func (r *containerRunner) cli(ctx context.Context, stdout, stderr io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, r.docker, args...)
	cmd.Env = r.cliEnv
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}

func (r *containerRunner) start(ctx context.Context, s runSpec) (running, error) {
	name := containerName(s.jobID, s.attempt)
	// A container left by a worker that died has the same name.
	_ = r.cli(ctx, io.Discard, io.Discard, "rm", "-f", name)

	f, err := os.CreateTemp(r.envDir, ".hourch-env-*")
	if err != nil {
		return nil, fmt.Errorf("env file: %w", err)
	}
	envFile := f.Name()
	defer os.Remove(envFile)
	args, body, pass := containerArgs(s, envFile)
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return nil, fmt.Errorf("env file: %w", err)
	}
	if _, err := f.Write(body); err != nil {
		f.Close()
		return nil, fmt.Errorf("env file: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("env file: %w", err)
	}
	createEnv := append([]string(nil), r.cliEnv...)
	for _, k := range pass {
		createEnv = append(createEnv, k+"="+s.env[k])
	}
	create := exec.CommandContext(ctx, r.docker, args...)
	create.Env = createEnv
	create.Stdout = io.Discard // the container id
	create.Stderr = s.stderr   // pull progress and errors belong in the job log
	if err := create.Run(); err != nil {
		return nil, fmt.Errorf("docker create: %w", err)
	}
	if err := os.Remove(envFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = r.cli(context.Background(), io.Discard, io.Discard, "rm", "-f", name)
		return nil, fmt.Errorf("env file: %w", err)
	}
	grp, err := startGroup([]string{r.docker, "start", "-a", name}, s.workRoot, r.cliEnv, s.stdout, s.stderr, envPath(r.cliEnv))
	if err != nil {
		_ = r.cli(context.Background(), io.Discard, io.Discard, "rm", "-f", name)
		return nil, fmt.Errorf("docker start: %w", err)
	}
	return &containerJob{r: r, name: name, grp: grp}, nil
}

func envPath(env []string) string {
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			return v
		}
	}
	return ""
}

func (c *containerJob) wait() int { return c.grp.wait() }

func (c *containerJob) kill(grace time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), grace+30*time.Second)
	defer cancel()
	_ = c.r.cli(ctx, io.Discard, io.Discard, "kill", "--signal", "TERM", c.name)
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-c.grp.done:
	case <-t.C:
	}
	_ = c.r.cli(ctx, io.Discard, io.Discard, "rm", "-f", c.name)
	c.grp.kill(0)
}
