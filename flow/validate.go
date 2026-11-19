package hourchflow

import (
	"fmt"
	"regexp"
	"strings"
)

// Limits from specs/openapi/hourchd.yml (the config.yaml.example defaults;
// a server configured lower still rejects what passes here).
const (
	MaxJobsPerRun      = 200
	MaxParams          = 64
	MaxSecretsPerRun   = 64
	MaxTags            = 32
	MaxArgv            = 1024
	MaxEnv             = 256
	MaxJobSecrets      = 64
	MaxInputs          = 64
	MaxArtifacts       = 64
	MaxDependsOn       = 200
	MaxAttempts        = 10
	MaxImageLen        = 512
	MaxPathLen         = 1024
	MaxConcurrencyLen  = 128
	MaxOutputValueSize = 4096
)

var (
	reWorkflow  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	reJobName   = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
	reTag       = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	reEnvName   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	reOutputKey = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
)

// Problem codes mirror the server's error codes for the same mistake.
const (
	CodeBadRequest    = "bad_request"
	CodeInvalidGraph  = "invalid_graph"
	CodeMissingSecret = "missing_secret"
)

// Problem is one validation failure.
type Problem struct {
	Job     string // empty for run-level problems
	Code    string // CodeBadRequest, CodeInvalidGraph or CodeMissingSecret
	Message string
}

func (p Problem) String() string {
	if p.Job != "" {
		return fmt.Sprintf("job %q: %s", p.Job, p.Message)
	}
	return p.Message
}

// ValidationError lists every problem Validate found.
type ValidationError struct {
	Problems []Problem
}

func (e *ValidationError) Error() string {
	parts := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		parts[i] = p.String()
	}
	return "invalid run: " + strings.Join(parts, "; ")
}

// Has reports whether any problem has code.
func (e *ValidationError) Has(code string) bool {
	for _, p := range e.Problems {
		if p.Code == code {
			return true
		}
	}
	return false
}

type validator struct {
	probs []Problem
}

func (v *validator) add(job, code, format string, args ...any) {
	v.probs = append(v.probs, Problem{Job: job, Code: code, Message: fmt.Sprintf(format, args...)})
}

// Validate checks the graph without any I/O: names, limits, that every Ref,
// Artifact, After and secret belongs to this run, that every output
// placeholder names a job in the run, that every input names a declared
// artifact of its job, and that the graph is acyclic. It returns a
// *ValidationError listing every problem, or nil.
func (r *Run) Validate() error {
	v := &validator{}

	if !reWorkflow.MatchString(r.workflow) {
		v.add("", CodeBadRequest, "workflow name %q must match %s", r.workflow, reWorkflow)
	}
	if len(r.params) > MaxParams {
		v.add("", CodeBadRequest, "%d params; at most %d", len(r.params), MaxParams)
	}
	if r.hasContext && r.contextDir == "" {
		v.add("", CodeBadRequest, "Context dir is empty")
	}
	for _, err := range r.errs {
		v.add("", CodeBadRequest, "%v", err)
	}
	for _, name := range r.secretOrder {
		if !reEnvName.MatchString(name) {
			v.add("", CodeBadRequest, "secret name %q must match %s", name, reEnvName)
		}
		if strings.HasPrefix(name, "HOURCH_") {
			v.add("", CodeBadRequest, "secret name %q: HOURCH_* is reserved", name)
		}
		if r.secrets[name] == "" {
			v.add("", CodeMissingSecret, "secret %q has an empty value", name)
		}
	}

	switch {
	case len(r.jobs) == 0:
		v.add("", CodeInvalidGraph, "run has no jobs")
	case len(r.jobs) > MaxJobsPerRun:
		v.add("", CodeInvalidGraph, "%d jobs; at most %d", len(r.jobs), MaxJobsPerRun)
	}

	usedSecrets := map[string]bool{}
	for _, j := range r.jobs {
		r.validateJob(v, j, usedSecrets)
	}
	if len(usedSecrets) > MaxSecretsPerRun {
		v.add("", CodeBadRequest, "%d secrets used; at most %d", len(usedSecrets), MaxSecretsPerRun)
	}

	if cyc := r.findCycle(); cyc != nil {
		v.add("", CodeInvalidGraph, "dependency cycle: %s", strings.Join(cyc, " -> "))
	}

	if len(v.probs) > 0 {
		return &ValidationError{Problems: v.probs}
	}
	return nil
}

func (r *Run) validateJob(v *validator, j *Job, usedSecrets map[string]bool) {
	n := j.name
	if !reJobName.MatchString(n) {
		v.add(n, CodeBadRequest, "name must match %s", reJobName)
	}
	if len(r.byName[n]) > 1 && r.byName[n][0] == j {
		v.add(n, CodeInvalidGraph, "job name used %d times", len(r.byName[n]))
	}
	for _, err := range j.errs {
		v.add(n, CodeBadRequest, "%v", err)
	}

	if len(j.tags) > MaxTags {
		v.add(n, CodeBadRequest, "%d tags; at most %d", len(j.tags), MaxTags)
	}
	for _, t := range j.tags {
		if !reTag.MatchString(t) {
			v.add(n, CodeBadRequest, "tag %q must match %s", t, reTag)
		}
	}

	switch {
	case !j.argvSet || len(j.argv) == 0:
		v.add(n, CodeBadRequest, "no command (use Cmd)")
	case len(j.argv) > MaxArgv:
		v.add(n, CodeBadRequest, "argv has %d elements; at most %d", len(j.argv), MaxArgv)
	}
	if len(j.image) > MaxImageLen {
		v.add(n, CodeBadRequest, "image longer than %d bytes", MaxImageLen)
	}
	if j.workdir != "" {
		if err := checkRelPath(j.workdir); err != nil {
			v.add(n, CodeBadRequest, "Dir %q: %v", j.workdir, err)
		}
	}

	secretNames := map[string]bool{}
	if len(j.secrets) > MaxJobSecrets {
		v.add(n, CodeBadRequest, "%d secrets; at most %d", len(j.secrets), MaxJobSecrets)
	}
	for _, s := range j.secrets {
		switch {
		case s.run == nil:
			v.add(n, CodeMissingSecret, "zero Secret value; use Run.Secret")
		case s.run != r:
			v.add(n, CodeMissingSecret, "secret %q belongs to another run", s.name)
		default:
			if _, ok := r.secrets[s.name]; !ok {
				v.add(n, CodeMissingSecret, "secret %q is not set on the run", s.name)
			}
			secretNames[s.name] = true
			usedSecrets[s.name] = true
		}
	}

	if len(j.env) > MaxEnv {
		v.add(n, CodeBadRequest, "%d env vars; at most %d", len(j.env), MaxEnv)
	}
	for _, k := range j.envOrder {
		switch {
		case !reEnvName.MatchString(k):
			v.add(n, CodeBadRequest, "env name %q must match %s", k, reEnvName)
		case strings.HasPrefix(k, "HOURCH_"):
			v.add(n, CodeBadRequest, "env name %q: HOURCH_* is reserved", k)
		case secretNames[k]:
			v.add(n, CodeBadRequest, "env name %q is also a secret of this job", k)
		}
	}

	for _, ref := range j.refs() {
		switch {
		case ref.job == nil:
			v.add(n, CodeInvalidGraph, "zero Ref value; use Job.Output")
		case ref.job.run != r:
			v.add(n, CodeInvalidGraph, "output reference to job %q of another run", ref.job.name)
		}
		if ref.job != nil && !reOutputKey.MatchString(ref.key) {
			v.add(n, CodeInvalidGraph, "output key %q must match %s", ref.key, reOutputKey)
		}
	}
	for _, s := range j.strings() {
		for _, p := range findPlaceholders(s) {
			if p.key == "" {
				v.add(n, CodeInvalidGraph, "malformed placeholder ${outputs.%s}", p.job)
				continue
			}
			if r.Lookup(p.job) == nil {
				v.add(n, CodeInvalidGraph, "output placeholder names unknown job %q", p.job)
			}
		}
	}

	if len(j.inputs) > MaxInputs {
		v.add(n, CodeBadRequest, "%d inputs; at most %d", len(j.inputs), MaxInputs)
	}
	for _, in := range j.inputs {
		if in.job == nil {
			v.add(n, CodeInvalidGraph, "zero Artifact value; use Job.Artifact")
			continue
		}
		if in.job.run != r {
			v.add(n, CodeInvalidGraph, "input from job %q of another run", in.job.name)
			continue
		}
		if err := checkRelPath(in.path); err != nil {
			v.add(n, CodeBadRequest, "input %s:%q: %v", in.job.name, in.path, err)
		} else if !declares(in.job, in.path) {
			v.add(n, CodeInvalidGraph, "input %q is not among the artifacts job %q declares %q", in.path, in.job.name, in.job.artifacts)
		}
		if in.as != "" {
			if err := checkRelPath(in.as); err != nil {
				v.add(n, CodeBadRequest, "input %s:%q As %q: %v", in.job.name, in.path, in.as, err)
			}
		}
	}

	if len(j.artifacts) > MaxArtifacts {
		v.add(n, CodeBadRequest, "%d artifacts; at most %d", len(j.artifacts), MaxArtifacts)
	}
	for _, a := range j.artifacts {
		if err := checkRelPath(a); err != nil {
			v.add(n, CodeBadRequest, "artifact %q: %v", a, err)
		}
	}

	if len(j.after) > MaxDependsOn {
		v.add(n, CodeBadRequest, "%d After jobs; at most %d", len(j.after), MaxDependsOn)
	}
	for _, d := range j.after {
		switch {
		case d == nil:
			v.add(n, CodeInvalidGraph, "After(nil)")
		case d.run != r:
			v.add(n, CodeInvalidGraph, "After job %q of another run", d.name)
		}
	}

	if len(j.concurrency) > MaxConcurrencyLen {
		v.add(n, CodeBadRequest, "concurrency key longer than %d bytes", MaxConcurrencyLen)
	}
	if j.attempts != 0 && (j.attempts < 1 || j.attempts > MaxAttempts) {
		v.add(n, CodeBadRequest, "Attempts(%d); want 1..%d", j.attempts, MaxAttempts)
	}
	if j.timeoutSet && j.timeout <= 0 {
		v.add(n, CodeBadRequest, "Timeout(%s); want > 0", j.timeout)
	}
}

// declares reports whether job j declares an artifact covering path: the
// same path, a declared directory containing it, or (for a directory path)
// a declared artifact inside it.
func declares(j *Job, path string) bool {
	for _, a := range j.artifacts {
		switch {
		case a == path:
			return true
		case strings.HasSuffix(a, "/") && strings.HasPrefix(path, a):
			return true
		case strings.HasSuffix(path, "/") && strings.HasPrefix(a, path):
			return true
		}
	}
	return false
}

// checkRelPath enforces the spec's ArtifactPath rules: relative, no `.` or
// `..` segments, no empty segments (a single trailing `/` names a
// directory), at most MaxPathLen bytes. Dir and As use the same rules.
func checkRelPath(p string) error {
	if p == "" {
		return fmt.Errorf("empty path")
	}
	if len(p) > MaxPathLen {
		return fmt.Errorf("longer than %d bytes", MaxPathLen)
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("must be relative")
	}
	segs := strings.Split(strings.TrimSuffix(p, "/"), "/")
	for _, s := range segs {
		switch s {
		case "":
			return fmt.Errorf("empty path segment")
		case ".", "..":
			return fmt.Errorf("%q segment not allowed", s)
		}
	}
	if strings.ContainsRune(p, 0) {
		return fmt.Errorf("contains NUL")
	}
	return nil
}

// findCycle returns one dependency cycle by job name, or nil. Dependencies
// on names that do not resolve are reported elsewhere and ignored here.
func (r *Run) findCycle() []string {
	const (
		white = iota
		grey
		black
	)
	color := map[string]int{}
	var stack []string
	var cycle []string
	var visit func(name string) bool
	visit = func(name string) bool {
		color[name] = grey
		stack = append(stack, name)
		j := r.Lookup(name)
		for _, d := range j.Deps() {
			if r.Lookup(d) == nil {
				continue
			}
			switch color[d] {
			case grey:
				for i, s := range stack {
					if s == d {
						cycle = append(append([]string(nil), stack[i:]...), d)
						return true
					}
				}
			case white:
				if visit(d) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[name] = black
		return false
	}
	for _, j := range r.jobs {
		if color[j.name] == white && visit(j.name) {
			return cycle
		}
	}
	return nil
}
