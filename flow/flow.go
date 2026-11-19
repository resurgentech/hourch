// Package hourchflow builds hourchd runs as Go dependency graphs and submits
// them (docs/DESIGN-hourchd.md, "Workflows as Go graphs").
//
// A Run is a builder: it does no I/O until Submit. Jobs reference upstream
// outputs (Job.Output) and artifacts (Job.Artifact); both add a dependency.
// After adds an ordering-only dependency. Submit validates the whole graph,
// streams the context tarball to POST /v1/contexts, then POSTs the run.
//
// The package also carries the helpers a workflow binary needs around a run
// (NewClientFromEnv, Wait, StreamLog) and the helpers code running inside a
// job needs (NewJobClientFromEnv, NextCounter, PutBuild, RecordBuild,
// WriteOutput).
package hourchflow

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	hourchdapi "github.com/resurgentech/hourch/api"
)

// Run is a workflow run under construction. Build it with NewRun, add jobs
// with Job, then Submit. A Run is not safe for concurrent use.
type Run struct {
	workflow string
	params   map[string]string

	hasContext bool
	contextDir string
	excludes   []string
	includes   []string

	secrets     map[string]string
	secretOrder []string

	jobs   []*Job
	byName map[string][]*Job

	errs []error // builder-time errors, reported by Validate
}

// NewRun starts a run of workflow with free-form params (shown in the UI).
func NewRun(workflow string, params map[string]string) *Run {
	p := make(map[string]string, len(params))
	for k, v := range params {
		p[k] = v
	}
	return &Run{
		workflow: workflow,
		params:   p,
		secrets:  map[string]string{},
		byName:   map[string][]*Job{},
	}
}

// Workflow returns the workflow name.
func (r *Run) Workflow() string { return r.workflow }

// Context packs dir at Submit time (honouring .gitignore files, the
// always-excluded `.git`, and excludes, all in gitignore syntax rooted at
// dir; see ContextInclude for files to pack regardless) and uploads it through
// hourchd. Every job starts in the unpacked tree. Without Context, jobs start
// in an empty workdir. See WriteContext for the exact rules.
func (r *Run) Context(dir string, excludes ...string) {
	r.hasContext = true
	r.contextDir = dir
	r.excludes = append([]string(nil), excludes...)
}

// ContextInclude packs these files (paths relative to the Context dir) even
// if .gitignore or the excludes drop them; for example the workflow's own
// compiled binary, whose step subcommands the jobs run.
func (r *Run) ContextInclude(paths ...string) {
	r.includes = append(r.includes, paths...)
}

// ContextIncludes returns the paths given to ContextInclude.
func (r *Run) ContextIncludes() []string { return append([]string(nil), r.includes...) }

// Secret records a value for this run and returns a handle jobs pass to
// Secrets. The value is sent write-only with the run and exposed to the jobs
// that name it as an environment variable called name. Only secrets that at
// least one job names are sent.
func (r *Run) Secret(name, value string) Secret {
	if old, ok := r.secrets[name]; ok {
		if old != value {
			r.errs = append(r.errs, fmt.Errorf("secret %q set twice with different values", name))
		}
		return Secret{run: r, name: name}
	}
	r.secrets[name] = value
	r.secretOrder = append(r.secretOrder, name)
	return Secret{run: r, name: name}
}

// DeclareSecret records a secret name whose value is set later with
// SetSecret (before Validate or Submit), so a graph can name its secrets
// without handling their values.
func (r *Run) DeclareSecret(name string) Secret {
	if _, ok := r.secrets[name]; !ok {
		r.secrets[name] = ""
		r.secretOrder = append(r.secretOrder, name)
	}
	return Secret{run: r, name: name}
}

// SetSecret sets the value of a secret declared with DeclareSecret (or
// replaces an empty one); an unknown name is an error.
func (r *Run) SetSecret(name, value string) error {
	old, ok := r.secrets[name]
	if !ok {
		return fmt.Errorf("secret %q is not declared on the run", name)
	}
	if old != "" && old != value {
		return fmt.Errorf("secret %q already set to a different value", name)
	}
	r.secrets[name] = value
	return nil
}

// SecretNames returns the run's secret names in declaration order.
func (r *Run) SecretNames() []string { return append([]string(nil), r.secretOrder...) }

// Job adds a job named name. Names must be unique within the run (checked
// by Validate).
func (r *Run) Job(name string, opts ...JobOpt) *Job {
	j := &Job{run: r, name: name, env: map[string]envValue{}}
	for _, o := range opts {
		if o != nil {
			o(j)
		}
	}
	r.jobs = append(r.jobs, j)
	r.byName[name] = append(r.byName[name], j)
	return j
}

// Jobs returns the jobs in the order they were added.
func (r *Run) Jobs() []*Job { return append([]*Job(nil), r.jobs...) }

// Lookup returns the job named name, or nil.
func (r *Run) Lookup(name string) *Job {
	if js := r.byName[name]; len(js) > 0 {
		return js[0]
	}
	return nil
}

// Secret is a handle to a run secret, from Run.Secret.
type Secret struct {
	run  *Run
	name string
}

// Name is the secret's name, which is also the job's environment variable.
func (s Secret) Name() string { return s.name }

// Ref is a reference to an upstream job's output, rendered as
// `${outputs.<job>.<key>}` and substituted by hourchd when the consuming job
// becomes ready. Using a Ref adds a dependency on its job.
type Ref struct {
	job *Job
	key string
}

// String renders the placeholder. A placeholder concatenated into a larger
// string (`"--v=" + ref.String()`) still counts as a dependency: Validate
// scans every argv and env string for placeholders.
func (r Ref) String() string {
	if r.job == nil {
		return ""
	}
	return placeholder(r.job.name, r.key)
}

// Job is the job the output comes from.
func (r Ref) Job() *Job { return r.job }

// Key is the output key.
func (r Ref) Key() string { return r.key }

func placeholder(job, key string) string { return "${outputs." + job + "." + key + "}" }

// Artifact is an upstream job's artifact (a file, or a directory when the
// path ends in `/`) for Inputs. Using one adds a dependency on its job.
type Artifact struct {
	job  *Job
	path string
	as   string
}

// As sets where the input lands, relative to the job's workdir root. The
// default (empty) is `inputs/<job>/<path>`.
func (a Artifact) As(dest string) Artifact { a.as = dest; return a }

// Job is the upstream job.
func (a Artifact) Job() *Job { return a.job }

// Path is the artifact path in the upstream job.
func (a Artifact) Path() string { return a.path }

type envValue struct {
	s   string
	ref *Ref
}

func (v envValue) render() string {
	if v.ref != nil {
		return v.ref.String()
	}
	return v.s
}

// Job is one job of a Run.
type Job struct {
	run *Run

	name        string
	tags        []string
	image       string
	argv        []string
	argvSet     bool
	argvRefs    []Ref // Refs passed as typed values to Cmd
	workdir     string
	env         map[string]envValue
	envOrder    []string
	secrets     []Secret
	inputs      []Artifact
	artifacts   []string
	after       []*Job
	concurrency string
	attempts    int
	timeout     time.Duration
	timeoutSet  bool

	errs []error
}

// Name returns the job name.
func (j *Job) Name() string { return j.name }

// Output references key from this job's `$HOURCH_OUTPUTS`.
func (j *Job) Output(key string) Ref { return Ref{job: j, key: key} }

// Artifact references one of this job's artifacts for another job's Inputs.
func (j *Job) Artifact(path string) Artifact { return Artifact{job: j, path: path} }

// JobOpt configures a Job.
type JobOpt func(*Job)

// Tags sets the worker tags the job needs (worker tags ⊇ job tags).
func Tags(tags ...string) JobOpt {
	return func(j *Job) { j.tags = append(j.tags, tags...) }
}

// Cmd sets argv. Each element is a string or a Ref (never a shell string; use
// Cmd("bash", "-c", "...") when a shell is really needed).
func Cmd(argv ...any) JobOpt {
	return func(j *Job) {
		j.argvSet = true
		j.argv = nil
		j.argvRefs = nil
		for i, a := range argv {
			switch v := a.(type) {
			case string:
				j.argv = append(j.argv, v)
			case Ref:
				j.argv = append(j.argv, v.String())
				j.argvRefs = append(j.argvRefs, v)
			case *Ref:
				if v == nil {
					j.errs = append(j.errs, fmt.Errorf("argv[%d] is a nil *Ref", i))
					continue
				}
				j.argv = append(j.argv, v.String())
				j.argvRefs = append(j.argvRefs, *v)
			default:
				j.errs = append(j.errs, fmt.Errorf("argv[%d] has type %T; want string or Ref", i, a))
			}
		}
	}
}

// Dir sets the working directory relative to the unpacked context.
func Dir(rel string) JobOpt { return func(j *Job) { j.workdir = rel } }

// Env sets an environment variable to a string or a Ref.
func Env(key string, v any) JobOpt {
	return func(j *Job) {
		var ev envValue
		switch x := v.(type) {
		case string:
			ev.s = x
		case Ref:
			ev.ref = &x
		case *Ref:
			if x == nil {
				j.errs = append(j.errs, fmt.Errorf("env %s is a nil *Ref", key))
				return
			}
			r := *x
			ev.ref = &r
		default:
			j.errs = append(j.errs, fmt.Errorf("env %s has type %T; want string or Ref", key, v))
			return
		}
		if _, ok := j.env[key]; !ok {
			j.envOrder = append(j.envOrder, key)
		}
		j.env[key] = ev
	}
}

// Secrets exposes run secrets to the job as environment variables named
// after each secret.
func Secrets(s ...Secret) JobOpt {
	return func(j *Job) { j.secrets = append(j.secrets, s...) }
}

// Inputs fetches upstream artifacts before the command runs, into
// `inputs/<job>/<path>` unless Artifact.As says otherwise.
func Inputs(a ...Artifact) JobOpt {
	return func(j *Job) { j.inputs = append(j.inputs, a...) }
}

// Artifacts uploads paths after the command succeeds. A trailing `/` uploads
// every file under the directory.
func Artifacts(paths ...string) JobOpt {
	return func(j *Job) { j.artifacts = append(j.artifacts, paths...) }
}

// Image runs the job in a container (only on workers with capability docker).
func Image(ref string) JobOpt { return func(j *Job) { j.image = ref } }

// Timeout bounds one attempt. It is sent in whole seconds, rounded up.
// Without it hourchd applies limits.default_timeout (2h).
func Timeout(d time.Duration) JobOpt {
	return func(j *Job) { j.timeout = d; j.timeoutSet = true }
}

// Attempts sets max_attempts (1..10; default 1).
func Attempts(n int) JobOpt { return func(j *Job) { j.attempts = n } }

// Concurrency sets a key: at most one leased job per key across all runs.
func Concurrency(key string) JobOpt { return func(j *Job) { j.concurrency = key } }

// After adds ordering-only dependencies.
func After(jobs ...*Job) JobOpt {
	return func(j *Job) { j.after = append(j.after, jobs...) }
}

// Deps returns the job's effective dependencies by name, sorted: After, every
// Inputs job, and every job named by an output placeholder in argv or env.
// hourchd computes the same set.
func (j *Job) Deps() []string {
	set := map[string]bool{}
	for _, d := range j.after {
		if d != nil {
			set[d.name] = true
		}
	}
	for _, in := range j.inputs {
		if in.job != nil {
			set[in.job.name] = true
		}
	}
	for _, r := range j.refs() {
		if r.job != nil {
			set[r.job.name] = true
		}
	}
	for _, s := range j.strings() {
		for _, p := range findPlaceholders(s) {
			set[p.job] = true
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// refs returns the typed Refs passed to Cmd and Env.
func (j *Job) refs() []Ref {
	out := append([]Ref(nil), j.argvRefs...)
	for _, k := range j.envOrder {
		if r := j.env[k].ref; r != nil {
			out = append(out, *r)
		}
	}
	return out
}

// strings returns every argv and env value, rendered.
func (j *Job) strings() []string {
	out := append([]string(nil), j.argv...)
	for _, k := range j.envOrder {
		out = append(out, j.env[k].render())
	}
	return out
}

// spec renders the job as the wire JobSpec. Call after Validate.
func (j *Job) spec() hourchdapi.JobSpec {
	s := hourchdapi.JobSpec{
		Name: j.name,
		Argv: append([]string{}, j.argv...),
	}
	if len(j.tags) > 0 {
		t := append(hourchdapi.Tags{}, j.tags...)
		s.Tags = &t
	}
	if j.image != "" {
		s.Image = ptr(j.image)
	}
	if j.workdir != "" {
		s.Workdir = ptr(j.workdir)
	}
	if len(j.env) > 0 {
		m := make(map[string]string, len(j.env))
		for k, v := range j.env {
			m[k] = v.render()
		}
		s.Env = &m
	}
	if len(j.secrets) > 0 {
		var names []hourchdapi.SecretName
		seen := map[string]bool{}
		for _, sec := range j.secrets {
			if !seen[sec.name] {
				seen[sec.name] = true
				names = append(names, sec.name)
			}
		}
		s.Secrets = &names
	}
	if len(j.inputs) > 0 {
		ins := make([]hourchdapi.JobInput, 0, len(j.inputs))
		for _, in := range j.inputs {
			ji := hourchdapi.JobInput{Job: in.job.name, Path: in.path}
			if in.as != "" {
				ji.As = ptr(in.as)
			}
			ins = append(ins, ji)
		}
		s.Inputs = &ins
	}
	if len(j.artifacts) > 0 {
		a := append([]hourchdapi.ArtifactPath{}, j.artifacts...)
		s.Artifacts = &a
	}
	if len(j.after) > 0 {
		var deps []hourchdapi.JobName
		seen := map[string]bool{}
		for _, d := range j.after {
			if !seen[d.name] {
				seen[d.name] = true
				deps = append(deps, d.name)
			}
		}
		s.DependsOn = &deps
	}
	if j.concurrency != "" {
		s.ConcurrencyKey = ptr(j.concurrency)
	}
	if j.attempts != 0 {
		s.MaxAttempts = ptr(j.attempts)
	}
	if j.timeoutSet {
		s.TimeoutSeconds = ptr(timeoutSeconds(j.timeout))
	}
	return s
}

// timeoutSeconds rounds d up to whole seconds (0 for d <= 0).
func timeoutSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	s := math.Ceil(d.Seconds())
	if s > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(s)
}

// Spec validates the run and returns the RunSubmission it would POST, with
// secret values and without context_ref (Submit fills that in after the
// upload). Use it for graph tests.
func (r *Run) Spec() (hourchdapi.RunSubmission, error) {
	if err := r.Validate(); err != nil {
		return hourchdapi.RunSubmission{}, err
	}
	sub := hourchdapi.RunSubmission{Workflow: r.workflow}
	if len(r.params) > 0 {
		p := make(map[string]string, len(r.params))
		for k, v := range r.params {
			p[k] = v
		}
		sub.Params = &p
	}
	used := map[string]string{}
	for _, j := range r.jobs {
		for _, s := range j.secrets {
			used[s.name] = r.secrets[s.name]
		}
		sub.Jobs = append(sub.Jobs, j.spec())
	}
	if len(used) > 0 {
		sub.Secrets = &used
	}
	return sub, nil
}

func ptr[T any](v T) *T { return &v }

// placeholderRef is one `${outputs.<job>.<key>}` found in a string.
type placeholderRef struct{ job, key string }

// findPlaceholders returns every `${outputs.<job>.<key>}` in s. The key is
// the text after the last '.', so output keys cannot contain '.' while job
// names may.
func findPlaceholders(s string) []placeholderRef {
	const open = "${outputs."
	var out []placeholderRef
	for {
		i := strings.Index(s, open)
		if i < 0 {
			return out
		}
		rest := s[i+len(open):]
		end := strings.IndexByte(rest, '}')
		if end < 0 {
			return out
		}
		body := rest[:end]
		if dot := strings.LastIndexByte(body, '.'); dot > 0 && dot < len(body)-1 {
			out = append(out, placeholderRef{job: body[:dot], key: body[dot+1:]})
		} else {
			out = append(out, placeholderRef{job: body, key: ""})
		}
		s = rest[end+1:]
	}
}
