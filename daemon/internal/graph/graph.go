// Package graph validates a run submission and resolves output placeholders.
//
// Checks are split by the error a client gets: shape and bound violations
// (patterns, sizes, reserved names) are *BadRequest (400 bad_request); graph
// errors (duplicate names, unknown references, cycles, too many jobs) are
// *InvalidGraph (422 invalid_graph); a job naming a secret the submission did
// not send is *MissingSecret (422 missing_secret).
package graph

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Spec bounds (specs/openapi/hourchd.yml).
var (
	WorkflowRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	JobNameRe  = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
	TagRe      = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	SecretRe   = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
	EnvRe      = SecretRe
	OutputKey  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	ContextRe  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	Sha256Re   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	CounterRe  = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,200}$`)
	BuildRe    = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	FlavourRe  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	WorkerIDRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	placeRe    = regexp.MustCompile(`\$\{outputs\.([^}]*)\}`)
)

// BadRequest is a shape or bound violation (400).
type BadRequest struct{ Msg string }

func (e *BadRequest) Error() string { return e.Msg }

// InvalidGraph is a graph error (422 invalid_graph).
type InvalidGraph struct{ Msg string }

func (e *InvalidGraph) Error() string { return e.Msg }

// MissingSecret is a named secret without a value (422 missing_secret).
type MissingSecret struct{ Msg string }

func (e *MissingSecret) Error() string { return e.Msg }

// MissingOutput is a placeholder whose output was never published.
type MissingOutput struct{ Job, Key string }

func (e *MissingOutput) Error() string {
	return fmt.Sprintf("output %s.%s was not published", e.Job, e.Key)
}

func bad(f string, a ...any) error     { return &BadRequest{Msg: fmt.Sprintf(f, a...)} }
func invalid(f string, a ...any) error { return &InvalidGraph{Msg: fmt.Sprintf(f, a...)} }

// Input is an upstream artifact.
type Input struct {
	Job, Path, As string
}

// Job is one submitted job, defaults not yet applied.
type Job struct {
	Name           string
	Tags           []string
	Image          string
	Argv           []string
	Workdir        string
	Env            map[string]string
	Secrets        []string
	Inputs         []Input
	Artifacts      []string
	DependsOn      []string
	ConcurrencyKey string
	MaxAttempts    int // 0: default 1
	TimeoutSeconds int // 0: default
}

// Submission is a run as submitted.
type Submission struct {
	Workflow   string
	Params     map[string]string
	ContextRef string
	Secrets    map[string]string
	Jobs       []Job
}

// Limits are the server-enforced bounds from config.
type Limits struct {
	MaxJobs int
}

// ValidPath checks an artifact path: relative, no "." or ".." or empty
// segments (a trailing "/" names a directory), at most 1024 bytes.
func ValidPath(p string) error {
	if p == "" || len(p) > 1024 {
		return fmt.Errorf("path must be 1 to 1024 bytes")
	}
	if strings.HasPrefix(p, "/") {
		return fmt.Errorf("path %q must be relative", p)
	}
	if strings.ContainsAny(p, "\x00\\") {
		return fmt.Errorf("path %q contains a NUL or backslash", p)
	}
	segs := strings.Split(strings.TrimSuffix(p, "/"), "/")
	for _, s := range segs {
		if s == "" || s == "." || s == ".." {
			return fmt.Errorf("path %q has an empty, . or .. segment", p)
		}
	}
	return nil
}

// IsDir reports whether an artifact path names a directory.
func IsDir(p string) bool { return strings.HasSuffix(p, "/") }

// Covers reports whether declared (a file or a directory ending in "/")
// includes path.
func Covers(declared, path string) bool {
	if IsDir(declared) {
		return strings.HasPrefix(path, declared) && len(path) > len(declared) || path == declared
	}
	return declared == path
}

func validRelDir(p string) error {
	if p == "" {
		return nil
	}
	return ValidPath(p)
}

// Placeholder is one ${outputs.<job>.<key>} reference.
type Placeholder struct{ Job, Key string }

// Placeholders returns the references in s. The key is the text after the
// last "." (job names may contain ".", output keys in placeholders may not).
// A reference whose job part names no job in names, or whose key is not a
// valid dot-free key, comes back with Job "" and Key the raw text.
func Placeholders(s string, names map[string]bool) []Placeholder {
	var out []Placeholder
	for _, m := range placeRe.FindAllStringSubmatch(s, -1) {
		out = append(out, splitRef(m[1], names))
	}
	return out
}

var refKeyRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

func splitRef(ref string, names map[string]bool) Placeholder {
	i := strings.LastIndexByte(ref, '.')
	if i <= 0 {
		return Placeholder{Key: ref}
	}
	job, key := ref[:i], ref[i+1:]
	if !names[job] || !refKeyRe.MatchString(key) {
		return Placeholder{Key: ref}
	}
	return Placeholder{Job: job, Key: key}
}

// Substitute replaces every placeholder in s from outputs[job][key].
func Substitute(s string, names map[string]bool, outputs map[string]map[string]string) (string, error) {
	var err error
	out := placeRe.ReplaceAllStringFunc(s, func(m string) string {
		p := splitRef(placeRe.FindStringSubmatch(m)[1], names)
		v, ok := outputs[p.Job][p.Key]
		if !ok && err == nil {
			err = &MissingOutput{Job: p.Job, Key: p.Key}
		}
		return v
	})
	return out, err
}

// Validate checks a submission and returns, for each job (by index), its
// effective dependencies: depends_on plus every job named in inputs and
// output placeholders, sorted and de-duplicated.
func Validate(s Submission, lim Limits) ([][]string, error) {
	if !WorkflowRe.MatchString(s.Workflow) {
		return nil, bad("workflow %q does not match %s", s.Workflow, WorkflowRe)
	}
	if len(s.Params) > 64 {
		return nil, bad("at most 64 params")
	}
	if len(s.Secrets) > 64 {
		return nil, bad("at most 64 secrets")
	}
	for name := range s.Secrets {
		if !SecretRe.MatchString(name) {
			return nil, bad("secret name %q does not match %s", name, SecretRe)
		}
	}
	if s.ContextRef != "" && !ContextRe.MatchString(s.ContextRef) {
		return nil, bad("context_ref must be sha256:<64 hex>")
	}
	if len(s.Jobs) == 0 {
		return nil, bad("a run needs at least one job")
	}
	if len(s.Jobs) > lim.MaxJobs {
		return nil, invalid("%d jobs; at most %d per run", len(s.Jobs), lim.MaxJobs)
	}
	for _, j := range s.Jobs {
		if err := validateJobShape(j); err != nil {
			return nil, err
		}
	}
	names := map[string]int{}
	for i, j := range s.Jobs {
		if _, dup := names[j.Name]; dup {
			return nil, invalid("job name %q is used twice", j.Name)
		}
		names[j.Name] = i
	}
	nameSet := map[string]bool{}
	for n := range names {
		nameSet[n] = true
	}
	deps := make([][]string, len(s.Jobs))
	for i, j := range s.Jobs {
		set := map[string]bool{}
		for _, d := range j.DependsOn {
			if !nameSet[d] {
				return nil, invalid("job %q: depends_on names unknown job %q", j.Name, d)
			}
			set[d] = true
		}
		for _, in := range j.Inputs {
			if !nameSet[in.Job] {
				return nil, invalid("job %q: input names unknown job %q", j.Name, in.Job)
			}
			set[in.Job] = true
		}
		texts := append([]string{}, j.Argv...)
		for _, v := range j.Env {
			texts = append(texts, v)
		}
		for _, t := range texts {
			for _, p := range Placeholders(t, nameSet) {
				if p.Job == "" {
					return nil, invalid("job %q: ${outputs.%s} names no job in this run", j.Name, p.Key)
				}
				set[p.Job] = true
			}
		}
		for _, sec := range j.Secrets {
			if _, ok := s.Secrets[sec]; !ok {
				return nil, &MissingSecret{Msg: fmt.Sprintf("job %q names secret %q, which the submission did not send", j.Name, sec)}
			}
		}
		for d := range set {
			deps[i] = append(deps[i], d)
		}
		sort.Strings(deps[i])
		if deps[i] == nil {
			deps[i] = []string{}
		}
	}
	if cyc := findCycle(s.Jobs, names, deps); cyc != "" {
		return nil, invalid("dependency cycle: %s", cyc)
	}
	return deps, nil
}

func validateJobShape(j Job) error {
	if !JobNameRe.MatchString(j.Name) {
		return bad("job name %q does not match %s", j.Name, JobNameRe)
	}
	if len(j.Tags) > 32 {
		return bad("job %q: at most 32 tags", j.Name)
	}
	for _, t := range j.Tags {
		if !TagRe.MatchString(t) {
			return bad("job %q: tag %q does not match %s", j.Name, t, TagRe)
		}
	}
	if len(j.Image) > 512 {
		return bad("job %q: image longer than 512", j.Name)
	}
	if len(j.Argv) == 0 || len(j.Argv) > 1024 {
		return bad("job %q: argv needs 1 to 1024 items", j.Name)
	}
	if len(j.Workdir) > 1024 {
		return bad("job %q: workdir longer than 1024", j.Name)
	}
	if err := validRelDir(j.Workdir); err != nil {
		return bad("job %q: workdir: %v", j.Name, err)
	}
	if len(j.Env) > 256 {
		return bad("job %q: at most 256 env vars", j.Name)
	}
	for k := range j.Env {
		if !EnvRe.MatchString(k) {
			return bad("job %q: env name %q does not match %s", j.Name, k, EnvRe)
		}
		if strings.HasPrefix(k, "HOURCH_") {
			return bad("job %q: env name %q is reserved (HOURCH_*)", j.Name, k)
		}
	}
	if len(j.Secrets) > 64 {
		return bad("job %q: at most 64 secrets", j.Name)
	}
	for _, sname := range j.Secrets {
		if !SecretRe.MatchString(sname) {
			return bad("job %q: secret name %q does not match %s", j.Name, sname, SecretRe)
		}
		if strings.HasPrefix(sname, "HOURCH_") {
			return bad("job %q: secret name %q is reserved (HOURCH_*)", j.Name, sname)
		}
	}
	if len(j.Inputs) > 64 {
		return bad("job %q: at most 64 inputs", j.Name)
	}
	for _, in := range j.Inputs {
		if !JobNameRe.MatchString(in.Job) {
			return bad("job %q: input job %q does not match %s", j.Name, in.Job, JobNameRe)
		}
		if err := ValidPath(in.Path); err != nil {
			return bad("job %q: input: %v", j.Name, err)
		}
		if len(in.As) > 1024 {
			return bad("job %q: input as longer than 1024", j.Name)
		}
		if in.As != "" {
			if err := ValidPath(in.As); err != nil {
				return bad("job %q: input as: %v", j.Name, err)
			}
		}
	}
	if len(j.Artifacts) > 64 {
		return bad("job %q: at most 64 artifacts", j.Name)
	}
	for _, a := range j.Artifacts {
		if err := ValidPath(a); err != nil {
			return bad("job %q: artifact: %v", j.Name, err)
		}
	}
	if len(j.DependsOn) > 200 {
		return bad("job %q: at most 200 depends_on", j.Name)
	}
	for _, d := range j.DependsOn {
		if !JobNameRe.MatchString(d) {
			return bad("job %q: depends_on %q does not match %s", j.Name, d, JobNameRe)
		}
	}
	if len(j.ConcurrencyKey) > 128 {
		return bad("job %q: concurrency_key longer than 128", j.Name)
	}
	if j.MaxAttempts < 0 || j.MaxAttempts > 10 {
		return bad("job %q: max_attempts must be 1 to 10", j.Name)
	}
	if j.TimeoutSeconds < 0 {
		return bad("job %q: timeout_seconds must be >= 1", j.Name)
	}
	return nil
}

func findCycle(jobs []Job, names map[string]int, deps [][]string) string {
	const (
		white = iota
		grey
		black
	)
	color := make([]int, len(jobs))
	var stack []string
	var found string
	var visit func(i int) bool
	visit = func(i int) bool {
		color[i] = grey
		stack = append(stack, jobs[i].Name)
		for _, d := range deps[i] {
			k := names[d]
			if color[k] == grey {
				// Report the cycle from d.
				start := 0
				for x, n := range stack {
					if n == d {
						start = x
					}
				}
				found = strings.Join(append(append([]string{}, stack[start:]...), d), " -> ")
				return true
			}
			if color[k] == white && visit(k) {
				return true
			}
		}
		stack = stack[:len(stack)-1]
		color[i] = black
		return false
	}
	for i := range jobs {
		if color[i] == white && visit(i) {
			return found
		}
	}
	return ""
}
