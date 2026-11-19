// Package view converts store rows to the generated API types (hourchdapi).
// No view carries a secret value.
package view

import (
	"encoding/json"
	"sort"
	"time"

	api "github.com/resurgentech/hourch/api"

	"github.com/resurgentech/hourch/daemon/internal/store"
)

// Counts converts a status → count map.
func Counts(m map[string]int) api.JobCounts {
	return api.JobCounts{
		Pending: m[store.JobPending], Ready: m[store.JobReady], Leased: m[store.JobLeased],
		Succeeded: m[store.JobSucceeded], Failed: m[store.JobFailed], Cancelled: m[store.JobCancelled],
	}
}

// RunSummary converts a run.
func RunSummary(r store.Run, counts map[string]int) api.RunSummary {
	params := r.Params
	if params == nil {
		params = map[string]string{}
	}
	return api.RunSummary{
		Id: r.ID, Workflow: r.Workflow, Number: r.Number, Params: params, Status: api.RunStatus(r.Status),
		SubmittedBy: r.SubmittedBy, CreatedAt: r.CreatedAt, FinishedAt: r.FinishedAt, JobCounts: Counts(counts),
	}
}

// Run converts a run with its jobs.
func Run(r store.Run, jobs []store.Job) api.Run {
	counts := map[string]int{}
	for _, j := range jobs {
		counts[j.Status]++
	}
	s := RunSummary(r, counts)
	out := api.Run{
		Id: s.Id, Workflow: s.Workflow, Number: s.Number, Params: s.Params, Status: s.Status,
		SubmittedBy: s.SubmittedBy, CreatedAt: s.CreatedAt, FinishedAt: s.FinishedAt, JobCounts: s.JobCounts,
		ContextRef: r.ContextRef, SecretNames: append([]string{}, r.SecretNames...), Jobs: []api.JobSummary{},
	}
	for _, j := range jobs {
		out.Jobs = append(out.Jobs, JobSummary(j))
	}
	return out
}

// JobSummary converts a job.
func JobSummary(j store.Job) api.JobSummary {
	return api.JobSummary{
		Id: j.ID, RunId: j.RunID, Name: j.Name, Status: api.JobStatus(j.Status), Tags: append([]string{}, j.Tags...),
		DependsOn: append([]string{}, j.DependsOn...), Attempt: j.Attempt, MaxAttempts: j.MaxAttempts, Worker: j.LeaseOwner,
		ExitCode: j.ExitCode, Error: j.Error, CreatedAt: j.CreatedAt, ReadyAt: j.ReadyAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt,
	}
}

// AttemptDetail is the event detail written when an attempt starts or ends
// (JSON in events.detail).
type AttemptDetail struct {
	Attempt  int    `json:"attempt"`
	Status   string `json:"status,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Error    string `json:"error,omitempty"`
}

// Event kinds that start and end an attempt.
const (
	KindAssigned = "assigned"
)

// EndKinds are the event kinds that end an attempt.
var EndKinds = map[string]bool{
	"finished": true, "requeued": true, "timeout": true, "lease_expired": true,
	"accept_timeout": true, "cancelled": true, "secret_unavailable": true,
}

// Attempts reconstructs a job's attempts from its events.
func Attempts(events []store.Event) []api.JobAttempt {
	byNum := map[int]*api.JobAttempt{}
	for _, e := range events {
		if e.Kind != KindAssigned && !EndKinds[e.Kind] {
			continue
		}
		var d AttemptDetail
		if json.Unmarshal([]byte(e.Detail), &d) != nil || d.Attempt < 1 {
			continue
		}
		if e.Kind == KindAssigned {
			byNum[d.Attempt] = &api.JobAttempt{Attempt: d.Attempt, Worker: e.Worker, StartedAt: e.TS}
			continue
		}
		a := byNum[d.Attempt]
		if a == nil {
			continue
		}
		ts := e.TS
		a.FinishedAt, a.ExitCode, a.Error = &ts, d.ExitCode, d.Error
	}
	out := make([]api.JobAttempt, 0, len(byNum))
	for _, a := range byNum {
		out = append(out, *a)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].Attempt < out[k].Attempt })
	return out
}

// JobDetail converts a job with outputs, events and artifacts.
func JobDetail(j store.Job, outputs map[string]string, events []store.Event, arts []store.Artifact) api.JobDetail {
	s := JobSummary(j)
	inputs := make([]api.JobInput, 0, len(j.Inputs))
	for _, in := range j.Inputs {
		ji := api.JobInput{Job: in.Job, Path: in.Path}
		if in.As != "" {
			as := in.As
			ji.As = &as
		}
		inputs = append(inputs, ji)
	}
	if outputs == nil {
		outputs = map[string]string{}
	}
	env := j.Env
	if env == nil {
		env = map[string]string{}
	}
	return api.JobDetail{
		Id: s.Id, RunId: s.RunId, Name: s.Name, Status: s.Status, Tags: s.Tags, DependsOn: s.DependsOn,
		Attempt: s.Attempt, MaxAttempts: s.MaxAttempts, Worker: s.Worker, ExitCode: s.ExitCode, Error: s.Error,
		CreatedAt: s.CreatedAt, ReadyAt: s.ReadyAt, StartedAt: s.StartedAt, FinishedAt: s.FinishedAt,
		Image: j.Image, Argv: append([]string{}, j.Argv...), Workdir: j.Workdir, Env: env,
		Secrets: append([]string{}, j.SecretNames...), Inputs: inputs, Artifacts: append([]string{}, j.Artifacts...),
		ConcurrencyKey: j.ConcurrencyKey, TimeoutSeconds: j.TimeoutS, LeaseUntil: leaseUntil(j),
		Outputs: outputs, Attempts: Attempts(events), UploadedArtifacts: Artifacts(arts),
	}
}

func leaseUntil(j store.Job) *time.Time {
	if j.Status != store.JobLeased {
		return nil
	}
	return j.LeaseUntil
}

// Artifacts converts artifact rows.
func Artifacts(arts []store.Artifact) []api.Artifact {
	out := make([]api.Artifact, 0, len(arts))
	for _, a := range arts {
		out = append(out, api.Artifact{Path: a.Path, Size: a.Size, Sha256: a.Sha256})
	}
	return out
}

// Worker converts a worker row.
func Worker(w store.Worker) api.Worker {
	caps := make([]api.WorkerCapabilities, 0, len(w.Capabilities))
	for _, c := range w.Capabilities {
		caps = append(caps, api.WorkerCapabilities(c))
	}
	var cur *string
	if w.CurrentJob != "" {
		c := w.CurrentJob
		cur = &c
	}
	return api.Worker{
		Id: w.ID, Hostname: w.Hostname, Tags: append([]string{}, w.Tags...), Capabilities: caps,
		Connected: w.Connected, CurrentJob: cur, LastSeen: w.LastSeen,
	}
}

// Build converts a build row.
func Build(b store.Build) api.Build {
	body := api.BuildBody{}
	_ = json.Unmarshal(b.Body, &body)
	return api.Build{
		BuildType: b.BuildType, Flavour: b.Flavour, BuildNum: b.BuildNum, RunId: b.RunID, JobId: b.JobID,
		CreatedAt: b.CreatedAt, Body: body,
	}
}
