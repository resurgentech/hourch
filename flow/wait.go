package hourchflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"

	hourchdapi "github.com/resurgentech/hourch/api"
)

// Terminal reports whether a run status is final.
func Terminal(s hourchdapi.RunStatus) bool {
	switch s {
	case "succeeded", "failed", "cancelled":
		return true
	}
	return false
}

type waitConfig struct {
	onJob       func(hourchdapi.JobSummary)
	pollInitial time.Duration
	pollMax     time.Duration
	noSSE       bool
}

// WaitOption tunes Wait.
type WaitOption func(*waitConfig)

// OnJob is called for every job change seen (SSE job events for this run,
// or a status change between polls). It runs on Wait's goroutine.
func OnJob(fn func(hourchdapi.JobSummary)) WaitOption {
	return func(c *waitConfig) { c.onJob = fn }
}

// PollInterval sets the polling backoff (default 1s doubling to 15s).
func PollInterval(initial, max time.Duration) WaitOption {
	return func(c *waitConfig) { c.pollInitial, c.pollMax = initial, max }
}

// WithoutSSE makes Wait poll only.
func WithoutSSE() WaitOption { return func(c *waitConfig) { c.noSSE = true } }

// Wait blocks until run id is terminal and returns it. It follows
// GET /v1/events (SSE, resuming with Last-Event-ID after a drop) and falls
// back to polling GET /v1/runs/{id} with backoff when the stream cannot be
// opened. A terminal run is returned with a nil error whatever its status;
// errors are transport, auth, not-found or context errors.
func Wait(ctx context.Context, c *hourchdapi.Client, id string, opts ...WaitOption) (*hourchdapi.Run, error) {
	cfg := waitConfig{pollInitial: time.Second, pollMax: 15 * time.Second}
	for _, o := range opts {
		o(&cfg)
	}
	w := &waiter{c: c, cr: withResponses(c), id: id, cfg: cfg, seen: map[string]hourchdapi.JobStatus{}}

	run, err := w.getRun(ctx)
	if err != nil {
		return nil, err
	}
	if Terminal(run.Status) {
		return run, nil
	}
	w.noteJobs(run)
	if !cfg.noSSE {
		run, done, err := w.followEvents(ctx)
		if done || err != nil {
			return run, err
		}
	}
	return w.poll(ctx)
}

type waiter struct {
	c    *hourchdapi.Client
	cr   *hourchdapi.ClientWithResponses
	id   string
	cfg  waitConfig
	seen map[string]hourchdapi.JobStatus
}

func (w *waiter) getRun(ctx context.Context) (*hourchdapi.Run, error) {
	resp, err := w.cr.GetRunWithResponse(ctx, w.id)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return nil, newAPIError("getRun", resp.StatusCode(), resp.Body)
	}
	return resp.JSON200, nil
}

// noteJobs reports jobs whose status changed since the last call.
func (w *waiter) noteJobs(run *hourchdapi.Run) {
	for _, j := range run.Jobs {
		if w.seen[j.Id] != j.Status {
			w.seen[j.Id] = j.Status
			if w.cfg.onJob != nil {
				w.cfg.onJob(j)
			}
		}
	}
}

// followEvents follows the SSE feed. done is true with the final run when the
// run finished; done false and err nil means fall back to polling.
func (w *waiter) followEvents(ctx context.Context) (*hourchdapi.Run, bool, error) {
	var last *int64
	empty := 0 // consecutive streams that ended without an event
	for {
		resp, err := w.c.StreamEvents(ctx, &hourchdapi.StreamEventsParams{LastEventID: last})
		if err != nil {
			if ctx.Err() != nil {
				return nil, true, ctx.Err()
			}
			return nil, false, nil
		}
		if !isEventStream(resp) {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
				return nil, true, newAPIError("streamEvents", resp.StatusCode, body)
			}
			return nil, false, nil
		}
		// The run may have finished before the subscription started.
		run, err := w.getRun(ctx)
		if err != nil {
			resp.Body.Close()
			return nil, true, err
		}
		if Terminal(run.Status) {
			resp.Body.Close()
			return run, true, nil
		}
		w.noteJobs(run)

		finished, got, err := w.readEvents(ctx, resp.Body, &last)
		resp.Body.Close()
		if err != nil {
			return nil, true, err
		}
		if finished {
			run, err := w.getRun(ctx)
			return run, true, err
		}
		if ctx.Err() != nil {
			return nil, true, ctx.Err()
		}
		// Stream dropped: reconnect with Last-Event-ID, unless it keeps
		// dropping without delivering anything; then poll instead.
		if got {
			empty = 0
		} else if empty++; empty >= 3 {
			return nil, false, nil
		}
		t := time.NewTimer(w.cfg.pollInitial)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, true, ctx.Err()
		case <-t.C:
		}
	}
}

// readEvents consumes one stream. finished is true when this run reached a
// terminal status (or a resync shows it has); got is true if any event
// arrived.
func (w *waiter) readEvents(ctx context.Context, body io.Reader, last **int64) (finished, got bool, err error) {
	sr := newSSEReader(body)
	for {
		ev, rerr := sr.Next()
		if rerr != nil {
			return false, got, nil // EOF or read error: reconnect
		}
		got = true
		if ev.ID != "" {
			if n, err := strconv.ParseInt(ev.ID, 10, 64); err == nil {
				*last = &n
			}
		}
		switch ev.Event {
		case "run":
			var re hourchdapi.RunEvent
			if json.Unmarshal([]byte(ev.Data), &re) != nil || re.Run.Id != w.id {
				continue
			}
			if Terminal(re.Run.Status) {
				return true, got, nil
			}
		case "job":
			var je hourchdapi.JobEvent
			if json.Unmarshal([]byte(ev.Data), &je) != nil || je.Job.RunId != w.id {
				continue
			}
			w.seen[je.Job.Id] = je.Job.Status
			if w.cfg.onJob != nil {
				w.cfg.onJob(je.Job)
			}
		case "resync":
			run, err := w.getRun(ctx)
			if err != nil {
				return false, got, err
			}
			if Terminal(run.Status) {
				return true, got, nil
			}
			w.noteJobs(run)
		}
	}
}

func (w *waiter) poll(ctx context.Context) (*hourchdapi.Run, error) {
	delay := w.cfg.pollInitial
	if delay <= 0 {
		delay = time.Second
	}
	for {
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
		run, err := w.getRun(ctx)
		if err != nil {
			var ae *APIError
			if errors.As(err, &ae) && ae.Status < 500 {
				return nil, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// Transient (network, 5xx): keep polling.
		} else {
			if Terminal(run.Status) {
				return run, nil
			}
			w.noteJobs(run)
		}
		delay *= 2
		if w.cfg.pollMax > 0 && delay > w.cfg.pollMax {
			delay = w.cfg.pollMax
		}
	}
}

func isEventStream(resp *http.Response) bool {
	if resp.StatusCode != http.StatusOK {
		return false
	}
	mt, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	return err == nil && mt == "text/event-stream"
}

// StreamLog copies one attempt's log of job id to w as it is written,
// following GET /v1/jobs/{id}/log/stream and resuming with Last-Event-ID
// after a drop. attempt 0 means the latest. It returns the `end` event when
// the attempt finishes.
func StreamLog(ctx context.Context, c *hourchdapi.Client, id string, attempt int, w io.Writer) (*hourchdapi.LogEndEvent, error) {
	params := &hourchdapi.StreamJobLogParams{}
	if attempt > 0 {
		params.Attempt = &attempt
	}
	failures := 0
	backoff := 500 * time.Millisecond
	for {
		resp, err := c.StreamJobLog(ctx, id, params)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			failures++
		} else if !isEventStream(resp) {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			return nil, newAPIError("streamJobLog", resp.StatusCode, body)
		} else {
			end, progressed, err := readLog(resp.Body, w, params)
			resp.Body.Close()
			if err != nil || end != nil {
				return end, err
			}
			if progressed {
				failures = 0
			} else {
				failures++
			}
		}
		if failures >= 5 {
			return nil, fmt.Errorf("stream log of job %s: stream keeps dropping", id)
		}
		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
	}
}

// readLog reads one log stream. It returns the end event, or nil with
// progressed telling whether any chunk arrived before the drop. A write
// error to w is returned.
func readLog(body io.Reader, w io.Writer, params *hourchdapi.StreamJobLogParams) (*hourchdapi.LogEndEvent, bool, error) {
	sr := newSSEReader(body)
	progressed := false
	for {
		ev, err := sr.Next()
		if err != nil {
			return nil, progressed, nil
		}
		switch ev.Event {
		case "log":
			var ch hourchdapi.LogChunkEvent
			if err := json.Unmarshal([]byte(ev.Data), &ch); err != nil {
				return nil, progressed, fmt.Errorf("log event: %w", err)
			}
			if _, err := io.WriteString(w, ch.Data); err != nil {
				return nil, progressed, err
			}
			off := ch.Offset
			params.LastEventID = &off
			progressed = true
		case "end":
			var end hourchdapi.LogEndEvent
			if err := json.Unmarshal([]byte(ev.Data), &end); err != nil {
				return nil, progressed, fmt.Errorf("end event: %w", err)
			}
			return &end, progressed, nil
		}
	}
}
