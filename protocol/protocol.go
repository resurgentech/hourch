// Package hourchdprotocol holds the Go types for the hourchd worker websocket
// protocol (GET /v1/worker/connect).
//
// Source of truth: specs/hourchd/worker-protocol.schema.json. Every message is
// one JSON object with "type" and "v" (Version). The structs here carry the
// other fields; Encode adds "type" and "v", Decode checks and strips them.
//
// Durations are integer seconds in fields named *_seconds.
package hourchdprotocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Version is the protocol version carried in every message's "v".
const Version = 1

// Type is a message's "type".
type Type string

// Message types, one per row of the protocol table.
const (
	TypeHello     Type = "hello"     // W→D
	TypeWelcome   Type = "welcome"   // D→W
	TypeAssign    Type = "assign"    // D→W
	TypeAccept    Type = "accept"    // W→D
	TypeHeartbeat Type = "heartbeat" // W→D
	TypeLog       Type = "log"       // W→D
	TypeOutput    Type = "output"    // W→D
	TypeCancel    Type = "cancel"    // D→W
	TypeResult    Type = "result"    // W→D
)

// Log streams.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// CapabilityDocker marks a worker that can run container jobs.
const CapabilityDocker = "docker"

// Message is implemented by every concrete message type.
type Message interface {
	MessageType() Type
	// Validate checks required fields and enums.
	Validate() error
}

// Hello is the worker's first message after connect.
type Hello struct {
	WorkerID     string   `json:"worker_id"`
	Hostname     string   `json:"hostname"`
	Tags         []string `json:"tags,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
	// RunningJobID re-attaches a job still running after a reconnect.
	RunningJobID string `json:"running_job_id,omitempty"`
	Version      string `json:"version"`
}

// Welcome accepts a hello.
type Welcome struct {
	HeartbeatSeconds int `json:"heartbeat_seconds"`
	LeaseSeconds     int `json:"lease_seconds"`
	// Abandon names the reported running job when it is no longer leased to
	// this worker; the worker kills it.
	Abandon string `json:"abandon,omitempty"`
	// LastLogSeq is the highest log seq stored for a re-attached job; the
	// worker resends chunks after it. Zero means none stored (or no job).
	LastLogSeq int64 `json:"last_log_seq,omitempty"`
}

// Input is an upstream artifact fetched before the command runs.
type Input struct {
	Job  string `json:"job"`
	Path string `json:"path"`
	// As is the destination relative to the work root; empty means
	// inputs/<job>/<path>.
	As string `json:"as,omitempty"`
}

// AssignedJob is the job as the worker runs it, placeholders substituted.
type AssignedJob struct {
	ID             string            `json:"id"`
	RunID          string            `json:"run_id"`
	Name           string            `json:"name"`
	Image          string            `json:"image,omitempty"`
	Argv           []string          `json:"argv"`
	Workdir        string            `json:"workdir,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	Artifacts      []string          `json:"artifacts,omitempty"`
	Inputs         []Input           `json:"inputs,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds"`
}

// Assign tells a worker to run a job. Never log it in full: it carries
// secret values and the job token.
type Assign struct {
	Job AssignedJob `json:"job"`
	// Secrets holds only the secrets the job names, name → value.
	Secrets map[string]string `json:"secrets,omitempty"`
	Attempt int               `json:"attempt"`
	// JobToken is set as HOURCH_JOB_TOKEN in the job's environment.
	JobToken string `json:"job_token"`
}

// Accept reports that the job started.
type Accept struct {
	JobID string `json:"job_id"`
}

// Heartbeat renews the lease; JobID is empty when idle.
type Heartbeat struct {
	JobID string `json:"job_id,omitempty"`
}

// Log is one chunk of job output. Seq starts at 1 per attempt.
type Log struct {
	JobID  string `json:"job_id"`
	Stream string `json:"stream"`
	Seq    int64  `json:"seq"`
	Data   string `json:"data"`
}

// Output is one key=value line from $HOURCH_OUTPUTS.
type Output struct {
	JobID string `json:"job_id"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Cancel tells the worker to kill the job and then send Result.
type Cancel struct {
	JobID  string `json:"job_id"`
	Reason string `json:"reason"`
}

// Result reports that the job ended; the worker is idle. ExitCode is -1 when
// the process never ran.
type Result struct {
	JobID    string `json:"job_id"`
	ExitCode int    `json:"exit_code"`
	Error    string `json:"error,omitempty"`
}

func (*Hello) MessageType() Type     { return TypeHello }
func (*Welcome) MessageType() Type   { return TypeWelcome }
func (*Assign) MessageType() Type    { return TypeAssign }
func (*Accept) MessageType() Type    { return TypeAccept }
func (*Heartbeat) MessageType() Type { return TypeHeartbeat }
func (*Log) MessageType() Type       { return TypeLog }
func (*Output) MessageType() Type    { return TypeOutput }
func (*Cancel) MessageType() Type    { return TypeCancel }
func (*Result) MessageType() Type    { return TypeResult }

// registry maps each Type to a constructor for its message.
var registry = map[Type]func() Message{
	TypeHello:     func() Message { return new(Hello) },
	TypeWelcome:   func() Message { return new(Welcome) },
	TypeAssign:    func() Message { return new(Assign) },
	TypeAccept:    func() Message { return new(Accept) },
	TypeHeartbeat: func() Message { return new(Heartbeat) },
	TypeLog:       func() Message { return new(Log) },
	TypeOutput:    func() Message { return new(Output) },
	TypeCancel:    func() Message { return new(Cancel) },
	TypeResult:    func() Message { return new(Result) },
}

// Types returns every message type.
func Types() []Type {
	out := make([]Type, 0, len(registry))
	for t := range registry {
		out = append(out, t)
	}
	return out
}

// New returns a zero message of type t, or false if t is unknown.
func New(t Type) (Message, bool) {
	f, ok := registry[t]
	if !ok {
		return nil, false
	}
	return f(), true
}

// Errors returned by Decode.
var (
	ErrUnknownType = errors.New("hourchdprotocol: unknown message type")
	ErrVersion     = errors.New("hourchdprotocol: unsupported protocol version")
)

// Envelope is the part of every message that identifies it.
type Envelope struct {
	Type Type `json:"type"`
	V    int  `json:"v"`
}

// Encode validates m and returns its JSON with "type" and "v" added.
func Encode(m Message) ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	head, err := json.Marshal(Envelope{Type: m.MessageType(), V: Version})
	if err != nil {
		return nil, err
	}
	// head is {"type":..,"v":1}; body is an object. Splice them.
	head = head[:len(head)-1]
	if string(body) == "{}" {
		return append(head, '}'), nil
	}
	return append(append(head, ','), body[1:]...), nil
}

// Decode reads one message. It fails on an unknown type, a "v" other than
// Version, unknown fields, or a message that does not validate.
func Decode(data []byte) (Message, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("hourchdprotocol: %w", err)
	}
	var env Envelope
	if raw, ok := fields["type"]; !ok {
		return nil, fmt.Errorf("hourchdprotocol: missing type")
	} else if err := json.Unmarshal(raw, &env.Type); err != nil {
		return nil, fmt.Errorf("hourchdprotocol: type: %w", err)
	}
	if raw, ok := fields["v"]; !ok {
		return nil, fmt.Errorf("%w: missing v", ErrVersion)
	} else if err := json.Unmarshal(raw, &env.V); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrVersion, raw)
	}
	if env.V != Version {
		return nil, fmt.Errorf("%w: %d", ErrVersion, env.V)
	}
	m, ok := New(env.Type)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownType, env.Type)
	}
	delete(fields, "type")
	delete(fields, "v")
	rest, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(rest))
	dec.DisallowUnknownFields()
	if err := dec.Decode(m); err != nil {
		return nil, fmt.Errorf("hourchdprotocol: %s: %w", env.Type, err)
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func required(t Type, pairs ...string) error {
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] == "" {
			return fmt.Errorf("hourchdprotocol: %s: %s is required", t, pairs[i])
		}
	}
	return nil
}

func positive(t Type, name string, v int64) error {
	if v < 1 {
		return fmt.Errorf("hourchdprotocol: %s: %s must be >= 1", t, name)
	}
	return nil
}

func (m *Hello) Validate() error {
	for _, c := range m.Capabilities {
		if c != CapabilityDocker {
			return fmt.Errorf("hourchdprotocol: hello: unknown capability %q", c)
		}
	}
	return required(TypeHello, "worker_id", m.WorkerID, "hostname", m.Hostname, "version", m.Version)
}

func (m *Welcome) Validate() error {
	if err := positive(TypeWelcome, "heartbeat_seconds", int64(m.HeartbeatSeconds)); err != nil {
		return err
	}
	if m.LastLogSeq < 0 {
		return fmt.Errorf("hourchdprotocol: welcome: last_log_seq must be >= 0")
	}
	return positive(TypeWelcome, "lease_seconds", int64(m.LeaseSeconds))
}

func (m *Assign) Validate() error {
	j := m.Job
	if err := required(TypeAssign, "job.id", j.ID, "job.run_id", j.RunID, "job.name", j.Name, "job_token", m.JobToken); err != nil {
		return err
	}
	if len(j.Argv) == 0 {
		return fmt.Errorf("hourchdprotocol: assign: job.argv is required")
	}
	for _, in := range j.Inputs {
		if err := required(TypeAssign, "job.inputs[].job", in.Job, "job.inputs[].path", in.Path); err != nil {
			return err
		}
	}
	if err := positive(TypeAssign, "job.timeout_seconds", int64(j.TimeoutSeconds)); err != nil {
		return err
	}
	return positive(TypeAssign, "attempt", int64(m.Attempt))
}

func (m *Accept) Validate() error { return required(TypeAccept, "job_id", m.JobID) }

func (m *Heartbeat) Validate() error { return nil }

func (m *Log) Validate() error {
	if m.Stream != StreamStdout && m.Stream != StreamStderr {
		return fmt.Errorf("hourchdprotocol: log: stream must be stdout or stderr, got %q", m.Stream)
	}
	if err := positive(TypeLog, "seq", m.Seq); err != nil {
		return err
	}
	return required(TypeLog, "job_id", m.JobID)
}

func (m *Output) Validate() error {
	return required(TypeOutput, "job_id", m.JobID, "key", m.Key)
}

func (m *Cancel) Validate() error {
	return required(TypeCancel, "job_id", m.JobID, "reason", m.Reason)
}

func (m *Result) Validate() error { return required(TypeResult, "job_id", m.JobID) }
