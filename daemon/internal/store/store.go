// Package store is hourchd's SQLite state: runs, jobs, secret-object
// ownership, outputs,
// artifacts, counters, builds, workers and events.
//
// One *sql.DB with a single connection (SetMaxOpenConns(1)), WAL,
// busy_timeout=5000, foreign keys on, and a schema version table with
// migrations applied at open — the same rules as powermaster and powerwarden.
// Multi-row state changes run inside Store.Tx.
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Run statuses.
const (
	RunRunning   = "running"
	RunSucceeded = "succeeded"
	RunFailed    = "failed"
	RunCancelled = "cancelled"
)

// Job statuses.
const (
	JobPending   = "pending"
	JobReady     = "ready"
	JobLeased    = "leased"
	JobSucceeded = "succeeded"
	JobFailed    = "failed"
	JobCancelled = "cancelled"
)

// TerminalJob reports whether a job status is final.
func TerminalJob(s string) bool { return s == JobSucceeded || s == JobFailed || s == JobCancelled }

// ErrNotFound is returned for a missing row.
var ErrNotFound = errors.New("not found")

const tsLayout = "2006-01-02T15:04:05.000000000Z"

// TS formats a time as stored (fixed width, so strings sort by time).
func TS(t time.Time) string { return t.UTC().Format(tsLayout) }

func tsPtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return TS(*t)
}

func parseTS(s string) time.Time {
	t, _ := time.Parse(tsLayout, s)
	return t
}

func parseTSPtr(ns sql.NullString) *time.Time {
	if !ns.Valid {
		return nil
	}
	t := parseTS(ns.String)
	return &t
}

// Schema notes (changes from docs/DESIGN-hourchd.md "Data model"):
//   - jobs.spec_argv_json / spec_env_json keep the submitted argv/env (with
//     placeholders) so a retry rebuilds the graph as submitted; argv_json /
//     env_json hold the substituted values once the job is ready.
//   - jobs.accept_by (assign + accept_timeout, NULL once accepted),
//     cancel_reason (cancel sent to a leased job), job_token_hash (sha256 of
//     HOURCH_JOB_TOKEN while leased), log_seq / log_size (last stored log seq
//     and byte length of the current attempt's log) persist dispatch state so
//     a restart loses nothing.
//   - jobs.lease_owner keeps the last holder after the job leaves `leased`
//     (the API's `worker` field); leased-ness is status = 'leased'.
//   - runs.secret_names_json keeps the names of the submitted secrets after
//     the secrets object is deleted (the Run view shows them).
//   - migration 2 replaces run_secrets (sealed values in SQLite) with
//     secret_objects: values live sealed in the object store under a
//     volatile key; SQLite records only which object hourchd owns for a run.
//   - builds.run_id / job_id carry no foreign key: build records outlive
//     pruned runs (build numbers are history).
var migrations = []string{
	`CREATE TABLE runs (
  id           TEXT PRIMARY KEY,
  workflow     TEXT NOT NULL,
  number       INTEGER NOT NULL,
  params_json  TEXT NOT NULL,
  status       TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
  submitted_by TEXT NOT NULL,
  context_ref  TEXT NOT NULL DEFAULT '',
  secret_names_json TEXT NOT NULL DEFAULT '[]',
  created_at   TEXT NOT NULL,
  finished_at  TEXT,
  UNIQUE (workflow, number)
);
CREATE INDEX runs_status ON runs (status);
CREATE INDEX runs_context ON runs (context_ref);

CREATE TABLE run_secrets (
  run_id     TEXT NOT NULL REFERENCES runs(id),
  name       TEXT NOT NULL,
  nonce      BLOB NOT NULL,
  ciphertext BLOB NOT NULL,
  PRIMARY KEY (run_id, name)
);

CREATE TABLE jobs (
  id                TEXT PRIMARY KEY,
  run_id            TEXT NOT NULL REFERENCES runs(id),
  name              TEXT NOT NULL,
  tags_json         TEXT NOT NULL,
  image             TEXT NOT NULL DEFAULT '',
  argv_json         TEXT NOT NULL,
  spec_argv_json    TEXT NOT NULL,
  workdir           TEXT NOT NULL DEFAULT '',
  env_json          TEXT NOT NULL DEFAULT '{}',
  spec_env_json     TEXT NOT NULL DEFAULT '{}',
  secret_names_json TEXT NOT NULL DEFAULT '[]',
  inputs_json       TEXT NOT NULL DEFAULT '[]',
  artifacts_json    TEXT NOT NULL DEFAULT '[]',
  depends_on_json   TEXT NOT NULL DEFAULT '[]',
  concurrency_key   TEXT NOT NULL DEFAULT '',
  status            TEXT NOT NULL CHECK (status IN ('pending', 'ready', 'leased', 'succeeded', 'failed', 'cancelled')),
  attempt           INTEGER NOT NULL DEFAULT 0,
  max_attempts      INTEGER NOT NULL DEFAULT 1,
  timeout_s         INTEGER NOT NULL,
  lease_owner       TEXT NOT NULL DEFAULT '',
  lease_until       TEXT,
  accept_by         TEXT,
  cancel_reason     TEXT NOT NULL DEFAULT '',
  job_token_hash    TEXT NOT NULL DEFAULT '',
  log_seq           INTEGER NOT NULL DEFAULT 0,
  log_size          INTEGER NOT NULL DEFAULT 0,
  exit_code         INTEGER,
  error             TEXT NOT NULL DEFAULT '',
  created_at        TEXT NOT NULL,
  ready_at          TEXT,
  started_at        TEXT,
  finished_at       TEXT,
  UNIQUE (run_id, name)
);
CREATE INDEX jobs_ready ON jobs (status, ready_at);
CREATE INDEX jobs_owner ON jobs (lease_owner, status);
CREATE INDEX jobs_token ON jobs (job_token_hash) WHERE job_token_hash != '';
CREATE INDEX jobs_concurrency ON jobs (concurrency_key, status) WHERE concurrency_key != '';

CREATE TABLE job_outputs (
  job_id TEXT NOT NULL REFERENCES jobs(id),
  key    TEXT NOT NULL,
  value  TEXT NOT NULL,
  PRIMARY KEY (job_id, key)
);

CREATE TABLE artifacts (
  job_id TEXT NOT NULL REFERENCES jobs(id),
  path   TEXT NOT NULL,
  object TEXT NOT NULL,
  size   INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  PRIMARY KEY (job_id, path)
);

CREATE TABLE counters (
  name  TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);

CREATE TABLE builds (
  build_type TEXT NOT NULL,
  flavour    TEXT NOT NULL,
  build_num  INTEGER NOT NULL,
  run_id     TEXT NOT NULL,
  job_id     TEXT NOT NULL,
  body_json  TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (build_type, flavour, build_num)
);

CREATE TABLE workers (
  id                TEXT PRIMARY KEY,
  hostname          TEXT NOT NULL,
  tags_json         TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,
  connected         INTEGER NOT NULL,
  current_job       TEXT NOT NULL DEFAULT '',
  last_seen         TEXT NOT NULL
);

CREATE TABLE events (
  id     INTEGER PRIMARY KEY,
  ts     TEXT NOT NULL,
  run_id TEXT, job_id TEXT, worker TEXT,
  kind   TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT ''
);
CREATE INDEX events_job ON events (job_id);
CREATE INDEX events_ts ON events (ts);`,
	// 2: secrets move to the object store (sealed under a key that dies with
	// the process). Rows written by an older binary are unreadable anyway.
	`DROP TABLE run_secrets;
CREATE TABLE secret_objects (
  run_id      TEXT PRIMARY KEY REFERENCES runs(id),
  object      TEXT NOT NULL,
  instance_id TEXT NOT NULL,
  created_at  TEXT NOT NULL
);`,
}

// Store wraps the database.
type Store struct {
	db *sql.DB
	Q
}

// Open opens (creating if needed) the database at path and migrates it.
func Open(path string) (*Store, error) {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_txlock", "immediate")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	s := &Store{db: db, Q: Q{x: db}}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the handle for tests and diagnostics.
func (s *Store) DB() *sql.DB { return s.db }

// Ping checks the database is usable.
func (s *Store) Ping() error { return s.db.Ping() }

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("schema_version: %w", err)
	}
	var v sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&v); err != nil {
		return err
	}
	for i := int(v.Int64); i < len(migrations); i++ {
		err := s.Tx(func(q Q) error {
			if _, err := q.x.Exec(migrations[i]); err != nil {
				return fmt.Errorf("migration %d: %w", i+1, err)
			}
			if _, err := q.x.Exec(`DELETE FROM schema_version`); err != nil {
				return err
			}
			_, err := q.x.Exec(`INSERT INTO schema_version (version) VALUES (?)`, i+1)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// SchemaVersion returns the applied migration count.
func (s *Store) SchemaVersion() (int, error) {
	var v sql.NullInt64
	err := s.db.QueryRow(`SELECT MAX(version) FROM schema_version`).Scan(&v)
	return int(v.Int64), err
}

// Tx runs fn in one immediate transaction; an error rolls back.
func (s *Store) Tx(fn func(q Q) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := fn(Q{x: tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// VacuumInto writes a consistent copy of the database to path.
func (s *Store) VacuumInto(path string) error {
	_, err := s.db.Exec(`VACUUM INTO ?`, path)
	return err
}

type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// Q runs queries against the database or inside a transaction.
type Q struct{ x execer }

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func fromJSON(s string, v any) {
	if s == "" {
		return
	}
	_ = json.Unmarshal([]byte(s), v)
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}

// ---- runs ----

// Run is a row of runs.
type Run struct {
	ID          string
	Workflow    string
	Number      int64
	Params      map[string]string
	Status      string
	SubmittedBy string
	ContextRef  string
	SecretNames []string
	CreatedAt   time.Time
	FinishedAt  *time.Time
}

const runCols = `id, workflow, number, params_json, status, submitted_by, context_ref, secret_names_json, created_at, finished_at`

func scanRun(sc interface{ Scan(...any) error }) (Run, error) {
	var r Run
	var params, secs, created string
	var fin sql.NullString
	if err := sc.Scan(&r.ID, &r.Workflow, &r.Number, &params, &r.Status, &r.SubmittedBy, &r.ContextRef, &secs, &created, &fin); err != nil {
		return Run{}, err
	}
	fromJSON(secs, &r.SecretNames)
	r.SecretNames = nonNil(r.SecretNames)
	r.Params = map[string]string{}
	fromJSON(params, &r.Params)
	r.CreatedAt = parseTS(created)
	r.FinishedAt = parseTSPtr(fin)
	return r, nil
}

// InsertRun stores r with the next number for its workflow (assigned here,
// inside the caller's transaction) and returns that number.
func (q Q) InsertRun(r Run) (int64, error) {
	var n int64
	if err := q.x.QueryRow(`SELECT COALESCE(MAX(number), 0) + 1 FROM runs WHERE workflow = ?`, r.Workflow).Scan(&n); err != nil {
		return 0, err
	}
	if r.Params == nil {
		r.Params = map[string]string{}
	}
	_, err := q.x.Exec(`INSERT INTO runs (`+runCols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ID, r.Workflow, n, mustJSON(r.Params), r.Status, r.SubmittedBy, r.ContextRef, mustJSON(nonNil(r.SecretNames)), TS(r.CreatedAt), tsPtr(r.FinishedAt))
	return n, err
}

// GetRun returns one run.
func (q Q) GetRun(id string) (Run, error) {
	r, err := scanRun(q.x.QueryRow(`SELECT `+runCols+` FROM runs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return r, err
}

// GetRunByNumber returns a run by workflow and number.
func (q Q) GetRunByNumber(workflow string, number int64) (Run, error) {
	r, err := scanRun(q.x.QueryRow(`SELECT `+runCols+` FROM runs WHERE workflow = ? AND number = ?`, workflow, number))
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, ErrNotFound
	}
	return r, err
}

// RunFilter selects runs for ListRuns.
type RunFilter struct {
	Workflow, Status, Before string
	Limit                    int
}

// ListRuns returns runs newest first.
func (q Q) ListRuns(f RunFilter) ([]Run, error) {
	var where []string
	var args []any
	if f.Workflow != "" {
		where, args = append(where, "workflow = ?"), append(args, f.Workflow)
	}
	if f.Status != "" {
		where, args = append(where, "status = ?"), append(args, f.Status)
	}
	if f.Before != "" {
		where, args = append(where, "id < ?"), append(args, f.Before)
	}
	sqlq := `SELECT ` + runCols + ` FROM runs`
	if len(where) > 0 {
		sqlq += " WHERE " + strings.Join(where, " AND ")
	}
	sqlq += " ORDER BY id DESC"
	if f.Limit > 0 {
		sqlq += fmt.Sprintf(" LIMIT %d", f.Limit)
	}
	rows, err := q.x.Query(sqlq, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		r, err := scanRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetRunStatus marks a run's status and finish time.
func (q Q) SetRunStatus(id, status string, finished *time.Time) error {
	_, err := q.x.Exec(`UPDATE runs SET status = ?, finished_at = ? WHERE id = ?`, status, tsPtr(finished), id)
	return err
}

// JobCounts counts a run's jobs by status.
func (q Q) JobCounts(runID string) (map[string]int, error) {
	rows, err := q.x.Query(`SELECT status, COUNT(*) FROM jobs WHERE run_id = ? GROUP BY status`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}

// CountRuns counts runs in a status.
func (q Q) CountRuns(status string) (int, error) {
	var n int
	err := q.x.QueryRow(`SELECT COUNT(*) FROM runs WHERE status = ?`, status).Scan(&n)
	return n, err
}

// ---- secret objects ----

// SecretObject is a row of secret_objects: hourchd owns object, which holds
// run_id's sealed secrets, written by instance_id.
type SecretObject struct {
	RunID      string
	Object     string
	InstanceID string
	CreatedAt  time.Time
}

// InsertSecretObject records a run's secrets object.
func (q Q) InsertSecretObject(o SecretObject) error {
	_, err := q.x.Exec(`INSERT INTO secret_objects (run_id, object, instance_id, created_at) VALUES (?, ?, ?, ?)`,
		o.RunID, o.Object, o.InstanceID, TS(o.CreatedAt))
	return err
}

// GetSecretObject returns a run's secrets object row, or ErrNotFound.
func (q Q) GetSecretObject(runID string) (SecretObject, error) {
	var o SecretObject
	var ts string
	err := q.x.QueryRow(`SELECT run_id, object, instance_id, created_at FROM secret_objects WHERE run_id = ?`, runID).
		Scan(&o.RunID, &o.Object, &o.InstanceID, &ts)
	if errors.Is(err, sql.ErrNoRows) {
		return SecretObject{}, ErrNotFound
	}
	o.CreatedAt = parseTS(ts)
	return o, err
}

// DeleteSecretObject removes a run's secrets object row (a missing row is
// not an error).
func (q Q) DeleteSecretObject(runID string) error {
	_, err := q.x.Exec(`DELETE FROM secret_objects WHERE run_id = ?`, runID)
	return err
}

// DeleteSecretObjectsNotOf removes every row written by an instance other
// than instance and returns how many it removed.
func (q Q) DeleteSecretObjectsNotOf(instance string) (int, error) {
	res, err := q.x.Exec(`DELETE FROM secret_objects WHERE instance_id != ?`, instance)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// CountSecretObjects is the number of runs that currently hold secrets.
func (q Q) CountSecretObjects() (int, error) {
	var n int
	err := q.x.QueryRow(`SELECT COUNT(*) FROM secret_objects`).Scan(&n)
	return n, err
}

// SecretObjectKeys returns the object key of every recorded secrets object.
func (q Q) SecretObjectKeys() (map[string]bool, error) {
	rows, err := q.x.Query(`SELECT object FROM secret_objects`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out[k] = true
	}
	return out, rows.Err()
}

// ---- jobs ----

// Input is an upstream artifact a job fetches.
type Input struct {
	Job  string `json:"job"`
	Path string `json:"path"`
	As   string `json:"as,omitempty"`
}

// Job is a row of jobs.
type Job struct {
	ID, RunID, Name string
	Tags            []string
	Image           string
	Argv            []string
	SpecArgv        []string
	Workdir         string
	Env             map[string]string
	SpecEnv         map[string]string
	SecretNames     []string
	Inputs          []Input
	Artifacts       []string
	DependsOn       []string
	ConcurrencyKey  string
	Status          string
	Attempt         int
	MaxAttempts     int
	TimeoutS        int
	LeaseOwner      string
	LeaseUntil      *time.Time
	AcceptBy        *time.Time
	CancelReason    string
	JobTokenHash    string
	LogSeq          int64
	LogSize         int64
	ExitCode        *int
	Error           string
	CreatedAt       time.Time
	ReadyAt         *time.Time
	StartedAt       *time.Time
	FinishedAt      *time.Time
}

const jobCols = `id, run_id, name, tags_json, image, argv_json, spec_argv_json, workdir, env_json, spec_env_json,
  secret_names_json, inputs_json, artifacts_json, depends_on_json, concurrency_key, status, attempt, max_attempts,
  timeout_s, lease_owner, lease_until, accept_by, cancel_reason, job_token_hash, log_seq, log_size, exit_code, error,
  created_at, ready_at, started_at, finished_at`

func scanJob(sc interface{ Scan(...any) error }) (Job, error) {
	var j Job
	var tags, argv, sargv, env, senv, secs, inputs, arts, deps, created string
	var lease, accept, ready, started, fin sql.NullString
	var exit sql.NullInt64
	err := sc.Scan(&j.ID, &j.RunID, &j.Name, &tags, &j.Image, &argv, &sargv, &j.Workdir, &env, &senv,
		&secs, &inputs, &arts, &deps, &j.ConcurrencyKey, &j.Status, &j.Attempt, &j.MaxAttempts,
		&j.TimeoutS, &j.LeaseOwner, &lease, &accept, &j.CancelReason, &j.JobTokenHash, &j.LogSeq, &j.LogSize, &exit, &j.Error,
		&created, &ready, &started, &fin)
	if err != nil {
		return Job{}, err
	}
	fromJSON(tags, &j.Tags)
	fromJSON(argv, &j.Argv)
	fromJSON(sargv, &j.SpecArgv)
	j.Env, j.SpecEnv = map[string]string{}, map[string]string{}
	fromJSON(env, &j.Env)
	fromJSON(senv, &j.SpecEnv)
	fromJSON(secs, &j.SecretNames)
	fromJSON(inputs, &j.Inputs)
	fromJSON(arts, &j.Artifacts)
	fromJSON(deps, &j.DependsOn)
	j.Tags, j.SecretNames, j.Inputs, j.Artifacts, j.DependsOn = nonNil(j.Tags), nonNil(j.SecretNames), nonNil(j.Inputs), nonNil(j.Artifacts), nonNil(j.DependsOn)
	j.LeaseUntil, j.AcceptBy = parseTSPtr(lease), parseTSPtr(accept)
	j.ReadyAt, j.StartedAt, j.FinishedAt = parseTSPtr(ready), parseTSPtr(started), parseTSPtr(fin)
	j.CreatedAt = parseTS(created)
	if exit.Valid {
		v := int(exit.Int64)
		j.ExitCode = &v
	}
	return j, nil
}

func intPtr(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

// InsertJob stores a new job.
func (q Q) InsertJob(j Job) error {
	if j.Env == nil {
		j.Env = map[string]string{}
	}
	if j.SpecEnv == nil {
		j.SpecEnv = map[string]string{}
	}
	_, err := q.x.Exec(`INSERT INTO jobs (`+jobCols+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, j.RunID, j.Name, mustJSON(nonNil(j.Tags)), j.Image, mustJSON(j.Argv), mustJSON(j.SpecArgv), j.Workdir, mustJSON(j.Env), mustJSON(j.SpecEnv),
		mustJSON(nonNil(j.SecretNames)), mustJSON(nonNil(j.Inputs)), mustJSON(nonNil(j.Artifacts)), mustJSON(nonNil(j.DependsOn)), j.ConcurrencyKey, j.Status, j.Attempt, j.MaxAttempts,
		j.TimeoutS, j.LeaseOwner, tsPtr(j.LeaseUntil), tsPtr(j.AcceptBy), j.CancelReason, j.JobTokenHash, j.LogSeq, j.LogSize, intPtr(j.ExitCode), j.Error,
		TS(j.CreatedAt), tsPtr(j.ReadyAt), tsPtr(j.StartedAt), tsPtr(j.FinishedAt))
	return err
}

// UpdateJob writes every mutable column of j.
func (q Q) UpdateJob(j Job) error {
	if j.Env == nil {
		j.Env = map[string]string{}
	}
	res, err := q.x.Exec(`UPDATE jobs SET argv_json = ?, env_json = ?, status = ?, attempt = ?, lease_owner = ?, lease_until = ?,
  accept_by = ?, cancel_reason = ?, job_token_hash = ?, log_seq = ?, log_size = ?, exit_code = ?, error = ?,
  ready_at = ?, started_at = ?, finished_at = ? WHERE id = ?`,
		mustJSON(j.Argv), mustJSON(j.Env), j.Status, j.Attempt, j.LeaseOwner, tsPtr(j.LeaseUntil),
		tsPtr(j.AcceptBy), j.CancelReason, j.JobTokenHash, j.LogSeq, j.LogSize, intPtr(j.ExitCode), j.Error,
		tsPtr(j.ReadyAt), tsPtr(j.StartedAt), tsPtr(j.FinishedAt), j.ID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrNotFound
	}
	return nil
}

// SetLease renews a job's lease if it is leased to owner.
func (q Q) SetLease(jobID, owner string, until time.Time) error {
	_, err := q.x.Exec(`UPDATE jobs SET lease_until = ? WHERE id = ? AND lease_owner = ? AND status = 'leased'`, TS(until), jobID, owner)
	return err
}

// SetLogPos records the last stored log seq and log size of a leased job.
func (q Q) SetLogPos(jobID string, seq, size int64) error {
	_, err := q.x.Exec(`UPDATE jobs SET log_seq = ?, log_size = ? WHERE id = ?`, seq, size, jobID)
	return err
}

// GetJob returns one job.
func (q Q) GetJob(id string) (Job, error) {
	j, err := scanJob(q.x.QueryRow(`SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

func (q Q) jobs(where string, args ...any) ([]Job, error) {
	rows, err := q.x.Query(`SELECT `+jobCols+` FROM jobs `+where, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// RunJobs returns a run's jobs in submission order.
func (q Q) RunJobs(runID string) ([]Job, error) {
	return q.jobs(`WHERE run_id = ? ORDER BY id`, runID)
}

// ReadyJobs returns ready jobs, oldest ready first.
func (q Q) ReadyJobs() ([]Job, error) {
	return q.jobs(`WHERE status = 'ready' ORDER BY ready_at, id`)
}

// UnleasedSecretJobs returns the pending and ready jobs that name a secret,
// in running runs, oldest first.
func (q Q) UnleasedSecretJobs() ([]Job, error) {
	return q.jobs(`WHERE status IN ('pending', 'ready') AND secret_names_json != '[]'
  AND run_id IN (SELECT id FROM runs WHERE status = 'running') ORDER BY id`)
}

// LeasedJobs returns every leased job.
func (q Q) LeasedJobs() ([]Job, error) {
	return q.jobs(`WHERE status = 'leased' ORDER BY id`)
}

// WorkerLeased returns the jobs leased to worker.
func (q Q) WorkerLeased(worker string) ([]Job, error) {
	return q.jobs(`WHERE lease_owner = ? AND status = 'leased' ORDER BY id`, worker)
}

// ConcurrencyHeld reports whether a leased job holds key.
func (q Q) ConcurrencyHeld(key string) (bool, error) {
	var n int
	err := q.x.QueryRow(`SELECT COUNT(*) FROM jobs WHERE concurrency_key = ? AND status = 'leased'`, key).Scan(&n)
	return n > 0, err
}

// JobByTokenHash returns the leased job holding a job token.
func (q Q) JobByTokenHash(hash string) (Job, error) {
	if hash == "" {
		return Job{}, ErrNotFound
	}
	j, err := scanJob(q.x.QueryRow(`SELECT `+jobCols+` FROM jobs WHERE job_token_hash = ? AND status = 'leased'`, hash))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

// JobStats are the queue numbers for /v1/stats.
type JobStats struct {
	Ready, Leased int
	OldestReady   *time.Time
}

// QueueStats counts ready and leased jobs.
func (q Q) QueueStats() (JobStats, error) {
	var st JobStats
	var oldest sql.NullString
	err := q.x.QueryRow(`SELECT
  (SELECT COUNT(*) FROM jobs WHERE status = 'ready'),
  (SELECT COUNT(*) FROM jobs WHERE status = 'leased'),
  (SELECT MIN(ready_at) FROM jobs WHERE status = 'ready')`).Scan(&st.Ready, &st.Leased, &oldest)
	st.OldestReady = parseTSPtr(oldest)
	return st, err
}

// ---- outputs ----

// Outputs returns a job's outputs.
func (q Q) Outputs(jobID string) (map[string]string, error) {
	rows, err := q.x.Query(`SELECT key, value FROM job_outputs WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, rows.Err()
}

// SetOutput stores (or replaces) one output.
func (q Q) SetOutput(jobID, key, value string) error {
	_, err := q.x.Exec(`INSERT INTO job_outputs (job_id, key, value) VALUES (?, ?, ?)
  ON CONFLICT (job_id, key) DO UPDATE SET value = excluded.value`, jobID, key, value)
	return err
}

// ClearOutputs removes a job's outputs (a new attempt starts clean).
func (q Q) ClearOutputs(jobID string) error {
	_, err := q.x.Exec(`DELETE FROM job_outputs WHERE job_id = ?`, jobID)
	return err
}

// ---- artifacts ----

// Artifact is a row of artifacts.
type Artifact struct {
	JobID, Path, Object string
	Size                int64
	Sha256              string
}

// PutArtifact stores (or replaces) an artifact row.
func (q Q) PutArtifact(a Artifact) error {
	_, err := q.x.Exec(`INSERT INTO artifacts (job_id, path, object, size, sha256) VALUES (?, ?, ?, ?, ?)
  ON CONFLICT (job_id, path) DO UPDATE SET object = excluded.object, size = excluded.size, sha256 = excluded.sha256`,
		a.JobID, a.Path, a.Object, a.Size, a.Sha256)
	return err
}

// DeleteArtifact removes one artifact row.
func (q Q) DeleteArtifact(jobID, path string) error {
	_, err := q.x.Exec(`DELETE FROM artifacts WHERE job_id = ? AND path = ?`, jobID, path)
	return err
}

// Artifacts returns a job's artifacts sorted by path.
func (q Q) Artifacts(jobID string) ([]Artifact, error) {
	rows, err := q.x.Query(`SELECT job_id, path, object, size, sha256 FROM artifacts WHERE job_id = ? ORDER BY path`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Artifact
	for rows.Next() {
		var a Artifact
		if err := rows.Scan(&a.JobID, &a.Path, &a.Object, &a.Size, &a.Sha256); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---- counters and builds ----

// NextCounter increments name (creating it at 0) and returns the new value.
func (q Q) NextCounter(name string) (int64, error) {
	var v int64
	err := q.x.QueryRow(`INSERT INTO counters (name, value) VALUES (?, 1)
  ON CONFLICT (name) DO UPDATE SET value = value + 1 RETURNING value`, name).Scan(&v)
	return v, err
}

// Build is a row of builds.
type Build struct {
	BuildType, Flavour string
	BuildNum           int64
	RunID, JobID       string
	Body               json.RawMessage
	CreatedAt          time.Time
}

// PutBuild stores or replaces a build.
func (q Q) PutBuild(b Build) error {
	_, err := q.x.Exec(`INSERT INTO builds (build_type, flavour, build_num, run_id, job_id, body_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT (build_type, flavour, build_num) DO UPDATE SET run_id = excluded.run_id, job_id = excluded.job_id,
  body_json = excluded.body_json, created_at = excluded.created_at`,
		b.BuildType, b.Flavour, b.BuildNum, b.RunID, b.JobID, string(b.Body), TS(b.CreatedAt))
	return err
}

// ListBuilds returns builds, newest build number first.
func (q Q) ListBuilds(buildType, flavour string, limit int) ([]Build, error) {
	var where []string
	var args []any
	if buildType != "" {
		where, args = append(where, "build_type = ?"), append(args, buildType)
	}
	if flavour != "" {
		where, args = append(where, "flavour = ?"), append(args, flavour)
	}
	sqlq := `SELECT build_type, flavour, build_num, run_id, job_id, body_json, created_at FROM builds`
	if len(where) > 0 {
		sqlq += " WHERE " + strings.Join(where, " AND ")
	}
	sqlq += fmt.Sprintf(" ORDER BY build_num DESC, build_type, flavour LIMIT %d", limit)
	rows, err := q.x.Query(sqlq, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Build
	for rows.Next() {
		var b Build
		var body, created string
		if err := rows.Scan(&b.BuildType, &b.Flavour, &b.BuildNum, &b.RunID, &b.JobID, &body, &created); err != nil {
			return nil, err
		}
		b.Body = json.RawMessage(body)
		b.CreatedAt = parseTS(created)
		out = append(out, b)
	}
	return out, rows.Err()
}

// ---- workers ----

// Worker is a row of workers.
type Worker struct {
	ID, Hostname string
	Tags         []string
	Capabilities []string
	Connected    bool
	CurrentJob   string
	LastSeen     time.Time
}

// UpsertWorker writes a worker row.
func (q Q) UpsertWorker(w Worker) error {
	conn := 0
	if w.Connected {
		conn = 1
	}
	_, err := q.x.Exec(`INSERT INTO workers (id, hostname, tags_json, capabilities_json, connected, current_job, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?)
  ON CONFLICT (id) DO UPDATE SET hostname = excluded.hostname, tags_json = excluded.tags_json,
  capabilities_json = excluded.capabilities_json, connected = excluded.connected, current_job = excluded.current_job,
  last_seen = excluded.last_seen`,
		w.ID, w.Hostname, mustJSON(nonNil(w.Tags)), mustJSON(nonNil(w.Capabilities)), conn, w.CurrentJob, TS(w.LastSeen))
	return err
}

// SetWorkerState updates connected, current job and last seen.
func (q Q) SetWorkerState(id string, connected bool, currentJob string, lastSeen time.Time) error {
	conn := 0
	if connected {
		conn = 1
	}
	_, err := q.x.Exec(`UPDATE workers SET connected = ?, current_job = ?, last_seen = ? WHERE id = ?`, conn, currentJob, TS(lastSeen), id)
	return err
}

// TouchWorker updates last_seen.
func (q Q) TouchWorker(id string, t time.Time) error {
	_, err := q.x.Exec(`UPDATE workers SET last_seen = ? WHERE id = ?`, TS(t), id)
	return err
}

// ClearWorkerJob clears a worker's current job if it is jobID.
func (q Q) ClearWorkerJob(id, jobID string) error {
	_, err := q.x.Exec(`UPDATE workers SET current_job = '' WHERE id = ? AND current_job = ?`, id, jobID)
	return err
}

// DisconnectAll marks every worker disconnected (at startup).
func (q Q) DisconnectAll() error {
	_, err := q.x.Exec(`UPDATE workers SET connected = 0`)
	return err
}

func scanWorker(sc interface{ Scan(...any) error }) (Worker, error) {
	var w Worker
	var tags, caps, seen string
	var conn int
	if err := sc.Scan(&w.ID, &w.Hostname, &tags, &caps, &conn, &w.CurrentJob, &seen); err != nil {
		return Worker{}, err
	}
	fromJSON(tags, &w.Tags)
	fromJSON(caps, &w.Capabilities)
	w.Tags, w.Capabilities = nonNil(w.Tags), nonNil(w.Capabilities)
	w.Connected = conn == 1
	w.LastSeen = parseTS(seen)
	return w, nil
}

// GetWorker returns one worker.
func (q Q) GetWorker(id string) (Worker, error) {
	w, err := scanWorker(q.x.QueryRow(`SELECT id, hostname, tags_json, capabilities_json, connected, current_job, last_seen FROM workers WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Worker{}, ErrNotFound
	}
	return w, err
}

// ListWorkers returns connected workers, then disconnected ones seen at or
// after since, each group by id.
func (q Q) ListWorkers(since time.Time) ([]Worker, error) {
	rows, err := q.x.Query(`SELECT id, hostname, tags_json, capabilities_json, connected, current_job, last_seen FROM workers
  WHERE connected = 1 OR last_seen >= ? ORDER BY connected DESC, id`, TS(since))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		w, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// ---- events ----

// Event is a row of events. Detail never carries secret values.
type Event struct {
	ID                   int64
	TS                   time.Time
	RunID, JobID, Worker string
	Kind                 string
	Detail               string
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// InsertEvent appends an event.
func (q Q) InsertEvent(e Event) error {
	_, err := q.x.Exec(`INSERT INTO events (ts, run_id, job_id, worker, kind, detail) VALUES (?, ?, ?, ?, ?, ?)`,
		TS(e.TS), nullStr(e.RunID), nullStr(e.JobID), nullStr(e.Worker), e.Kind, e.Detail)
	return err
}

// JobEvents returns a job's events in order.
func (q Q) JobEvents(jobID string) ([]Event, error) {
	rows, err := q.x.Query(`SELECT id, ts, COALESCE(run_id, ''), COALESCE(job_id, ''), COALESCE(worker, ''), kind, detail FROM events WHERE job_id = ? ORDER BY id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var ts string
		if err := rows.Scan(&e.ID, &ts, &e.RunID, &e.JobID, &e.Worker, &e.Kind, &e.Detail); err != nil {
			return nil, err
		}
		e.TS = parseTS(ts)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---- retention ----

// PruneRows deletes terminal runs created before cutoff with their jobs,
// outputs, artifact rows, secret-object rows and events; events older than cutoff; and
// disconnected workers not seen since cutoff. It returns the pruned run ids.
func (q Q) PruneRows(cutoff time.Time) ([]string, error) {
	c := TS(cutoff)
	rows, err := q.x.Query(`SELECT id FROM runs WHERE created_at < ? AND status != 'running'`, c)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		for _, stmt := range []string{
			`DELETE FROM job_outputs WHERE job_id IN (SELECT id FROM jobs WHERE run_id = ?)`,
			`DELETE FROM artifacts WHERE job_id IN (SELECT id FROM jobs WHERE run_id = ?)`,
			`DELETE FROM events WHERE run_id = ?`,
			`DELETE FROM secret_objects WHERE run_id = ?`,
			`DELETE FROM jobs WHERE run_id = ?`,
			`DELETE FROM runs WHERE id = ?`,
		} {
			if _, err := q.x.Exec(stmt, id); err != nil {
				return nil, err
			}
		}
	}
	if _, err := q.x.Exec(`DELETE FROM events WHERE ts < ?`, c); err != nil {
		return nil, err
	}
	if _, err := q.x.Exec(`DELETE FROM workers WHERE connected = 0 AND last_seen < ?`, c); err != nil {
		return nil, err
	}
	return ids, nil
}

// RunStatuses returns the status of every run in ids that still exists.
func (q Q) RunStatuses() (map[string]string, error) {
	rows, err := q.x.Query(`SELECT id, status FROM runs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var id, st string
		if err := rows.Scan(&id, &st); err != nil {
			return nil, err
		}
		out[id] = st
	}
	return out, rows.Err()
}

// ContextRefs returns every context_ref used by a stored run.
func (q Q) ContextRefs() (map[string]bool, error) {
	rows, err := q.x.Query(`SELECT DISTINCT context_ref FROM runs WHERE context_ref != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out[s] = true
	}
	return out, rows.Err()
}

// DeleteArtifactRows removes artifact rows of the given runs' jobs.
func (q Q) DeleteArtifactRows(runID string) error {
	_, err := q.x.Exec(`DELETE FROM artifacts WHERE job_id IN (SELECT id FROM jobs WHERE run_id = ?)`, runID)
	return err
}
