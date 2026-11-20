# hourchd: a daemon-owned job system

| | |
|---|---|
| Status | Implemented |
| Spec | `specs/openapi/hourchd.yml` (HTTP) and `specs/hourchd/worker-protocol.schema.json` (worker websocket) |
| Code | Go modules `github.com/resurgentech/hourch/<dir>` for `daemon`, `worker`, `flow` (workflow library), `api` and `protocol`; see [Code layout](#code-layout) |

## Overview

hourchd runs build and release workflows on a small fleet of worker machines.

One always-on daemon, **hourchd**, owns every run and job in a SQLite file and is the only process that talks to the object store (MinIO or any S3-compatible store). Workers hold one websocket to hourchd and are pushed work the moment it is ready. Submitters send secret values with the run; hourchd keeps them sealed in the object store under a key that lives only in its memory, and hands each job only the secrets it names. A workflow is a dependency graph of jobs built in Go code. Jobs may run in a container or directly on the worker. Operators use a web UI served by hourchd. Optionally, hourchd publishes demand to powerwarden, a separate power-management service, so powered-off workers can be woken when work arrives.

## Motivation

The design answers the failure modes of a common pattern: a job library in which every process (submitter, worker, monitor) reads and writes job state directly in a shared key-value store, job liveness is a key TTL that a worker keeps renewing, and idle workers find work by scanning the store every second. In that pattern:

- **Jobs vanish.** If a worker dies, renewal stops and the job's keys expire. A reader sees an empty record, not an error, and a parent waiting on the job waits forever.
- **Timeouts and retries are fields, not behaviour.** Nothing enforces them.
- **Parents hold workers.** A workflow implemented as a parent job that submits and polls children occupies a worker slot for its whole duration.
- **Hot-path load and latency.** Every idle worker scans the store each second; a claim takes up to a second plus the scan.
- **Secrets at rest and in the environment.** Job secrets are written in plaintext to shared storage, and the worker's own credentials leak into every job's environment.
- **Everyone holds storage credentials.** Submitters, workers and workflow binaries all connect to the store and the object store directly.

## Goals & Non-Goals

### Goals

- One process, hourchd, is the only reader and writer of job state and the only object-store client.
- A job is never lost silently. Every job ends `succeeded`, `failed`, or `cancelled` with a reason, or is visibly `pending`, `ready`, or `leased`.
- `timeout` and `max_attempts` are enforced.
- Push dispatch. A ready job is assigned to a connected, idle, matching worker in **< 1s** on a LAN.
- A dead worker's job is requeued or failed within one lease (default **45s**).
- Workers and submitters hold no object-store or secret-store credentials. Files move through hourchd.
- Secrets come from the submitter, are stored only encrypted (under a key that dies with the hourchd process), are deleted once the jobs that use them have succeeded, and reach a job only if it names them. Known values are redacted from logs.
- Workflows are dependency graphs built in Go. No YAML, no DSL.
- Jobs may run in a container (`image`) or directly on the worker.
- An hourchd restart does not kill running jobs.
- A web UI for runs, jobs, logs, artifacts, and workers.
- Every time, interval, and limit is in `config.yaml`, with defaults in `config.yaml.example`.
- Spec first: OpenAPI for HTTP, a JSON Schema for websocket messages, in `specs/`.
- A Dockerfile for the daemon and for container workers; deployment works under any orchestrator.

### Non-Goals

- High availability or multiple hourchd instances.
- Container isolation for every job. Containers are per job and optional.
- Power policy or sequencing (that is powerwarden's job; hourchd only reports demand).
- A general CI product: no git webhooks, no matrix syntax, no plugins.

## Key Decisions

1. **hourchd owns all job state in SQLite.** `modernc.org/sqlite` (no cgo), one `*sql.DB`, `SetMaxOpenConns(1)`, WAL, `busy_timeout`, `foreign_keys`, migrations at open.

2. **hourchd is the only object-store client.** Submitters upload the context tarball to hourchd. Workers download it from hourchd, upload artifacts to hourchd, and download upstream artifacts from hourchd. hourchd streams to and from the store without buffering whole files. Operators download artifacts and logs through hourchd.

3. **Push dispatch over one websocket per worker.** The worker dials hourchd and sends `hello` with its id, hostname, tags, capabilities, and any job it is still running. hourchd assigns a job when one becomes `ready` and a matching worker is idle, when a worker connects idle, or when a worker finishes. The socket carries assignment, heartbeat, log chunks, outputs, cancel, and result. Bulk data uses plain HTTP routes authenticated with the same worker identity.

4. **Leases, not TTLs.** Assignment sets `lease_until = now + lease`. Any worker message renews it. A worker that reconnects within the lease and reports the job keeps it. A reaper requeues (`attempt < max_attempts`) or fails a job whose lease lapses. A job past its timeout gets `cancel` and is marked `failed` with reason `timeout`.

5. **Secrets come from the submitter.** The workflow binary reads secret values on the submitting machine (from whatever secret store the operator uses) and sends them with the run. hourchd seals them (AES-256-GCM) into one object per run in its bucket, under a key generated at hourchd startup and held only in memory. A restart therefore loses every stored secret: hourchd deletes the old objects and fails pending jobs that need them. This is a deliberate trade of availability for confidentiality. hourchd deletes a run's object once every job that names a secret has succeeded; a failed or cancelled run keeps it so a retry can reuse it. A job names the secrets it needs; hourchd decrypts only those into the `assign` message. Values never reach SQLite, logs, or the events table, and reach the object store only sealed.

6. **Workers carry one credential: their hourchd identity.** A worker proves its identity with a per-worker token (a random secret per worker id, listed in hourchd's tokens file and kept on the worker in a 0600 file) sent on connect over TLS (`wss://`, typically terminated at a reverse proxy). The worker strips that credential and every `HOURCH_*` variable from the job environment, and passes only an allowlist of its own environment (`PATH`, `HOME`, `LANG`, `TERM`, `TMPDIR` by default) plus the job's `env`, its secrets and the job variables. Submitters and operators have their own named tokens. mTLS was considered and deferred.

7. **Workflows are dependency graphs built in Go.** The `hourchflow` package builds a run: jobs, tags, commands, references to upstream outputs and artifacts, secrets, concurrency keys, containers. Dependencies are inferred from references. Workflow binaries build the graph and submit it; the per-step logic stays in Go as subcommands of the same binary.

8. **Mutual exclusion is a scheduler feature.** A job may set a concurrency key (for example `publish`). At most one job per key is `leased` at a time, across all runs.

9. **Containers are optional per job.** A job with an `image` runs under `docker` on the worker, with the workdir mounted at `/work` and the job env and secrets passed through a 0600 `--env-file` deleted after start. A job without `image` runs as a process. A worker advertises whether it can run containers (`capabilities: [docker]`); a container job is only assigned to such a worker.

10. **hourchd can be the demand source for powerwarden.** With `powerwarden.url` set, it publishes demand and forwards worker liveness ([Powerwarden](#powerwarden)). Workers never call powerwarden. With `powerwarden.url: ""` (the default) the publisher is off.

11. **Web UI served by hourchd.** Go `html/template` plus a small script using `EventSource` for live updates and the log stream, assets embedded with `go:embed`. No JS build step.

12. **Build counters and build records live in hourchd.** Workflows that number their builds need an atomic counter and a place to keep each build's metadata. `POST /v1/counters/{name}/next` increments a named counter; `PUT /v1/builds/{type}/{flavour}/{num}` stores a build record whose body is your build metadata (any JSON object). Both are rows in hourchd's SQLite. A job calls them with `HOURCH_JOB_TOKEN`, a token hourchd puts in the job's environment at assignment that is valid only while that job is leased and only for these routes. `hourchflow`'s `JobClient.RecordBuild` combines the two: for build type `hello` and flavour `debian12_amd64` (`BuildFlavour("debian12","amd64","")`) it increments counter `build:hello:debian12_amd64`, lets the caller stamp the number into the metadata, and stores it. Runs also get a `number`, sequential per workflow, for display.

13. **JSON everywhere.** OpenAPI 3.0.3 for HTTP in `specs/openapi/hourchd.yml`; the websocket messages as JSON Schema in `specs/hourchd/`. Not gRPC.

## Design

### Scale and timing

Defaults; all in `config.yaml`.

- Design capacity **64 connected workers**, tens of runs per day, **10,000 job rows per month** before pruning.
- Dispatch **p99 < 1s** from `ready` to `assign` on a connected idle worker.
- `heartbeat` **10s**, `lease` **45s**, `accept_timeout` **10s**.
- Reconnect backoff 0.5s doubling to 10s with jitter (worker config).
- Log chunks up to **64 KiB** or **250ms**; live tail lag **< 1s**.
- `retention` **90 days** for rows, logs, and artifacts.
- Transfer: context tarballs tens of MB, artifacts up to a few hundred MB. hourchd streams; memory per transfer is one buffer.

### Components

```
 submitting machine                       always-on host                                 workers
 ──────────────────                       ──────────────                                 ───────
 workflow binary ── POST /v1/contexts ──▶ ┌──────────────── hourchd ────────────────┐
   (reads secrets)── POST /v1/runs ─────▶ │ HTTP API + web UI   SQLite (runs, jobs, │ ◀══ websocket ══▶ hourch-worker
 browser ────────── /ui (auth)       ───▶ │ dispatcher, reaper    outputs, workers, │ ◀── HTTP data ──▶  (process or
                                          │ log store, pruner     counters, events) │                     container jobs)
                                          └──────┬──────────────────────┬───────────┘
                                                 │ objects              │ demand, liveness (optional)
                                                 ▼                      ▼
                                          MinIO / S3 bucket        powerwarden
```

TLS is terminated in front of hourchd (a reverse proxy that passes websockets through); hourchd itself listens on plain HTTP (`listen: ":8093"`).

### Data model

The schema as migrated by `daemon/internal/store` (migration 1, then migration 2, which replaced an earlier `run_secrets` table with `secret_objects`). Indexes omitted.

```sql
CREATE TABLE runs (
  id                TEXT PRIMARY KEY,          -- ULID
  workflow          TEXT NOT NULL,             -- e.g. "hello"
  number            INTEGER NOT NULL,          -- MAX(number)+1 for the workflow, in the insert transaction
  params_json       TEXT NOT NULL,
  status            TEXT NOT NULL,             -- running | succeeded | failed | cancelled
  submitted_by      TEXT NOT NULL,             -- token name
  context_ref       TEXT NOT NULL DEFAULT '',  -- content address of the uploaded tarball
  secret_names_json TEXT NOT NULL DEFAULT '[]',-- names only; kept after the values are deleted
  created_at        TEXT NOT NULL,
  finished_at       TEXT,
  UNIQUE (workflow, number)
);

CREATE TABLE secret_objects (                -- which sealed object hourchd owns for a run; no values
  run_id      TEXT PRIMARY KEY REFERENCES runs(id),
  object      TEXT NOT NULL,                 -- secrets/<instance_id>/<run_id>
  instance_id TEXT NOT NULL,                 -- ULID of the hourchd process that sealed it
  created_at  TEXT NOT NULL
);

CREATE TABLE jobs (
  id                TEXT PRIMARY KEY,        -- ULID
  run_id            TEXT NOT NULL REFERENCES runs(id),
  name              TEXT NOT NULL,           -- unique within run, e.g. "build:hello"
  tags_json         TEXT NOT NULL,
  image             TEXT NOT NULL DEFAULT '',-- empty: run as a process
  argv_json         TEXT NOT NULL,           -- argv after ${outputs.<job>.<key>} substitution
  spec_argv_json    TEXT NOT NULL,           -- argv as submitted (placeholders kept, for retry)
  workdir           TEXT NOT NULL DEFAULT '',-- relative to the unpacked context
  env_json          TEXT NOT NULL DEFAULT '{}',
  spec_env_json     TEXT NOT NULL DEFAULT '{}',
  secret_names_json TEXT NOT NULL DEFAULT '[]',
  inputs_json       TEXT NOT NULL DEFAULT '[]', -- upstream artifacts to fetch: [{job, path, as}]
  artifacts_json    TEXT NOT NULL DEFAULT '[]', -- paths to upload after success
  depends_on_json   TEXT NOT NULL DEFAULT '[]',
  concurrency_key   TEXT NOT NULL DEFAULT '',
  status            TEXT NOT NULL,           -- pending | ready | leased | succeeded | failed | cancelled
  attempt           INTEGER NOT NULL DEFAULT 0,
  max_attempts      INTEGER NOT NULL DEFAULT 1,
  timeout_s         INTEGER NOT NULL,
  lease_owner       TEXT NOT NULL DEFAULT '',-- current or last worker
  lease_until       TEXT,
  accept_by         TEXT,                    -- assign + accept_timeout; NULL once accepted
  cancel_reason     TEXT NOT NULL DEFAULT '',
  job_token_hash    TEXT NOT NULL DEFAULT '',-- sha256 of HOURCH_JOB_TOKEN while leased
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

CREATE TABLE job_outputs (
  job_id TEXT NOT NULL REFERENCES jobs(id),
  key    TEXT NOT NULL,
  value  TEXT NOT NULL,                      -- capped by limits.output_value_bytes
  PRIMARY KEY (job_id, key)
);

CREATE TABLE artifacts (
  job_id TEXT NOT NULL REFERENCES jobs(id),
  path   TEXT NOT NULL,                      -- relative path as uploaded
  object TEXT NOT NULL,                      -- object key
  size   INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  PRIMARY KEY (job_id, path)
);

CREATE TABLE counters (
  name  TEXT PRIMARY KEY,                    -- e.g. "build:hello:debian12_amd64"
  value INTEGER NOT NULL
);

CREATE TABLE builds (                        -- no foreign keys: build records outlive pruned runs
  build_type TEXT NOT NULL,                  -- e.g. "hello"
  flavour    TEXT NOT NULL,                  -- distro_arch[_flavour], e.g. "debian12_amd64"
  build_num  INTEGER NOT NULL,
  run_id     TEXT NOT NULL,
  job_id     TEXT NOT NULL,
  body_json  TEXT NOT NULL,                  -- your build metadata (any JSON object)
  created_at TEXT NOT NULL,
  PRIMARY KEY (build_type, flavour, build_num)
);

CREATE TABLE workers (
  id                TEXT PRIMARY KEY,        -- worker id, e.g. "builder01", "tiny-1"
  hostname          TEXT NOT NULL,           -- must equal powermaster's name for the host when powerwarden is used
  tags_json         TEXT NOT NULL,
  capabilities_json TEXT NOT NULL,           -- e.g. ["docker"]
  connected         INTEGER NOT NULL,
  current_job       TEXT NOT NULL DEFAULT '',
  last_seen         TEXT NOT NULL
);

CREATE TABLE events (
  id     INTEGER PRIMARY KEY,
  ts     TEXT NOT NULL,
  run_id TEXT, job_id TEXT, worker TEXT,
  kind   TEXT NOT NULL,                      -- submitted, ready, assigned, renewed, requeued, timeout, cancelled, finished, ...
  detail TEXT NOT NULL DEFAULT ''
);
```

The live log for a leased job is an append-only file in hourchd's state directory. On finish it is uploaded to the object store and deleted locally. Object layout: `contexts/<sha256>.tar.zst` (content-addressed and shared across runs; pruned only when no run in retention references it), `artifacts/<run>/<job>/<path>`, `logs/<run>/<job>/<attempt>.log`, `secrets/<instance>/<run>`, `backups/hourchd/…`.

### Job lifecycle

```
pending ──all depends_on succeeded──▶ ready ──assign──▶ leased ──result exit 0──▶ succeeded
   │                                    ▲                 │ ├─exit≠0 / error──▶ attempt < max ? ready : failed
   └─a dependency failed/cancelled ──▶ cancelled          │ ├─lease lapsed──────▶ attempt < max ? ready : failed
                                        └─────────────────┘ └─past timeout ─────▶ cancel sent; failed ("timeout")
```

- A run is `succeeded` when every job is `succeeded`; `failed` when a job is `failed` and nothing else can run; `cancelled` on operator cancel. A run's secrets are deleted as soon as every job that names a secret has succeeded (so always on `succeeded`); a `failed` or `cancelled` run keeps them for a retry until hourchd restarts or the run is pruned.
- When a job becomes `ready`, `${outputs.<job>.<key>}` placeholders in its argv and env are substituted from `job_outputs`. A missing output fails the job with `missing_output`.
- `concurrency_key`: a `ready` job whose key is held by a `leased` job is not assignable until that job finishes.
- Assignment is one transaction: the oldest `ready` job whose tags are a subset of the worker's tags, whose image requirement the worker can meet, and whose concurrency key is free. Set `leased`, `lease_owner`, `lease_until`, `attempt++`, `started_at`; then write `assign`. If the write fails, the lease lapses and the reaper requeues. A worker never holds two jobs. A worker that does not `accept` within `accept_timeout` loses the job without using an attempt.
- Tag match is "worker tags ⊇ job tags" (the same rule powerwarden uses for host tags).
- **Satisfiability.** `POST /v1/runs` and retry reject with 422 `unsatisfiable` a run containing a job that no worker hourchd has ever seen (connected or not) can run (its tags, plus `docker` for an image), unless the job has no image and an unlocked, non-excluded powerwarden host has tags covering it. The message names, per job, what it needs and what the known workers and hosts offer, e.g. `job "build:hello" needs tags [kernel] + docker; known workers offer [tiny]`. `?validate_only=true` runs every submission check and stores nothing (`hourchflow`'s `Run.Check`, a workflow's dry run). At runtime the reaper fails with `no_worker` a `ready` job that no connected worker (idle or busy) has covered for `dispatch.unschedulable_timeout` (default 30m; `0s` disables), so a busy fleet never trips it.

### Workflows as Go graphs

`hourchflow` is a builder. It does no I/O until `Submit` (or `Check`). The main API, as in `flow/flow.go` and `flow/submit.go`:

```go
package hourchflow

func NewRun(workflow string, params map[string]string) *Run

// Context packs dir (honouring .gitignore and the excludes; .git always
// dropped) and uploads it through hourchd at Submit time. Every job starts
// in the unpacked tree. ContextInclude adds files the ignore rules would
// drop (for example the gitignored workflow binary itself).
func (r *Run) Context(dir string, excludes ...string)
func (r *Run) ContextInclude(paths ...string)

// Secret records a value for this run. Jobs reference it by name; only
// secrets some job names are sent.
func (r *Run) Secret(name, value string) Secret

func (r *Run) Job(name string, opts ...JobOpt) *Job

func (j *Job) Output(key string) Ref           // "${outputs.<job>.<key>}"; adds a dependency
func (j *Job) Artifact(path string) Artifact   // an upstream file or directory; adds a dependency
func (a Artifact) As(dest string) Artifact     // where the input lands (default inputs/<job>/<path>)

// Job options
func Tags(tags ...string) JobOpt
func Cmd(argv ...any) JobOpt                    // strings and Refs, never a shell string
func Dir(rel string) JobOpt                     // working directory inside the context
func Env(key string, v any) JobOpt              // string or Ref
func Secrets(s ...Secret) JobOpt                // exposed as env vars named after the secret
func Inputs(a ...Artifact) JobOpt               // fetched before the command
func Artifacts(paths ...string) JobOpt          // uploaded after success
func Image(ref string) JobOpt                   // run in a container
func Timeout(d time.Duration) JobOpt
func Attempts(n int) JobOpt
func Concurrency(key string) JobOpt
func After(jobs ...*Job) JobOpt                 // explicit ordering with no data dependency

func (r *Run) Validate() error                                          // local checks only
func (r *Run) Spec() (hourchdapi.RunSubmission, error)                  // the body Submit would POST
func (r *Run) Check(ctx context.Context, c *hourchdapi.Client) error    // server-side dry run
func (r *Run) Submit(ctx context.Context, c *hourchdapi.Client) (Submitted, error)
func Wait(ctx context.Context, c *hourchdapi.Client, id string, opts ...WaitOption) (*hourchdapi.Run, error)
```

A job publishes outputs by writing `key=value` lines to the file named by `$HOURCH_OUTPUTS` (`WriteOutput`); the worker sends them to hourchd on exit. `Submit` validates the graph (acyclic, names unique, every Ref and Artifact points at a job in this run, every secret named is set) before uploading anything. Inside a job, `NewJobClientFromEnv` gives a client for the counter and build routes (`RecordBuild`).

**Example: build → record → publish.** A workflow binary `workflows/hello/hello` builds the program in `apps/hello`, records the build, and publishes it. It submits the run and, as subcommands, does the work of each job:

```go
r := flow.NewRun("hello", map[string]string{"version": version})
r.Context(repoRoot, "apps/*/output")
r.ContextInclude("workflows/hello/hello")
token := r.Secret("PUBLISH_TOKEN", publishToken) // read on the submitting machine

settings := r.Job("settings",
    flow.Tags("tiny"),
    flow.Cmd("workflows/hello/hello", "settings", "--version", version),
    flow.Timeout(2*time.Minute),
)
build := r.Job("build:hello",
    flow.Tags("kernel"),
    flow.Dir("apps/hello"),
    flow.Cmd("just", "package", "--version", settings.Output("version")),
    flow.Artifacts("output/", "build.log", "build_metadata.json"),
    flow.Timeout(2*time.Hour),
)
record := r.Job("record:hello",
    flow.Tags("tiny"),
    flow.Cmd("workflows/hello/hello", "record", "--metadata", "inputs/build:hello/build_metadata.json"),
    flow.Inputs(build.Artifact("build_metadata.json")),
)
r.Job("publish:hello",
    flow.Tags("tiny"),
    flow.Concurrency("publish"),
    flow.Secrets(token),
    flow.Cmd("workflows/hello/hello", "publish", "--dir", "dist"),
    flow.Inputs(build.Artifact("output/").As("dist/")),
    flow.After(record),
)
sub, err := r.Submit(ctx, client)
```

`settings` writes `version=…` to `$HOURCH_OUTPUTS`; `build:hello` waits for it because its argv references that output. `record:hello` calls `RecordBuild(ctx, "hello", BuildFlavour("debian12","amd64",""), stamp)`, which takes the next value of counter `build:hello:debian12_amd64` and stores your build metadata (any JSON object) under that number. `publish:hello` runs after `record:hello` (ordering only), receives the build output in `dist/`, and holds the `publish` concurrency key so two runs never publish at once. The complete program, including waiting for the run, is in `flow/README.md`.

**Fan-out.** A graph is ordinary Go, so a loop builds one branch per item:

```go
for _, app := range apps { // e.g. "hello"
    build := r.Job("build:"+app,
        flow.Tags("kernel"),
        flow.Dir("apps/"+app),
        flow.Cmd("just", "package"),
        flow.Artifacts("output/"),
    )
    r.Job("publish:"+app,
        flow.Tags("tiny"),
        flow.Concurrency("publish"),
        flow.Cmd("workflows/hello/hello", "publish", "--dir", "inputs/build:"+app+"/output"),
        flow.Inputs(build.Artifact("output/")),
    )
}
```

**A container job.** A job that needs a toolchain the worker host lacks:

```go
r.Job("lint",
    flow.Image("registry.example.com/golangci-lint:1.61"),
    flow.Dir("apps/hello"),
    flow.Cmd("golangci-lint", "run", "./..."),
)
```

The worker runs `docker create --rm --env-file <0600 file> -v <root>:/work -w /work/apps/hello registry.example.com/golangci-lint:1.61 golangci-lint run ./...`, deletes the env file, then `docker start -a`. The job is assigned only to a worker with the `docker` capability.

### Worker protocol

`GET /v1/worker/connect` upgrades to a websocket (`github.com/coder/websocket`). Messages are JSON objects with `type` and `v` (schema version, currently 1).

| Direction | `type` | Fields | Meaning |
|---|---|---|---|
| W → D | `hello` | `worker_id`, `hostname`, `tags`, `capabilities`, `running_job_id?`, `version` | First message. `running_job_id` re-attaches a job after reconnect |
| D → W | `welcome` | `heartbeat_seconds`, `lease_seconds`, `abandon?`, `last_log_seq?` | Accepted. `abandon: <id>` if the reported job is no longer leased to this worker (the worker kills it); `last_log_seq` tells a re-attaching worker where to resume logs |
| D → W | `assign` | `job` (id, run_id, name, image, argv, workdir, env, artifacts, inputs, timeout_seconds), `secrets` (name → value), `attempt`, `job_token` | Run this job |
| W → D | `accept` | `job_id` | Started. No `accept` within `accept_timeout` requeues the job |
| W → D | `heartbeat` | `job_id?` | Renews the lease |
| W → D | `log` | `job_id`, `stream`, `seq`, `data` | Ordered by `seq`; deduplicated on reconnect |
| W → D | `output` | `job_id`, `key`, `value` | From `$HOURCH_OUTPUTS` |
| D → W | `cancel` | `job_id`, `reason` | Kill the process group or container, then send `result` |
| W → D | `result` | `job_id`, `exit_code`, `error?` | Job ended; worker is idle |

Bulk data uses HTTP with the worker's identity, scoped to the job it holds:

| Route | Use |
|---|---|
| `GET /v1/worker/jobs/{id}/context` | Stream the run's context tarball |
| `GET /v1/worker/jobs/{id}/inputs/{upstream_job}/{path}` | Stream an upstream artifact (a directory comes as a tarball) |
| `PUT /v1/worker/jobs/{id}/artifacts/{path}` | Stream one artifact up; `X-Sha256` header checked |

hourchd rejects these unless the job is `leased` to the caller. A newer connection for the same worker id closes the older one.

A job sees: the context unpacked in `work_dir/<job_id>-<attempt>`, its inputs under `inputs/<job>/<path>` (or `as`), cwd at the job's `workdir`, and the environment allowlist, the job's `env`, its secrets, and `HOURCH_URL`, `HOURCH_JOB_TOKEN`, `HOURCH_OUTPUTS`, `HOURCH_RUN_ID`, `HOURCH_JOB_ID`, `HOURCH_ATTEMPT`. Context and input entries that would resolve outside their destination fail the job before it runs.

### HTTP API (submitters, operators, web UI)

| Route | Purpose |
|---|---|
| `POST /v1/contexts` | Stream a context tarball; returns `context_ref` (content-addressed, deduplicated) |
| `POST /v1/runs` | Submit a run: workflow, params, `context_ref`, secrets (values), jobs. Validates the graph and satisfiability; `?validate_only=true` stores nothing |
| `GET /v1/runs?workflow=`, `GET /v1/runs/{id}`, `GET /v1/workflows/{workflow}/runs/{number}` | List and read runs with job states |
| `POST /v1/runs/{id}/cancel` | Cancel every non-terminal job |
| `POST /v1/runs/{id}/retry` | New run with the same graph and context; reuses the original's stored secrets while they exist (body values override), else they must be resent (422 `missing_secret`) |
| `GET /v1/jobs/{id}` | Job row, outputs, attempts from `events` |
| `GET /v1/jobs/{id}/log` | Full log (from the object store once finished) |
| `GET /v1/jobs/{id}/log/stream` | Live tail (SSE) |
| `GET /v1/jobs/{id}/artifacts`, `GET /v1/jobs/{id}/artifacts/{path}` | List and download |
| `POST /v1/jobs/{id}/cancel` | Cancel one job |
| `GET /v1/workers` | Connected and recent workers, current job |
| `POST /v1/counters/{name}/next` | Atomic increment; returns the new value. Job token or submitter token |
| `PUT /v1/builds/{type}/{flavour}/{num}`, `GET /v1/builds?type=&flavour=` | Store and list build records. PUT needs the job token |
| `GET /v1/events` | SSE for the UI and `Wait`: run, job, worker changes, with `Last-Event-ID` resume |
| `GET /v1/healthz`, `GET /v1/stats` | Liveness (object store and powerwarden state); ready/leased counts, oldest ready age, dispatch latency p50/p99, `unschedulable_ready`, `runs_holding_secrets` (what a restart would lose), and `personal_keys` (the key list's origin, entries and age) when `personal_keys.url` is set |
| `GET /v1/key-request` | Key-request discovery (no authentication): `200 {"request_url": ...}` when `personal_keys.request_url` is set, else `404` |

Every route outside `/v1/worker/` takes a submitter token (`Authorization: Bearer`), except the counter and build routes, which also take the job token, and `GET /v1/healthz` and `GET /v1/key-request`, which take none.

### Key-request discovery

hourchd can tell a tool that has no token where to ask for one. Keys are issued by a separate key provider, which takes a request from the tool, has a person approve it in a browser, and hands the tool its key once; hourchd only advertises the provider's address. The feature is optional and off by default.

- **Setting.** `personal_keys.request_url`: the absolute `http` or `https` URL (scheme compared case-insensitively) with a host, no user info and no fragment, where a key request for this service is filed (with `POST`). Default `""`. Anything else (a relative URL, another scheme, no host, user info, a fragment) fails startup and `check-config`, naming the key; a refused value with user info is not echoed. The value is stored and served exactly as written. When it is set, `check-config` prints ` key_request=on` after the rest of its line (`config ok key_request=on`, or `config ok ui=proxy key_request=on`); unset, its output is as before.
- **Route.** `GET /v1/key-request`, beside `GET /v1/healthz`, needs no authentication (a token, if sent, is ignored). With the setting it answers `200` with `{"request_url": "<the setting>"}`; without it, `404` with the usual `Error` body (`not_found`), which a client must not rely on. With the setting absent nothing else changes.
- **Client side.** A tool that already has a token never asks. A tool may be given a request URL directly (a flag or an environment variable), which replaces discovery. Discovery that fails any other way (a network error, a status other than `200` and `404`, a `request_url` that is not an absolute `http` or `https` URL) is an error the tool reports; it files nothing. With no request URL from either source, a missing token is the same error as before.
- **Contract.** The route and the `KeyRequestDiscovery` schema are in `specs/openapi/hourchd.yml`; the two exchanges are `specs/fixtures/keyrequest/discovery.json` and `discovery-none.json`. Filing, polling and approval are between the tool and the provider; hourchd takes no part in them.

### Key list

hourchd can also accept the personal keys of a key provider: it pulls a list of SHA-256 hashes of the keys to accept and checks a presented bearer token against it. The key list is optional and off by default; with `personal_keys.url` unset nothing changes. The wire contract (pull, `ETag`/`If-None-Match`, cache, refresh on miss) is the key provider contract at https://github.com/resurgentech/keyclient/blob/main/docs/CONTRACT.md, and its states are in `docs/STATES.md` of the same repository; this section covers only what is specific to hourchd.

- **Settings**, all under `personal_keys`. `url` (default `""`): the list URL; `""` turns the key list off. `token_file` (default `""`): the pull token, optional. `cache_file` (no default): where the last good list is kept; required when `url` is set. `refresh` (default `1m`): the time between pulls, greater than zero. `refresh_on_miss` (default `5s`): the least time between pulls that an unknown key starts; `0` turns refresh on miss off. `allow_insecure_http` (default `false`): lets `url` be plain `http` to a host that is not loopback (`localhost`, `127.0.0.0/8`, `::1`); it applies only to `url`, never to `request_url`, which is advertised and not fetched.
- **Validation.** A key counts as set when it is written in the file, whatever its value. A sub-key set while `url` is `""` fails with `personal_keys.<key> is set but personal_keys.url is empty`. A malformed `url` (not absolute `http(s)`, with user info, with a fragment) fails with `personal_keys.url "<redacted url>": want an absolute http(s)://host/... URL (or "" to turn the key list off)`. A missing cache file fails with `personal_keys.cache_file: required when personal_keys.url is set`; a bad duration with `personal_keys.refresh: must be greater than zero` or `personal_keys.refresh_on_miss: must not be negative`. Anything the key client refuses (a plain-http `url` without `allow_insecure_http`, an unreadable `token_file`) fails with `personal_keys: <its error>`, followed for the http rule by `; set personal_keys.allow_insecure_http: true only for a network path you trust`. hourchd reports the first error.
- **Roles.** Only the role `submitter` is accepted, and it maps to the submitter kind. An entry with another role is skipped. An entry whose name or SHA-256 matches any token of `tokens_file` (workers included) is skipped: the local token keeps its own role. The check is fixed at start. If this daemon ever reloads its tokens while running, it must build a new `Source`.
- **Bearer order.** (1) `tokens_file`. (2) A token with the `hjt_` prefix is a job token and is only ever looked up as one; it never reaches the list. (3) Otherwise, on a miss and only on a route a submitter may call, the key list (`LookupOrRefresh`, which may start one throttled pull); a hit is a submitter named after the entry. Every other case is `401`. The worker routes (`/v1/worker/...`) and `PUT /v1/builds/...` never consult the list, so a pulled key is never a worker or a job and an unknown token there never starts a pull. An empty bearer is never looked up.
- **Lifecycle.** The source is built after the config and tokens are loaded; a refusal stops the start before any state is opened. It is started, with the server's context, before the listener accepts requests: the cache is loaded first, so a usable cache serves before the first pull, and the daemon starts even when the provider is down. One log line follows, `personal_keys: list origin=<none|cache|provider> entries=<n> age=<duration>`; the key client logs pull failures and recoveries itself.
- **Health.** `GET /v1/healthz` is unchanged. `GET /v1/stats` gains the optional object `personal_keys` when `url` is set: `origin`, `entries`, `cache_failing`, `age_seconds` and `list_time` (absent when origin is `none`), `last_success` (absent until a good pull) and `last_error` (present while the latest pull failed; printable characters only, at most 200). It holds no key, hash, pull token or URL.
- **`check-config`** builds the source with the options `serve` uses, without starting it, so it sends nothing, and fails with the messages above. On success it adds ` key_list=on` after ` key_request=on` when `url` is set, and ` key_list_insecure_http=on` when `allow_insecure_http` is true and `url` is plain `http`. It never prints a token or a hash.

### Secrets

- **Key (volatile).** At startup hourchd generates 32 bytes from `crypto/rand` and an instance id (ULID). The key is held only in memory: never logged, written, or read from a file. There is no configuration for it; a leftover `secrets:` section (`key_file`) is a startup error.
- **Storage.** The submitter sends values in `POST /v1/runs`. If any job names a secret, hourchd seals the run's name → value map (AES-256-GCM, random nonce, additional data binding run id and instance id) into one object, `secrets/<instance_id>/<run_id>`, and records ownership in `secret_objects`. No value or ciphertext is in SQLite. Plaintext is held in memory only while building an `assign` message, re-sealing for a retry, and while redacting that job's log.
- A job receives only the secrets it names, as environment variables of the job process (or through a 0600 `--env-file` deleted after the container starts).
- Values are never written to SQLite (`events`, `jobs`, any table), to the object store except sealed, or to hourchd's own logs. `assign` is never logged in full.
- **Redaction.** hourchd and the worker both replace exact secret values (and the job token) in log chunks with `***`. A job that transforms a secret (base64, substrings) defeats this, and a value split across two chunks is not redacted (chunk cuts prefer newlines to make that rare).
- **Assign.** hourchd reads and opens the run's object and puts only the names the job lists into `assign`. A missing object, one written by another instance, or one that does not decrypt fails the job `secret_unavailable` (dependents `dependency_failed`). An unreachable object store leaves the job `ready` for the next dispatch.
- **Deletion.** hourchd knows which objects it owns and deletes them itself; there is no bucket lifecycle rule. A run's object is deleted as soon as every job that names a secret has succeeded (always when the run succeeds). A failed or cancelled run keeps it for a retry; run cancel does not delete it. The retention pruner deletes it with the run, and also any `secrets/` object with no `secret_objects` row older than an hour (a failed delete, a lost race).
- **Restart.** The key dies with the process. At startup hourchd deletes every `secrets/` object of another instance and its row, then fails every `pending`/`ready` job in a running run that names a secret with `secret_unavailable` (dependents cancelled), so the queue is honest at once. Leased jobs continue and re-attach (their workers hold the values; hourchd cannot redact a re-attached job's log any more and relies on the worker's own redaction). healthz, stats and the UI header show `runs_holding_secrets` so an operator can see what a restart would lose.
- **Retry.** `POST /v1/runs/{id}/retry` (and the UI Retry button) reuses the original run's stored secrets, re-sealed into the new run's object, with request values overriding. Once they are gone (restart, or every secret-using job succeeded) a needed value not in the request is 422 `missing_secret`; the UI then replaces Retry with that explanation (re-run the workflow binary, which sends the values again).

### Web UI

Served at `/ui` (`/` redirects to `/ui/runs`), or at `{ui.path_prefix}/ui` when `ui.path_prefix` is set (default `""`; e.g. `/p` serves `/p/ui/...`, and `/`, `/p` and `/p/` redirect to `/p/ui/runs`). The prefix is for a reverse proxy that shares a host between services and forwards the full path without stripping or rewriting it; every URL the pages, script and redirects emit carries it. It must start with `/`, must not end with `/`, and must not contain `..`, and each segment may hold only `A-Z a-z 0-9 . _ ~ -` (so `//`, `.`, spaces, `{`, `}`, `?`, `#` and `%` are refused). With no prefix the pages are exactly as before the setting existed; with one, `<body>` carries `data-base`, which the script reads. The `/v1` API is never prefixed. Authentication is `ui.auth`: `htpasswd` (the default) is basic auth from `ui.htpasswd_file` (bcrypt only); `proxy` trusts a reverse proxy that authenticates users itself (forward-auth style, e.g. oauth2-proxy). In proxy mode the proxy must strip any client-supplied identity headers, set the username header, and add a shared-secret header; hourchd trusts the username only when the secret matches. The contract:

- **Settings.** `ui.proxy.secret_file`, `ui.proxy.secret_header` and `ui.proxy.user_header` are all required in proxy mode, with no defaults (header names are settings, not conventions); the two header names must be valid HTTP header field names and must differ. Setting any of them while `ui.auth` is `htpasswd` (or unset) is a config error naming the key.
- **Secret check.** A request is accepted only when the secret header, present exactly once, equals a line of the secret file (SHA-256 of the value compared in constant time against every line, no early exit). The file holds one secret per line (surrounding whitespace trimmed; blank lines and lines starting with `#` are skipped), each at least 16 characters, at least one; it must have no group or other permission bits (`mode & 0077 == 0`, e.g. 0600, the tokens file's rule). Otherwise startup and `check-config` fail; in proxy mode `check-config` prints `config ok ui=proxy`. It is read once at startup. Rotate by adding the new line, restarting, switching the proxy to it, removing the old line, and restarting.
- **Username.** Taken from the user header, present exactly once, and must match `^[A-Za-z0-9_.@-]{1,64}$` (exported as `ui.ProxyUserRe`). Otherwise the request gets 401.
- **No other login.** Proxy mode has no basic-auth prompt and no htpasswd; a failed check is a plain 401 with no `WWW-Authenticate` header, and basic-auth credentials are ignored.
- **Default unchanged.** `ui.auth: htpasswd` is the default.
- **Caller.** The UI caller is `ui:<user>` in both modes (retry's `submitted_by`, log lines, and the in-process API calls). The secret and username headers are removed before the in-process API call, so the API never sees the shared secret. The `/v1` API knows nothing of proxy headers: a request to `/v1` needs a bearer token as ever.

Proxy mode is only as safe as the network path: hourchd must be reachable only through the proxy, or the proxy must be the only party that knows the secret. The POST actions (cancel run, retry run, cancel job) also need a same-origin `Origin` or `Referer`.

- **Runs.** List with status, number, workflow, submitter, age, duration, job counts; filter by workflow and status.
- **Run.** The job graph (SVG, boxes and arrows from dependencies, coloured by status), params, outputs, cancel and retry buttons.
- **Job.** Live log with follow, attempts, exit code, error, outputs, artifacts with download links, which worker ran it.
- **Workers.** Connected and recent workers, tags, capabilities, current job, last seen.
- **Header.** Ready count, oldest ready age, leased count, connected workers, runs holding secrets (what a restart would lose), powerwarden state.

No page shows a secret value: views carry secret names only, and logs and outputs are redacted when stored.

### Powerwarden

powerwarden is a separate service that powers hosts on and off according to demand. With `powerwarden.url` set, hourchd is a demand source for it, through powerwarden's generated client (`powerwardenapi`):

- A ready job that no connected idle worker covers and whose concurrency key is free is published as `PUT /v1/demands/hourchd:<job id>` (`source: hourchd`, the job's tags, `lease_seconds: 3600`). It is deleted when the job is assigned, cancelled, or finished.
- Every connected worker is published as `PUT /v1/workers/{id}` (`hostname` from the worker's `hello`, its tags, `idle`, or `busy` with `demand_id: hourchd:<job id>`), lease 6 × `refresh`. A worker that leaves idle is deleted; one that leaves holding a job is left to expire, so powerwarden keeps its host on.
- Every `powerwarden.refresh` (15s) hourchd re-PUTs all of that and deletes `source=hourchd` demands it no longer has (stale rows after a restart of either side).
- A worker's `hostname` must equal the name powerwarden knows the host by.
- Publishing never blocks dispatch: the engine only wakes the publisher, which sends diffs from its own goroutine, coalesces changes and retries (backoff up to 5s) while powerwarden is down. healthz and stats report `connected`, `unreachable` or `disabled`.

### What a crash does

| Failure | Effect |
|---|---|
| Worker process dies mid-job | Socket closes; lease runs out; reaper requeues (`attempt < max_attempts`) or fails with `lease_expired`. The service restart kills the old process group |
| Worker loses network briefly | Job keeps running; worker reconnects within the lease with `running_job_id`, keeps the lease, and resends log chunks after `welcome.last_log_seq` |
| hourchd restarts | Leased rows survive. Workers reconnect and re-attach; leases are extended on reconnect, not reaped by downtime. Live log files survive on disk. Uploads in progress fail and are retried by the worker. **Stored secrets are lost** (volatile key): every `pending`/`ready` job that names a secret fails `secret_unavailable` at startup and its dependents are cancelled; a retry needs the values resent. Check `runs_holding_secrets` before restarting |
| hourchd down for minutes | Running jobs continue and buffer logs on the worker (bounded by `log_buffer`, then the oldest chunks are dropped with a marker). No new dispatch |
| Object store down | Context and artifact transfers fail; jobs waiting on them stay `ready` or fail their upload and retry per `max_attempts` |
| SQLite file lost | History gone; in-flight jobs abandoned on reconnect. Restore from the periodic backup in the object store (secrets objects become unowned and the pruner deletes them) |

## Configuration

`/etc/hourchd/config.yaml` for the daemon and `/etc/hourch/worker.yaml` for workers, paths overridable with `--config`. `daemon/config.yaml.example` and `worker/worker.yaml.example` hold every key with its default (the per-host worker identity keys show illustrative values). Unknown keys are a startup error. `hourchd check-config --config FILE` validates the config, the tokens file, the powerwarden token (when enabled) and, when the UI is enabled, the htpasswd file (`ui.auth: htpasswd`) or the proxy secret file and its permissions (`ui.auth: proxy`).

Required, with no default: `minio.endpoint` (daemon); `hourchd_url` and `worker_id` (worker). `powerwarden.url` defaults to `""`, which disables the publisher. `personal_keys.request_url` defaults to `""`, which makes `GET /v1/key-request` answer `404`. `personal_keys.url` defaults to `""`, which turns the key list off; `personal_keys.cache_file` has no default and is required when `url` is set (see Key list).

```yaml
# hourchd (abridged; see daemon/config.yaml.example)
listen: ":8093"
state_dir: /var/lib/hourchd                  # state.db, live logs
tokens_file: /etc/hourchd/tokens.yaml        # submitters: {name: token}, workers: {worker id: token}; mode 0600
log_level: info

minio:
  endpoint: minio.example.com:9000           # REQUIRED (no default)
  bucket: hourch
  use_ssl: false
  access_key_env: HOURCHD_MINIO_ACCESS_KEY   # names of environment variables, not values
  secret_key_env: HOURCHD_MINIO_SECRET_KEY

dispatch:
  heartbeat: 10s
  lease: 45s
  accept_timeout: 10s
  reaper_interval: 5s
  unschedulable_timeout: 30m                 # 0s disables

logs:     {chunk_max_bytes: 65536, chunk_max_delay: 250ms}
limits:   {max_jobs_per_run: 200, output_value_bytes: 4096, context_max_bytes: 2GiB, artifact_max_bytes: 4GiB, default_timeout: 2h}
retention: {rows: 90d, logs: 90d, artifacts: 90d, prune_interval: 1h}
backup:   {interval: 24h, minio_prefix: backups/hourchd/}

powerwarden:
  url: ""                                    # e.g. https://powerwarden.example.com; "" disables
  token_file: /etc/hourchd/powerwarden.token # mode 0600
  refresh: 15s

events: {ring_size: 1000, heartbeat: 15s}
ui:     {enabled: true, auth: htpasswd, htpasswd_file: /etc/hourchd/ui.htpasswd, path_prefix: ""}  # "/p" serves /p/ui/...
# ui.proxy (auth: proxy only; all three required then, no defaults; an error under htpasswd):
#   {secret_file: "", secret_header: "", user_header: ""}
personal_keys:
  request_url: ""                            # advertised at GET /v1/key-request; "" answers 404
  url: ""                                    # the key list; "" turns it off. With it set, also (see Key list):
  # token_file: ""                           #   optional pull token
  # cache_file: /var/lib/hourchd/personal-keys.json   # required with url, no default
  # refresh: 1m                              #   must be > 0
  # refresh_on_miss: 5s                      #   0 turns refresh on miss off
  # allow_insecure_http: false               #   plain-http url to a non-loopback host
```

```yaml
# hourch worker (see worker/worker.yaml.example)
hourchd_url: wss://hourchd.example.com/v1/worker/connect   # REQUIRED (no default)
worker_id: builder01                         # REQUIRED; the name of this worker's token in hourchd's tokens_file
hostname: builder01.example.com              # default os.Hostname(); must match powerwarden's name for the host
tags: [kernel]
capabilities: [docker]                       # omit if the worker cannot run container jobs
token_file: /etc/hourch/worker.token         # mode 0600; the worker refuses a group/other-readable file
work_dir: /opt/hourch/work
env_allowlist: [PATH, HOME, LANG, TERM, TMPDIR]
reconnect: {initial: 500ms, max: 10s}
log_buffer: 64MiB
```

## Deployment

hourchd is one binary or container with a state volume; workers are one binary each, or a container. Run them under any orchestrator (compose, systemd, Kubernetes, ...).

### Images

`just image [tag]` in `daemon/` and `worker/` builds `hourchd` and `hourch-worker` from `cmd/<name>/Dockerfile`; the build context is the repository root. Two settings come from the environment or from an `images.env` file that just finds by searching upward from the leaf (`images.env.example` documents them):

| Setting | Effect |
|---|---|
| `IMAGE_REGISTRY` | Registry prefix: images are `$IMAGE_REGISTRY/<name>:<tag>`. Empty gives a local-only `<name>:<tag>`, and `just push` fails before building |
| `IMAGE_CA_CERT` | Absolute path to an extra CA certificate (PEM) to trust inside the image, e.g. a private CA that signed the object store's or hourchd's certificate. The recipe copies it into `cmd/<name>/extra-ca/` (an always-present, otherwise empty directory the Dockerfile copies into the trust store), and removes it when the build ends. Empty: system CAs only |

`just push [tag]` builds and then pushes. The daemon image is `debian:bookworm-slim` with `ca-certificates`, exposes 8093, declares `/var/lib/hourchd` a volume and runs `hourchd serve --config /etc/hourchd/config.yaml`. The worker image is `archlinux:base` with gnupg, openssh and ca-certificates; it has no docker CLI, so it does not serve container jobs. `just image-smoke` in `worker/` runs each tool in the built image and, when `IMAGE_CA_CERT` is set, checks the image trusts it.

### Running hourchd

1. Provide `config.yaml` (at least `minio.endpoint`), `tokens.yaml`, `powerwarden.token` (if the publisher is enabled) and `ui.htpasswd` (if the UI is enabled with `ui.auth: htpasswd`) or the proxy secret file (with `ui.auth: proxy`), all mode 0600, from your secret store; mount them read-only at the paths in `config.yaml.example`, and give `/var/lib/hourchd` a persistent volume.
2. Set the object-store credentials in the environment variables `minio.access_key_env` and `minio.secret_key_env` name (`HOURCHD_MINIO_ACCESS_KEY`, `HOURCHD_MINIO_SECRET_KEY`).
3. Put port 8093 behind a TLS reverse proxy (e.g. `hourchd.example.com`) that passes websockets through.
4. Run `hourchd check-config`, then `hourchd serve`. Before restarting a running hourchd, check that `runs_holding_secrets` in `/v1/healthz` is 0, or accept that those runs lose their secrets.
5. Check `/v1/healthz` (object store `connected`; powerwarden `connected` or `disabled`) and log in to `/ui`.

### Running workers

Each worker needs `worker.yaml`, its token file (mode 0600, the same token as its entry under `workers:` in hourchd's tokens file) and a `work_dir`. A worker whose jobs need the host's docker daemon and disk runs the binary directly, for example under systemd with `KillMode=mixed` so `systemctl stop` lets the worker drain its running job (a long `TimeoutStopSec` bounds that). The first SIGTERM drains (no new jobs; the running job finishes and its result is delivered); a second kills the job and exits, and hourchd's lease lapses. Advertise `capabilities: [docker]` only where the worker can run containers.

## Code layout

| Directory | Package | What |
|---|---|---|
| `api/` | `hourchdapi` | Client and types generated from `specs/openapi/hourchd.yml` (oapi-codegen; `just generate`, `just drift`) |
| `protocol/` | `hourchdprotocol` | Go types for the websocket messages; a test checks them against `specs/hourchd/worker-protocol.schema.json`. Imported by daemon and worker |
| `daemon/` | (binary `hourchd`) | The daemon: `cmd/hourchd`, `internal/…` (store, dispatch, wsserver, httpapi, secrets, objects, logs, events, maint, pwpub, ui, e2e), `config.yaml.example`, Dockerfile, justfile |
| `worker/` | `hourchworker` | Worker binary `hourch-worker`, `worker.yaml.example`, Dockerfile, justfile |
| `flow/` | `hourchflow` | Graph builder, `Submit`, `Check`, `Wait`, `StreamLog`, and the in-job helpers (`JobClient`, `WriteOutput`) |

Each leaf has `just test` and `just vet` (`api` has `test` and `drift`). The daemon's `just test` includes an in-process end-to-end test: the daemon, two real workers, a fake powerwarden and `hourchflow` runs covering context, inputs, artifacts, outputs, secrets, counters and builds, a concurrency key, a container job (fake `docker`), cancel, lease lapse and an hourchd restart.

## Alternatives Considered

### A. Off-the-shelf CI (Concourse, Woodpecker, Forgejo/GitHub Actions with self-hosted runners)

Rejected: it does not remove scheduler debugging, and power-aware scheduling stays ours either way.

### B. Fix the key-value-store design in place

Every fix is still a key-layout convention shared across processes, with TTL as the crash model and no transactions spanning job, worker, and concurrency state.

### C. HTTP long-poll claims

Rejected on responsiveness; cancel, log streaming, and lease renewal want a persistent connection anyway.

### D. A message bus (NATS, Redis Streams) for dispatch

A second stateful service whose delivery state must agree with SQLite.

### E. YAML workflow definitions

Rejected: YAML grows into a DSL. Graphs are Go code; step logic stays in Go.

### F. Presigned object-store URLs for workers

Workers would not hold storage credentials but would still need network access and URL handling for the store. Rejected in favour of hourchd moving all data, so workers and submitters only ever talk to hourchd.

### G. Secrets key from a file or a secret store

Considered and rejected: any persisted key makes a copy of the bucket plus the key a full disclosure. The volatile key means nothing at rest can be opened; the cost is that a restart loses stored secrets (see Risks).

## Security & Privacy Considerations

- Secret values exist sealed (AES-256-GCM) in hourchd's bucket until the jobs that use them have succeeded (or until a restart, prune, or the run's retry window closes); in hourchd memory while building an assignment, re-sealing for a retry, or redacting; on the wire in `assign` (TLS at the reverse proxy); and in the job's environment. The key exists only in hourchd's memory: a copy of the bucket, a SQLite backup, or both together reveal nothing, and no one else holds the key. The trade is availability: a restart loses every stored secret.
- Object-store credentials let someone delete or replace a secrets object (a denial of service: the job fails `secret_unavailable`), not read it; the additional data binds an object to its run and instance, so a swapped object does not open.
- Workers and submitters hold no object-store credentials. Submitters still need access to wherever the secret values come from.
- A job can read the worker's filesystem; jobs and workers share a trust domain. Container jobs narrow that only as far as the mounts allow, and a worker container given the docker socket is root-equivalent on its host.
- `HOURCH_JOB_TOKEN` is minted per assignment, stored only as a hash, accepted only for counter and build routes, and revoked when the job ends. It is not a worker credential.
- Tokens are compared in constant time against every entry. The UI's write actions (cancel, retry) require UI authentication (basic auth, or a proxy that presents the shared secret) and a same-origin request. In proxy mode the shared secret is compared in constant time against every line of its file, the file must have no group or other permission bits, and the secret never reaches the API or any log. A UI retry reuses the run's stored secrets server side (they never reach the browser); it is refused once they are gone.
- A job that prints its environment prints its secrets; the log shows them as `***` only if they appear verbatim.

## Observability

- Structured logs: assign, accept, requeue, timeout, cancel, lease lapse, reconnect, transfer start and end, with run, job, worker, attempt.
- The `events` table is the audit trail and the UI's activity feed.
- `GET /v1/stats`: ready, leased, connected workers by tag, oldest ready age, dispatch latency p50/p99 over the last hour, `unschedulable_ready`, `runs_holding_secrets`, and, with `personal_keys.url` set, `personal_keys` (origin, entries, age, last success and error).
- A warning is logged the first time the reaper finds a `ready` job that no connected worker covers.

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| hourchd is a single point of failure | Medium | Running jobs survive a restart. Periodic SQLite backup to the object store |
| hourchd becomes a bandwidth bottleneck for artifacts | Medium | Streaming, no full buffering; sized for artifacts of hundreds of MB, not GB |
| Reconnect races assign one job to two workers | High | Assignment is one transaction keyed on `lease_owner`; `abandon` on stale `hello`; newer connection closes older. Tested with a fake clock and fake sockets |
| Lease lapses during a blip and a publish runs twice | Medium | Publish steps are separate jobs with a concurrency key and should be idempotent |
| Secrets key lost (every hourchd restart, by design) | Medium | Accepted: pending secret-using jobs fail `secret_unavailable` at startup and their runs must be re-run or retried with the values. `runs_holding_secrets` in healthz, stats and the UI header shows what a restart would lose; check it before a planned restart |
| Docker socket in a container worker | Medium | Only container-capable workers get it; give it only to workers whose jobs are trusted |

## Open Questions

None.
