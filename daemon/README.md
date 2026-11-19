# hourchd daemon

The hourch job daemon: owns every run and job in SQLite, pushes jobs to
workers over one websocket each, and is the only MinIO client. Design:
[`docs/DESIGN-hourchd.md`](../docs/DESIGN-hourchd.md).
Contracts: [`specs/openapi/hourchd.yml`](../specs/openapi/hourchd.yml) (HTTP, types in
the `api` module, package `hourchdapi`) and
[`specs/hourchd/worker-protocol.schema.json`](../specs/hourchd/worker-protocol.schema.json)
(websocket, types in the `protocol` module, package `hourchdprotocol`).

It also serves the operator web UI at `/ui` and publishes demand and worker
liveness to powerwarden.

## Layout

| Path | What |
|---|---|
| `cmd/hourchd` | `serve` and `check-config`; Dockerfile |
| `internal/config` | `config.yaml` parsing; defaults equal `config.yaml.example` |
| `internal/store` | SQLite schema, migrations and every query |
| `internal/graph` | Submission validation, effective `depends_on`, `${outputs.<job>.<key>}` substitution |
| `internal/dispatch` | The engine: submission, lifecycle, one-transaction assignment, leases, reaper, timeouts, cancel, job tokens, live-log upload |
| `internal/wsserver` | `GET /v1/worker/connect` (coder/websocket) |
| `internal/httpapi` | Every route in the spec; tokens file |
| `internal/secrets` | Volatile-key AES-256-GCM sealing of run-secrets objects; log redaction |
| `internal/objects` | Object store interface; MinIO and in-memory implementations |
| `internal/logs` | Live log files per job attempt and tail subscriptions |
| `internal/events` | SSE hub (seq ring, `Last-Event-ID`, resync) |
| `internal/maint` | Retention pruning and the SQLite backup |
| `internal/pwpub` | Powerwarden publisher: demands and worker liveness (`powerwardenapi` client) |
| `internal/pwfake` | In-memory powerwarden for tests |
| `internal/ui` | Web UI at `/ui` (html/template, embedded assets, htpasswd) |
| `internal/app` | Wiring |
| `internal/e2e` | End-to-end test: daemon, two real workers, fake powerwarden, `flow` runs |

## Configuration

`/etc/hourchd/config.yaml`; every key with its default is in
[`config.yaml.example`](config.yaml.example). Unknown keys are an error.
`minio.endpoint` has no default and is required; the example's host is only
an illustration. `powerwarden.url` defaults to `""` (publisher disabled).
`hourchd check-config --config FILE` validates the config, the tokens file,
the powerwarden token (when enabled) and the UI htpasswd file or proxy secret file (when the UI is enabled).

`tokens_file` (mode 0600):

```yaml
submitters:            # submitter/operator tokens: every route outside /v1/worker/
  alice: <token>
workers:               # one per worker id: only /v1/worker/ routes
  builder01: <token>
  tiny-1: <token>
```

Run secrets have no configuration. hourchd generates a 32-byte key and an
instance id (ULID) at startup and keeps the key only in memory. A run's
values are sealed (AES-256-GCM, bound to the run and instance id) into one
object, `secrets/<instance>/<run>` in the bucket; SQLite records only which
object hourchd owns (`secret_objects`). hourchd deletes the object as soon
as every job naming a secret has succeeded; a failed or cancelled run keeps
it so a retry (API or UI) reuses it. The pruner deletes it with the run, and
any `secrets/` object hourchd does not own. There is no bucket lifecycle rule.

**A restart loses every stored secret.** At startup hourchd deletes the
earlier instance's objects and fails every pending or ready job that names a
secret with `secret_unavailable` (dependents cancelled); leased jobs keep
running. healthz, stats (`runs_holding_secrets`) and the UI header
("holding secrets") show how many runs a restart would hit. A leftover
`secrets:` section (`key_file`) is a startup error.

MinIO credentials come from the environment variables named in `minio:`
(`HOURCHD_MINIO_ACCESS_KEY`, `HOURCHD_MINIO_SECRET_KEY`).

`powerwarden.token_file` (mode 0600): the bearer token powerwarden's own
tokens file names `hourchd`. `powerwarden.url: ""` disables the publisher.

`ui.auth` is `htpasswd` (default) or `proxy`. `ui.htpasswd_file` (only for
`htpasswd`): `htpasswd -B -c ui.htpasswd alice` (bcrypt only; other
hash types are a startup error). With `ui.auth: proxy` a reverse proxy that
authenticates users (e.g. oauth2-proxy) names the user: set all of
`ui.proxy.secret_file` (mode 0600 with no group or other bits; one shared
secret per line, each at least 16 characters; blank lines and `#` lines
skipped), `ui.proxy.secret_header` and `ui.proxy.user_header` (no defaults;
setting any of them while `ui.auth` is `htpasswd` is an error); the proxy
must strip client-supplied copies of both headers and set them itself. See
the DESIGN doc, "Web UI". In proxy mode `check-config` prints
`config ok ui=proxy`. `ui.enabled: false` turns the UI off.
`ui.path_prefix` (default `""`) moves the UI to `{prefix}/ui` for a reverse
proxy that keeps the prefix in the path (`/p` serves `/p/ui/...`; it must start
with `/`, not end with `/`, not contain `..`, and each segment may hold only
`A-Z a-z 0-9 . _ ~ -`). The `/v1` API is not prefixed.

`personal_keys.request_url` (default `""`) is optional: the absolute `http(s)`
URL of an external key provider where a tool without a token files a key
request. hourchd advertises it, without authentication, at
`GET /v1/key-request` as `{"request_url": "..."}`; unset, that route answers
404 and nothing else changes. See the DESIGN doc, "Key-request discovery".

## Web UI

`https://hourchd.example.com/` redirects to `/ui/runs` (or to
`{ui.path_prefix}/ui/runs`). Every `/ui` route needs
basic auth from `ui.htpasswd_file` (or, with `ui.auth: proxy`, the proxy headers); the POST actions (cancel run, retry run,
cancel job) also need a same-origin `Origin` or `Referer` (CSRF guard).

- **Runs**: newest first, filter by workflow and status; status, number,
  submitter, age, duration, job counts.
- **Run**: job graph (SVG, coloured by status), params, jobs, outputs,
  cancel; retry for a finished run that used no secrets or still holds its
  stored secrets (the retry reuses them). Once they are gone (restart, or
  every secret-using job succeeded) the button is replaced by an
  explanation: re-run the workflow binary, which sends the values again.
- **Job**: details, attempts, outputs, artifacts (downloads stream through
  hourchd), live log (`EventSource` on `/ui/jobs/{id}/log/stream`, with
  follow).
- **Workers**: connected and recent workers, current job.
- **Header**: ready count, oldest ready age, leased count, connected workers,
  runs holding secrets (what a restart would lose), powerwarden state.

Live updates use `/ui/events` (the `/v1/events` hub). No page shows a secret
value: views carry secret names only, logs and outputs are redacted when
stored.

## Powerwarden

With `powerwarden.url` set, hourchd is powerwarden's demand source
(`docs/DESIGN-powerwarden.md`):

- A ready job that no connected idle worker covers (tags ⊆ worker tags,
  `docker` capability for container jobs) and whose concurrency key is free
  is `PUT /v1/demands/hourchd:<job id>` (`source: hourchd`, the job's tags,
  `lease_seconds: 3600`). It is deleted when the job is assigned,
  cancelled or finished.
- Every connected worker is `PUT /v1/workers/<id>` (`hostname` from the
  worker's `hello`, its tags, `idle`, or `busy` with `demand_id:
  hourchd:<job id>`), lease 6 × `refresh` (90s at 15s). A worker that leaves
  idle is deleted; one that leaves holding a job is left to expire, so
  powerwarden keeps its host busy-held.
- Every `refresh` it re-PUTs all of that and deletes `source=hourchd`
  demands hourchd no longer has (stale rows after a restart of either side).
- Publishing never blocks dispatch: the engine only wakes the publisher,
  which sends diffs from its own goroutine, coalesces changes and retries
  (backoff up to 5s) while powerwarden is down. healthz and stats report
  `connected`, `unreachable` or `disabled`; the UI header shows the last
  error.

## Unsatisfiable and unschedulable jobs

- **At submission** (`POST /v1/runs`, retry, and `?validate_only=true`): a
  job needs a worker whose tags include its tags, plus `docker` for an
  `image` (the dispatch rule). If no worker hourchd has ever seen (the
  `workers` table, connected or not, until `retention.rows` prunes it)
  offers that, and, for a job without an image, no powerwarden host that is
  neither locked nor excluded has non-empty tags covering the job's, the
  submission is 422 `unsatisfiable`, one line per job:
  `job "build:hello" needs tags [kernel] + docker; known workers offer
  [tiny]; powerwarden hosts offer [kernel] (no docker known)`. A host never
  seen as a worker has unknown capabilities, so hosts never satisfy an image
  job. hourchd asks powerwarden (`GET /v1/hosts`, cached 15s, 3s timeout)
  only when the workers do not cover a job; when powerwarden is disabled or
  unreachable only workers count and the message says powered-off hosts
  could not be checked.
- **`validate_only=true`** runs every check (graph, secrets present,
  satisfiability, `context_ref` only when given) and stores nothing; 200
  `{"valid": true}` or the same 4xx. flow's `Run.Check` and the workflows'
  `--dry-run` use it.
- **At runtime** the reaper fails with `no_worker` (dependents
  `dependency_failed`) a `ready` job that no *connected* worker, idle or
  busy, has covered for `dispatch.unschedulable_timeout` (default 30m, `0s`
  disables), measured from when it became ready, when it was last covered or
  when hourchd started, whichever is latest. A busy fleet never trips it.
  The first tick that finds a job uncovered logs a warning;
  `/v1/stats.unschedulable_ready` counts such jobs now.

## Deploy

hourchd is one container (or one binary) with a state volume; run it under
any orchestrator (compose, systemd, Kubernetes, ...). Your deployment
tooling should:

1. Build and publish the image: set `IMAGE_REGISTRY` (and `IMAGE_CA_CERT` if
   hourchd must trust a private CA, e.g. for MinIO or powerwarden) in the
   environment or in an `images.env` file that just finds by searching upward
   from this directory (see `images.env.example`), then `just push`, which
   builds `$IMAGE_REGISTRY/hourchd:latest` first. See [Recipes](#recipes).
2. Provide `config.yaml` (from `config.yaml.example`; at least
   `minio.endpoint`), `tokens.yaml`, `powerwarden.token` (when the publisher
   is enabled) and `ui.htpasswd` (when the UI is enabled), all mode 0600 and
   taken from your secret store, plus a persistent volume for
   `/var/lib/hourchd`.
3. Mount the files read-only at the paths in `config.yaml.example`, set
   `HOURCHD_MINIO_ACCESS_KEY`/`HOURCHD_MINIO_SECRET_KEY`, and put port 8093
   behind a TLS reverse proxy (e.g. `hourchd.example.com`) that passes
   websockets through.
4. Run `hourchd check-config --config /etc/hourchd/config.yaml` (it validates
   config, tokens, powerwarden token and htpasswd), then
   `hourchd serve --config /etc/hourchd/config.yaml` (the image's default
   command). Before restarting a running hourchd, check
   `runs_holding_secrets` in `/v1/healthz` is 0 (or accept that those runs
   lose their secrets).
5. Check `curl https://hourchd.example.com/v1/healthz` (`minio` and
   `powerwarden` should say `connected`, or `disabled` for powerwarden) and
   log in to `/ui`.

## Run locally

```sh
mkdir -p /tmp/hourchd && cd /tmp/hourchd
printf 'submitters:\n  me: devtoken\nworkers:\n  w1: workertoken\n' > tokens.yaml && chmod 600 tokens.yaml
cat > config.yaml <<'EOF'
state_dir: /tmp/hourchd/state
tokens_file: /tmp/hourchd/tokens.yaml
minio:
  endpoint: localhost:9000
powerwarden:
  url: ""                # no powerwarden locally
ui:
  htpasswd_file: /tmp/hourchd/ui.htpasswd
EOF
htpasswd -B -b -c /tmp/hourchd/ui.htpasswd me devpass   # then http://localhost:8093/ as me/devpass
export HOURCHD_MINIO_ACCESS_KEY=minioadmin HOURCHD_MINIO_SECRET_KEY=minioadmin
cd -   # back to this directory
just build
./cmd/hourchd/hourchd check-config --config /tmp/hourchd/config.yaml
./cmd/hourchd/hourchd serve --config /tmp/hourchd/config.yaml
curl -s localhost:8093/v1/healthz
```

## Recipes

`just test` (race detector), `just vet`, `just build`, `just image [tag]`,
`just push [tag]` (builds the image first; fails when `IMAGE_REGISTRY` is
empty), `just logs [host]` (follows the compose logs in `$LOGS_DIR/hourchd` on
`host` or `LOGS_HOST` over ssh; no defaults: it fails when either is unset). `just test` includes the end-to-end test
(`internal/e2e`, about 10s).

`just image` tags `$IMAGE_REGISTRY/hourchd:<tag>`, or a local-only
`hourchd:<tag>` when `IMAGE_REGISTRY` is empty. When `IMAGE_CA_CERT` names
an absolute PEM path, the recipe copies it to `cmd/hourchd/extra-ca/` for the
build (removed afterwards) and the image trusts it; otherwise the image
trusts the system CAs only.
