# hourch-worker

The hourchd worker (module `github.com/resurgentech/hourch/worker`, package
`hourchworker`, binary `cmd/hourch-worker`). It
holds one websocket to hourchd, runs one job at a time, streams its logs, and
reports outputs, artifacts and the result. Design:
[`docs/DESIGN-hourchd.md`](../docs/DESIGN-hourchd.md) (Worker protocol).
Messages are `hourchdprotocol` types
([`specs/hourchd/worker-protocol.schema.json`](../specs/hourchd/worker-protocol.schema.json)).

## Config

`/etc/hourch/worker.yaml` (`--config` overrides). Every key and its default is
in [`worker.yaml.example`](worker.yaml.example); unknown keys are a startup
error. `hourchd_url` (hourchd's worker websocket, e.g.
`wss://hourchd.example.com/v1/worker/connect`) has no default and is
required. `worker_id` is required and must equal the name of this worker's token
in hourchd's `tokens_file`. The token file must be mode 0600 (no group or other
bits) or the worker refuses to start. Only advertise `capabilities: [docker]`
on hosts that can run containers.

## What a job sees

- Work root `work_dir/<job_id>-<attempt>`: the context tarball unpacked, then
  inputs (`inputs/<job>/<path>` or `as`; a `dir/` input is an unpacked tar).
  Removed after the result, success or failure. Stale `<ULID>-<n>` dirs are
  removed at startup.
- cwd: work root + job `workdir` (must exist in the context).
- argv runs as is (no shell), in its own process group.
- Environment: only `env_allowlist` names from the worker's own environment
  (process jobs only; container jobs get none of the host's), then the job's
  `env`, then its secrets, then `HOURCH_URL`, `HOURCH_JOB_TOKEN`,
  `HOURCH_OUTPUTS`, `HOURCH_RUN_ID`, `HOURCH_JOB_ID`, `HOURCH_ATTEMPT`.
  Inherited `HOURCH_*` variables and the worker token never pass.
- Context and input tars: any entry (file, directory, symlink target or hard
  link) that would resolve outside its destination directory, absolute or via
  `..`, fails the job before it runs.
- `$HOURCH_OUTPUTS` (`<root>/.hourch/outputs`): `key=value` lines, sent as
  `output` messages after exit, before `result`. Blank lines are ignored; a
  value ends at the newline and may be at most 4096 bytes. A malformed line
  (no `=`, a key outside `[A-Za-z0-9_.-]{1,128}`, an oversized value) fails a
  job that exited 0 with `result.error` `bad_output: line N: ...`; no outputs
  are sent and no artifacts uploaded. After a non-zero exit it is only noted
  in the log.
- Paths in `artifacts` and `inputs` are relative to the work root (the
  context root), not to `workdir`.
- Artifacts are uploaded only after exit 0; `dir/` uploads every file under it.
  A failed upload fails the job (`result.error`), exit code unchanged.
- Container jobs (`image` set): `docker create --rm --name hourch-<job>-<attempt>
  --env-file <0600 file> -v <root>:/work -w /work/<workdir> <image> <argv>`,
  the env file deleted, then `docker start -a`. Cancel is `docker kill
  --signal TERM`, then `docker rm -f` after the grace period.
- Logs: chunks of at most 64 KiB or 250ms, secret values (and the job token)
  replaced by `***`, invalid UTF-8 replaced by U+FFFD. A secret split across
  two chunks is not redacted; size cuts prefer a newline to make that rare.

## Run locally

```bash
just build
cat > /tmp/worker.yaml <<'EOF'
hourchd_url: ws://127.0.0.1:8093/v1/worker/connect
worker_id: dev-1
token_file: /tmp/worker.token
work_dir: /tmp/hourch-work
EOF
printf '%s\n' "$TOKEN" > /tmp/worker.token && chmod 600 /tmp/worker.token
./cmd/hourch-worker/hourch-worker --config /tmp/worker.yaml
```

`just test` runs everything against an in-process fake hourchd
(`internal/fakehourchd`); container tests use a fake `docker` script, so no
docker daemon is needed.

## Stopping

The first SIGTERM/SIGINT drains: no new jobs, the running job keeps running and
its result is delivered, then the worker exits (at once if idle). A second
signal kills the job's process group or container and exits without a result;
hourchd's lease lapses and the job is requeued or failed (`lease_expired`).

## On a host (systemd)

Workers whose jobs need the host's docker and disk run the binary under
systemd. Use `KillMode=mixed` so `systemctl stop` signals only the
worker (which then drains) and SIGKILL reaches the job only at
`TimeoutStopSec`; the default `control-group` would SIGTERM the job at once.

```ini
[Service]
ExecStart=/usr/local/bin/hourch-worker --config /etc/hourch/worker.yaml
Restart=always
KillMode=mixed
TimeoutStopSec=2h
```

Your deployment tooling owns the real unit, the token file (mode 0600) and
the config.

## Image

`just image` builds `hourch-worker:<tag>` from `cmd/hourch-worker/Dockerfile`,
for workers that run as containers. Two settings, read from the environment
or from an `images.env` file that just finds by searching upward from this
directory (see `images.env.example`):

| Setting | Effect |
|---|---|
| `IMAGE_REGISTRY` | Registry prefix: the image is `$IMAGE_REGISTRY/hourch-worker:<tag>`. Empty gives a local-only tag, and `just push` fails |
| `IMAGE_CA_CERT` | Absolute path to an extra CA certificate (PEM). The recipe stages it in `cmd/hourch-worker/extra-ca/` for the build and removes it afterwards; the image trusts it. Empty: system CAs only |

The runtime stage is `archlinux:base` plus gnupg, openssh and ca-certificates
(pacman, repo-add, vercmp and bsdtar come with the base). It has no docker CLI,
so it cannot serve container jobs. `just image-smoke` runs each tool in the
built image and, when `IMAGE_CA_CERT` is set, checks the image trusts it.
`just push` builds the image first.
