# hourch

A small job system for build and release workflows on a fleet of worker machines.

- **hourchd** is the one always-on daemon. It owns every run and job in a SQLite file, pushes jobs to workers over
  one websocket per worker, enforces timeouts, retries and leases, and is the only client of the object store (MinIO
  or any S3-compatible store): contexts, artifacts and logs stream through it. It keeps submitted secrets sealed
  under a key that lives only in its memory, serves an HTTP API and an operator web UI, and can publish demand and
  worker liveness to [powerwarden](https://github.com/resurgentech/powerwarden) so powered-off workers are woken
  when work arrives.
- **hourch-worker** holds one websocket to hourchd and runs one job at a time, as a process or in a container,
  streaming its logs and reporting outputs, artifacts and the result.
- **flow** is the Go library a workflow binary uses to build a run as a dependency graph of jobs, submit it, wait
  for it, and, inside a job, read inputs, write outputs and record builds. Workflows are Go code: no YAML, no DSL.

```
workflow binary ──context, run (graph + secrets)──▶ hourchd ◀══websocket══▶ hourch-worker (jobs)
browser ──────────────────────────/ui────────────▶    │  └────demand, liveness (optional)────▶ powerwarden
                                                      └──contexts, artifacts, logs──▶ object store
```

Design: [`docs/DESIGN-hourchd.md`](docs/DESIGN-hourchd.md).

## Layout

| Path | Module | What |
|---|---|---|
| `specs/openapi/hourchd.yml` | | HTTP API contract (source of truth) |
| `specs/hourchd/worker-protocol.schema.json` | | Worker websocket messages (JSON Schema) |
| `specs/fixtures/` | | HTTP exchanges of the key-request discovery contract ([README](specs/fixtures/README.md)) |
| `api` | `github.com/resurgentech/hourch/api` | Generated Go client and types (`package hourchdapi`) |
| `protocol` | `github.com/resurgentech/hourch/protocol` | Go types for the worker messages, checked against the schema (`package hourchdprotocol`) |
| `daemon` | `github.com/resurgentech/hourch/daemon` | hourchd: `cmd/hourchd` and its internal packages ([README](daemon/README.md)) |
| `worker` | `github.com/resurgentech/hourch/worker` | hourch-worker: `cmd/hourch-worker` and the worker library (`package hourchworker`, [README](worker/README.md)) |
| `flow` | `github.com/resurgentech/hourch/flow` | The workflow library (`package hourchflow`, [README](flow/README.md)) |
| `docs/` | | Design doc, work log |
| `go.work` | | Joins the five modules for in-repo work |

The daemon talks to powerwarden through powerwarden's published client,
`github.com/resurgentech/powerwarden/powerwarden/api`, required at a tag like any other dependency.

## Build and test

Needs Go 1.25 or newer and [`just`](https://github.com/casey/just). Each module has its own `justfile`; run
recipes from the module directory (or `just --justfile <dir>/justfile -d <dir> <recipe>` from the root).

```sh
cd daemon && just test && just vet     # likewise flow, worker, protocol
cd api && just test && just drift      # spec coverage; fails when the generated client is stale
cd daemon && just build                # cmd/hourchd/hourchd
cd worker && just build                # cmd/hourch-worker/hourch-worker
```

The daemon's `just test` includes an in-process end-to-end test: the daemon, two real workers, a fake powerwarden
and `flow` runs. Tests need no docker, object store or network.

`just generate` in `api` regenerates the client from the spec; `just drift` fails when the committed client is
stale. Change the spec first, regenerate, then the handlers, clients and tests together.

## Images

`just image [tag]` in `daemon` or `worker` builds the container image with the repo root as context (the
Dockerfiles copy only `go.work`, `go.work.sum`, the five module directories and `specs/`). `just push [tag]` pushes
it. The worker image is the small container variant (archlinux base, no docker CLI, so no container jobs);
`just image-smoke [tag]` in `worker` runs each tool in it, including `hourch-worker --version`. Workers that run
container jobs run the binary directly on the host.

Settings come from the environment or a gitignored `images.env` at the repo root; copy
[`images.env.example`](images.env.example):

| Setting | Effect |
|---|---|
| `IMAGE_REGISTRY` | Registry prefix, e.g. `registry.example.com`. Empty: local tag `<name>:<tag>`, and `push` refuses |
| `IMAGE_CA_CERT` | Path to an extra CA (PEM) to trust inside the image. Empty: system CAs only |
| `LOGS_HOST` | Host for `just logs [host]` in `daemon` (SSH, then `docker compose logs -f`). No default |
| `LOGS_DIR` | Directory on `LOGS_HOST` holding the compose projects; `just logs` runs in `$LOGS_DIR/hourchd`. No default |

How to configure and run hourchd and the workers: the package READMEs, `daemon/config.yaml.example`,
`worker/worker.yaml.example` and the design doc.

## Development notes

- **Versions.** Each module is versioned on its own with a directory-prefixed tag, e.g. `flow/v0.1.0`. Stay on
  v0.x.
- **Intra-repo requires and `go.work`.** `flow` requires `api`, `worker` requires `protocol`, and `daemon` requires
  all four, each at a tag (e.g. `v0.1.0`), so `go install <module>@<tag>` and builds with `GOWORK=off` resolve real
  versions. Go reads the `go.mod` of the required version even inside a workspace, so `go.work` also carries one
  *versioned* `replace` per intra-repo require, pointing that version at the local copy; in-repo work never needs a
  published tag. **When an intra-repo `require` moves to a new version, the matching `go.work` replace moves with
  it.** `go.mod` files carry no `replace` lines.
- **Releasing a change to `api` or `protocol`**: tag `<dir>/vX.Y.Z` and push the tag, then in the dependent modules
  bump the `require`, run `GOWORK=off go mod tidy`, and move the `replace` in `go.work`, in one commit; tag the
  dependents only on a commit whose `go.mod` and `go.sum` are tidy.
- **powerwarden's client** is an ordinary dependency: `go get github.com/resurgentech/powerwarden/powerwarden/api@<tag>`
  in `daemon`, then `GOWORK=off go mod tidy` there.
- **No site values in code.** Hosts, registries and paths of a particular deployment live in config files,
  `images.env` or the `*.example` files.

## License

MIT, see [`LICENSE`](LICENSE).
