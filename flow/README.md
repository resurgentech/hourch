# hourchflow (`flow/`)

Module `github.com/resurgentech/hourch/flow`, package `hourchflow`.
Builds hourchd runs as Go dependency graphs and submits them through the
generated client (`hourchdapi`, from `specs/openapi/hourchd.yml`). Design:
[`docs/DESIGN-hourchd.md`](../docs/DESIGN-hourchd.md), "Workflows as Go
graphs".

```
just test   # go test -race ./...
just vet
```

## Worked example: build → record → publish

A workflow binary `workflows/hello/hello` builds the program in `apps/hello`,
records the build, and publishes it. The same binary submits the run and,
as subcommands, does the work of each job.

```go
import (
    "context"
    "fmt"
    "time"

    hourchdapi "github.com/resurgentech/hourch/api"
    flow "github.com/resurgentech/hourch/flow"
)

func startRun(repoRoot, version string, secrets map[string]string) error {
    r := flow.NewRun("hello", map[string]string{"version": version})
    // Packs repoRoot (honours .gitignore, always drops .git).
    r.Context(repoRoot, "apps/*/output", "**/target")
    // Ship the gitignored workflow binary; the jobs run its subcommands.
    r.ContextInclude("workflows/hello/hello")

    // Values are read on the submitting machine and sent write-only.
    token := r.Secret("PUBLISH_TOKEN", secrets["PUBLISH_TOKEN"])

    // Writes version=… to $HOURCH_OUTPUTS (flow.WriteOutput).
    settings := r.Job("settings",
        flow.Tags("tiny"),
        flow.Cmd("workflows/hello/hello", "settings", "--version", version),
        flow.Timeout(2*time.Minute),
    )

    // Output refs render as ${outputs.settings.version} and add a dependency.
    build := r.Job("build:hello",
        flow.Tags("kernel"),
        flow.Dir("apps/hello"),
        flow.Cmd("just", "package", "--version", settings.Output("version")),
        flow.Artifacts("output/", "build.log", "build_metadata.json"),
        flow.Timeout(2*time.Hour),
    )

    // Inputs land in inputs/<job>/<path> unless .As(...) says otherwise.
    record := r.Job("record:hello",
        flow.Tags("tiny"),
        flow.Cmd("workflows/hello/hello", "record", "--metadata", "inputs/build:hello/build_metadata.json"),
        flow.Inputs(build.Artifact("build_metadata.json")),
    )

    // At most one leased "publish" job at a time, across all runs.
    r.Job("publish:hello",
        flow.Tags("tiny"),
        flow.Concurrency("publish"),
        flow.Secrets(token),
        flow.Cmd("workflows/hello/hello", "publish", "--dir", "dist"),
        flow.Inputs(build.Artifact("output/").As("dist/")),
        flow.After(record), // ordering only
        flow.Attempts(2),
    )

    c, err := flow.NewClientFromEnv()
    if err != nil {
        return err
    }
    ctx := context.Background()
    sub, err := r.Submit(ctx, c) // validate → stream context → POST /v1/runs
    if err != nil {
        return err
    }
    fmt.Printf("run %s (hello #%d)\n", sub.ID, sub.Number)

    run, err := flow.Wait(ctx, c, sub.ID, flow.OnJob(func(j hourchdapi.JobSummary) {
        fmt.Printf("  %-14s %s\n", j.Name, j.Status)
    }))
    if err != nil {
        return err
    }
    if run.Status != "succeeded" {
        return fmt.Errorf("run %s %s", run.Id, run.Status)
    }
    return nil
}
```

Inside the jobs (subcommands of the same workflow binary):

```go
// settings
err := flow.WriteOutput("version", version)

// record: md is your build metadata (any JSON object), read from --metadata.
// RecordBuild takes the next number from counter build:hello:debian12_amd64,
// lets the callback stamp it into md, and stores md under
// hello/debian12_amd64/<num>, all with HOURCH_JOB_TOKEN.
jc, err := flow.NewJobClientFromEnv()
num, _, err := jc.RecordBuild(ctx, "hello", flow.BuildFlavour("debian12", "amd64", ""),
    func(n int64) any { md["build_num"] = n; return md })
```

`r.Spec()` returns the `RunSubmission` without any I/O; a workflow's graph
test compares it with a golden file (see `flow_test.go`), and
`Job.Deps()` gives the effective dependency set.

## API

| | |
|---|---|
| `NewRun(workflow, params)` | Start a run |
| `(*Run).Context(dir, excludes...)` | Pack and upload `dir` at Submit |
| `(*Run).Secret(name, value) Secret` | Write-only value; env var `name` in jobs that name it. Only secrets some job uses are sent |
| `(*Run).Job(name, opts...) *Job` | Add a job |
| `(*Job).Output(key) Ref` / `(*Job).Artifact(path) Artifact` / `Artifact.As(dest)` | References; each adds a dependency |
| `Tags`, `Cmd(argv ...string\|Ref)`, `Dir`, `Env(key, string\|Ref)`, `Secrets`, `Inputs`, `Artifacts`, `Image`, `Timeout`, `Attempts`, `Concurrency`, `After` | Job options |
| `(*Run).Validate() error` | `*ValidationError{Problems}`; codes mirror the server's (`bad_request`, `invalid_graph`, `missing_secret`) |
| `(*Run).Spec() (hourchdapi.RunSubmission, error)` | The body Submit would POST (no `context_ref`) |
| `(*Run).Submit(ctx, *hourchdapi.Client) (Submitted, error)` | `Submitted{ID, Number, Context, Run}`; prints as the id |
| `(*Run).Check(ctx, *hourchdapi.Client) error` | Server-side dry run: `POST /v1/runs?validate_only=true` (graph, secrets named, a worker for every job: 422 `unsatisfiable`); no context upload, no `context_ref`, secret values replaced by a placeholder; nothing stored |
| `WriteContext(w, dir, excludes...)`, `WriteContextOpts`, `UploadContext(ctx, c, dir, excludes...)` | The context tarball on its own |
| `Wait(ctx, c, runID, OnJob(...), PollInterval(...), WithoutSSE())` | SSE `/v1/events` with resume, polling fallback |
| `StreamLog(ctx, c, jobID, attempt, w)` | Live log to `w`, resumes after drops, returns the `end` event |
| `NewClient(url, token, opts...)`, `NewClientFromEnv(opts...)` | Submitter/operator client |
| `NewJobClient`, `NewJobClientFromEnv`, `JobEnvFromEnv` | Inside a job |
| `(*JobClient).NextCounter`, `PutBuild`, `RecordBuild`; `BuildFlavour`, `BuildCounterName` | Counters and builds with the job token |
| `WriteOutput`, `WriteOutputs`, `AppendOutput` | `key=value` lines to `$HOURCH_OUTPUTS` |
| `APIError{Op, Status, Code, Message}` | Any non-success response |

## Environment

Submitters and operators (`NewClientFromEnv`):

| Variable | Meaning |
|---|---|
| `HOURCHD_URL` | hourchd base URL, e.g. `https://hourchd.example.com`. No built-in default: when unset, the one-line file `$XDG_CONFIG_HOME/hourchd/url` or `~/.config/hourchd/url` (`DefaultURLFile()`, beside the token file; whitespace trimmed) is read. With neither, `NewClientFromEnv` fails naming both |
| `HOURCHD_TOKEN` | Submitter/operator token (wins over the file) |
| `HOURCHD_TOKEN_FILE` | File with the token; default `$XDG_CONFIG_HOME/hourchd/token` or `~/.config/hourchd/token` |
| `HOURCHD_KEY_REQUEST_URL` | Where to file a key request when there is no token (below). When unset, hourchd is asked (`GET /v1/key-request`) |

### Requesting a key on first use

With neither `HOURCHD_TOKEN` nor a token in the token file (missing or empty),
`NewClientFromEnv` asks a key provider for a personal key, using
[`keyclient`](https://github.com/resurgentech/keyclient) (package `request`;
standard library only). The request goes to `HOURCHD_KEY_REQUEST_URL`, else to
the URL hourchd advertises. When neither gives one, the error is the usual
"no token" error. The request asks for the role `submitter`, names the tool
`hourchflow/<module version>`, and suggests the name `$USER@<short host>`.
A person approves it in a browser; the key is then written to the token file
(mode 0600) and used from then on.

- **Standard error is a terminal:** the notice (where to approve, and the code
  to check) is printed there on one line, `NewClientFromEnv` waits for the
  decision, and returns the client.
- **It is not:** `NewClientFromEnv` returns at once an error that wraps the
  `*request.ErrPending` (`errors.As` works), whose text is the notice, then
  `key request pending; run this again after approving`. Print it and exit;
  the request stays pending, and the next run collects the key once it is
  approved (and shows the notice again while it is not).
- **Other outcomes**, each an error that wraps keyclient's (`errors.Is` works):

  | Outcome | Error text |
  |---|---|
  | `request.ErrBusy` | `another process is handling the key request for <token file>; try again shortly` |
  | `request.ErrDenied` | `key request denied` |
  | `request.ErrExpired`, `request.ErrConsumed` | keyclient's message, then `run this again to file a new request` |
  | `request.ErrUnauthorized`, `*request.ProviderError` | keyclient's message; with a `Retry-After`, then `try again in <n>s` |

  "Then" is a new line. Anything else keyclient returns (discovery failing,
  a plain-`http` URL to a host that is not loopback, an unusable pending file)
  is returned unchanged.

`NewClientFromEnv` never exits the process and never opens the approval
address. A provider's service lists a new key at its next pull, so a client
built right after it received a key retries its first call once, 5 s after a
`401`; a body that cannot be replayed (a streamed context upload) is not
retried. With a token, nothing of this happens.

TLS uses the system trust store; if hourchd's certificate comes from a
private CA, install that CA there. The HTTP client has no overall timeout;
bound calls with `ctx`.

Inside a job (set by the worker): `HOURCH_URL`,
`HOURCH_JOB_TOKEN`, `HOURCH_OUTPUTS`, `HOURCH_RUN_ID`, `HOURCH_JOB_ID`,
`HOURCH_ATTEMPT`. The job token works only on the counter and build routes.

## Context tarball

`.tar.zst`, streamed through an `io.Pipe` into `POST /v1/contexts` (chunked;
nothing buffered in memory or on disk); the digest is computed while sending
and checked against hourchd's reply. A `200 created:false` reply (bytes
already stored) is reused like a `201`.

- Walk order: depth first, names in byte order; directories are entries.
- Headers normalised for dedupe: mtime = Unix epoch (as tarballer does today),
  uid/gid 0, no names; permission bits kept.
- `.gitignore`: comments, `!`, trailing `/` (dirs only), anchored patterns
  (containing `/`), unanchored names at any depth, `*` `?` `[]` `**`; deeper
  and later lines win; an ignored directory is not entered.
  Global excludes and `.git/info/exclude` are not read.
- `ContextInclude` files are packed even if `.gitignore` or `excludes` drop
  them (their directories must still be walked); a missing one is an error.
- `.git` is always excluded; `excludes` use the same syntax rooted at the
  context dir and cannot be negated.
- Symlinks are stored as links, never followed (also to directories);
  targets are verbatim, so the unpacker must not follow links out of the
  tree. Hard links become separate files; sockets/FIFOs/devices are skipped;
  unreadable files are an error.
- Stops at 2 GiB compressed (`ContextMaxBytes`) with `ErrContextTooLarge`.
