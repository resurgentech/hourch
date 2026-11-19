# Work log

Append-only history of what shipped from `ROADMAP.md`. FOREMAN appends
an entry per landed step or tranche. Newest at the bottom. Corrections
are appended, never edited in; a hook refuses rewrites.

```
## YYYY-MM-DD — <step id and title>

- What shipped
- Commits: <shas>
```

---

## 2026-11-19 — initial import

- hourchd (`daemon`), the worker (`worker`), the workflow library (`flow`), the generated client (`api`) and the
  worker protocol types (`protocol`), their OpenAPI spec, worker-protocol schema and design doc imported as five Go
  modules under `github.com/resurgentech/hourch/`, joined by a committed `go.work`; each module tagged
  `<dir>/v0.1.0`. orch process files seeded.
- Commits: the initial commit.

## 2026-11-19 — A7a hourchd UI: path prefix

- `ui.path_prefix` (default `""`): with `/p` the web UI is served under `/p/ui/...` and `/`, `/p`, `/p/` redirect to
  `/p/ui/runs`; every page link, the script's event stream, the log stream, the in-process API forwards and the
  POST redirects carry the prefix. Must start with `/`, no trailing `/`, no `..`. Empty prefix: every URL as before.
  The `/v1` API is not prefixed. Example config, DESIGN doc and daemon README updated.
- Commits: the commit "A7a: hourchd UI path prefix (ui.path_prefix)".
