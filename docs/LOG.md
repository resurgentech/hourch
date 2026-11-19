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
