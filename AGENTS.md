# hourch

**hourch**: a small job system for build and release workflows. **hourchd** owns every run and job in SQLite,
pushes jobs to workers over websockets and brokers contexts, artifacts and logs through an object store;
**hourch-worker** runs the jobs; the **flow** library builds and submits runs as Go dependency graphs.

- `api`: generated Go client for `specs/openapi/hourchd.yml` (spec-first). `protocol`: Go types for
  `specs/hourchd/worker-protocol.schema.json`.
- `daemon`: hourchd; `worker`: hourch-worker; `flow`: the workflow library.
- `specs/`: the API and worker-protocol contracts. `docs/`: design doc and the work log.
- Tooling: plain `just` per package (`just test`, `just vet`, `just drift`, `just image`); a committed `go.work`
  joins the five modules. See `README.md`.

## Roles

The default role of a session is **XO**: a request in chat becomes a
`ROADMAP.md` step, not an edit. XO drafts a spawn prompt, shows it, and
spawns **ARCHITECT** (plans into `ROADMAP.md`) at once, or waits for
approval and then spawns **FOREMAN** (builds from it); either runs with
a clean context, and XO reviews the report. When the user asks to run
a request unattended (`/orch run`), the roadmap is accepted in advance
and XO chains the two without stopping. Answer directly only when no
code change is needed and the context is in hand, or when the user
says to just do it.

## Hard rules

1. **FOREMAN builds only from `ROADMAP.md`.** No scope from chat.
2. **`docs/LOG.md` is append-only.**
3. **The gate stays green** for packages you touch before a commit.
   Default gate: `just test` in every module, `just vet` in every module but `api`, `just drift` in `api`
   (`.orch.json`).
4. **Every built step leaves a report** in `docs/reports/`, with its
   assumptions: context, question, decision, alternatives, confidence.
5. **Secrets** never go in the repo, the roadmap, the reports or the
   log.
6. **Spec-first APIs**: change `specs/openapi/hourchd.yml` (or `specs/hourchd/worker-protocol.schema.json`) first,
   `just generate` in `api`, then handlers, clients and tests together.
7. **Intra-repo versions**: when an intra-repo `require` moves to a new version, move the matching versioned
   `replace` in `go.work` with it. No `replace` lines in any `go.mod`.
8. **No site values compiled in**: hosts, registries and paths of a particular deployment belong in config files,
   `images.env` (gitignored) or examples, never in code defaults.

## Where to look

| Need | Where |
|---|---|
| The queue | `ROADMAP.md` (local, gitignored; `/orch init` makes one) |
| What shipped | `docs/LOG.md` |
| What was done, and the judgment calls | `docs/reports/` (local, gitignored): one file per built step, recap plus assumptions |
| Design | `docs/DESIGN-hourchd.md` |
| Roles and process | the orch plugin (`/orch`) |
| Agent definitions | `architect`, `foreman`, `implementer`, `recon` (plugin) |
