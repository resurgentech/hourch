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

## 2026-11-19 — A7b hourchd UI: proxy identity

- `ui.auth` (`htpasswd`, the default, or `proxy`) and `ui.proxy.secret_file`, `ui.proxy.secret_header`,
  `ui.proxy.user_header` (all required in proxy mode, no defaults). In proxy mode a UI request is accepted only when
  the secret header (exactly once) equals a line of the secret file (constant-time, every line) and the user header
  (exactly once) matches `^[A-Za-z0-9_.@-]{1,64}$`; otherwise 401 with no `WWW-Authenticate`. No basic auth in that
  mode. The caller is `ui:<user>` (retry, logs, `AsUIUser` on the in-process API calls); the proxy headers are
  removed before those calls. A group- or other-readable secret file fails startup and `check-config`. `sameOrigin`
  and `/v1` unchanged; with `ui.auth` unset htpasswd works as before. Example config, DESIGN doc, daemon README.
- Release: `daemon/v0.2.0` (A7a + A7b) to be tagged after review; no intra-repo require moves.
- Commits: the commit "A7b: hourchd UI proxy identity (ui.auth: proxy)".

## 2026-11-19 — A7 alignment: UI prefix and proxy settings match the shared contract

- `ui.proxy.*` set while `ui.auth` is htpasswd is now a config error naming the key; secret file skips `#` lines,
  needs each secret >= 16 characters, and is refused with any group/other bit (`mode & 0077`); `ui.path_prefix`
  segments may hold only `A-Z a-z 0-9 . _ ~ -`; an unprefixed page has no `data-base` (byte-for-byte as before);
  `check-config` prints `config ok ui=proxy` in proxy mode. Docs and example updated. Release still `daemon/v0.2.0`.

## 2026-11-19 — B6 hourchd: key-request discovery

- Spec first: `GET /v1/key-request` (no authentication, operationId `getKeyRequestDiscovery`, tag `key-request`) and
  the `KeyRequestDiscovery` schema (`{"request_url": <absolute http(s) URL>}`) in `specs/openapi/hourchd.yml`
  (spec version 0.2.0); the api client regenerated. `specs/fixtures/keyrequest/discovery.json` and
  `discovery-none.json` hold the two exchanges; the api tests check the spec's example against them and the daemon
  tests check hourchd's answers.
- hourchd: optional `personal_keys.request_url` (default `""`). Set: the route answers `200 {"request_url": ...}`;
  unset: `404` (`not_found` Error body) and nothing else changes. Must be an absolute `http`/`https` URL with a host,
  else startup and `check-config` fail naming the key. Example config, DESIGN doc ("Key-request discovery") and
  daemon README updated.
- `daemon` and `flow` require `api v0.2.0`; the `go.work` replace moved with them. go.sum lines for the untagged
  `api v0.2.0` came from a local file proxy built with `zip.CreateFromVCS` from the commit below.
- Release (after review): `api/v0.2.0`, then `daemon/v0.3.0`, both on the second commit below; no follow-up commit.
- Commits: "B6: hourchd key-request discovery (GET /v1/key-request)" and "B6: go.sum for api v0.2.0; log".
- Commits: the commit "A7: align UI prefix and proxy settings with the shared contract".

## 2026-11-19 — B6 alignment: request URL rule and check-config line match the shared contract

- `personal_keys.request_url` must also have no user info and no fragment (scheme still case-insensitive, value
  still served exactly as written); a refused value with user info is not echoed in the error. When the setting is
  set, `check-config` appends ` key_request=on` (after ` ui=proxy`); unset, its output is unchanged. Example config,
  DESIGN doc and daemon README updated. `api` unchanged. Release unchanged: `api/v0.2.0`, then `daemon/v0.3.0`, now
  both on this commit.
- Correction to the log: the B6 entry above was inserted inside the "A7 alignment" entry, so the last line above it
  ("Commits: the commit \"A7: align UI prefix and proxy settings with the shared contract\"") belongs to that A7
  alignment entry, not to B6.
- Commits: the commit "B6: align request_url rule and check-config line with the shared contract".
