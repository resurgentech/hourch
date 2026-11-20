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

## 2026-11-19 — B7 hourchd: key list source

- Spec first: the shared `PersonalKeysStatus` schema and an optional `personal_keys` on `GET /v1/stats` (present
  only when the key list is on); spec version 0.3.0; api client regenerated. The `last_error` description is
  double-quoted: unquoted, its commas end the value inside the YAML flow mapping and the spec does not validate.
- hourchd: `personal_keys.url`, `token_file`, `cache_file` (required with `url`, no default), `refresh` (1m),
  `refresh_on_miss` (5s), `allow_insecure_http` (false), through `github.com/resurgentech/keyclient` `keylist`. A
  sub-key written while `url` is `""` is a config error. Bearer order: `tokens_file`, then `hjt_` job tokens (never
  the list), then the list, only on routes a submitter may call; only role `submitter` is accepted, and an entry
  that clashes with a local token by name or SHA-256 is skipped. Worker and job-only routes never consult the list.
  `check-config` builds the source without starting it and appends ` key_list=on` (and
  ` key_list_insecure_http=on`); `serve` starts it before the listener and logs one line. `GET /v1/healthz`
  unchanged. Without `url`, nothing changes. Tests named after the keyclient STATES rows (K-1, K-10, K-5/K-8, R-2,
  R-7), plus revoke, local-token collision, unknown role, worker routes and job tokens, insecure http, stats.
  Example config, DESIGN doc ("Key list") and daemon README updated.
- `daemon` requires `api v0.3.0` and `keyclient v0.1.1`; the `go.work` api replace moved to v0.3.0. `flow` stays on
  `api v0.2.0`. go.sum lines for the untagged `api v0.3.0` came from a local file proxy built with
  `zip.CreateFromVCS` from the first commit below (api unchanged after it); `GOWORK=off go mod tidy` leaves every
  module clean.
- Release (after review): `api/v0.3.0`, then `daemon/v0.4.0`, both on the commit that carries this entry.
- Commits: "B7: PersonalKeysStatus on /v1/stats (spec, api)", "B7: hourchd key list source (personal_keys.url)"
  and "B7: log".

## 2026-11-19 — B7 alignment: start log age in whole seconds; durations with a unit

- The start line `personal_keys: list origin=<origin> entries=<n> age=<age>` gives the age rounded to whole
  seconds, `0s` when origin is `none`. hourchd already refuses a bare number as a duration (`refresh_on_miss: 0`
  is a decode error; `0s` turns refresh on miss off), now pinned by a test; the example config, README and DESIGN
  doc write durations with a unit. `api` unchanged.
- Release: `api/v0.3.0` stays as planned (api unchanged); `daemon/v0.4.0` moves to this commit.
- Commits: the commit "B7: start log age in whole seconds; durations with a unit".

## 2026-11-19 — B7 fix: a malformed tokens file never echoes a token

- `LoadTokens` (used by `serve` and `check-config`) returned `tokens file <path>: <yaml error>`, and yaml.v3 quotes
  the offending value in backticks: a file holding only a bare token printed its first characters, and a value
  tagged `!!int` printed the whole token. Every backtick-quoted span of that error is now replaced with
  `` `<redacted>` ``, as in the sibling daemons; the `tokens file <path>:` context is kept. The rest of the loader
  prints names, paths and modes only. Test: `TestLoadTokensNeverEchoesAToken` (bare token, `!!int` value, a list
  instead of a map, a syntax error; through the loader and through `check-config`'s `app.Check`). `api` unchanged.
- Release: `api/v0.3.0` stays as planned; `daemon/v0.4.0` moves to this commit.
- Commits: the commit "B7: a malformed tokens file never echoes a token".

## 2026-11-20 — B10: flow requests a key on first use

- `NewClientFromEnv`, with neither `HOURCHD_TOKEN` nor a token in the token file (missing or empty), runs
  keyclient's `request.Ensure` for the token file, role `submitter`, tool `hourchflow/<module version>`, name hint
  `$USER@<short host>`. New `HOURCHD_KEY_REQUEST_URL` (`EnvHourchdKeyRequestURL`) overrides the request URL;
  otherwise hourchd's `GET /v1/key-request` is asked. With no provider the "no token" errors are unchanged.
- Stderr a terminal: `Wait: true`, the notice on one line to stderr, the client returned. Otherwise `Wait: false`
  and an error wrapping `*request.ErrPending` whose text is the notice, a new line, then
  `key request pending; run this again after approving`; the next run collects the key. `ErrBusy`, denied,
  expired, consumed and provider errors are worded as the shared tool rules and wrap keyclient's errors. It never
  exits and never opens the approval address. With a token, nothing changes.
- A client built right after a fresh key (`Result.Name != ""`) retries its first call once, 5 s after a `401`,
  through a wrapper of the client's doer that is active for that call only; the wait ends with the request's
  context. A body without `GetBody` (the streamed context upload) is not retried.
- Tests named after keyclient STATES rows (T-1, T-7, T-10/T-16, T-15, T-17, T-22, L-13..L-16, T-8) against
  `keyclienttest`'s provider, plus the retry's rules. README section "Requesting a key on first use".
- `flow` requires `keyclient v0.1.1` (standard library only) and stays on `api v0.2.0`; `GOWORK=off go mod tidy`
  leaves it clean; `go.work` unchanged.
- Release (after review): `flow/v0.2.0`, on the commit that carries this entry.
- Commits: "B10: flow requests a key on first use" and "B10: log".
