# Fixtures

HTTP exchanges of the key-request contract that hourchd takes part in: key-request discovery,
`GET /v1/key-request` (see `docs/DESIGN-hourchd.md`, "Key-request discovery"). They are copies of the contract's
shared fixtures; keep them byte-for-byte equal to those.

Each file is one exchange written as an OpenAPI Example Object:

| Member | Meaning |
|---|---|
| `summary`, `description` | Text for a reader. |
| `x-status` | The HTTP status of the reply. |
| `x-headers` | The headers a test asserts. Header names are case-insensitive. |
| `value` | The body, as JSON. Absent when there is no body to rely on. |

URLs use the origin `https://provider.example`.

| File | What it shows |
|---|---|
| `keyrequest/discovery.json` | `GET /v1/key-request` `200` with a `request_url` (the example in `specs/openapi/hourchd.yml`). |
| `keyrequest/discovery-none.json` | `GET /v1/key-request` `404`: no request URL configured; a client must not rely on the body. |

Tests: `api/keyrequest_test.go` checks the spec against them; `daemon/internal/httpapi/keyrequest_test.go` checks
hourchd's answers.
