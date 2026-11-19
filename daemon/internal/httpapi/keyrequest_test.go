package httpapi_test

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	api "github.com/resurgentech/hourch/api"

	"github.com/resurgentech/hourch/daemon/internal/config"
)

// keyRequestFixture is one exchange of the key-request contract
// (specs/fixtures/keyrequest/*.json, OpenAPI Example Objects).
type keyRequestFixture struct {
	Status  int               `json:"x-status"`
	Headers map[string]string `json:"x-headers"`
	Value   json.RawMessage   `json:"value"`
}

func loadKeyRequestFixture(t *testing.T, name string) keyRequestFixture {
	t.Helper()
	data, err := os.ReadFile("../../../specs/fixtures/keyrequest/" + name)
	if err != nil {
		t.Fatal(err)
	}
	var f keyRequestFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return f
}

// TestKeyRequestDiscoveryUnset: with personal_keys.request_url unset (the
// default) GET /v1/key-request is 404 with or without a token, as the
// discovery-none fixture says.
func TestKeyRequestDiscoveryUnset(t *testing.T) {
	f := loadKeyRequestFixture(t, "discovery-none.json")
	e := newEnv(t, nil)
	for _, tok := range []string{"", subTok, w1Tok, "not-a-token"} {
		r, b := e.do("GET", "/v1/key-request", tok, nil, nil)
		if r.StatusCode != f.Status {
			t.Fatalf("token %q: status %d, want %d: %s", tok, r.StatusCode, f.Status, b)
		}
		if errCode(t, b) != api.ErrorCodeNotFound {
			t.Fatalf("token %q: body %s", tok, b)
		}
		if strings.Contains(string(b), "no such route") {
			t.Fatalf("token %q: the route is not registered: %s", tok, b)
		}
	}
	// Only GET is served; other methods fall through to the catch-all, as on
	// every other route.
	if r, b := e.do("POST", "/v1/key-request", "", strings.NewReader("{}"), nil); r.StatusCode != 404 || !strings.Contains(string(b), "no such route") {
		t.Fatalf("POST /v1/key-request = %d %s, want the catch-all 404", r.StatusCode, b)
	}
}

// TestKeyRequestDiscoverySet: with personal_keys.request_url set the route
// answers the discovery fixture without authentication.
func TestKeyRequestDiscoverySet(t *testing.T) {
	f := loadKeyRequestFixture(t, "discovery.json")
	var want api.KeyRequestDiscovery
	if err := json.Unmarshal(f.Value, &want); err != nil {
		t.Fatal(err)
	}
	e := newEnv(t, func(c *config.Config) { c.PersonalKeys.RequestURL = want.RequestUrl })
	for _, tok := range []string{"", subTok, "not-a-token"} {
		r, b := e.do("GET", "/v1/key-request", tok, nil, nil)
		if r.StatusCode != f.Status {
			t.Fatalf("token %q: status %d, want %d: %s", tok, r.StatusCode, f.Status, b)
		}
		for k, v := range f.Headers {
			if got := r.Header.Get(k); got != v {
				t.Fatalf("token %q: header %s = %q, want %q", tok, k, got, v)
			}
		}
		var got, wantBody any
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("body %s: %v", b, err)
		}
		_ = json.Unmarshal(f.Value, &wantBody)
		if !reflect.DeepEqual(got, wantBody) {
			t.Fatalf("token %q: body %s, want %s", tok, b, f.Value)
		}
	}
	// The setting changes nothing else: other routes still need a token.
	if r, b := e.do("GET", "/v1/runs", "", nil, nil); r.StatusCode != 401 {
		t.Fatalf("GET /v1/runs without a token = %d: %s", r.StatusCode, b)
	}
}
