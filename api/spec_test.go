package hourchdapi

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

const specFile = "../specs/openapi/hourchd.yml"

// operations returns "METHOD path" -> operationId for every operation in doc.
func operations(doc *openapi3.T) map[string]string {
	ops := map[string]string{}
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			ops[method+" "+path] = op.OperationID
		}
	}
	return ops
}

func loadSpecs(t *testing.T) (file, embedded *openapi3.T) {
	t.Helper()
	file, err := openapi3.NewLoader().LoadFromFile(specFile)
	if err != nil {
		t.Fatalf("load %s: %v", specFile, err)
	}
	if err := file.Validate(context.Background()); err != nil {
		t.Fatalf("validate %s: %v", specFile, err)
	}
	embedded, err = GetSwagger()
	if err != nil {
		t.Fatalf("GetSwagger: %v", err)
	}
	return file, embedded
}

// TestEmbeddedSpecCoversSpecFile checks that every path/operation in the spec
// file is present in the embedded (generated) spec, and vice versa.
func TestEmbeddedSpecCoversSpecFile(t *testing.T) {
	file, embedded := loadSpecs(t)
	want, got := operations(file), operations(embedded)
	if len(want) == 0 {
		t.Fatal("spec file has no operations")
	}
	for k, id := range want {
		gid, ok := got[k]
		if !ok {
			t.Errorf("operation %s (%s) missing from embedded spec; run `just generate`", k, id)
			continue
		}
		if goName(gid) != goName(id) {
			t.Errorf("operation %s: operationId %q in spec file, %q embedded", k, id, gid)
		}
	}
	for k := range got {
		if _, ok := want[k]; !ok {
			t.Errorf("embedded spec has stale operation %s; run `just generate`", k)
		}
	}
}

// TestClientHasEveryOperation checks that the generated client exposes one
// method per operationId.
func TestClientHasEveryOperation(t *testing.T) {
	file, _ := loadSpecs(t)
	iface := reflect.TypeOf((*ClientInterface)(nil)).Elem()
	var ids []string
	for k, id := range operations(file) {
		if id == "" {
			t.Errorf("operation %s has no operationId", k)
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		name := goName(id)
		_, ok := iface.MethodByName(name)
		if !ok {
			// Streamed (non-JSON) request bodies only get the WithBody form.
			_, ok = iface.MethodByName(name + "WithBody")
		}
		if !ok {
			t.Errorf("ClientInterface has no method %s for operationId %s", name, id)
		}
	}
}

// TestDesignRoutesPresent pins the route list from docs/DESIGN-hourchd.md
// (HTTP API table, worker connect and worker bulk-data routes).
func TestDesignRoutesPresent(t *testing.T) {
	file, _ := loadSpecs(t)
	want := []string{
		"POST /v1/contexts",
		"POST /v1/runs",
		"GET /v1/runs",
		"GET /v1/runs/{id}",
		"GET /v1/workflows/{workflow}/runs/{number}",
		"POST /v1/runs/{id}/cancel",
		"POST /v1/runs/{id}/retry",
		"GET /v1/jobs/{id}",
		"GET /v1/jobs/{id}/log",
		"GET /v1/jobs/{id}/log/stream",
		"GET /v1/jobs/{id}/artifacts",
		"GET /v1/jobs/{id}/artifacts/{path}",
		"POST /v1/jobs/{id}/cancel",
		"GET /v1/workers",
		"POST /v1/counters/{name}/next",
		"PUT /v1/builds/{type}/{flavour}/{num}",
		"GET /v1/builds",
		"GET /v1/events",
		"GET /v1/healthz",
		"GET /v1/stats",
		"GET /v1/worker/connect",
		"GET /v1/worker/jobs/{id}/context",
		"GET /v1/worker/jobs/{id}/inputs/{upstream_job}/{path}",
		"PUT /v1/worker/jobs/{id}/artifacts/{path}",
	}
	got := operations(file)
	for _, k := range want {
		if _, ok := got[k]; !ok {
			t.Errorf("route %s missing from spec", k)
		}
	}
	if len(got) != len(want) {
		t.Errorf("spec has %d operations, design lists %d; update this test with the design", len(got), len(want))
	}
}

// TestSecurity checks the global bearerAuth requirement, the empty override
// on /v1/healthz, and that the job token is accepted only on the counter and
// build routes (counters also with bearerAuth; builds PUT only the job token).
func TestSecurity(t *testing.T) {
	file, _ := loadSpecs(t)
	if len(file.Security) != 1 {
		t.Fatalf("want one global security requirement, got %v", file.Security)
	}
	if _, ok := file.Security[0]["bearerAuth"]; !ok {
		t.Fatalf("global security is not bearerAuth: %v", file.Security)
	}
	want := map[string][]string{
		"GET /v1/healthz":                       {},
		"POST /v1/counters/{name}/next":         {"bearerAuth", "jobToken"},
		"PUT /v1/builds/{type}/{flavour}/{num}": {"jobToken"},
	}
	for k, op := range allOps(file) {
		w, special := want[k]
		if !special {
			if op.Security != nil {
				t.Errorf("%s: unexpected security override %v", k, *op.Security)
			}
			continue
		}
		if op.Security == nil {
			t.Errorf("%s: want security override %v, got global", k, w)
			continue
		}
		var got []string
		for _, req := range *op.Security {
			for name := range req {
				got = append(got, name)
			}
		}
		sort.Strings(got)
		if strings.Join(got, ",") != strings.Join(w, ",") {
			t.Errorf("%s: security %v, want %v", k, got, w)
		}
	}
}

// TestViewsHaveNoSecretValues checks that no response view schema declares a
// property that could carry secret values or tokens. Secret names appear only
// as `secret_names` / `secrets` arrays of strings; integer counts
// (`runs_holding_secrets`) are allowed.
func TestViewsHaveNoSecretValues(t *testing.T) {
	file, _ := loadSpecs(t)
	for _, name := range []string{"RunSummary", "Run", "JobSummary", "JobDetail", "Worker", "Event", "Stats", "Healthz"} {
		ref, ok := file.Components.Schemas[name]
		if !ok {
			t.Fatalf("schema %s missing", name)
		}
		walk(ref.Value, map[*openapi3.Schema]bool{}, func(prop string, s *openapi3.Schema) {
			if strings.Contains(prop, "token") || strings.Contains(prop, "password") {
				t.Errorf("schema %s has forbidden property %q", name, prop)
			}
			if strings.Contains(prop, "secret") {
				if s.Type != nil && s.Type.Is("integer") {
					return // a count (runs_holding_secrets) carries no value
				}
				if s.Type == nil || !s.Type.Is("array") || s.Items == nil || s.Items.Value.Type == nil || !s.Items.Value.Type.Is("string") {
					t.Errorf("schema %s property %q must be an array of secret names", name, prop)
				}
			}
		})
	}
	for _, name := range []string{"RunSubmission", "RetryRequest"} {
		p := file.Components.Schemas[name].Value.Properties["secrets"]
		if p == nil || !p.Value.WriteOnly {
			t.Errorf("%s.secrets must be writeOnly", name)
		}
	}
}

// TestErrorCodes checks that every code the server returns is in the enum.
func TestErrorCodes(t *testing.T) {
	file, _ := loadSpecs(t)
	enum := map[string]bool{}
	for _, v := range file.Components.Schemas["ErrorCode"].Value.Enum {
		enum[v.(string)] = true
	}
	for _, c := range []string{"bad_request", "checksum_mismatch", "unauthorized", "forbidden", "not_found",
		"not_leased", "too_large", "invalid_graph", "missing_secret", "context_not_found", "internal", "unavailable"} {
		if !enum[c] {
			t.Errorf("ErrorCode enum lacks %q", c)
		}
	}
}

// goName is oapi-codegen's Go name for an operationId (first letter upper);
// the embedded spec stores operationIds in this form.
func goName(id string) string {
	if id == "" {
		return id
	}
	return strings.ToUpper(id[:1]) + id[1:]
}

func allOps(doc *openapi3.T) map[string]*openapi3.Operation {
	ops := map[string]*openapi3.Operation{}
	for path, item := range doc.Paths.Map() {
		for method, op := range item.Operations() {
			ops[method+" "+path] = op
		}
	}
	return ops
}

// walk visits every property (name and schema) declared by s, following
// properties, allOf/oneOf/anyOf, items and additionalProperties.
func walk(s *openapi3.Schema, seen map[*openapi3.Schema]bool, fn func(string, *openapi3.Schema)) {
	if s == nil || seen[s] {
		return
	}
	seen[s] = true
	for name, p := range s.Properties {
		fn(name, p.Value)
		walk(p.Value, seen, fn)
	}
	for _, group := range []openapi3.SchemaRefs{s.AllOf, s.OneOf, s.AnyOf} {
		for _, r := range group {
			walk(r.Value, seen, fn)
		}
	}
	if s.Items != nil {
		walk(s.Items.Value, seen, fn)
	}
	if s.AdditionalProperties.Schema != nil {
		walk(s.AdditionalProperties.Schema.Value, seen, fn)
	}
}
