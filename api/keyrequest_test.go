package hourchdapi

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

const fixtureDir = "../specs/fixtures/keyrequest/"

// fixture is one exchange of the key-request contract, written as an OpenAPI
// Example Object (specs/fixtures/README.md).
type fixture struct {
	Status  int               `json:"x-status"`
	Headers map[string]string `json:"x-headers"`
	Value   json.RawMessage   `json:"value"`
}

func loadFixture(t *testing.T, name string) fixture {
	t.Helper()
	data, err := os.ReadFile(fixtureDir + name)
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return f
}

// TestKeyRequestDiscoverySpec checks GET /v1/key-request against the
// contract's fixtures: no authentication, a 200 whose example is the
// discovery fixture's body and whose schema requires request_url, and a 404.
func TestKeyRequestDiscoverySpec(t *testing.T) {
	file, _ := loadSpecs(t)
	item := file.Paths.Find("/v1/key-request")
	if item == nil || item.Get == nil {
		t.Fatal("GET /v1/key-request missing from spec")
	}
	op := item.Get
	if op.OperationID != "getKeyRequestDiscovery" {
		t.Errorf("operationId %q, want getKeyRequestDiscovery", op.OperationID)
	}
	if op.Security == nil || len(*op.Security) != 0 {
		t.Errorf("GET /v1/key-request must need no authentication (security: [])")
	}

	ok := loadFixture(t, "discovery.json")
	if ok.Status != 200 {
		t.Fatalf("discovery.json x-status %d", ok.Status)
	}
	r200 := op.Responses.Status(200)
	if r200 == nil || r200.Value.Content["application/json"] == nil {
		t.Fatal("GET /v1/key-request has no 200 application/json response")
	}
	mt := r200.Value.Content["application/json"]
	schema := mt.Schema.Value
	if len(schema.Required) != 1 || schema.Required[0] != "request_url" {
		t.Errorf("200 schema required %v, want [request_url]", schema.Required)
	}
	ex, found := mt.Examples["discovery"]
	if !found || ex.Value == nil {
		t.Fatal("200 response has no example named discovery")
	}
	var want, got any
	if err := json.Unmarshal(ok.Value, &want); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(ex.Value.Value)
	_ = json.Unmarshal(b, &got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("spec example %v, fixture value %v", got, want)
	}
	if err := schema.VisitJSON(want); err != nil {
		t.Errorf("fixture value does not match the schema: %v", err)
	}

	// The generated type decodes the fixture.
	var d KeyRequestDiscovery
	if err := json.Unmarshal(ok.Value, &d); err != nil || d.RequestUrl == "" {
		t.Errorf("KeyRequestDiscovery from fixture: %+v, %v", d, err)
	}

	none := loadFixture(t, "discovery-none.json")
	if none.Status != 404 || len(none.Value) != 0 {
		t.Fatalf("discovery-none.json: status %d, value %s", none.Status, none.Value)
	}
	if op.Responses.Status(404) == nil {
		t.Error("GET /v1/key-request has no 404 response")
	}
}
