package hourchdapi

import (
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// personalKeysStatusSchema is the PersonalKeysStatus schema of the shared
// key-list contract, word for word. Every daemon that serves the key list
// copies it into its spec unchanged, so this test compares text, not shape.
//
// One deviation from the shared text: the last_error description is in
// double quotes. Unquoted, the commas in it end the value inside the YAML
// flow mapping, which cuts the description short and adds two keys that make
// the spec invalid.
const personalKeysStatusSchema = `PersonalKeysStatus:
  type: object
  required: [origin, entries, cache_failing]
  properties:
    origin:        {type: string, enum: [none, cache, provider]}
    entries:       {type: integer}
    age_seconds:   {type: integer, description: Age of the live list; absent when origin is none}
    list_time:     {type: string, format: date-time, description: When the live list was last known good; absent when origin is none}
    last_success:  {type: string, format: date-time, description: Last good pull in this process; absent if none}
    last_error:    {type: string, description: "The latest pull error, made safe (printable, at most 200 characters); absent when the latest pull succeeded"}
    cache_failing: {type: boolean}
`

// TestPersonalKeysStatusSchemaVerbatim checks that the spec carries the
// shared PersonalKeysStatus schema word for word (under components.schemas,
// so indented by four spaces).
func TestPersonalKeysStatusSchemaVerbatim(t *testing.T) {
	data, err := os.ReadFile(specFile)
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, line := range strings.SplitAfter(personalKeysStatusSchema, "\n") {
		if line != "" {
			b.WriteString("    " + line)
		}
	}
	if !strings.Contains(string(data), b.String()) {
		t.Errorf("spec does not hold the PersonalKeysStatus schema word for word; want:\n%s", b.String())
	}
}

// TestStatsPersonalKeys checks that /v1/stats carries personal_keys as an
// optional PersonalKeysStatus, and that the schema validates as specified.
func TestStatsPersonalKeys(t *testing.T) {
	file, _ := loadSpecs(t)
	st := file.Components.Schemas["Stats"]
	if st == nil {
		t.Fatal("schema Stats missing")
	}
	pk, ok := st.Value.Properties["personal_keys"]
	if !ok {
		t.Fatal("Stats has no personal_keys property")
	}
	if pk.Ref != "#/components/schemas/PersonalKeysStatus" {
		t.Errorf("Stats.personal_keys ref = %q, want #/components/schemas/PersonalKeysStatus", pk.Ref)
	}
	if slices.Contains(st.Value.Required, "personal_keys") {
		t.Error("Stats.personal_keys must be optional (present only when personal_keys.url is set)")
	}
	pks := file.Components.Schemas["PersonalKeysStatus"]
	if pks == nil {
		t.Fatal("schema PersonalKeysStatus missing")
	}
	if got := strings.Join(pks.Value.Required, ","); got != "origin,entries,cache_failing" {
		t.Errorf("PersonalKeysStatus required = %s", got)
	}
	var props []string
	for name := range pks.Value.Properties {
		props = append(props, name)
	}
	slices.Sort(props)
	if got := strings.Join(props, ","); got != "age_seconds,cache_failing,entries,last_error,last_success,list_time,origin" {
		t.Errorf("PersonalKeysStatus properties = %s", got)
	}
	const lastErr = "The latest pull error, made safe (printable, at most 200 characters); absent when the latest pull succeeded"
	if got := pks.Value.Properties["last_error"].Value.Description; got != lastErr {
		t.Errorf("last_error description = %q, want %q", got, lastErr)
	}
	ok200 := map[string]any{"origin": "provider", "entries": 2, "age_seconds": 3,
		"list_time": "2026-01-02T03:04:05Z", "last_success": "2026-01-02T03:04:05Z", "cache_failing": false}
	if err := pks.Value.VisitJSON(ok200); err != nil {
		t.Errorf("a full status does not validate: %v", err)
	}
	if err := pks.Value.VisitJSON(map[string]any{"origin": "none", "entries": 0, "cache_failing": false}); err != nil {
		t.Errorf("a minimal status does not validate: %v", err)
	}
	if err := pks.Value.VisitJSON(map[string]any{"origin": "elsewhere", "entries": 0, "cache_failing": false}); err == nil {
		t.Error("an unknown origin validates")
	}
}

// TestPersonalKeysStatusType checks the generated type: Stats.PersonalKeys
// is a pointer (absent when nil) and the optional fields are omitted when
// unset.
func TestPersonalKeysStatusType(t *testing.T) {
	var s Stats
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "personal_keys") {
		t.Errorf("Stats without personal keys marshals %s", b)
	}
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	age := 3
	s.PersonalKeys = &PersonalKeysStatus{Origin: Provider, Entries: 2, AgeSeconds: &age,
		ListTime: &ts, LastSuccess: &ts}
	b, err = json.Marshal(s.PersonalKeys)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"age_seconds":3,"cache_failing":false,"entries":2,"last_success":"2026-01-02T03:04:05Z","list_time":"2026-01-02T03:04:05Z","origin":"provider"}`
	if string(b) != want {
		t.Errorf("marshal = %s\nwant      %s", b, want)
	}
	none := PersonalKeysStatus{Origin: None}
	b, _ = json.Marshal(none)
	if string(b) != `{"cache_failing":false,"entries":0,"origin":"none"}` {
		t.Errorf("none marshals %s", b)
	}
	_ = Cache
}
