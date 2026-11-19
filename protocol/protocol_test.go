package hourchdprotocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const schemaFile = "../specs/hourchd/worker-protocol.schema.json"

const (
	jobID = "01JBF6Q9W2X7K3M4N5P6Q7R8S9"
	runID = "01JBF6P000000000000000000Z"
)

// examples has one fully populated message per type (every optional field
// set), so schema validation exercises every Go field.
func examples() []Message {
	return []Message{
		&Hello{WorkerID: "veritech01", Hostname: "veritech01.labtwo", Tags: []string{"kernel"},
			Capabilities: []string{CapabilityDocker}, RunningJobID: jobID, Version: "0.1.0"},
		&Welcome{HeartbeatSeconds: 10, LeaseSeconds: 45, Abandon: jobID, LastLogSeq: 7},
		&Assign{
			Job: AssignedJob{
				ID: jobID, RunID: runID, Name: "publish:hello",
				Image:          "registry.labtwo/golangci-lint:1.61",
				Argv:           []string{"workflows/hello/hello", "publish", "--dir", "inputs/build/output"},
				Workdir:        "apps/hello",
				Env:            map[string]string{"VERSION": "0.2.0"},
				Artifacts:      []string{"output/", "build.log"},
				Inputs:         []Input{{Job: "build", Path: "output/", As: "pkgs"}},
				TimeoutSeconds: 7200,
			},
			Secrets:  map[string]string{"APTLYTOOL_JSON": `{"url":"x"}`},
			Attempt:  1,
			JobToken: "jt-0123456789",
		},
		&Accept{JobID: jobID},
		&Heartbeat{JobID: jobID},
		&Log{JobID: jobID, Stream: StreamStderr, Seq: 3, Data: "building…\n"},
		&Output{JobID: jobID, Key: "build_num", Value: "42"},
		&Cancel{JobID: jobID, Reason: "timeout"},
		&Result{JobID: jobID, ExitCode: 2, Error: "artifact upload failed"},
	}
}

func TestRoundTrip(t *testing.T) {
	for _, m := range examples() {
		data, err := Encode(m)
		if err != nil {
			t.Fatalf("encode %s: %v", m.MessageType(), err)
		}
		got, err := Decode(data)
		if err != nil {
			t.Fatalf("decode %s: %v\n%s", m.MessageType(), err, data)
		}
		if !reflect.DeepEqual(got, m) {
			t.Errorf("%s: round trip\n got %#v\nwant %#v", m.MessageType(), got, m)
		}
		var env Envelope
		if err := json.Unmarshal(data, &env); err != nil || env.Type != m.MessageType() || env.V != Version {
			t.Errorf("%s: envelope %+v, err %v", m.MessageType(), env, err)
		}
	}
}

func TestRoundTripMinimal(t *testing.T) {
	// Optional fields omitted, including an empty body after the envelope.
	for _, m := range []Message{
		&Heartbeat{},
		&Result{JobID: jobID, ExitCode: 0},
		&Hello{WorkerID: "sdf1-1", Hostname: "sdf1.labtwo", Version: "dev"},
	} {
		data, err := Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(data)
		if err != nil {
			t.Fatalf("decode %s: %v", data, err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Errorf("round trip %s: got %#v", data, got)
		}
	}
	data, _ := Encode(&Heartbeat{})
	if string(data) != `{"type":"heartbeat","v":1}` {
		t.Errorf("idle heartbeat = %s", data)
	}
}

func compileSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	sch, err := c.Compile(schemaFile)
	if err != nil {
		t.Fatalf("compile %s: %v", schemaFile, err)
	}
	return sch
}

func TestExamplesValidateAgainstSchema(t *testing.T) {
	sch := compileSchema(t)
	for _, m := range examples() {
		data, err := Encode(m)
		if err != nil {
			t.Fatal(err)
		}
		inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		if err := sch.Validate(inst); err != nil {
			t.Errorf("%s does not validate:\n%s\n%v", m.MessageType(), data, err)
		}
	}
}

func TestSchemaRejectsBadMessages(t *testing.T) {
	sch := compileSchema(t)
	for _, raw := range []string{
		`{"type":"heartbeat","v":2}`,
		`{"type":"nope","v":1}`,
		`{"type":"heartbeat","v":1,"extra":true}`,
		`{"type":"log","v":1,"job_id":"` + jobID + `","stream":"stdin","seq":1,"data":""}`,
		`{"type":"accept","v":1}`,
	} {
		inst, err := jsonschema.UnmarshalJSON(strings.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if sch.Validate(inst) == nil {
			t.Errorf("schema accepted %s", raw)
		}
	}
}

type schemaDoc struct {
	OneOf []struct {
		Ref string `json:"$ref"`
	} `json:"oneOf"`
	Defs map[string]struct {
		Properties map[string]struct {
			Const any `json:"const"`
		} `json:"properties"`
	} `json:"$defs"`
}

// TestSchemaAndGoTypesMatch checks that every message in the schema's oneOf
// has a Go type with the same JSON field names, and vice versa.
func TestSchemaAndGoTypesMatch(t *testing.T) {
	raw, err := os.ReadFile(schemaFile)
	if err != nil {
		t.Fatal(err)
	}
	var doc schemaDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	seen := map[Type]bool{}
	for _, ref := range doc.OneOf {
		name := strings.TrimPrefix(ref.Ref, "#/$defs/")
		def, ok := doc.Defs[name]
		if !ok {
			t.Fatalf("oneOf ref %s has no $defs entry", ref.Ref)
		}
		typ, _ := def.Properties["type"].Const.(string)
		if typ == "" {
			t.Errorf("$defs/%s has no type const", name)
			continue
		}
		seen[Type(typ)] = true
		m, ok := New(Type(typ))
		if !ok {
			t.Errorf("schema message %q has no Go type", typ)
			continue
		}
		var schemaProps []string
		for p := range def.Properties {
			if p != "type" && p != "v" {
				schemaProps = append(schemaProps, p)
			}
		}
		goProps := jsonFields(reflect.TypeOf(m).Elem())
		sort.Strings(schemaProps)
		if strings.Join(schemaProps, ",") != strings.Join(goProps, ",") {
			t.Errorf("%s: schema fields %v, Go fields %v", typ, schemaProps, goProps)
		}
	}
	for _, ty := range Types() {
		if !seen[ty] {
			t.Errorf("Go type %q is not in the schema's oneOf", ty)
		}
	}
}

func jsonFields(rt reflect.Type) []string {
	var out []string
	for i := 0; i < rt.NumField(); i++ {
		name, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
		if name != "" && name != "-" {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func TestDecodeErrors(t *testing.T) {
	cases := []struct {
		raw  string
		want error
	}{
		{`{"type":"nope","v":1}`, ErrUnknownType},
		{`{"type":"heartbeat","v":2}`, ErrVersion},
		{`{"type":"heartbeat","v":0}`, ErrVersion},
		{`{"type":"heartbeat"}`, ErrVersion},
		{`{"type":"heartbeat","v":"1"}`, ErrVersion},
	}
	for _, c := range cases {
		if _, err := Decode([]byte(c.raw)); !errors.Is(err, c.want) {
			t.Errorf("Decode(%s) = %v, want %v", c.raw, err, c.want)
		}
	}
	for _, raw := range []string{
		`not json`,
		`{"v":1}`,
		`{"type":"heartbeat","v":1,"extra":1}`,
		`{"type":"accept","v":1}`,
		`{"type":"log","v":1,"job_id":"` + jobID + `","stream":"stdin","seq":1,"data":""}`,
		`{"type":"log","v":1,"job_id":"` + jobID + `","stream":"stdout","seq":0,"data":""}`,
	} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Errorf("Decode(%s) succeeded", raw)
		}
	}
}

func TestEncodeValidates(t *testing.T) {
	if _, err := Encode(&Assign{}); err == nil {
		t.Error("Encode(empty assign) succeeded")
	}
}
