package hourchflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	hourchdapi "github.com/resurgentech/hourch/api"
)

var update = flag.Bool("update", false, "rewrite testdata golden files")

// helloGraph is a small build pipeline: settings -> build -> record ->
// publish (a concurrency key, secrets) plus a container job.
func helloGraph() *Run {
	r := NewRun("hello", map[string]string{"variant": "static", "version": "0.2.0", "repo": "staging"})
	token := r.Secret("PUBLISH_TOKEN", `{"token":"example-token"}`)
	host := r.Secret("PUBLISH_HOST", `{"url":"https://registry.example"}`)
	r.Secret("UNUSED", "never sent")

	settings := r.Job("settings",
		Tags("tiny"),
		Cmd("workflows/hello/hello", "settings", "--variant", "static"),
		Timeout(2*time.Minute),
	)
	build := r.Job("build",
		Tags("kernel"),
		Dir("apps/hello"),
		Cmd("just", "package", "--static",
			"--version", settings.Output("version"),
			"--buildnum="+settings.Output("build_num").String()),
		Env("RUST_LOG", "info"),
		Env("BUILD_NUM", settings.Output("build_num")),
		Artifacts("output/", "build.log", "build_metadata.json"),
		Timeout(2*time.Hour),
		Attempts(2),
	)
	record := r.Job("record",
		Tags("tiny"),
		Cmd("workflows/hello/hello", "record", "--metadata", "inputs/build/build_metadata.json"),
		Inputs(build.Artifact("build_metadata.json")),
	)
	r.Job("publish",
		Tags("tiny"),
		Concurrency("registry"),
		Secrets(token, host),
		Cmd("workflows/hello/hello", "publish", "--dir", "debs"),
		Inputs(build.Artifact("output/").As("debs/")),
		After(record),
	)
	r.Job("lint",
		Image("registry.labtwo/golangci-lint:1.61"),
		Dir("libs/go/hourchd/flow"),
		Cmd("golangci-lint", "run", "./..."),
		Timeout(1500*time.Millisecond), // rounds up to 2s
	)
	return r
}

func TestSpecGolden(t *testing.T) {
	sub, err := helloGraph().Spec()
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.MarshalIndent(sub, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	const golden = "testdata/submission.golden.json"
	if *update {
		if err := os.WriteFile(golden, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run go test -update)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("RunSubmission differs from %s (run go test -update after checking):\n%s", golden, got)
	}
}

func TestDepsInference(t *testing.T) {
	r := helloGraph()
	want := map[string][]string{
		"settings": {},
		"build":    {"settings"},        // Ref in argv, Ref in a string, Ref in env
		"record":   {"build"},           // input
		"publish":  {"build", "record"}, // input + After
		"lint":     {},
	}
	for name, deps := range want {
		got := r.Lookup(name).Deps()
		if !reflect.DeepEqual(got, deps) {
			t.Errorf("%s deps = %v, want %v", name, got, deps)
		}
	}
	// depends_on on the wire is the explicit After list only.
	sub, err := r.Spec()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range sub.Jobs {
		switch j.Name {
		case "publish":
			if j.DependsOn == nil || !reflect.DeepEqual(*j.DependsOn, []string{"record"}) {
				t.Errorf("publish depends_on = %v", j.DependsOn)
			}
		default:
			if j.DependsOn != nil {
				t.Errorf("%s depends_on = %v, want omitted", j.Name, *j.DependsOn)
			}
		}
	}
}

func TestSpecSendsOnlyUsedSecrets(t *testing.T) {
	sub, err := helloGraph().Spec()
	if err != nil {
		t.Fatal(err)
	}
	if sub.Secrets == nil {
		t.Fatal("no secrets")
	}
	if _, ok := (*sub.Secrets)["UNUSED"]; ok {
		t.Error("unused secret was sent")
	}
	if len(*sub.Secrets) != 2 {
		t.Errorf("secrets = %v", *sub.Secrets)
	}
}

func TestPlaceholderParsing(t *testing.T) {
	got := findPlaceholders("a ${outputs.build:x.y.version} b ${outputs.s.k}${outputs.bad} ${outputs.unterminated")
	want := []placeholderRef{{"build:x.y", "version"}, {"s", "k"}, {"bad", ""}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if s := (Ref{}).String(); s != "" {
		t.Errorf("zero Ref renders %q", s)
	}
}

// TestValidationErrors builds one broken run per case and checks the
// problem is reported with the right code.
func TestValidationErrors(t *testing.T) {
	other := NewRun("other", nil)
	otherJob := other.Job("x", Cmd("true"), Artifacts("out"))
	otherSecret := other.Secret("OTHER", "v")

	cases := []struct {
		name  string
		build func(r *Run)
		code  string
		msg   string
	}{
		{"bad workflow", func(r *Run) { r.workflow = "bad name" }, CodeBadRequest, "workflow name"},
		{"too many params", func(r *Run) {
			for i := 0; i <= MaxParams; i++ {
				r.params[strings.Repeat("p", i+1)] = "v"
			}
		}, CodeBadRequest, "params"},
		{"no jobs", func(r *Run) { r.jobs = nil; r.byName = map[string][]*Job{} }, CodeInvalidGraph, "no jobs"},
		{"too many jobs", func(r *Run) {
			for i := 0; i < MaxJobsPerRun; i++ {
				r.Job("j"+strings.Repeat("x", i%100)+string(rune('a'+i/100)), Cmd("true"))
			}
		}, CodeInvalidGraph, "jobs; at most"},
		{"duplicate name", func(r *Run) { r.Job("ok", Cmd("true")) }, CodeInvalidGraph, "used 2 times"},
		{"bad job name", func(r *Run) { r.Job("has space", Cmd("true")) }, CodeBadRequest, "name must match"},
		{"no command", func(r *Run) { r.Job("nocmd") }, CodeBadRequest, "no command"},
		{"empty command", func(r *Run) { r.Job("empty", Cmd()) }, CodeBadRequest, "no command"},
		{"bad argv type", func(r *Run) { r.Job("t", Cmd("echo", 42)) }, CodeBadRequest, "argv[1] has type int"},
		{"bad env type", func(r *Run) { r.Job("t", Cmd("true"), Env("X", 1)) }, CodeBadRequest, "env X has type int"},
		{"bad tag", func(r *Run) { r.Job("t", Cmd("true"), Tags("no space")) }, CodeBadRequest, "tag"},
		{"too many tags", func(r *Run) {
			var tags []string
			for i := 0; i <= MaxTags; i++ {
				tags = append(tags, "t"+strings.Repeat("x", i))
			}
			r.Job("t", Cmd("true"), Tags(tags...))
		}, CodeBadRequest, "tags; at most"},
		{"long image", func(r *Run) { r.Job("t", Cmd("true"), Image(strings.Repeat("i", MaxImageLen+1))) }, CodeBadRequest, "image longer"},
		{"absolute dir", func(r *Run) { r.Job("t", Cmd("true"), Dir("/abs")) }, CodeBadRequest, "must be relative"},
		{"dotdot dir", func(r *Run) { r.Job("t", Cmd("true"), Dir("a/../b")) }, CodeBadRequest, `".." segment`},
		{"bad env name", func(r *Run) { r.Job("t", Cmd("true"), Env("1X", "v")) }, CodeBadRequest, "env name"},
		{"reserved env", func(r *Run) { r.Job("t", Cmd("true"), Env("HOURCH_URL", "v")) }, CodeBadRequest, "reserved"},
		{"env clashes with secret", func(r *Run) {
			s := r.Secret("TOKEN", "v")
			r.Job("t", Cmd("true"), Env("TOKEN", "x"), Secrets(s))
		}, CodeBadRequest, "also a secret"},
		{"secret of other run", func(r *Run) { r.Job("t", Cmd("true"), Secrets(otherSecret)) }, CodeMissingSecret, "another run"},
		{"zero secret", func(r *Run) { r.Job("t", Cmd("true"), Secrets(Secret{})) }, CodeMissingSecret, "zero Secret"},
		{"empty secret value", func(r *Run) { r.Job("t", Cmd("true"), Secrets(r.Secret("EMPTY", ""))) }, CodeMissingSecret, "empty value"},
		{"bad secret name", func(r *Run) { r.Job("t", Cmd("true"), Secrets(r.Secret("bad-name", "v"))) }, CodeBadRequest, "secret name"},
		{"reserved secret name", func(r *Run) { r.Job("t", Cmd("true"), Secrets(r.Secret("HOURCH_X", "v"))) }, CodeBadRequest, "reserved"},
		{"secret set twice", func(r *Run) { r.Secret("S", "a"); r.Secret("S", "b") }, CodeBadRequest, "set twice"},
		{"ref to other run", func(r *Run) { r.Job("t", Cmd("echo", otherJob.Output("v"))) }, CodeInvalidGraph, "another run"},
		{"zero ref", func(r *Run) { r.Job("t", Cmd("echo", Ref{})) }, CodeInvalidGraph, "zero Ref"},
		{"bad output key", func(r *Run) { r.Job("t", Cmd("echo", r.Lookup("ok").Output("a.b"))) }, CodeInvalidGraph, "output key"},
		{"placeholder unknown job", func(r *Run) { r.Job("t", Cmd("echo", "${outputs.nope.v}")) }, CodeInvalidGraph, `unknown job "nope"`},
		{"malformed placeholder", func(r *Run) { r.Job("t", Cmd("echo", "${outputs.ok}")) }, CodeInvalidGraph, "malformed"},
		{"input from other run", func(r *Run) { r.Job("t", Cmd("true"), Inputs(otherJob.Artifact("out"))) }, CodeInvalidGraph, "another run"},
		{"zero input", func(r *Run) { r.Job("t", Cmd("true"), Inputs(Artifact{})) }, CodeInvalidGraph, "zero Artifact"},
		{"input not declared", func(r *Run) { r.Job("t", Cmd("true"), Inputs(r.Lookup("ok").Artifact("missing.txt"))) }, CodeInvalidGraph, "not among the artifacts"},
		{"bad input path", func(r *Run) { r.Job("t", Cmd("true"), Inputs(r.Lookup("ok").Artifact("../x"))) }, CodeBadRequest, `".." segment`},
		{"bad input as", func(r *Run) { r.Job("t", Cmd("true"), Inputs(r.Lookup("ok").Artifact("out/").As("/etc"))) }, CodeBadRequest, "must be relative"},
		{"bad artifact", func(r *Run) { r.Job("t", Cmd("true"), Artifacts("a//b")) }, CodeBadRequest, "empty path segment"},
		{"empty artifact", func(r *Run) { r.Job("t", Cmd("true"), Artifacts("")) }, CodeBadRequest, "empty path"},
		{"too many artifacts", func(r *Run) {
			var a []string
			for i := 0; i <= MaxArtifacts; i++ {
				a = append(a, "f"+strings.Repeat("x", i))
			}
			r.Job("t", Cmd("true"), Artifacts(a...))
		}, CodeBadRequest, "artifacts; at most"},
		{"after nil", func(r *Run) { r.Job("t", Cmd("true"), After(nil)) }, CodeInvalidGraph, "After(nil)"},
		{"after other run", func(r *Run) { r.Job("t", Cmd("true"), After(otherJob)) }, CodeInvalidGraph, "another run"},
		{"long concurrency", func(r *Run) { r.Job("t", Cmd("true"), Concurrency(strings.Repeat("c", MaxConcurrencyLen+1))) }, CodeBadRequest, "concurrency key"},
		{"attempts too high", func(r *Run) { r.Job("t", Cmd("true"), Attempts(MaxAttempts+1)) }, CodeBadRequest, "Attempts"},
		{"attempts negative", func(r *Run) { r.Job("t", Cmd("true"), Attempts(-1)) }, CodeBadRequest, "Attempts"},
		{"zero timeout", func(r *Run) { r.Job("t", Cmd("true"), Timeout(0)) }, CodeBadRequest, "Timeout"},
		{"self cycle", func(r *Run) {
			j := r.Job("t", Cmd("true"))
			After(j)(j)
		}, CodeInvalidGraph, "cycle: t -> t"},
		{"three cycle", func(r *Run) {
			a := r.Job("a", Cmd("echo", "${outputs.c.v}"))
			b := r.Job("b", Cmd("echo", a.Output("v")))
			r.Job("c", Cmd("true"), After(b))
		}, CodeInvalidGraph, "cycle"},
		{"empty context dir", func(r *Run) { r.Context("") }, CodeBadRequest, "Context dir"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRun("wf", nil)
			r.Job("ok", Cmd("true"), Artifacts("out/"))
			tc.build(r)
			err := r.Validate()
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("Validate() = %v, want *ValidationError", err)
			}
			found := false
			for _, p := range ve.Problems {
				if p.Code == tc.code && strings.Contains(p.Message, tc.msg) {
					found = true
				}
			}
			if !found {
				t.Errorf("no %s problem containing %q in: %v", tc.code, tc.msg, err)
			}
			if !ve.Has(tc.code) {
				t.Errorf("Has(%s) = false", tc.code)
			}
			if _, err := r.Spec(); err == nil {
				t.Error("Spec() succeeded on an invalid run")
			}
		})
	}
}

func TestValidRunPasses(t *testing.T) {
	if err := helloGraph().Validate(); err != nil {
		t.Fatal(err)
	}
	// Input coverage rules: a file under a declared dir, a dir over declared files.
	r := NewRun("wf", nil)
	a := r.Job("a", Cmd("true"), Artifacts("out/", "logs/x.log"))
	r.Job("b", Cmd("true"), Inputs(a.Artifact("out/x.deb"), a.Artifact("logs/"), a.Artifact("out/")))
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
}

// TestGoldenMatchesSpecSchema checks the golden submission against the
// RunSubmission schema embedded in hourchdapi (the contract).
func TestGoldenMatchesSpecSchema(t *testing.T) {
	doc, err := hourchdapi.GetSwagger()
	if err != nil {
		t.Fatal(err)
	}
	ref := doc.Components.Schemas["RunSubmission"]
	if ref == nil || ref.Value == nil {
		t.Fatal("RunSubmission schema missing")
	}
	b, err := os.ReadFile("testdata/submission.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	if err := ref.Value.VisitJSON(v); err != nil {
		t.Fatalf("golden does not match RunSubmission: %v", err)
	}
	// A broken document must fail, so the check has teeth.
	v.(map[string]any)["jobs"].([]any)[0].(map[string]any)["bogus"] = 1
	if err := ref.Value.VisitJSON(v); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestCmdReplacesArgv(t *testing.T) {
	r := NewRun("wf", nil)
	s := r.Job("s", Cmd("true"))
	j := r.Job("j", Cmd("echo", s.Output("v")), Cmd("echo", "plain"))
	if got := j.Deps(); len(got) != 0 {
		t.Errorf("deps after second Cmd = %v, want none", got)
	}
}

// A graph can name secrets and have the values filled in before Validate.
func TestDeclareAndSetSecret(t *testing.T) {
	r := NewRun("wf", nil)
	s := r.DeclareSecret("TOKEN")
	r.Job("a", Tags("tiny"), Cmd("true"), Secrets(s))
	if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "TOKEN") {
		t.Fatalf("unset secret: %v", err)
	}
	if err := r.SetSecret("TOKEN", "v1"); err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := r.SetSecret("TOKEN", "v2"); err == nil {
		t.Fatal("changing a set secret should fail")
	}
	if err := r.SetSecret("OTHER", "x"); err == nil {
		t.Fatal("undeclared secret should fail")
	}
	if got := r.SecretNames(); len(got) != 1 || got[0] != "TOKEN" {
		t.Fatalf("SecretNames = %v", got)
	}
}
