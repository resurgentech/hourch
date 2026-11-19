package graph

import (
	"errors"
	"reflect"
	"testing"
)

func sub(jobs ...Job) Submission {
	return Submission{Workflow: "wf", Secrets: map[string]string{"S": "v"}, Jobs: jobs}
}

func TestEffectiveDepends(t *testing.T) {
	s := sub(
		Job{Name: "settings", Argv: []string{"x"}},
		Job{Name: "build:a.b", Argv: []string{"x", "${outputs.settings.version}"}},
		Job{Name: "record", Argv: []string{"x"}, Inputs: []Input{{Job: "build:a.b", Path: "out/"}}},
		Job{Name: "publish", Argv: []string{"x"}, DependsOn: []string{"record"}, Env: map[string]string{"V": "${outputs.build:a.b.k}"}, Secrets: []string{"S"}},
	)
	deps, err := Validate(s, Limits{MaxJobs: 200})
	if err != nil {
		t.Fatal(err)
	}
	want := [][]string{{}, {"settings"}, {"build:a.b"}, {"build:a.b", "record"}}
	if !reflect.DeepEqual(deps, want) {
		t.Fatalf("deps = %v, want %v", deps, want)
	}
}

func TestPlaceholderKeyIsAfterLastDot(t *testing.T) {
	names := map[string]bool{"a": true, "a.b": true}
	p := Placeholders("${outputs.a.b.c}", names)
	if len(p) != 1 || p[0].Job != "a.b" || p[0].Key != "c" {
		t.Fatalf("got %+v", p)
	}
	if p := Placeholders("${outputs.x.y}", names); len(p) != 1 || p[0].Job != "" {
		t.Fatalf("unknown job resolved: %+v", p)
	}
	out, err := Substitute("v=${outputs.a.b.c} w=${outputs.a.x}", names, map[string]map[string]string{"a.b": {"c": "1"}, "a": {"x": "2"}})
	if err != nil || out != "v=1 w=2" {
		t.Fatalf("substitute = %q %v", out, err)
	}
	_, err = Substitute("${outputs.a.nope}", names, map[string]map[string]string{})
	var mo *MissingOutput
	if !errors.As(err, &mo) || mo.Job != "a" || mo.Key != "nope" {
		t.Fatalf("missing output err = %v", err)
	}
}

func TestValidateErrors(t *testing.T) {
	var bq *BadRequest
	var ig *InvalidGraph
	var ms *MissingSecret
	cases := []struct {
		name string
		s    Submission
		want any
	}{
		{"duplicate", sub(Job{Name: "a", Argv: []string{"x"}}, Job{Name: "a", Argv: []string{"x"}}), &ig},
		{"unknown dep", sub(Job{Name: "a", Argv: []string{"x"}, DependsOn: []string{"b"}}), &ig},
		{"unknown input", sub(Job{Name: "a", Argv: []string{"x"}, Inputs: []Input{{Job: "b", Path: "p"}}}), &ig},
		{"unknown output", sub(Job{Name: "a", Argv: []string{"${outputs.b.k}"}}), &ig},
		{"self cycle", sub(Job{Name: "a", Argv: []string{"x"}, DependsOn: []string{"a"}}), &ig},
		{"cycle", sub(Job{Name: "a", Argv: []string{"x"}, DependsOn: []string{"b"}}, Job{Name: "b", Argv: []string{"${outputs.a.k}"}}), &ig},
		{"too many", Submission{Workflow: "wf", Jobs: []Job{{Name: "a", Argv: []string{"x"}}, {Name: "b", Argv: []string{"x"}}}}, &ig},
		{"missing secret", sub(Job{Name: "a", Argv: []string{"x"}, Secrets: []string{"NOPE"}}), &ms},
		{"bad name", sub(Job{Name: "a b", Argv: []string{"x"}}), &bq},
		{"no argv", sub(Job{Name: "a"}), &bq},
		{"reserved env", sub(Job{Name: "a", Argv: []string{"x"}, Env: map[string]string{"HOURCH_X": "1"}}), &bq},
		{"bad artifact", sub(Job{Name: "a", Argv: []string{"x"}, Artifacts: []string{"../x"}}), &bq},
		{"abs workdir", sub(Job{Name: "a", Argv: []string{"x"}, Workdir: "/etc"}), &bq},
		{"attempts", sub(Job{Name: "a", Argv: []string{"x"}, MaxAttempts: 11}), &bq},
		{"bad workflow", Submission{Workflow: "w f", Jobs: []Job{{Name: "a", Argv: []string{"x"}}}}, &bq},
		{"no jobs", Submission{Workflow: "wf"}, &bq},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			max := 200
			if c.name == "too many" {
				max = 1
			}
			_, err := Validate(c.s, Limits{MaxJobs: max})
			if err == nil || !errors.As(err, c.want) {
				t.Fatalf("err = %v (%T)", err, err)
			}
		})
	}
}

func TestPaths(t *testing.T) {
	for _, p := range []string{"a", "a/b", "out/", "a.b/c-d"} {
		if ValidPath(p) != nil {
			t.Fatalf("%q rejected", p)
		}
	}
	for _, p := range []string{"", "/a", "a/../b", "./a", "a//b", "a/./b", ".."} {
		if ValidPath(p) == nil {
			t.Fatalf("%q accepted", p)
		}
	}
	if !Covers("out/", "out/a/b") || Covers("out/", "outx") || !Covers("f", "f") || Covers("f", "f/x") {
		t.Fatal("covers")
	}
}
