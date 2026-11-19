package hourchflow

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	hourchdapi "github.com/resurgentech/hourch/api"
)

// buildMetadata is an example of build metadata (any JSON object).
type buildMetadata struct {
	Version  string `json:"version"`
	Distro   string `json:"distro"`
	Arch     string `json:"arch"`
	Flavour  string `json:"flavour,omitempty"`
	BuildNum string `json:"build_num,omitempty"`
}

func jobClient(t *testing.T, f *fakeHourchd, token string) *JobClient {
	t.Helper()
	jc, err := NewJobClient(f.srv.URL, token, hourchdapi.WithHTTPClient(f.srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	return jc
}

func TestJobClientCounterAndBuild(t *testing.T) {
	f := newFake(t)
	jc := jobClient(t, f, testJobToken)
	ctx := context.Background()

	for want := int64(1); want <= 2; want++ {
		n, err := jc.NextCounter(ctx, "build:hello:debian12_amd64")
		if err != nil || n != want {
			t.Fatalf("NextCounter = %d, %v; want %d", n, err, want)
		}
	}

	md := buildMetadata{Version: "0.2.0", Distro: "debian12", Arch: "amd64"}
	flavour := BuildFlavour(md.Distro, md.Arch, md.Flavour)
	if flavour != "debian12_amd64" {
		t.Fatalf("flavour = %s", flavour)
	}
	num, b, err := jc.RecordBuild(ctx, "hello", flavour, func(n int64) any {
		md.BuildNum = strconv.FormatInt(n, 10)
		return md
	})
	if err != nil {
		t.Fatal(err)
	}
	if num != 3 || b.BuildNum != 3 || b.BuildType != "hello" || b.Flavour != flavour {
		t.Errorf("RecordBuild = %d, %+v", num, b)
	}
	var stored buildMetadata
	if err := json.Unmarshal(f.builds["hello/debian12_amd64/3"], &stored); err != nil {
		t.Fatal(err)
	}
	if stored != md || stored.BuildNum != "3" {
		t.Errorf("stored %+v, want %+v", stored, md)
	}
	if f.counters[BuildCounterName("hello", flavour)] != 3 {
		t.Errorf("counters = %v", f.counters)
	}
	if BuildFlavour("debian12", "amd64", "slim") != "debian12_amd64_slim" {
		t.Error("BuildFlavour with flavour")
	}
}

func TestJobClientErrors(t *testing.T) {
	f := newFake(t)
	ctx := context.Background()
	bad := jobClient(t, f, "not-the-job-token")
	_, err := bad.NextCounter(ctx, "c")
	var ae *APIError
	if !errors.As(err, &ae) || ae.Status != 401 || ae.Op != "nextCounter" {
		t.Fatalf("NextCounter err = %v", err)
	}
	if _, err := bad.PutBuild(ctx, "kernel", "f", 1, map[string]string{"a": "b"}); !errors.As(err, &ae) || ae.Status != 401 {
		t.Fatalf("PutBuild err = %v", err)
	}
	good := jobClient(t, f, testJobToken)
	if _, err := good.PutBuild(ctx, "kernel", "f", 1, []int{1}); err == nil {
		t.Error("non-object body accepted")
	}
	if _, _, err := bad.RecordBuild(ctx, "kernel", "f", func(int64) any { t.Error("stamp called after counter failed"); return nil }); err == nil {
		t.Error("RecordBuild succeeded with a bad token")
	}
	if _, err := NewJobClient("", "x"); err == nil {
		t.Error("empty URL accepted")
	}
}

func TestJobEnvFromEnv(t *testing.T) {
	t.Setenv(EnvJobURL, "")
	t.Setenv(EnvJobToken, "")
	if _, err := JobEnvFromEnv(); err == nil || !strings.Contains(err.Error(), EnvJobURL) || !strings.Contains(err.Error(), EnvJobToken) {
		t.Fatalf("err = %v", err)
	}
	if _, err := NewJobClientFromEnv(); err == nil {
		t.Error("NewJobClientFromEnv without env")
	}

	f := newFake(t)
	t.Setenv(EnvJobURL, f.srv.URL)
	t.Setenv(EnvJobToken, testJobToken)
	t.Setenv(EnvJobOutputs, "/tmp/out")
	t.Setenv(EnvJobRunID, "R")
	t.Setenv(EnvJobJobID, "J")
	t.Setenv(EnvJobAttempt, "2")
	e, err := JobEnvFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	want := JobEnv{URL: f.srv.URL, Token: testJobToken, Outputs: "/tmp/out", RunID: "R", JobID: "J", Attempt: 2}
	if e != want {
		t.Errorf("env = %+v", e)
	}
	jc, err := NewJobClientFromEnv(hourchdapi.WithHTTPClient(f.srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	if n, err := jc.NextCounter(context.Background(), "c"); err != nil || n != 1 {
		t.Errorf("NextCounter via env = %d, %v", n, err)
	}
	t.Setenv(EnvJobAttempt, "x")
	if _, err := JobEnvFromEnv(); err == nil {
		t.Error("bad attempt accepted")
	}
}

func TestOutputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outputs")
	t.Setenv(EnvJobOutputs, path)
	if err := WriteOutput("version", "0.2.0"); err != nil {
		t.Fatal(err)
	}
	if err := WriteOutputs(map[string]string{"build_num": "7", "a.b": "x=y"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "version=0.2.0\na.b=x=y\nbuild_num=7\n" {
		t.Errorf("outputs file = %q", b)
	}
	fi, _ := os.Stat(path)
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %o", fi.Mode().Perm())
	}

	for _, c := range []struct{ k, v string }{
		{"", "v"},
		{"bad key", "v"},
		{"k", "two\nlines"},
		{"k", strings.Repeat("v", MaxOutputValueSize+1)},
	} {
		if err := WriteOutput(c.k, c.v); err == nil {
			t.Errorf("WriteOutput(%q, %d bytes) accepted", c.k, len(c.v))
		}
	}
	t.Setenv(EnvJobOutputs, "")
	if err := WriteOutput("k", "v"); err == nil {
		t.Error("WriteOutput without HOURCH_OUTPUTS")
	}
}

func TestNewClientFromEnv(t *testing.T) {
	f := newFake(t)
	seedRun(f, "succeeded")
	ctx := context.Background()

	t.Setenv(EnvHourchdURL, f.srv.URL)
	t.Setenv(EnvHourchdToken, testToken)
	c, err := NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Wait(ctx, c, runID); err != nil {
		t.Fatalf("token from env: %v", err)
	}

	tokFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokFile, []byte("  "+testToken+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvHourchdToken, "")
	t.Setenv(EnvHourchdTokenFile, tokFile)
	c, err = NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Wait(ctx, c, runID); err != nil {
		t.Fatalf("token from file: %v", err)
	}

	t.Setenv(EnvHourchdTokenFile, filepath.Join(t.TempDir(), "missing"))
	if _, err := NewClientFromEnv(); err == nil {
		t.Error("missing token file accepted")
	}
	empty := filepath.Join(t.TempDir(), "empty")
	os.WriteFile(empty, []byte("\n"), 0o600)
	t.Setenv(EnvHourchdTokenFile, empty)
	if _, err := NewClientFromEnv(); err == nil {
		t.Error("empty token file accepted")
	}

	t.Setenv("XDG_CONFIG_HOME", "/xdg")
	if got := DefaultTokenFile(); got != "/xdg/hourchd/token" {
		t.Errorf("DefaultTokenFile = %s", got)
	}
}

func TestDefaultURLFile(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/xdg")
	if got := DefaultURLFile(); got != "/xdg/hourchd/url" {
		t.Errorf("DefaultURLFile = %s", got)
	}
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("HOME", "/home/x")
	if got := DefaultURLFile(); got != "/home/x/.config/hourchd/url" {
		t.Errorf("DefaultURLFile = %s", got)
	}
}

// URL: env beats the per-user file; with neither the error names both.
func TestNewClientFromEnvURLSources(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv(EnvHourchdToken, "x")
	t.Setenv(EnvHourchdURL, "")

	_, err := NewClientFromEnv()
	if err == nil || !strings.Contains(err.Error(), EnvHourchdURL) || !strings.Contains(err.Error(), filepath.Join(xdg, "hourchd", "url")) {
		t.Fatalf("nothing set: %v", err)
	}

	if err := os.MkdirAll(filepath.Join(xdg, "hourchd"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(DefaultURLFile(), []byte("  https://file.example.com \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := NewClientFromEnv()
	if err != nil || c.Server != "https://file.example.com/" {
		t.Fatalf("file: %v %v", c, err)
	}

	t.Setenv(EnvHourchdURL, "https://env.example.com")
	c, err = NewClientFromEnv()
	if err != nil || c.Server != "https://env.example.com/" {
		t.Fatalf("env over file: %v %v", c, err)
	}
}
