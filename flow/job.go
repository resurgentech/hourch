package hourchflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	hourchdapi "github.com/resurgentech/hourch/api"
)

// Environment the worker sets for every job.
const (
	EnvJobURL     = "HOURCH_URL"       // hourchd's HTTPS base
	EnvJobToken   = "HOURCH_JOB_TOKEN" // valid while the job is leased; counters and builds only
	EnvJobOutputs = "HOURCH_OUTPUTS"   // file of key=value lines
	EnvJobRunID   = "HOURCH_RUN_ID"
	EnvJobJobID   = "HOURCH_JOB_ID"
	EnvJobAttempt = "HOURCH_ATTEMPT"
)

// JobEnv is the hourch environment of a running job.
type JobEnv struct {
	URL     string
	Token   string
	Outputs string
	RunID   string
	JobID   string
	Attempt int
}

// JobEnvFromEnv reads the HOURCH_* variables. URL and Token are required;
// the rest are filled when present.
func JobEnvFromEnv() (JobEnv, error) {
	e := JobEnv{
		URL:     os.Getenv(EnvJobURL),
		Token:   os.Getenv(EnvJobToken),
		Outputs: os.Getenv(EnvJobOutputs),
		RunID:   os.Getenv(EnvJobRunID),
		JobID:   os.Getenv(EnvJobJobID),
	}
	if a := os.Getenv(EnvJobAttempt); a != "" {
		n, err := strconv.Atoi(a)
		if err != nil {
			return e, fmt.Errorf("%s=%q: %w", EnvJobAttempt, a, err)
		}
		e.Attempt = n
	}
	var missing []string
	if e.URL == "" {
		missing = append(missing, EnvJobURL)
	}
	if e.Token == "" {
		missing = append(missing, EnvJobToken)
	}
	if len(missing) > 0 {
		return e, fmt.Errorf("not running under an hourch worker: %s unset", strings.Join(missing, ", "))
	}
	return e, nil
}

// JobClient calls the job-scoped routes (counters, builds) with the job
// token.
type JobClient struct {
	api *hourchdapi.ClientWithResponses
}

// NewJobClient returns a JobClient for baseURL and the job token. opts are
// applied last (tests pass hourchdapi.WithHTTPClient).
func NewJobClient(baseURL, token string, opts ...hourchdapi.ClientOption) (*JobClient, error) {
	c, err := NewClient(baseURL, token, opts...)
	if err != nil {
		return nil, err
	}
	return &JobClient{api: withResponses(c)}, nil
}

// NewJobClientFromEnv returns a JobClient from HOURCH_URL and
// HOURCH_JOB_TOKEN.
func NewJobClientFromEnv(opts ...hourchdapi.ClientOption) (*JobClient, error) {
	e, err := JobEnvFromEnv()
	if err != nil {
		return nil, err
	}
	return NewJobClient(e.URL, e.Token, opts...)
}

// NextCounter atomically increments counter name and returns the new value
// (the first is 1). Replaces RDB.Incr.
func (jc *JobClient) NextCounter(ctx context.Context, name string) (int64, error) {
	resp, err := jc.api.NextCounterWithResponse(ctx, name)
	if err != nil {
		return 0, fmt.Errorf("next counter %s: %w", name, err)
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return 0, newAPIError("nextCounter", resp.StatusCode(), resp.Body)
	}
	return resp.JSON200.Value, nil
}

// PutBuild stores body (your build metadata, any JSON object)
// under build type, flavour and number. A second PUT for the same key
// replaces the row.
func (jc *JobClient) PutBuild(ctx context.Context, buildType, flavour string, num int64, body any) (*hourchdapi.Build, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("build body: %w", err)
	}
	var obj hourchdapi.BuildBody
	if err := json.Unmarshal(b, &obj); err != nil || obj == nil {
		return nil, errors.New("build body must be a JSON object")
	}
	resp, err := jc.api.PutBuildWithResponse(ctx, buildType, flavour, num, obj)
	if err != nil {
		return nil, fmt.Errorf("put build %s/%s/%d: %w", buildType, flavour, num, err)
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return nil, newAPIError("putBuild", resp.StatusCode(), resp.Body)
	}
	return resp.JSON200, nil
}

// RecordBuild takes the next number from counter
// BuildCounterName(buildType, flavour), lets stamp put it into your build
// metadata (any JSON object) and return the body, then PUTs it. It returns
// the number and the stored build.
func (jc *JobClient) RecordBuild(ctx context.Context, buildType, flavour string, stamp func(num int64) any) (int64, *hourchdapi.Build, error) {
	num, err := jc.NextCounter(ctx, BuildCounterName(buildType, flavour))
	if err != nil {
		return 0, nil, err
	}
	b, err := jc.PutBuild(ctx, buildType, flavour, num, stamp(num))
	if err != nil {
		return num, nil, err
	}
	return num, b, nil
}

// BuildFlavour is `distro_arch[_flavour]`, e.g. BuildFlavour("debian12",
// "amd64", "") is `debian12_amd64`.
func BuildFlavour(distro, arch, flavour string) string {
	f := distro + "_" + arch
	if flavour != "" {
		f += "_" + flavour
	}
	return f
}

// BuildCounterName is the counter for a build type and flavour,
// `build:<type>:<flavour>`, e.g. `build:hello:debian12_amd64`.
func BuildCounterName(buildType, flavour string) string {
	return "build:" + buildType + ":" + flavour
}

var reOutputLineKey = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

// AppendOutput appends `key=value\n` to the outputs file at path. The key
// must match the protocol's output key (`[A-Za-z0-9_.-]{1,128}`; flow Refs
// further exclude '.'); the value is one line of at most MaxOutputValueSize
// bytes.
func AppendOutput(path, key, value string) error {
	if !reOutputLineKey.MatchString(key) {
		return fmt.Errorf("output key %q must match %s", key, reOutputLineKey)
	}
	if strings.ContainsAny(value, "\r\n") {
		return fmt.Errorf("output %s: value must be one line", key)
	}
	if len(value) > MaxOutputValueSize {
		return fmt.Errorf("output %s: value is %d bytes; at most %d", key, len(value), MaxOutputValueSize)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(key + "=" + value + "\n"); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// WriteOutput appends key=value to $HOURCH_OUTPUTS. Downstream jobs read it
// as Job.Output(key).
func WriteOutput(key, value string) error {
	path := os.Getenv(EnvJobOutputs)
	if path == "" {
		return fmt.Errorf("%s is unset (not running under an hourch worker)", EnvJobOutputs)
	}
	return AppendOutput(path, key, value)
}

// WriteOutputs writes several outputs in key order.
func WriteOutputs(kv map[string]string) error {
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := WriteOutput(k, kv[k]); err != nil {
			return err
		}
	}
	return nil
}
