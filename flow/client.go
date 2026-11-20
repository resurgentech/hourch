package hourchflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	hourchdapi "github.com/resurgentech/hourch/api"
	"github.com/resurgentech/keyclient/request"
)

// Environment for submitters and operators (workflow binaries, CLIs).
const (
	// EnvHourchdURL is hourchd's HTTPS base URL. When unset, the one-line
	// file DefaultURLFile() is read; there is no built-in default.
	EnvHourchdURL = "HOURCHD_URL"
	// EnvHourchdToken is a submitter/operator bearer token. It wins over
	// EnvHourchdTokenFile.
	EnvHourchdToken = "HOURCHD_TOKEN"
	// EnvHourchdTokenFile names a file holding the token (whitespace
	// trimmed). Default DefaultTokenFile().
	EnvHourchdTokenFile = "HOURCHD_TOKEN_FILE"
)

// DefaultTokenFile is $XDG_CONFIG_HOME/hourchd/token, or
// ~/.config/hourchd/token.
func DefaultTokenFile() string { return configFile("token") }

// DefaultURLFile is $XDG_CONFIG_HOME/hourchd/url, or ~/.config/hourchd/url:
// one line holding hourchd's base URL, beside the token file.
func DefaultURLFile() string { return configFile("url") }

func configFile(name string) string {
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "hourchd", name)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "hourchd", name)
}

// urlFromEnvOrFile is HOURCHD_URL, else the trimmed contents of DefaultURLFile().
func urlFromEnvOrFile() (string, error) {
	if u := strings.TrimSpace(os.Getenv(EnvHourchdURL)); u != "" {
		return u, nil
	}
	file := DefaultURLFile()
	if file == "" {
		file = "~/.config/hourchd/url"
	}
	b, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("hourchd url file: %w", err)
	}
	if u := strings.TrimSpace(string(b)); u != "" {
		return u, nil
	}
	return "", fmt.Errorf("hourchd URL not set: use %s, or write it to %s", EnvHourchdURL, file)
}

// BearerToken returns a request editor that sets `Authorization: Bearer`.
func BearerToken(token string) hourchdapi.RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
}

// NewClient returns a generated hourchd client for baseURL that sends token
// on every request. TLS uses the system trust store, which holds the lab CA
// on lab machines. The HTTP client has no overall timeout (contexts are large
// and event streams are long-lived); bound calls with the context. opts are
// applied last, so hourchdapi.WithHTTPClient replaces the default client.
func NewClient(baseURL, token string, opts ...hourchdapi.ClientOption) (*hourchdapi.Client, error) {
	if baseURL == "" {
		return nil, errors.New("hourchd URL is empty")
	}
	if token == "" {
		return nil, errors.New("hourchd token is empty")
	}
	if !strings.HasSuffix(baseURL, "/") {
		baseURL += "/"
	}
	all := []hourchdapi.ClientOption{
		hourchdapi.WithHTTPClient(&http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}),
		hourchdapi.WithRequestEditorFn(BearerToken(token)),
	}
	all = append(all, opts...)
	return hourchdapi.NewClient(baseURL, all...)
}

// NewClientFromEnv builds a submitter/operator client from HOURCHD_URL and
// HOURCHD_TOKEN or HOURCHD_TOKEN_FILE (see the Env* constants). The URL is
// HOURCHD_URL, else the file DefaultURLFile(); with neither, it is an error.
//
// With neither HOURCHD_TOKEN nor a token in the token file (missing or
// empty), it asks a key provider for a key (github.com/resurgentech/keyclient,
// package request), for the role "submitter". The request URL is
// HOURCHD_KEY_REQUEST_URL, else what hourchd advertises at
// GET /v1/key-request. When neither gives one, the error is the usual "no
// token" error. Otherwise:
//
//   - Standard error is a terminal: the notice (approve at an address, check a
//     code) is printed there on one line, NewClientFromEnv waits for the
//     decision, stores the key in the token file, and returns the client.
//   - It is not: it returns at once an error that wraps the
//     *request.ErrPending (errors.As works), whose text is the notice and then
//     "key request pending; run this again after approving". The request stays
//     pending; the next run polls it and, once approved, stores the key and
//     returns the client.
//
// Denied, expired and other final outcomes are returned as errors that wrap
// keyclient's. NewClientFromEnv never exits the process and never opens the
// approval address. When this call received the key, it sends one
// GET /v1/stats before returning the client, and once more 5 s later if that
// is refused with 401 (hourchd has not yet pulled the new key); this takes at
// most 20 s, its outcome is ignored, and the caller's own calls are never
// retried. With a token already present nothing extra is sent.
func NewClientFromEnv(opts ...hourchdapi.ClientOption) (*hourchdapi.Client, error) {
	url, err := urlFromEnvOrFile()
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(os.Getenv(EnvHourchdToken))
	fresh := false
	if token == "" {
		file := os.Getenv(EnvHourchdTokenFile)
		if file == "" {
			file = DefaultTokenFile()
		}
		b, err := os.ReadFile(file)
		token = strings.TrimSpace(string(b))
		var noToken error
		switch {
		case err != nil:
			noToken = fmt.Errorf("hourchd token: set %s or %s (%w)", EnvHourchdToken, EnvHourchdTokenFile, err)
		case token == "":
			noToken = fmt.Errorf("hourchd token file %s is empty", file)
		}
		if noToken != nil {
			if file == "" || (err != nil && !errors.Is(err, os.ErrNotExist)) {
				return nil, noToken
			}
			res, err := requestKey(url, file)
			if err != nil {
				return nil, keyRequestError(err, file, noToken)
			}
			token, fresh = res.Token, res.Name != ""
		}
	}
	return newClientFor(url, token, fresh, opts...)
}

// EnvHourchdKeyRequestURL is the key provider's request URL. When set,
// NewClientFromEnv files key requests there instead of asking hourchd
// (GET /v1/key-request) where they go.
const EnvHourchdKeyRequestURL = "HOURCHD_KEY_REQUEST_URL"

// keyRequestRole is the role a key request asks for.
const keyRequestRole = "submitter"

// keyRequestTool is the tool name sent with a key request, before "/<version>".
const keyRequestTool = "hourchflow"

// Seams for tests; they are not options.
var (
	keyRequestStderr  io.Writer = os.Stderr
	stderrIsTerminal            = func() bool { return request.IsTerminal(os.Stderr) }
	keyRequestContext           = context.Background
	// freshKeyRetryDelay is the wait before the one retry of a fresh key's
	// preflight: the contract's default refresh_on_miss.
	freshKeyRetryDelay = 5 * time.Second
	// preflightTimeout bounds the whole preflight, its wait included.
	preflightTimeout = 20 * time.Second
)

// requestKey runs request.Ensure for the token file: waiting for the decision
// when standard error is a terminal, else returning *ErrPending at once.
func requestKey(serviceURL, tokenPath string) (request.Result, error) {
	o := request.Options{
		ServiceURL: serviceURL,
		Override:   strings.TrimSpace(os.Getenv(EnvHourchdKeyRequestURL)),
		TokenPath:  tokenPath,
		Role:       keyRequestRole,
		NameHint:   nameHint(),
		Tool:       keyRequestTool + "/" + moduleVersion(),
		Wait:       stderrIsTerminal(),
	}
	if o.Wait {
		o.Notify = func(n request.Notice) { fmt.Fprintln(keyRequestStderr, n.String()) }
	}
	return request.Ensure(keyRequestContext(), o)
}

// nameHint is $USER@<short host>, or the short host alone when $USER is empty
// (keyclient requires a hint).
func nameHint() string {
	if u := os.Getenv("USER"); u != "" {
		return u + "@" + shortHost()
	}
	return shortHost()
}

// shortHost is the host name up to its first dot.
func shortHost() string {
	h, _ := os.Hostname()
	h, _, _ = strings.Cut(h, ".")
	return h
}

// moduleVersion is this module's version in the binary's build info, or
// "devel" when it has none (a build inside this module, or a workspace).
func moduleVersion() string {
	const path = "github.com/resurgentech/hourch/flow"
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	v := ""
	if bi.Main.Path == path {
		v = bi.Main.Version
	}
	for _, d := range bi.Deps {
		if d.Path == path {
			v = d.Version
		}
	}
	if v == "" || v == "(devel)" {
		return "devel"
	}
	return v
}

// keyError is an error whose text is the tool rules' wording, wrapping
// keyclient's error.
type keyError struct {
	msg string
	err error
}

func (e *keyError) Error() string { return e.msg }
func (e *keyError) Unwrap() error { return e.err }

// keyRequestError words what request.Ensure returned. noToken is the error
// NewClientFromEnv gave before key requests, kept when there is no provider.
func keyRequestError(err error, tokenPath string, noToken error) error {
	var pending *request.ErrPending
	var pe *request.ProviderError
	switch {
	case errors.Is(err, request.ErrNoProvider):
		return noToken
	case errors.As(err, &pending):
		return &keyError{pending.Notice.String() + "\nkey request pending; run this again after approving", err}
	case errors.Is(err, request.ErrBusy):
		return &keyError{"another process is handling the key request for " + tokenPath + "; try again shortly", err}
	case errors.Is(err, request.ErrDenied):
		return &keyError{"key request denied", err}
	case errors.Is(err, request.ErrExpired), errors.Is(err, request.ErrConsumed):
		return &keyError{err.Error() + "\nrun this again to file a new request", err}
	case errors.As(err, &pe) && pe.RetryAfter > 0:
		secs := int64((pe.RetryAfter + time.Second - 1) / time.Second)
		return &keyError{fmt.Sprintf("%s\ntry again in %ds", err.Error(), secs), err}
	default:
		// ErrUnauthorized, *ProviderError and anything else: keyclient's own
		// sanitised message, unchanged.
		return err
	}
}

// newClientFor is NewClient; fresh (the key was received just now) adds the
// preflight before the client is returned.
func newClientFor(url, token string, fresh bool, opts ...hourchdapi.ClientOption) (*hourchdapi.Client, error) {
	c, err := NewClient(url, token, opts...)
	if err != nil || !fresh {
		return c, err
	}
	preflight(c)
	return c, nil
}

// preflight is the first call with a key received just now: GET /v1/stats
// (authenticated, read-only, a small reply). A service that has not pulled
// its key list since the key was issued answers 401; then preflight waits
// freshKeyRetryDelay and sends it once more. It is best effort, bounded by
// preflightTimeout in all, and its outcome is ignored: the caller's own calls,
// which are never retried, report anything that is still wrong.
func preflight(c *hourchdapi.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), preflightTimeout)
	defer cancel()
	if preflightStatus(ctx, c) != http.StatusUnauthorized {
		return
	}
	t := time.NewTimer(freshKeyRetryDelay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
	}
	preflightStatus(ctx, c)
}

func preflightStatus(ctx context.Context, c *hourchdapi.Client) int {
	resp, err := c.GetStats(ctx)
	if err != nil {
		return 0
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	return resp.StatusCode
}

// APIError is a non-success response from hourchd.
type APIError struct {
	Op      string // operationId, e.g. "submitRun"
	Status  int    // HTTP status
	Code    string // Error.error, e.g. "invalid_graph"; empty if the body was not an Error
	Message string // Error.message, or the start of the body
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("hourchd %s: %d %s: %s", e.Op, e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("hourchd %s: HTTP %d: %s", e.Op, e.Status, e.Message)
}

func newAPIError(op string, status int, body []byte) *APIError {
	e := &APIError{Op: op, Status: status}
	var eb hourchdapi.Error
	if json.Unmarshal(body, &eb) == nil && eb.Error != "" {
		e.Code = string(eb.Error)
		e.Message = eb.Message
		return e
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 512 {
		msg = msg[:512] + "…"
	}
	e.Message = msg
	return e
}

func withResponses(c *hourchdapi.Client) *hourchdapi.ClientWithResponses {
	return &hourchdapi.ClientWithResponses{ClientInterface: c}
}
