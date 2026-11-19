package hourchflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	hourchdapi "github.com/resurgentech/hourch/api"
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
func NewClientFromEnv(opts ...hourchdapi.ClientOption) (*hourchdapi.Client, error) {
	url, err := urlFromEnvOrFile()
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(os.Getenv(EnvHourchdToken))
	if token == "" {
		file := os.Getenv(EnvHourchdTokenFile)
		if file == "" {
			file = DefaultTokenFile()
		}
		b, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("hourchd token: set %s or %s (%w)", EnvHourchdToken, EnvHourchdTokenFile, err)
		}
		token = strings.TrimSpace(string(b))
		if token == "" {
			return nil, fmt.Errorf("hourchd token file %s is empty", file)
		}
	}
	return NewClient(url, token, opts...)
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
