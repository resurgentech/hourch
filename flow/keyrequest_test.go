package hourchflow

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	hourchdapi "github.com/resurgentech/hourch/api"
	"github.com/resurgentech/keyclient/keyclienttest"
	"github.com/resurgentech/keyclient/request"
)

const pendingTail = "key request pending; run this again after approving"

// keyEnv points NewClientFromEnv at hourchd (baseURL) with no HOURCHD_TOKEN and
// a token file that does not exist yet, and replaces the seams: stderr, the
// terminal check, the key request's context (cancelled at cleanup, so a
// background poll ends with the test) and the retry delay. It returns the
// token path and what was written to stderr.
func keyEnv(t *testing.T, baseURL, override string, terminal bool) (string, *syncBuffer) {
	t.Helper()
	tok := filepath.Join(t.TempDir(), "hourchd", "token")
	t.Setenv(EnvHourchdURL, baseURL)
	t.Setenv(EnvHourchdToken, "")
	t.Setenv(EnvHourchdTokenFile, tok)
	t.Setenv(EnvHourchdKeyRequestURL, override)
	t.Setenv("USER", "submitter-user")

	var buf syncBuffer
	oldW, oldTerm, oldCtx, oldDelay := keyRequestStderr, stderrIsTerminal, keyRequestContext, freshKeyRetryDelay
	ctx, cancel := context.WithCancel(context.Background())
	keyRequestStderr = &buf
	stderrIsTerminal = func() bool { return terminal }
	keyRequestContext = func() context.Context { return ctx }
	freshKeyRetryDelay = 50 * time.Millisecond
	t.Cleanup(func() {
		cancel()
		keyRequestStderr, stderrIsTerminal, keyRequestContext, freshKeyRetryDelay = oldW, oldTerm, oldCtx, oldDelay
	})
	return tok, &buf
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// keyedHourchd is a minimal hourchd: GET /v1/runs/{id} and POST /v1/runs answer
// 401 for the first `refuse` requests, then accept the bearer `key`. It records
// every request's method and body.
type keyedHourchd struct {
	srv    *httptest.Server
	mu     sync.Mutex
	key    string
	refuse int
	reqs   []string // "METHOD path body"
}

func newKeyedHourchd(t *testing.T, key string, refuse int) *keyedHourchd {
	h := &keyedHourchd{key: key, refuse: refuse}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.reqs = append(h.reqs, r.Method+" "+r.URL.Path+" "+string(body))
		refuse := h.refuse > 0
		if refuse {
			h.refuse--
		}
		h.mu.Unlock()
		if r.URL.Path == "/v1/key-request" {
			http.NotFound(w, r)
			return
		}
		if refuse || r.Header.Get("Authorization") != "Bearer "+h.key {
			writeErr(w, 401, "unauthorized", "bad token")
			return
		}
		writeJSON(w, 200, map[string]any{})
	}))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *keyedHourchd) requests() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.reqs...)
}

func (h *keyedHourchd) setRefuse(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.refuse = n
}

func noSecret(t *testing.T, p *keyclienttest.Provider, texts ...string) {
	t.Helper()
	for _, s := range texts {
		if sec := p.PollSecret(); (sec != "" && strings.Contains(s, sec)) || strings.Contains(s, p.Key()) {
			t.Fatalf("poll secret or key in %q", s)
		}
	}
}

// T-10 then T-16: no token, no terminal. NewClientFromEnv files a request with
// Wait false and returns an error that wraps *ErrPending, whose text is the
// notice, then SR-9's line. Nothing goes to stderr (the caller prints the
// error). The next run makes the due poll first and returns a client, with no
// notice.
func TestT10_T16_NoTerminalPendingThenNextRunCollects(t *testing.T) {
	p := keyclienttest.NewProvider(t)
	p.SetInterval(1)
	h := newKeyedHourchd(t, p.Key(), 0)
	tok, stderr := keyEnv(t, h.srv.URL, p.RequestURL(), false)

	p.Script(keyclienttest.Pending())
	ctx, cancel := context.WithCancel(context.Background())
	keyRequestContext = func() context.Context { return ctx }
	c, err := NewClientFromEnv()
	var pending *request.ErrPending
	if c != nil || !errors.As(err, &pending) {
		t.Fatalf("got %v, %v; want an error wrapping *ErrPending", c, err)
	}
	want := pending.Notice.String() + "\n" + pendingTail
	if err.Error() != want {
		t.Fatalf("error text\n got %q\nwant %q", err.Error(), want)
	}
	if !strings.Contains(err.Error(), pending.Notice.UserCode) || !strings.Contains(err.Error(), pending.Notice.ApproveURL) {
		t.Fatalf("error text lacks the code or approval address: %q", err)
	}
	noSecret(t, p, err.Error(), stderr.String())
	if stderr.String() != "" {
		t.Fatalf("stderr written without a terminal: %q", stderr.String())
	}
	filed := p.Filed()
	if len(filed) != 1 || filed[0].Role != "submitter" || !strings.HasPrefix(filed[0].Tool, "hourchflow/") ||
		!strings.HasPrefix(filed[0].NameHint, "submitter-user@") {
		t.Fatalf("filed %+v", filed)
	}
	if _, err := os.Stat(request.PendingPath(tok)); err != nil {
		t.Fatalf("pending file: %v", err)
	}
	cancel() // the process exits; its background poll ends, the request stays pending
	<-pending.Done()

	// The next run, once the poll is due: approved, collected, no notice.
	p.Script(keyclienttest.Approved())
	time.Sleep(1100 * time.Millisecond)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	keyRequestContext = func() context.Context { return ctx2 }
	c, err = NewClientFromEnv()
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if stderr.String() != "" {
		t.Fatalf("notice shown on the run that collected the key: %q", stderr.String())
	}
	if len(p.Filed()) != 1 {
		t.Fatalf("filed again: %+v", p.Filed())
	}
	b, err := os.ReadFile(tok)
	if err != nil || strings.TrimSpace(string(b)) != p.Key() {
		t.Fatalf("token file: %v", err)
	}
	resp, err := c.GetRun(context.Background(), "r1")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("call with the new key: %v %v", resp, err)
	}
	resp.Body.Close()
}

// T-1: a token in the environment or in the file: no request, behaviour as
// before, and no retry on 401.
func TestT1_TokenOnDiskNoRequestNoRetry(t *testing.T) {
	p := keyclienttest.NewProvider(t)
	h := newKeyedHourchd(t, "disk-token-0123456789", 1)
	tok, stderr := keyEnv(t, h.srv.URL, p.RequestURL(), true)
	if err := os.MkdirAll(filepath.Dir(tok), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tok, []byte("disk-token-0123456789\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.GetRun(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 || len(h.requests()) != 1 {
		t.Fatalf("status %d after %d requests; want 401 after 1 (no retry for a key on disk)", resp.StatusCode, len(h.requests()))
	}
	if len(p.Filed()) != 0 || stderr.String() != "" {
		t.Fatalf("token on disk, yet filed %v / printed %q", p.Filed(), stderr.String())
	}
}

// T-10 then T-15 with Wait true: stderr is a terminal. The notice goes to
// stderr on one line, Ensure waits for the approval, and the client carries
// on. Its first call, refused once (the service has not pulled the key yet),
// is retried once after the delay.
func TestT15_TerminalWaitsAndRetriesFirstCallOnce(t *testing.T) {
	p := keyclienttest.NewProvider(t)
	p.SetInterval(1)
	h := newKeyedHourchd(t, p.Key(), 1)
	tok, stderr := keyEnv(t, h.srv.URL, p.RequestURL(), true)
	p.Script(keyclienttest.Pending(), keyclienttest.Approved())

	c, err := NewClientFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	out := stderr.String()
	if strings.Count(out, "\n") != 1 || !strings.HasPrefix(out, "Approve this key request for ") ||
		!strings.Contains(out, "role submitter, tool hourchflow/") {
		t.Fatalf("stderr %q; want the notice on one line", out)
	}
	noSecret(t, p, out)
	if b, _ := os.ReadFile(tok); strings.TrimSpace(string(b)) != p.Key() {
		t.Fatal("key not stored")
	}

	start := time.Now()
	resp, err := c.GetRun(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("first call: %d", resp.StatusCode)
	}
	if got := len(h.requests()); got != 2 {
		t.Fatalf("first call made %d requests; want 2 (one retry)", got)
	}
	if d := time.Since(start); d < freshKeyRetryDelay {
		t.Fatalf("retried after %v; want at least %v", d, freshKeyRetryDelay)
	}

	// Only the first call: a later 401 is returned as it is.
	h.setRefuse(1)
	resp, err = c.GetRun(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 || len(h.requests()) != 3 {
		t.Fatalf("second call: %d after %d requests; want 401, no retry", resp.StatusCode, len(h.requests()))
	}
}

// The retry happens once: a first call that is refused twice returns the
// second 401.
func TestFreshKeyRetryNeverTwice(t *testing.T) {
	h := newKeyedHourchd(t, "fresh-key-0123456789", 5)
	setRetryDelay(t, 10*time.Millisecond)
	c := freshClient(t, h.srv.URL, "fresh-key-0123456789")
	resp, err := c.GetRun(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 || len(h.requests()) != 2 {
		t.Fatalf("%d after %d requests; want 401 after 2", resp.StatusCode, len(h.requests()))
	}
}

// Only on 401: a first call answered 200, or another error status, is not
// retried, and disarms the retry.
func TestFreshKeyRetryOnlyOn401(t *testing.T) {
	h := newKeyedHourchd(t, "fresh-key-0123456789", 0)
	setRetryDelay(t, 10*time.Millisecond)
	c := freshClient(t, h.srv.URL, "fresh-key-0123456789")
	for i := 0; i < 2; i++ {
		resp, err := c.GetRun(context.Background(), "r1")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		h.setRefuse(1)
	}
	if got := len(h.requests()); got != 2 {
		t.Fatalf("%d requests; want 2 (no retry after a good first call)", got)
	}
}

// The wait respects the request's context: cancelled during the wait, the
// first call returns at once with the 401 it got, and sends nothing more.
func TestFreshKeyRetryWaitRespectsContext(t *testing.T) {
	h := newKeyedHourchd(t, "fresh-key-0123456789", 1)
	setRetryDelay(t, time.Hour)
	c := freshClient(t, h.srv.URL, "fresh-key-0123456789")
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	resp, err := c.GetRun(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 || len(h.requests()) != 1 || time.Since(start) > 10*time.Second {
		t.Fatalf("%d after %d requests in %v", resp.StatusCode, len(h.requests()), time.Since(start))
	}
}

// A first call with a body that can be replayed (the generated client's JSON
// bodies) is retried with the same body; one whose body cannot be replayed
// (a streamed context upload) is not retried.
func TestFreshKeyRetryReplaysBodyOrSkips(t *testing.T) {
	setRetryDelay(t, 10*time.Millisecond)

	h := newKeyedHourchd(t, "fresh-key-0123456789", 1)
	c := freshClient(t, h.srv.URL, "fresh-key-0123456789")
	resp, err := c.SubmitRun(context.Background(), nil, hourchdapi.RunSubmission{Workflow: "wf"})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	reqs := h.requests()
	if resp.StatusCode != 200 || len(reqs) != 2 || reqs[0] != reqs[1] || !strings.Contains(reqs[1], `"workflow":"wf"`) {
		t.Fatalf("replayable body: %d, %q", resp.StatusCode, reqs)
	}

	h2 := newKeyedHourchd(t, "fresh-key-0123456789", 1)
	c2 := freshClient(t, h2.srv.URL, "fresh-key-0123456789")
	pr, pw := io.Pipe()
	go func() { pw.Write([]byte("streamed")); pw.Close() }()
	resp, err = c2.UploadContextWithBody(context.Background(), nil, "application/octet-stream", pr)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 || len(h2.requests()) != 1 {
		t.Fatalf("streamed body: %d after %d requests; want 401, no retry", resp.StatusCode, len(h2.requests()))
	}
}

// The retry wraps a caller's own HTTP client (hourchdapi.WithHTTPClient).
func TestFreshKeyRetryWrapsCallersClient(t *testing.T) {
	h := newKeyedHourchd(t, "fresh-key-0123456789", 1)
	setRetryDelay(t, 10*time.Millisecond)
	var used bool
	doer := doerFunc(func(r *http.Request) (*http.Response, error) {
		used = true
		return http.DefaultClient.Do(r)
	})
	c := freshClient(t, h.srv.URL, "fresh-key-0123456789", hourchdapi.WithHTTPClient(doer))
	resp, err := c.GetRun(context.Background(), "r1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !used || resp.StatusCode != 200 || len(h.requests()) != 2 {
		t.Fatalf("used=%v status %d requests %d", used, resp.StatusCode, len(h.requests()))
	}
}

type doerFunc func(*http.Request) (*http.Response, error)

func (f doerFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

func setRetryDelay(t *testing.T, d time.Duration) {
	old := freshKeyRetryDelay
	freshKeyRetryDelay = d
	t.Cleanup(func() { freshKeyRetryDelay = old })
}

// freshClient is the client NewClientFromEnv returns right after a fresh key.
func freshClient(t *testing.T, url, key string, opts ...hourchdapi.ClientOption) *hourchdapi.Client {
	t.Helper()
	c, err := newClientFor(url, key, true, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// Final outcomes, worded as SR-9, each wrapping keyclient's error.
func TestFinalOutcomesWordedAsSR9(t *testing.T) {
	cases := []struct {
		name   string
		script []keyclienttest.Outcome
		file   *keyclienttest.Outcome
		is     error
		want   func(err error) string
	}{
		{name: "L-13 denied", script: []keyclienttest.Outcome{keyclienttest.Denied()}, is: request.ErrDenied,
			want: func(error) string { return "key request denied" }},
		{name: "L-15 expired", script: []keyclienttest.Outcome{keyclienttest.Expired()}, is: request.ErrExpired,
			want: func(error) string { return request.ErrExpired.Error() + "\nrun this again to file a new request" }},
		{name: "L-14 consumed", script: []keyclienttest.Outcome{keyclienttest.Consumed()}, is: request.ErrConsumed,
			want: func(error) string { return request.ErrConsumed.Error() + "\nrun this again to file a new request" }},
		{name: "L-16 unauthorized", script: []keyclienttest.Outcome{keyclienttest.Unauthorized()}, is: request.ErrUnauthorized,
			// keyclient's message, unchanged: its sentinel, then the provider's sanitised message.
			want: func(error) string { return request.ErrUnauthorized.Error() + ": unauthorized" }},
		{name: "T-8 provider error with Retry-After",
			file: ptr(keyclienttest.Status(429, `{"error":"rate_limited","message":"too many requests"}`).WithHeader("Retry-After", "30")),
			want: func(err error) string {
				var pe *request.ProviderError
				if !errors.As(err, &pe) {
					return "not a *ProviderError"
				}
				return pe.Error() + "\ntry again in 30s"
			}},
		{name: "T-8 provider error",
			file: ptr(keyclienttest.Status(503, `{"error":"capacity","message":"at capacity"}`)),
			want: func(err error) string {
				var pe *request.ProviderError
				if !errors.As(err, &pe) {
					return "not a *ProviderError"
				}
				return pe.Error()
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := keyclienttest.NewProvider(t)
			p.SetInterval(1)
			h := newKeyedHourchd(t, p.Key(), 0)
			keyEnv(t, h.srv.URL, p.RequestURL(), true)
			if tc.file != nil {
				p.FileWith(*tc.file)
			}
			p.Script(tc.script...)
			c, err := NewClientFromEnv()
			if c != nil || err == nil {
				t.Fatalf("got %v, %v", c, err)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Fatalf("%v does not wrap %v", err, tc.is)
			}
			if want := tc.want(err); err.Error() != want {
				t.Fatalf("error text\n got %q\nwant %q", err.Error(), want)
			}
			noSecret(t, p, err.Error())
		})
	}
}

// T-2 / T-7: no override and discovery answers 404: today's "no token" errors,
// unchanged, and nothing on disk.
func TestT7_NoProviderKeepsTodaysErrors(t *testing.T) {
	h := newKeyedHourchd(t, "unused-0123456789", 0)
	tok, _ := keyEnv(t, h.srv.URL, "", false)
	_, err := NewClientFromEnv()
	_, statErr := os.ReadFile(tok)
	want := "hourchd token: set HOURCHD_TOKEN or HOURCHD_TOKEN_FILE (" + statErr.Error() + ")"
	if err == nil || err.Error() != want || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: %v; want %q", err, want)
	}
	if _, err := os.Stat(filepath.Dir(tok)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("something was created: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(tok), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tok, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = NewClientFromEnv()
	if err == nil || err.Error() != "hourchd token file "+tok+" is empty" {
		t.Fatalf("empty file: %v", err)
	}
	if reqs := h.requests(); len(reqs) != 2 || !strings.HasPrefix(reqs[0], "GET /v1/key-request") {
		t.Fatalf("hourchd saw %q; want two discovery requests", reqs)
	}
}

// Discovery at HOURCHD_URL finds the provider when there is no override.
func TestT10_DiscoveryAtServiceURL(t *testing.T) {
	p := keyclienttest.NewProvider(t)
	keyEnv(t, p.URL(), "", false)
	p.Script(keyclienttest.Pending())
	_, err := NewClientFromEnv()
	var pending *request.ErrPending
	if !errors.As(err, &pending) || len(p.Filed()) != 1 {
		t.Fatalf("%v, filed %d", err, len(p.Filed()))
	}
}

// T-17: a second call in the same process while the first request is open
// joins it: the same notice, no second filing.
func TestT17_SecondCallJoinsNoSecondFiling(t *testing.T) {
	p := keyclienttest.NewProvider(t)
	p.SetInterval(1)
	h := newKeyedHourchd(t, p.Key(), 0)
	keyEnv(t, h.srv.URL, p.RequestURL(), false)
	p.Script(keyclienttest.Pending(), keyclienttest.Pending(), keyclienttest.Pending())
	_, err1 := NewClientFromEnv()
	_, err2 := NewClientFromEnv()
	if err1 == nil || err2 == nil || err1.Error() != err2.Error() || len(p.Filed()) != 1 {
		t.Fatalf("%v / %v, filed %d", err1, err2, len(p.Filed()))
	}
}
