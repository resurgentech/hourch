package hourchworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
)

// dataClient calls hourchd's worker data routes (bulk transfers) with the
// worker token.
type dataClient struct {
	base     string // https://host
	token    string
	hc       *http.Client
	attempts int
	backoff  func(attempt int) time.Duration
}

// httpError is a non-2xx response.
type httpError struct {
	status int
	body   string
}

func (e *httpError) Error() string {
	if e.body != "" {
		return fmt.Sprintf("hourchd: %d %s", e.status, e.body)
	}
	return fmt.Sprintf("hourchd: %d", e.status)
}

// retryable: network errors and 5xx. 4xx (not_leased, not_found,
// checksum_mismatch, ...) are final.
func retryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var he *httpError
	if errors.As(err, &he) {
		return he.status >= 500
	}
	var fe finalError
	return !errors.As(err, &fe)
}

// finalError wraps an error that must not be retried.
type finalError struct{ err error }

func (e finalError) Error() string { return e.err.Error() }
func (e finalError) Unwrap() error { return e.err }

// retry runs op up to c.attempts times with backoff on retryable errors.
func (c *dataClient) retry(ctx context.Context, op func() error) error {
	var err error
	for i := 0; i < c.attempts; i++ {
		if i > 0 {
			t := time.NewTimer(c.backoff(i))
			select {
			case <-ctx.Done():
				t.Stop()
				return errors.Join(err, context.Cause(ctx))
			case <-t.C:
			}
		}
		err = op()
		if !retryable(err) {
			return err
		}
	}
	return err
}

// escape percent-encodes a relative artifact path as one URL segment
// (slashes become %2F, as the spec asks).
func escape(p string) string { return url.PathEscape(p) }

func (c *dataClient) url(jobID string, parts ...string) string {
	return c.base + "/v1/worker/jobs/" + url.PathEscape(jobID) + "/" + strings.Join(parts, "/")
}

func (c *dataClient) do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, &httpError{status: resp.StatusCode, body: strings.TrimSpace(string(b))}
	}
	return resp, nil
}

func (c *dataClient) get(ctx context.Context, u string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, finalError{err}
	}
	return c.do(req)
}

// hashCheck verifies a streamed body against the X-Sha256 header when the
// server sent one.
func hashCheck(resp *http.Response, got string) error {
	want := strings.ToLower(strings.TrimSpace(resp.Header.Get("X-Sha256")))
	if want != "" && want != got {
		return fmt.Errorf("sha256 mismatch: header %s, body %s", want, got)
	}
	return nil
}

// fetchContext streams the run's context (.tar.zst) into dir. A run
// without a context (404) leaves dir empty.
func (c *dataClient) fetchContext(ctx context.Context, jobID, dir string) error {
	return c.retry(ctx, func() error {
		if err := emptyDir(dir); err != nil {
			return finalError{err}
		}
		resp, err := c.get(ctx, c.url(jobID, "context"))
		var he *httpError
		if errors.As(err, &he) && he.status == http.StatusNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		h := sha256.New()
		zr, err := zstd.NewReader(io.TeeReader(resp.Body, h))
		if err != nil {
			return finalError{err}
		}
		defer zr.Close()
		if err := extractTar(zr, dir); err != nil {
			return err
		}
		// Hash the whole body, including anything after the tar trailer.
		if _, err := io.Copy(io.Discard, io.TeeReader(resp.Body, h)); err != nil {
			return err
		}
		return hashCheck(resp, hex.EncodeToString(h.Sum(nil)))
	})
}

// fetchInput streams one upstream artifact. A path ending in "/" comes as
// an uncompressed tar and is unpacked into dest (relative to workRoot);
// otherwise the file is written at dest.
func (c *dataClient) fetchInput(ctx context.Context, jobID, upstream, p, workRoot, dest string) error {
	u := c.url(jobID, "inputs", url.PathEscape(upstream), escape(p))
	isDir := strings.HasSuffix(p, "/")
	return c.retry(ctx, func() error {
		root, err := os.OpenRoot(workRoot)
		if err != nil {
			return finalError{err}
		}
		defer root.Close()
		resp, err := c.get(ctx, u)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if isDir {
			if err := mkdirAllIn(root, dest); err != nil {
				return finalError{err}
			}
			return extractTar(resp.Body, workRoot+"/"+dest)
		}
		if err := mkdirAllIn(root, path.Dir(dest)); err != nil {
			return finalError{err}
		}
		sum, err := writeFileIn(root, dest, 0o644, resp.Body)
		if err != nil {
			return err
		}
		return hashCheck(resp, sum)
	})
}

// putArtifact streams one file up with its X-Sha256.
func (c *dataClient) putArtifact(ctx context.Context, jobID, name, local string) error {
	sum, size, err := fileSHA256(local)
	if err != nil {
		return err
	}
	u := c.url(jobID, "artifacts", escape(name))
	return c.retry(ctx, func() error {
		f, err := os.Open(local)
		if err != nil {
			return finalError{err}
		}
		defer f.Close()
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, f)
		if err != nil {
			return finalError{err}
		}
		req.ContentLength = size
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Sha256", sum)
		resp, err := c.do(req)
		if err != nil {
			return err
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		return nil
	})
}

// emptyDir makes dir exist and be empty.
func emptyDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return os.MkdirAll(dir, 0o755)
}
