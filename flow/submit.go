package hourchflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"

	hourchdapi "github.com/resurgentech/hourch/api"
)

// ContextUpload is the result of UploadContext.
type ContextUpload struct {
	Ref     string // "sha256:<hex>", for RunSubmission.context_ref
	SHA256  string // hex digest of the uploaded bytes
	Size    int64  // compressed bytes
	Created bool   // false when hourchd already had these bytes (deduped)
	Stats   ContextStats
}

// UploadContext streams a context tarball of dir (see WriteContext) to
// POST /v1/contexts through an io.Pipe, so the tarball is never held in
// memory or on disk. It hashes what it sends and checks the digest hourchd
// returns. A 200 (`created: false`, bytes already stored) is as good as a
// 201: the returned Ref is reused.
func UploadContext(ctx context.Context, c *hourchdapi.Client, dir string, excludes ...string) (*ContextUpload, error) {
	return uploadContext(ctx, c, dir, ContextOptions{Excludes: excludes})
}

func uploadContext(ctx context.Context, c *hourchdapi.Client, dir string, opts ContextOptions) (*ContextUpload, error) {
	pr, pw := io.Pipe()
	h := sha256.New()
	type result struct {
		st  ContextStats
		err error
	}
	done := make(chan result, 1)
	go func() {
		st, err := WriteContextOpts(io.MultiWriter(pw, h), dir, opts)
		pw.CloseWithError(err) // nil err closes with io.EOF
		done <- result{st, err}
	}()

	resp, err := withResponses(c).UploadContextWithBodyWithResponse(ctx, nil, "application/octet-stream", pr)
	// Unblock the writer if the request ended before reading everything.
	pr.CloseWithError(errUploadEnded)
	res := <-done
	writeErr := res.err
	if errors.Is(writeErr, errUploadEnded) {
		writeErr = nil
	}
	if err != nil {
		if writeErr != nil {
			return nil, fmt.Errorf("context %s: %w", dir, writeErr)
		}
		return nil, fmt.Errorf("upload context: %w", err)
	}
	var body *hourchdapi.ContextUpload
	switch resp.StatusCode() {
	case http.StatusCreated:
		body = resp.JSON201
	case http.StatusOK:
		body = resp.JSON200
	default:
		return nil, newAPIError("uploadContext", resp.StatusCode(), resp.Body)
	}
	if res.err != nil {
		return nil, fmt.Errorf("context %s: %w", dir, res.err)
	}
	if body == nil {
		return nil, fmt.Errorf("upload context: %d with no ContextUpload body", resp.StatusCode())
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if body.Sha256 != sum || body.ContextRef != "sha256:"+sum {
		return nil, fmt.Errorf("upload context: hourchd stored sha256 %s (%s), sent %s", body.Sha256, body.ContextRef, sum)
	}
	return &ContextUpload{
		Ref:     body.ContextRef,
		SHA256:  body.Sha256,
		Size:    body.Size,
		Created: body.Created,
		Stats:   res.st,
	}, nil
}

var errUploadEnded = errors.New("context upload request ended")

// Submitted is what Submit returns.
type Submitted struct {
	ID      string          // run ULID
	Number  int64           // sequential per workflow
	Context *ContextUpload  // nil when the run has no Context
	Run     *hourchdapi.Run // the stored run as hourchd returned it
}

// String is the run id, so `fmt.Printf("run %s", sub)` prints the id.
func (s Submitted) String() string { return s.ID }

// Submit validates the graph (no I/O when it is invalid), uploads the
// context if the run has one, then POSTs the run with its secrets. Every
// call goes through the generated client c (whose request editors carry the
// token). Submit does not retry: a failed POST /v1/runs may or may not have
// created the run, so the caller decides.
func (r *Run) Submit(ctx context.Context, c *hourchdapi.Client) (Submitted, error) {
	sub, err := r.Spec()
	if err != nil {
		return Submitted{}, err
	}
	var out Submitted
	if r.hasContext {
		cu, err := uploadContext(ctx, c, r.contextDir, ContextOptions{Excludes: r.excludes, Include: r.includes})
		if err != nil {
			return Submitted{}, err
		}
		out.Context = cu
		ref := cu.Ref
		sub.ContextRef = &ref
	}
	resp, err := withResponses(c).SubmitRunWithResponse(ctx, nil, sub)
	if err != nil {
		return Submitted{}, fmt.Errorf("submit run: %w", err)
	}
	if resp.StatusCode() != http.StatusCreated || resp.JSON201 == nil {
		return Submitted{}, newAPIError("submitRun", resp.StatusCode(), resp.Body)
	}
	out.ID = resp.JSON201.Id
	out.Number = resp.JSON201.Number
	out.Run = resp.JSON201
	return out, nil
}

// CheckSecretPlaceholder is the value Check sends for every secret: the
// server only checks that each named secret has a non-empty value, so a
// dry run never sends the real ones.
const CheckSecretPlaceholder = "<checked-by-name>"

// Check asks hourchd to run every submission check without storing
// anything (POST /v1/runs?validate_only=true): the graph, that each named
// secret is sent, and that some worker hourchd has seen (or, for jobs
// without an image, a powerwarden host) can run every job. It validates
// locally first (no I/O when invalid), uploads no context and sends no
// context_ref, and sends CheckSecretPlaceholder instead of secret values.
// nil means hourchd would accept the run; a rejection is *APIError (e.g.
// Code "unsatisfiable").
func (r *Run) Check(ctx context.Context, c *hourchdapi.Client) error {
	sub, err := r.Spec()
	if err != nil {
		return err
	}
	if sub.Secrets != nil {
		masked := make(map[string]string, len(*sub.Secrets))
		for k := range *sub.Secrets {
			masked[k] = CheckSecretPlaceholder
		}
		sub.Secrets = &masked
	}
	validate := true
	resp, err := withResponses(c).SubmitRunWithResponse(ctx, &hourchdapi.SubmitRunParams{ValidateOnly: &validate}, sub)
	if err != nil {
		return fmt.Errorf("check run: %w", err)
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil || !resp.JSON200.Valid {
		return newAPIError("submitRun", resp.StatusCode(), resp.Body)
	}
	return nil
}
