package hourchflow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func contextRun(t *testing.T) (*Run, string) {
	t.Helper()
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{
		"README.md":             "hi",
		".git/HEAD":             "ref",
		"apps/hello/output/old": "stale",
	})
	r := helloGraph()
	r.Context(dir, "apps/*/output")
	return r, dir
}

func TestSubmitUploadsContextThenRun(t *testing.T) {
	f := newFake(t)
	c := f.client(t)
	r, _ := contextRun(t)

	got, err := r.Submit(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == "" || got.Number != 1 || got.String() != got.ID || got.Run == nil {
		t.Fatalf("Submitted = %+v", got)
	}
	if got.Context == nil || !got.Context.Created || !strings.HasPrefix(got.Context.Ref, "sha256:") {
		t.Fatalf("Context = %+v", got.Context)
	}

	if len(f.ctxReqs) != 1 {
		t.Fatalf("%d context uploads", len(f.ctxReqs))
	}
	cr := f.ctxReqs[0]
	if cr.auth != "Bearer "+testToken {
		t.Errorf("context Authorization = %q", cr.auth)
	}
	if cr.contentType != "application/octet-stream" {
		t.Errorf("context Content-Type = %q", cr.contentType)
	}
	// Streamed: no Content-Length, chunked transfer.
	if cr.contentLength != -1 || !reflect.DeepEqual(cr.transferEncoding, []string{"chunked"}) {
		t.Errorf("context not streamed: Content-Length %d, Transfer-Encoding %v", cr.contentLength, cr.transferEncoding)
	}
	entries, _ := untar(t, cr.body)
	if _, ok := entries["README.md"]; !ok {
		t.Error("README.md missing from uploaded context")
	}
	for name := range entries {
		if strings.HasPrefix(name, ".git") || strings.HasPrefix(name, "apps/hello/output") {
			t.Errorf("excluded %s uploaded", name)
		}
	}

	if len(f.submissions) != 1 {
		t.Fatalf("%d submissions", len(f.submissions))
	}
	sub := f.submissions[0]
	want, err := r.Spec()
	if err != nil {
		t.Fatal(err)
	}
	ref := got.Context.Ref
	want.ContextRef = &ref
	if !reflect.DeepEqual(sub, want) {
		t.Errorf("submitted run differs from Spec:\n got %+v\nwant %+v", sub, want)
	}
	if sub.Secrets == nil || (*sub.Secrets)["PUBLISH_HOST"] == "" {
		t.Error("secret values not sent")
	}
}

func TestSubmitReusesDedupedContext(t *testing.T) {
	f := newFake(t)
	c := f.client(t)
	r, dir := contextRun(t)
	first, err := r.Submit(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	// A fresh Run over the same tree produces the same bytes: 200 created:false.
	r2 := helloGraph()
	r2.Context(dir, "apps/*/output")
	second, err := r2.Submit(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if second.Context.Created {
		t.Error("second upload reported created:true")
	}
	if second.Context.Ref != first.Context.Ref {
		t.Errorf("refs differ: %s vs %s", first.Context.Ref, second.Context.Ref)
	}
	if *f.submissions[1].ContextRef != first.Context.Ref {
		t.Error("deduped ref not used in the run")
	}
	if second.Number != 2 {
		t.Errorf("second run number = %d", second.Number)
	}
}

func TestSubmitWithoutContext(t *testing.T) {
	f := newFake(t)
	r := NewRun("wf", nil)
	r.Job("a", Cmd("true"))
	got, err := r.Submit(context.Background(), f.client(t))
	if err != nil {
		t.Fatal(err)
	}
	if got.Context != nil || len(f.ctxReqs) != 0 || f.submissions[0].ContextRef != nil {
		t.Error("context uploaded or referenced for a run without Context")
	}
}

func TestSubmitInvalidGraphDoesNoIO(t *testing.T) {
	f := newFake(t)
	r, _ := contextRun(t)
	r.Job("bad", Cmd("echo", "${outputs.nope.v}"))
	_, err := r.Submit(context.Background(), f.client(t))
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v", err)
	}
	if len(f.ctxReqs) != 0 || f.runReqs != 0 {
		t.Error("requests made for an invalid graph")
	}
}

func TestSubmitServerErrors(t *testing.T) {
	t.Run("run 422", func(t *testing.T) {
		f := newFake(t)
		f.runStatus = 422
		r := NewRun("wf", nil)
		r.Job("a", Cmd("true"))
		_, err := r.Submit(context.Background(), f.client(t))
		var ae *APIError
		if !errors.As(err, &ae) || ae.Status != 422 || ae.Code != "invalid_graph" || ae.Op != "submitRun" {
			t.Fatalf("err = %#v", err)
		}
		if !strings.Contains(ae.Error(), "biuld") {
			t.Errorf("message lost: %v", ae)
		}
	})
	t.Run("context 413", func(t *testing.T) {
		f := newFake(t)
		f.ctxStatus = 413
		r, _ := contextRun(t)
		_, err := r.Submit(context.Background(), f.client(t))
		var ae *APIError
		if !errors.As(err, &ae) || ae.Status != 413 || ae.Code != "too_large" {
			t.Fatalf("err = %#v", err)
		}
		if f.runReqs != 0 {
			t.Error("run submitted after a failed upload")
		}
	})
	t.Run("checksum mismatch", func(t *testing.T) {
		f := newFake(t)
		f.ctxBadSum = true
		r, _ := contextRun(t)
		if _, err := r.Submit(context.Background(), f.client(t)); err == nil || !strings.Contains(err.Error(), "sha256") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("bad token", func(t *testing.T) {
		f := newFake(t)
		c, err := NewClient(f.srv.URL, "wrong")
		if err != nil {
			t.Fatal(err)
		}
		r := NewRun("wf", nil)
		r.Job("a", Cmd("true"))
		_, err = r.Submit(context.Background(), c)
		var ae *APIError
		if !errors.As(err, &ae) || ae.Status != 401 {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unreadable context", func(t *testing.T) {
		f := newFake(t)
		r := NewRun("wf", nil)
		r.Job("a", Cmd("true"))
		r.Context(filepath.Join(t.TempDir(), "missing"))
		if _, err := r.Submit(context.Background(), f.client(t)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("err = %v", err)
		}
		if f.runReqs != 0 {
			t.Error("run submitted after a failed context")
		}
	})
}

func TestUploadContextDirect(t *testing.T) {
	f := newFake(t)
	dir := t.TempDir()
	writeTree(t, dir, map[string]string{"a": "x"})
	cu, err := UploadContext(context.Background(), f.client(t), dir)
	if err != nil {
		t.Fatal(err)
	}
	if cu.Stats.Files != 1 || cu.Size != int64(len(f.ctxReqs[0].body)) || "sha256:"+cu.SHA256 != cu.Ref {
		t.Errorf("ContextUpload = %+v", cu)
	}
}
