package maint_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/resurgentech/hourch/daemon/internal/app"
	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/config"
	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/graph"
	"github.com/resurgentech/hourch/daemon/internal/maint"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/secrets"
	"github.com/resurgentech/hourch/daemon/internal/store"
)

func setup(t *testing.T) (*app.App, *clock.Fake, *objects.Mem) {
	t.Helper()
	dir := t.TempDir()
	cfg := config.Default()
	cfg.StateDir = filepath.Join(dir, "state")
	cfg.TokensFile = filepath.Join(dir, "tokens.yaml")
	cfg.Powerwarden.URL, cfg.UI.Enabled = "", false
	if err := os.WriteFile(cfg.TokensFile, []byte("submitters:\n  a: tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	clk := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	obj := objects.NewMem()
	obj.Now = clk.Now
	a, err := app.Build(cfg, app.Deps{Clock: clk, Objects: obj, Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	seedSeen(t, a.Store)
	t.Cleanup(func() { _ = a.Close() })
	return a, clk, obj
}

func put(t *testing.T, obj *objects.Mem, key string) {
	t.Helper()
	if err := obj.Put(context.Background(), key, strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
}

func ref(c byte) string { return "sha256:" + strings.Repeat(string(c), 64) }

func TestPruneRetention(t *testing.T) {
	a, clk, obj := setup(t)
	ctx := context.Background()
	put(t, obj, dispatch.ContextKey(ref('a'))) // used by an old run
	put(t, obj, dispatch.ContextKey(ref('b'))) // never used
	put(t, obj, dispatch.ContextKey(ref('d'))) // used by an old run still running
	put(t, obj, "tmp/contexts/abandoned")
	sub := func(ctxRef string) store.Run {
		r, err := a.Engine.Submit(ctx, "t", graph.Submission{Workflow: "wf", ContextRef: ctxRef, Jobs: []graph.Job{{Name: "j", Argv: []string{"x"}}}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	old := sub(ref('a'))
	running := sub(ref('d'))
	if err := a.Engine.CancelRun(old.ID); err != nil {
		t.Fatal(err)
	}
	oldJobs, _ := a.Store.RunJobs(old.ID)
	put(t, obj, dispatch.LogKey(old.ID, oldJobs[0].ID, 1))
	put(t, obj, dispatch.ArtifactKey(old.ID, oldJobs[0].ID, "out/a"))
	if err := a.Store.PutArtifact(store.Artifact{JobID: oldJobs[0].ID, Path: "out/a", Object: "k", Size: 1, Sha256: "s"}); err != nil {
		t.Fatal(err)
	}
	runJobs, _ := a.Store.RunJobs(running.ID)
	put(t, obj, dispatch.LogKey(running.ID, runJobs[0].ID, 1))
	if err := a.Store.PutBuild(store.Build{BuildType: "kernel", Flavour: "f", BuildNum: 1, RunID: old.ID, JobID: oldJobs[0].ID, Body: []byte(`{}`), CreatedAt: clk.Now()}); err != nil {
		t.Fatal(err)
	}
	m := maint.New(maint.Options{Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Engine: a.Engine, Store: a.Store, Objects: obj, Clock: clk, StateDir: a.Config.StateDir,
		Rows: 90 * 24 * time.Hour, Logs: 30 * 24 * time.Hour, Artifacts: 90 * 24 * time.Hour, BackupPrefix: "backups/hourchd/"})

	// Nothing is old enough yet.
	if res, err := m.PruneOnce(ctx); err != nil || res != (maint.PruneResult{}) {
		t.Fatalf("early prune = %+v %v", res, err)
	}

	clk.Advance(31 * 24 * time.Hour)           // past logs retention only
	put(t, obj, dispatch.ContextKey(ref('c'))) // fresh upload, run not submitted yet
	res, err := m.PruneOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Logs != 1 || res.Runs != 0 || res.Artifacts != 0 || res.Temp != 1 {
		t.Fatalf("31d prune = %+v", res)
	}
	if _, ok := obj.Bytes(dispatch.LogKey(running.ID, runJobs[0].ID, 1)); !ok {
		t.Fatal("running run's log pruned")
	}

	clk.Advance(60 * 24 * time.Hour) // 91 days
	res, err = m.PruneOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Runs != 1 || res.Artifacts != 1 || res.Contexts != 2 {
		t.Fatalf("91d prune = %+v", res)
	}
	if _, err := a.Store.GetRun(old.ID); err == nil {
		t.Fatal("old run row kept")
	}
	if _, err := a.Store.GetRun(running.ID); err != nil {
		t.Fatal("running run pruned")
	}
	keys := strings.Join(obj.Keys(), "\n")
	for _, gone := range []string{strings.Repeat("a", 64), strings.Repeat("b", 64)} {
		if strings.Contains(keys, gone) {
			t.Fatalf("context %s kept:\n%s", gone[:4], keys)
		}
	}
	for _, kept := range []string{strings.Repeat("c", 64), strings.Repeat("d", 64)} {
		if !strings.Contains(keys, kept) {
			t.Fatalf("context %s pruned:\n%s", kept[:4], keys)
		}
	}
	if bs, _ := a.Store.ListBuilds("", "", 10); len(bs) != 1 {
		t.Fatal("build records pruned with their run")
	}
}

// The pruner deletes a pruned run's secrets object with its row, and any
// secrets object hourchd does not own (after a grace period); objects of
// live runs stay.
func TestPruneSecretsObjects(t *testing.T) {
	a, clk, obj := setup(t)
	ctx := context.Background()
	sub := func() store.Run {
		seedSeen(t, a.Store) // a prune may have removed it
		r, err := a.Engine.Submit(ctx, "t", graph.Submission{Workflow: "wf", Secrets: map[string]string{"S": "v"},
			Jobs: []graph.Job{{Name: "j", Argv: []string{"x"}, Secrets: []string{"S"}}}})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	old := sub()
	if err := a.Engine.CancelRun(old.ID); err != nil { // cancelled: keeps its secrets
		t.Fatal(err)
	}
	oldKey := secrets.ObjectKey(a.Engine.Instance(), old.ID)
	if _, ok := obj.Bytes(oldKey); !ok {
		t.Fatal("no object")
	}
	m := maint.New(maint.Options{Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Engine: a.Engine, Store: a.Store, Objects: obj, Clock: clk,
		StateDir: a.Config.StateDir, Rows: 90 * 24 * time.Hour, Logs: 90 * 24 * time.Hour, Artifacts: 90 * 24 * time.Hour})
	orphan := secrets.ObjectKey(a.Engine.Instance(), "01JBF6Q9W2X7K3M4N5P6Q7R8S9") // e.g. a delete that failed
	put(t, obj, orphan)
	if res, err := m.PruneOnce(ctx); err != nil || res.Secrets != 0 {
		t.Fatalf("early prune = %+v %v", res, err) // the orphan is within the grace period
	}
	clk.Advance(2 * time.Hour)
	res, err := m.PruneOnce(ctx)
	if err != nil || res.Secrets != 1 {
		t.Fatalf("orphan prune = %+v %v", res, err)
	}
	if _, ok := obj.Bytes(orphan); ok {
		t.Fatal("orphan kept")
	}
	if _, ok := obj.Bytes(oldKey); !ok {
		t.Fatal("an owned object was swept")
	}
	clk.Advance(91 * 24 * time.Hour)
	live := sub() // running, fresh
	res, err = m.PruneOnce(ctx)
	if err != nil || res.Runs != 1 || res.Secrets != 1 {
		t.Fatalf("91d prune = %+v %v", res, err)
	}
	if _, ok := obj.Bytes(oldKey); ok {
		t.Fatal("pruned run's secrets object kept")
	}
	if _, err := a.Store.GetSecretObject(old.ID); err == nil {
		t.Fatal("pruned run's ownership row kept")
	}
	if !a.Engine.HoldsSecrets(live.ID) {
		t.Fatal("live run lost its secrets")
	}
}

func TestBackup(t *testing.T) {
	a, clk, obj := setup(t)
	m := maint.New(maint.Options{Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Store: a.Store, Objects: obj, Clock: clk, StateDir: a.Config.StateDir, BackupPrefix: "backups/hourchd/", Rows: time.Hour})
	if err := m.Backup(context.Background()); err != nil {
		t.Fatal(err)
	}
	b, ok := obj.Bytes(maint.BackupKey("backups/hourchd/", clk.Now()))
	if !ok || !bytes.HasPrefix(b, []byte("SQLite format 3")) {
		t.Fatalf("backup missing or not SQLite (%d bytes)", len(b))
	}
	ents, _ := os.ReadDir(a.Config.StateDir)
	for _, e := range ents {
		if strings.Contains(e.Name(), "backup") {
			t.Fatalf("temp backup left: %s", e.Name())
		}
	}
	clk.Advance(2 * time.Hour)
	res, err := m.PruneOnce(context.Background())
	if err != nil || res.Backups != 1 {
		t.Fatalf("backup prune = %+v %v", res, err)
	}
}
