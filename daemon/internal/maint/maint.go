// Package maint runs hourchd's housekeeping: retention pruning (rows, logs,
// artifacts, contexts, stale temp objects, unowned secrets objects, old
// backups) and the SQLite backup to the object store.
package maint

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/resurgentech/hourch/daemon/internal/clock"
	"github.com/resurgentech/hourch/daemon/internal/dispatch"
	"github.com/resurgentech/hourch/daemon/internal/objects"
	"github.com/resurgentech/hourch/daemon/internal/secrets"
	"github.com/resurgentech/hourch/daemon/internal/store"
	"github.com/resurgentech/hourch/daemon/internal/ulid"
)

// Options configure maintenance.
type Options struct {
	Engine        *dispatch.Engine
	Store         *store.Store
	Objects       objects.Store
	Clock         clock.Clock
	Log           *slog.Logger
	StateDir      string
	Rows          time.Duration
	Logs          time.Duration
	Artifacts     time.Duration
	PruneInterval time.Duration
	BackupEvery   time.Duration
	BackupPrefix  string
}

// tmpMaxAge is how long an abandoned temp upload (tmp/) is kept.
const tmpMaxAge = 24 * time.Hour

// secretsGrace is how old a secrets object with no secret_objects row must
// be before the sweep deletes it (a submission writes the object just
// before its row).
const secretsGrace = time.Hour

// Maint runs pruning and backups.
type Maint struct {
	o  Options
	wg sync.WaitGroup
}

// New returns a Maint.
func New(o Options) *Maint {
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return &Maint{o: o}
}

// Start runs the prune and backup loops until ctx ends.
func (m *Maint) Start(ctx context.Context) {
	loop := func(every time.Duration, fn func(context.Context) error, what string) {
		defer m.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.o.Clock.After(every):
				if err := fn(ctx); err != nil {
					m.o.Log.Error(what+" failed", "error", err)
				}
			}
		}
	}
	m.wg.Add(2)
	go loop(m.o.PruneInterval, m.Prune, "prune")
	go loop(m.o.BackupEvery, m.Backup, "backup")
}

// Wait waits for the loops to stop.
func (m *Maint) Wait() { m.wg.Wait() }

// PruneResult counts what one prune removed.
type PruneResult struct {
	Runs, Logs, Artifacts, Contexts, Temp, Secrets, Backups int
}

// Prune applies retention once.
func (m *Maint) Prune(ctx context.Context) error {
	_, err := m.PruneOnce(ctx)
	return err
}

// PruneOnce applies retention and reports what it removed.
func (m *Maint) PruneOnce(ctx context.Context) (PruneResult, error) {
	var res PruneResult
	now := m.o.Clock.Now()
	var pruned []string
	if err := m.o.Store.Tx(func(q store.Q) error {
		var err error
		pruned, err = q.PruneRows(now.Add(-m.o.Rows))
		return err
	}); err != nil {
		return res, fmt.Errorf("rows: %w", err)
	}
	res.Runs = len(pruned)
	statuses, err := m.o.Store.RunStatuses()
	if err != nil {
		return res, err
	}
	// A run's objects go once the run is older than the cutoff and not
	// running (or its row is gone).
	expired := func(runID string, cutoff time.Time) bool {
		t, err := ulid.Time(runID)
		if err != nil || !t.Before(cutoff) {
			return false
		}
		st, ok := statuses[runID]
		return !ok || st != store.RunRunning
	}
	obj := m.o.Objects
	logObjs, err := obj.List(ctx, "logs/")
	if err != nil {
		return res, fmt.Errorf("list logs: %w", err)
	}
	for _, o := range logObjs {
		if run := segment(o.Key, 1); expired(run, now.Add(-m.o.Logs)) {
			if err := obj.Delete(ctx, o.Key); err != nil {
				return res, err
			}
			res.Logs++
		}
	}
	artObjs, err := obj.List(ctx, "artifacts/")
	if err != nil {
		return res, fmt.Errorf("list artifacts: %w", err)
	}
	cleared := map[string]bool{}
	for _, o := range artObjs {
		run := segment(o.Key, 1)
		if !expired(run, now.Add(-m.o.Artifacts)) {
			continue
		}
		if err := obj.Delete(ctx, o.Key); err != nil {
			return res, err
		}
		res.Artifacts++
		if _, ok := statuses[run]; ok && !cleared[run] {
			cleared[run] = true
			if err := m.o.Store.DeleteArtifactRows(run); err != nil {
				return res, err
			}
		}
	}
	// Contexts are shared: delete one only when no stored run uses it and
	// it is older than the row retention (so a fresh upload whose run is not
	// submitted yet stays).
	refs, err := m.o.Store.ContextRefs()
	if err != nil {
		return res, err
	}
	ctxObjs, err := obj.List(ctx, "contexts/")
	if err != nil {
		return res, fmt.Errorf("list contexts: %w", err)
	}
	for _, o := range ctxObjs {
		hex := strings.TrimSuffix(strings.TrimPrefix(o.Key, "contexts/"), ".tar.zst")
		if refs["sha256:"+hex] || !o.LastModified.Before(now.Add(-m.o.Rows)) {
			continue
		}
		if err := obj.Delete(ctx, o.Key); err != nil {
			return res, err
		}
		res.Contexts++
	}
	tmpObjs, err := obj.List(ctx, "tmp/")
	if err != nil {
		return res, err
	}
	for _, o := range tmpObjs {
		if o.LastModified.Before(now.Add(-tmpMaxAge)) {
			if err := obj.Delete(ctx, o.Key); err != nil {
				return res, err
			}
			res.Temp++
		}
	}
	// Secrets objects hourchd no longer owns: the run was pruned (its row
	// went with it), a delete after a run released them failed, or the
	// startup scrub of an earlier instance's objects could not reach the
	// store. hourchd owns exactly the objects recorded in secret_objects;
	// there is no bucket lifecycle rule.
	owned, err := m.o.Store.SecretObjectKeys()
	if err != nil {
		return res, err
	}
	secObjs, err := obj.List(ctx, secrets.Prefix)
	if err != nil {
		return res, fmt.Errorf("list secrets: %w", err)
	}
	for _, o := range secObjs {
		if owned[o.Key] || !o.LastModified.Before(now.Add(-secretsGrace)) {
			continue
		}
		if err := obj.Delete(ctx, o.Key); err != nil {
			return res, err
		}
		res.Secrets++
	}
	if m.o.BackupPrefix != "" {
		bks, err := obj.List(ctx, m.o.BackupPrefix)
		if err != nil {
			return res, err
		}
		for _, o := range bks {
			if o.LastModified.Before(now.Add(-m.o.Rows)) {
				if err := obj.Delete(ctx, o.Key); err != nil {
					return res, err
				}
				res.Backups++
			}
		}
	}
	if m.o.Engine != nil {
		m.o.Engine.RetryLogUploads()
	}
	m.o.Log.Info("pruned", "runs", res.Runs, "logs", res.Logs, "artifacts", res.Artifacts,
		"contexts", res.Contexts, "temp", res.Temp, "secrets", res.Secrets, "backups", res.Backups)
	return res, nil
}

func segment(key string, i int) string {
	parts := strings.Split(key, "/")
	if i < len(parts) {
		return parts[i]
	}
	return ""
}

// BackupKey is the object key of a backup taken at t.
func BackupKey(prefix string, t time.Time) string {
	return prefix + "state-" + t.UTC().Format("20060102T150405Z") + ".db"
}

// Backup writes a consistent copy of the database (VACUUM INTO a temp file)
// and uploads it under backup.minio_prefix.
func (m *Maint) Backup(ctx context.Context) error {
	now := m.o.Clock.Now()
	tmp := filepath.Join(m.o.StateDir, fmt.Sprintf("backup-%d.db.tmp", now.UnixNano()))
	defer os.Remove(tmp)
	if err := m.o.Store.VacuumInto(tmp); err != nil {
		return fmt.Errorf("vacuum into: %w", err)
	}
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	key := BackupKey(m.o.BackupPrefix, now)
	if err := m.o.Objects.Put(ctx, key, f, fi.Size()); err != nil {
		return fmt.Errorf("upload backup: %w", err)
	}
	m.o.Log.Info("backup uploaded", "key", key, "bytes", fi.Size())
	return nil
}
