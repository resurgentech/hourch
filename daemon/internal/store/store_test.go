package store

import (
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func open(t *testing.T) (*Store, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, p
}

func TestMigrationsReopen(t *testing.T) {
	s, p := open(t)
	v, err := s.SchemaVersion()
	if err != nil || v != len(migrations) {
		t.Fatalf("version = %d %v", v, err)
	}
	s.Close()
	s2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, _ := s2.SchemaVersion(); v != len(migrations) {
		t.Fatalf("reopened version = %d", v)
	}
	var mode string
	_ = s2.DB().QueryRow(`PRAGMA journal_mode`).Scan(&mode)
	if mode != "wal" {
		t.Fatalf("journal_mode = %s", mode)
	}
}

func TestCountersMonotonicPerName(t *testing.T) {
	s, _ := open(t)
	var wg sync.WaitGroup
	got := make(chan int64, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := s.NextCounter("build:hello:a")
			if err != nil {
				t.Error(err)
			}
			got <- v
		}()
	}
	wg.Wait()
	close(got)
	seen := map[int64]bool{}
	for v := range got {
		if seen[v] || v < 1 || v > 40 {
			t.Fatalf("duplicate or out of range value %d", v)
		}
		seen[v] = true
	}
	if v, _ := s.NextCounter("build:hello:b"); v != 1 {
		t.Fatalf("other counter starts at %d", v)
	}
	if v, _ := s.NextCounter("build:hello:a"); v != 41 {
		t.Fatalf("next = %d", v)
	}
}

func TestRunNumberPerWorkflowInTx(t *testing.T) {
	s, _ := open(t)
	now := time.Now()
	ins := func(id, wf string) int64 {
		var n int64
		if err := s.Tx(func(q Q) error {
			var err error
			n, err = q.InsertRun(Run{ID: id, Workflow: wf, Status: RunRunning, SubmittedBy: "t", CreatedAt: now})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if ins("A", "x") != 1 || ins("B", "x") != 2 || ins("C", "y") != 1 {
		t.Fatal("numbers")
	}
	r, err := s.GetRunByNumber("x", 2)
	if err != nil || r.ID != "B" {
		t.Fatalf("by number = %+v %v", r, err)
	}
}

// Migration 2 drops run_secrets (sealed values in SQLite, unreadable once
// the key file is gone) and adds secret_objects.
func TestMigrationDropsRunSecrets(t *testing.T) {
	p := filepath.Join(t.TempDir(), "state.db")
	db, err := sql.Open("sqlite", "file:"+p)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		migrations[0],
		`CREATE TABLE schema_version (version INTEGER NOT NULL)`,
		`INSERT INTO schema_version (version) VALUES (1)`,
		`INSERT INTO runs (id, workflow, number, params_json, status, submitted_by, created_at) VALUES ('R', 'wf', 1, '{}', 'running', 't', 'x')`,
		`INSERT INTO run_secrets (run_id, name, nonce, ciphertext) VALUES ('R', 'S', X'00', X'00')`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if v, _ := s.SchemaVersion(); v != 2 {
		t.Fatalf("version = %d", v)
	}
	if _, err := s.DB().Exec(`SELECT 1 FROM run_secrets`); err == nil {
		t.Fatal("run_secrets survived")
	}
	now := time.Now()
	if err := s.InsertSecretObject(SecretObject{RunID: "R", Object: "secrets/I/R", InstanceID: "I", CreatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if o, err := s.GetSecretObject("R"); err != nil || o.Object != "secrets/I/R" || o.InstanceID != "I" {
		t.Fatalf("row = %+v %v", o, err)
	}
	if n, _ := s.CountSecretObjects(); n != 1 {
		t.Fatal("count")
	}
	if n, err := s.DeleteSecretObjectsNotOf("J"); err != nil || n != 1 {
		t.Fatalf("delete not of J = %d %v", n, err)
	}
	if _, err := s.GetSecretObject("R"); !errors.Is(err, ErrNotFound) {
		t.Fatal("row survived")
	}
}
