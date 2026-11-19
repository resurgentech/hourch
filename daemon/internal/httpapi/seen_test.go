package httpapi_test

import (
	"testing"
	"time"

	"github.com/resurgentech/hourch/daemon/internal/store"
)

// seenID is a worker recorded as seen a year before the tests' clocks
// (outside retention.rows, so no worker listing shows it) whose tags and
// capabilities cover every job these tests submit, so submissions pass the
// satisfiability check. Tests of that check forget it (forgetSeen).
const seenID = "seen-long-ago"

func seedSeen(t *testing.T, st *store.Store) {
	t.Helper()
	if err := st.UpsertWorker(store.Worker{
		ID: seenID, Hostname: "gone.lab",
		Tags:         []string{"busy", "busy2", "idle", "kernel", "gpu", "nobody", "none", "tiny"},
		Capabilities: []string{"docker"},
		LastSeen:     time.Date(2025, 10, 1, 0, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatal(err)
	}
}

func forgetSeen(t *testing.T, st *store.Store) {
	t.Helper()
	if _, err := st.DB().Exec(`DELETE FROM workers WHERE id = ?`, seenID); err != nil {
		t.Fatal(err)
	}
}
