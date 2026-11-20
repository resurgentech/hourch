//go:build unix

package hourchflow

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/resurgentech/keyclient/keyclienttest"
	"github.com/resurgentech/keyclient/request"
)

// T-22: another process holds the lock and there is no pending request to
// report: ErrBusy at once, worded as SR-9, with nothing filed or shown.
func TestT22_OtherProcessBusy(t *testing.T) {
	p := keyclienttest.NewProvider(t)
	h := newKeyedHourchd(t, p.Key(), 0)
	tok, stderr := keyEnv(t, h.srv.URL, p.RequestURL(), false)
	if err := os.MkdirAll(filepath.Dir(tok), 0o700); err != nil {
		t.Fatal(err)
	}
	lf, err := os.OpenFile(tok+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lf.Close()
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	_, err = NewClientFromEnv()
	want := "another process is handling the key request for " + tok + "; try again shortly"
	if !errors.Is(err, request.ErrBusy) || err.Error() != want {
		t.Fatalf("got %v; want %q wrapping ErrBusy", err, want)
	}
	if len(p.Filed()) != 0 || stderr.String() != "" {
		t.Fatalf("filed %v, printed %q", p.Filed(), stderr.String())
	}
}
