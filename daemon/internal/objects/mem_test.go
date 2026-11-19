package objects

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// The fake must behave like the interface promises, since every daemon test
// runs against it.
func TestMemContract(t *testing.T) {
	ctx := context.Background()
	var s Store = NewMem()
	if err := s.Put(ctx, "a/1", strings.NewReader("one"), 3); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "a/2", strings.NewReader("two!"), -1); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "a/3", strings.NewReader("short"), 99); err == nil {
		t.Fatal("size mismatch accepted")
	}
	rc, info, err := s.Get(ctx, "a/2")
	if err != nil || info.Size != 4 {
		t.Fatalf("get: %+v %v", info, err)
	}
	b, _ := io.ReadAll(rc)
	rc.Close()
	if string(b) != "two!" {
		t.Fatalf("got %q", b)
	}
	if _, err := s.Stat(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stat missing: %v", err)
	}
	if err := s.Copy(ctx, "a/1", "b/1"); err != nil {
		t.Fatal(err)
	}
	l, _ := s.List(ctx, "a/")
	if len(l) != 2 || l[0].Key != "a/1" || l[1].Key != "a/2" {
		t.Fatalf("list = %+v", l)
	}
	if err := s.Delete(ctx, "a/1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a/1"); err != nil {
		t.Fatal("delete of missing key errored")
	}
	m := s.(*Mem)
	m.SetFail(errors.New("down"))
	if s.Ping(ctx) == nil {
		t.Fatal("ping ok while failing")
	}
	if _, _, err := s.Get(ctx, "a/2"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("get while failing: %v", err)
	}
}
