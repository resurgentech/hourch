package ulid

import (
	"regexp"
	"testing"
	"time"
)

var pattern = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

func TestMonotonicAndTime(t *testing.T) {
	g := New()
	at := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	prev := ""
	for i := 0; i < 1000; i++ {
		id := g.Make(at) // same millisecond every time
		if !pattern.MatchString(id) || !Valid(id) {
			t.Fatalf("bad ulid %q", id)
		}
		if id <= prev {
			t.Fatalf("not increasing: %s <= %s", id, prev)
		}
		prev = id
	}
	got, err := Time(prev)
	if err != nil || !got.Equal(at) {
		t.Fatalf("time = %v %v", got, err)
	}
	// A clock going backwards still yields larger ids.
	if id := g.Make(at.Add(-time.Hour)); id <= prev {
		t.Fatalf("backwards clock: %s <= %s", id, prev)
	}
	if later := g.Make(at.Add(time.Second)); later <= prev {
		t.Fatal("later time not larger")
	}
	if Valid("01JBF6Q9W2X7K3M4N5P6Q7R8SI") || Valid("short") {
		t.Fatal("invalid accepted")
	}
}
