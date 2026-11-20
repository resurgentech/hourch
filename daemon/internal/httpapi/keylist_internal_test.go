package httpapi

import (
	"errors"
	"strings"
	"testing"
)

func TestSafeError(t *testing.T) {
	if got := safeError(errors.New("pull: status 500\n\x00\x1b[31mred\ttab")); got != "pull: status 500???[31mred?tab" {
		t.Errorf("control characters: %q", got)
	}
	long := strings.Repeat("é", 300)
	if got := safeError(errors.New(long)); got != strings.Repeat("é", 200) {
		t.Errorf("cut: %d runes", len([]rune(got)))
	}
	if got := safeError(errors.New(strings.Repeat("a", 200))); len(got) != 200 {
		t.Errorf("200 runes are kept whole: %d", len(got))
	}
}
