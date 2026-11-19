// Package ulid generates ULIDs (26 Crockford base32 characters, upper case):
// a 48-bit millisecond timestamp and 80 random bits. Within one generator,
// ids are strictly increasing, so sorting by id is creation order.
package ulid

import (
	"crypto/rand"
	"errors"
	"strings"
	"sync"
	"time"
)

const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// Generator makes monotonic ULIDs.
type Generator struct {
	mu      sync.Mutex
	lastMs  uint64
	lastRnd [10]byte
}

// New returns a generator.
func New() *Generator { return &Generator{} }

// Make returns a new ULID for time t, greater than every id this generator
// returned before.
func (g *Generator) Make(t time.Time) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	ms := uint64(t.UnixMilli())
	if ms <= g.lastMs {
		ms = g.lastMs
		// Increment the random part (big endian).
		for i := 9; i >= 0; i-- {
			g.lastRnd[i]++
			if g.lastRnd[i] != 0 {
				break
			}
		}
	} else {
		if _, err := rand.Read(g.lastRnd[:]); err != nil {
			panic(err)
		}
		// Leave headroom so increments within one ms never overflow.
		g.lastRnd[0] &= 0x7f
	}
	g.lastMs = ms
	return encode(ms, g.lastRnd)
}

func encode(ms uint64, rnd [10]byte) string {
	var b [16]byte
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	copy(b[6:], rnd[:])
	// 128 bits -> 26 chars of 5 bits (the first char carries 3 bits).
	var out [26]byte
	var acc uint64
	var bits uint
	// Process from the least significant end.
	pos := 25
	for i := 15; i >= 0; i-- {
		acc |= uint64(b[i]) << bits
		bits += 8
		for bits >= 5 && pos >= 0 {
			out[pos] = alphabet[acc&31]
			acc >>= 5
			bits -= 5
			pos--
		}
	}
	if pos >= 0 {
		out[pos] = alphabet[acc&31]
	}
	return string(out[:])
}

// Time returns the timestamp encoded in id.
func Time(id string) (time.Time, error) {
	if !Valid(id) {
		return time.Time{}, errors.New("ulid: invalid")
	}
	var ms uint64
	for i := 0; i < 10; i++ {
		ms = ms<<5 | uint64(strings.IndexByte(alphabet, id[i]))
	}
	return time.UnixMilli(int64(ms)).UTC(), nil
}

// Valid reports whether s is a well-formed ULID.
func Valid(s string) bool {
	if len(s) != 26 || s[0] > '7' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if strings.IndexByte(alphabet, s[i]) < 0 {
			return false
		}
	}
	return true
}
