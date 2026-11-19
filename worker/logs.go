package hourchworker

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Log chunking defaults (design: "Log chunks up to 64 KiB or 250ms").
const (
	ChunkMaxBytes = 64 << 10
	ChunkMaxDelay = 250 * time.Millisecond
)

// logChunk is one log message waiting to be sent or kept for a resend.
type logChunk struct {
	stream string
	data   string
	// seq is 0 until the chunk is handed to a connection; seqs are assigned
	// in buffer order, so the buffer is a prefix of sent chunks followed by
	// unsent ones.
	seq int64
}

// logBuffer holds one job attempt's log chunks, bounded by max bytes.
//
// Seqs are assigned when a chunk is handed to the socket. Sent chunks stay
// until the next welcome tells us (last_log_seq) which ones hourchd stored;
// reset then drops those and renumbers the rest from last_log_seq+1 (hourchd
// never saw those seqs, so renumbering is safe and keeps seqs contiguous).
//
// When the buffer is full the oldest chunk is evicted. Evicting an unsent
// chunk counts its bytes as dropped; the next chunk handed out is then a
// marker line on stderr saying how much was lost.
type logBuffer struct {
	mu      sync.Mutex
	max     int
	size    int
	chunks  []*logChunk
	nsent   int   // leading chunks with seq != 0
	nextSeq int64 // seq for the next chunk handed out
	// dropped is unsent bytes evicted since the last marker; lostSent is
	// set when reset finds that sent-but-unstored chunks were evicted.
	dropped  int64
	lostSent bool
	// evictedSeq is the highest seq evicted after being sent.
	evictedSeq int64
	closed     bool
	wake       func()
}

func newLogBuffer(max int, wake func()) *logBuffer {
	if wake == nil {
		wake = func() {}
	}
	return &logBuffer{max: max, nextSeq: 1, wake: wake}
}

// add appends a chunk (already redacted and valid UTF-8).
func (b *logBuffer) add(stream, data string) {
	if data == "" {
		return
	}
	b.mu.Lock()
	b.chunks = append(b.chunks, &logChunk{stream: stream, data: data})
	b.size += len(data)
	for b.size > b.max && len(b.chunks) > 1 {
		old := b.chunks[0]
		b.chunks[0] = nil
		b.chunks = b.chunks[1:]
		b.size -= len(old.data)
		if old.seq != 0 {
			b.nsent--
			if old.seq > b.evictedSeq {
				b.evictedSeq = old.seq
			}
		} else {
			b.dropped += int64(len(old.data))
		}
	}
	b.mu.Unlock()
	b.wake()
}

// close marks the end of the job's output.
func (b *logBuffer) close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.wake()
}

func markerText(dropped int64, lostSent bool) string {
	switch {
	case dropped > 0:
		return fmt.Sprintf("[hourch-worker: %d bytes of log output dropped: log buffer full while disconnected]\n", dropped)
	default:
		return "[hourch-worker: log output dropped: log buffer full while disconnected]\n"
	}
}

// take hands out the next unsent chunk with its seq assigned, or nil.
func (b *logBuffer) take() *logChunk {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dropped > 0 || b.lostSent {
		m := &logChunk{stream: "stderr", data: markerText(b.dropped, b.lostSent), seq: b.nextSeq}
		b.nextSeq++
		b.dropped, b.lostSent = 0, false
		// Insert after the sent prefix, before the remaining unsent chunks.
		b.chunks = append(b.chunks, nil)
		copy(b.chunks[b.nsent+1:], b.chunks[b.nsent:])
		b.chunks[b.nsent] = m
		b.nsent++
		b.size += len(m.data)
		c := *m
		return &c
	}
	if b.nsent >= len(b.chunks) {
		return nil
	}
	c := b.chunks[b.nsent]
	c.seq = b.nextSeq
	b.nextSeq++
	b.nsent++
	out := *c
	return &out
}

// drained reports that the output ended and every chunk was handed out.
func (b *logBuffer) drained() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed && b.nsent == len(b.chunks) && b.dropped == 0 && !b.lostSent
}

// reset applies welcome.last_log_seq after a re-attach: chunks hourchd
// stored are dropped, the rest are renumbered from last+1 on resend.
func (b *logBuffer) reset(last int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := 0
	for i < len(b.chunks) && b.chunks[i].seq != 0 && b.chunks[i].seq <= last {
		b.size -= len(b.chunks[i].data)
		i++
	}
	b.chunks = append([]*logChunk(nil), b.chunks[i:]...)
	// Sent chunks after `last` that were evicted were never stored.
	if b.evictedSeq > last {
		b.lostSent = true
	}
	b.evictedSeq = 0
	for _, c := range b.chunks {
		c.seq = 0
	}
	b.nsent = 0
	b.nextSeq = last + 1
	b.wake()
}

// redactor replaces exact secret values with ***.
type redactor struct{ r *strings.Replacer }

func newRedactor(values []string) redactor {
	vs := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, v := range values {
		if v != "" && !seen[v] {
			seen[v] = true
			vs = append(vs, v)
		}
	}
	if len(vs) == 0 {
		return redactor{}
	}
	// Longest first so a secret containing another is replaced whole.
	sort.Slice(vs, func(i, j int) bool { return len(vs[i]) > len(vs[j]) })
	args := make([]string, 0, 2*len(vs))
	for _, v := range vs {
		args = append(args, v, "***")
	}
	return redactor{strings.NewReplacer(args...)}
}

func (r redactor) apply(s string) string {
	if r.r == nil {
		return s
	}
	return r.r.Replace(s)
}

// sanitizeUTF8 replaces every invalid byte with U+FFFD (assumption 13).
func sanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s) + 8)
	for i := 0; i < len(s); {
		r, n := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && n == 1 {
			sb.WriteRune(utf8.RuneError)
		} else {
			sb.WriteString(s[i : i+n])
		}
		i += n
	}
	return sb.String()
}

// splitRunes splits valid UTF-8 into pieces of at most max bytes without
// splitting a rune.
func splitRunes(s string, max int) []string {
	var out []string
	for len(s) > max {
		cut := max
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		if cut == 0 {
			cut = max
		}
		out = append(out, s[:cut])
		s = s[cut:]
	}
	if s != "" {
		out = append(out, s)
	}
	return out
}

// incompleteTail returns how many trailing bytes of b form the start of a
// rune that is not complete yet (0..3).
func incompleteTail(b []byte) int {
	for n := 1; n <= 3 && n <= len(b); n++ {
		i := len(b) - n
		if utf8.RuneStart(b[i]) {
			if !utf8.FullRune(b[i:]) {
				return n
			}
			return 0
		}
	}
	return 0
}

// pump reads one stream and emits chunks of at most maxBytes, flushing
// pending bytes maxDelay after the first of them arrived, and everything at
// EOF. A size cut prefers the last newline (so a secret on one line is not
// split across chunks and escapes redaction) and never splits a rune.
// emit gets raw bytes; the caller redacts and sanitizes.
func pump(r io.Reader, maxBytes int, maxDelay time.Duration, emit func([]byte)) {
	data := make(chan []byte)
	go func() {
		defer close(data)
		buf := make([]byte, 32<<10)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				data <- append([]byte(nil), buf[:n]...)
			}
			if err != nil {
				return
			}
		}
	}()

	var pending []byte
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	timerOn := false
	flush := func(all bool) {
		for len(pending) > 0 {
			cut := len(pending)
			if cut > maxBytes {
				cut = maxBytes
				if nl := bytes.LastIndexByte(pending[:cut], '\n'); nl >= 0 {
					cut = nl + 1
				} else {
					cut -= incompleteTail(pending[:cut])
					if cut == 0 {
						cut = maxBytes
					}
				}
			} else if !all {
				cut -= incompleteTail(pending)
				if cut == 0 {
					return
				}
			}
			emit(pending[:cut])
			pending = append([]byte(nil), pending[cut:]...)
			if !all && len(pending) <= maxBytes {
				return
			}
		}
	}
	for {
		select {
		case b, ok := <-data:
			if !ok {
				timer.Stop()
				flush(true)
				return
			}
			pending = append(pending, b...)
			if len(pending) >= maxBytes {
				for len(pending) >= maxBytes {
					before := len(pending)
					flush(false)
					if len(pending) == before {
						break
					}
				}
			}
			if len(pending) > 0 && !timerOn {
				timer.Reset(maxDelay)
				timerOn = true
			} else if len(pending) == 0 && timerOn {
				timer.Stop()
				timerOn = false
			}
		case <-timer.C:
			timerOn = false
			flush(false)
			if len(pending) > 0 {
				timer.Reset(maxDelay)
				timerOn = true
			}
		}
	}
}
