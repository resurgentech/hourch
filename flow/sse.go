package hourchflow

import (
	"bufio"
	"io"
	"strings"
)

// sseEvent is one server-sent event.
type sseEvent struct {
	ID    string
	Event string
	Data  string
}

// sseReader parses a text/event-stream (comments ignored, multi-line data
// joined with "\n").
type sseReader struct {
	sc *bufio.Scanner
}

func newSSEReader(r io.Reader) *sseReader {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	return &sseReader{sc: sc}
}

// Next returns the next event, or io.EOF / the read error.
func (s *sseReader) Next() (sseEvent, error) {
	var ev sseEvent
	var data []string
	seen := false
	for s.sc.Scan() {
		line := s.sc.Text()
		if line == "" {
			if seen {
				ev.Data = strings.Join(data, "\n")
				return ev, nil
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			ev.ID = value
			seen = true
		case "event":
			ev.Event = value
			seen = true
		case "data":
			data = append(data, value)
			seen = true
		}
	}
	if err := s.sc.Err(); err != nil {
		return sseEvent{}, err
	}
	return sseEvent{}, io.EOF
}
