package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	api "github.com/resurgentech/hourch/api"

	"github.com/resurgentech/hourch/daemon/internal/logs"
)

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// followLog streams live chunks after pos until the attempt ends (then one
// `end` event), the client goes away, or the subscriber lags (the client
// resumes with Last-Event-ID).
func (s *Server) followLog(w http.ResponseWriter, r *http.Request, flusher http.Flusher, sub *logs.Sub, pos int64) {
	hb := time.NewTicker(s.o.Heartbeat)
	defer hb.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-s.done: // shutting down: end the stream so the client reconnects elsewhere
			return
		case ch, ok := <-sub.C:
			if !ok {
				if end := sub.Ended(); end != nil {
					off := end.Offset
					if off < pos {
						off = pos
					}
					_ = writeSSE(w, off, "end", api.LogEndEvent{Offset: off, Status: api.JobStatus(end.Status), ExitCode: end.ExitCode})
					flusher.Flush()
				}
				return
			}
			if ch.Offset <= pos {
				continue // already sent in the replay
			}
			pos = ch.Offset
			if writeSSE(w, pos, "log", api.LogChunkEvent{Offset: pos, Stream: api.LogStream(ch.Stream), Data: ch.Data}) != nil {
				return
			}
			flusher.Flush()
		case <-hb.C:
			if _, err := io.WriteString(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
