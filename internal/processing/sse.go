package processing

import (
	"encoding/json"
	"net/http"
	"time"

	"griddog/internal/sse"
)

// handleSSEStream emits an incrementing counter (1..20) once every 500ms for
// ~10 seconds, then a final "done" event. This is the source stream that the
// gateway proxies to the browser in flow 1.
func (s *Server) handleSSEStream(w http.ResponseWriter, r *http.Request) {
	flusher, err := sse.Prepare(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	const total = 20
	for i := 1; i <= total; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		data, _ := json.Marshal(map[string]any{
			"count": i,
			"total": total,
			"ts":    time.Now().Format(time.RFC3339Nano),
		})
		if err := sse.WriteEvent(w, flusher, "", string(data)); err != nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	_ = sse.WriteEvent(w, flusher, "done", `{"message":"stream complete"}`)
}
