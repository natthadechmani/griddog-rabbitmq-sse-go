package gateway

import (
	"net/http"

	"griddog/internal/sse"
)

// handleSSECall proxies the processing-backend SSE stream to the browser (flow 1).
func (s *Server) handleSSECall(w http.ResponseWriter, r *http.Request) {
	flusher, err := sse.Prepare(w)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, s.cfg.ProcessingBaseURL+"/sse-stream", nil)
	if err != nil {
		_ = sse.WriteEvent(w, flusher, "error", `{"error":"build upstream request"}`)
		return
	}
	resp, err := s.client.Do(req)
	if err != nil {
		_ = sse.WriteEvent(w, flusher, "error", `{"error":"processing-backend unreachable"}`)
		return
	}
	defer resp.Body.Close()

	_ = sse.Relay(w, flusher, resp.Body)
}
