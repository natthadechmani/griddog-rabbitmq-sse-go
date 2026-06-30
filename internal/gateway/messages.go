package gateway

import (
	"net/http"
	"strconv"

	"griddog/internal/db"
	"griddog/internal/httpx"
)

// handleMessages returns persisted message_logs rows for a given flow so the
// frontend can show what flow 2 / flow 3 wrote to MySQL.
func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	flow := r.URL.Query().Get("flow")
	if flow == "" {
		flow = "rabbitmq"
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	logs, err := db.ListLogsByFlow(r.Context(), s.db, flow, limit)
	if err != nil {
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	httpx.WriteJSON(w, http.StatusOK, logs)
}
