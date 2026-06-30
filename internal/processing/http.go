package processing

import (
	"log"
	"net/http"
	"time"

	"griddog/internal/db"
	"griddog/internal/httpx"
	"griddog/internal/models"
)

// handleProcess is the flow-3 HTTP target. It logs the incoming request and the
// outgoing response (the "processing side" of flow 3), computing a small result.
func (s *Server) handleProcess(w http.ResponseWriter, r *http.Request) {
	var task models.Task
	if err := httpx.ReadJSON(r, &task); err != nil {
		httpx.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	ctx := r.Context()

	if err := db.InsertLog(ctx, s.db, "http", task.CorrelationID, "processing", "request_in", task); err != nil {
		log.Printf("flow3 processing request_in log error: %v", err)
	}

	result := models.EnrichedTask{
		CorrelationID: task.CorrelationID,
		OriginalValue: task.Value,
		Doubled:       task.Value * 2,
		Squared:       task.Value * task.Value,
		ProcessedBy:   "processing-backend",
		Note:          "computed via HTTP /process",
		EnrichedAt:    time.Now(),
	}
	resp := map[string]any{"correlation_id": task.CorrelationID, "result": result}

	if err := db.InsertLog(ctx, s.db, "http", task.CorrelationID, "processing", "response_out", resp); err != nil {
		log.Printf("flow3 processing response_out log error: %v", err)
	}

	httpx.WriteJSON(w, http.StatusOK, resp)
}
