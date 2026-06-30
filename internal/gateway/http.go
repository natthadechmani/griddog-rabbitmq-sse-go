package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"time"

	"github.com/google/uuid"

	"griddog/internal/db"
	"griddog/internal/httpx"
	"griddog/internal/logx"
	"griddog/internal/models"
)

// handleHTTPCall is flow 3: the gateway calls processing-backend over HTTP and
// logs its own request/response (the gateway side). processing-backend logs its
// own side inside /process.
func (s *Server) handleHTTPCall(w http.ResponseWriter, r *http.Request) {
	var req flowRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		req.Value = 0
	}
	if req.Value == 0 {
		req.Value = rand.Intn(100) + 1
	}
	ctx := r.Context()
	corrID := uuid.NewString()
	task := models.Task{CorrelationID: corrID, Value: req.Value, CreatedAt: time.Now()}

	logx.Printf(ctx, "flow3 http-call received value=%d correlation_id=%s", req.Value, corrID)

	// gateway request_in
	if err := db.InsertLog(ctx, s.db, "http", corrID, "gateway", "request_in", map[string]any{"value": req.Value}); err != nil {
		logx.Printf(ctx, "http request_in log error: %v", err)
	}

	body, _ := json.Marshal(task)
	upstream, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.ProcessingBaseURL+"/process", bytes.NewReader(body))
	if err != nil {
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "build request"})
		return
	}
	upstream.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(upstream)
	if err != nil {
		httpx.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": "processing-backend unreachable"})
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)

	var result map[string]any
	_ = json.Unmarshal(respBody, &result)

	out := map[string]any{
		"correlation_id": corrID,
		"input":          map[string]any{"value": req.Value},
		"result":         result,
	}

	// gateway response_out
	if err := db.InsertLog(ctx, s.db, "http", corrID, "gateway", "response_out", out); err != nil {
		logx.Printf(ctx, "http response_out log error: %v", err)
	}

	httpx.WriteJSON(w, http.StatusOK, out)
}
