package models

import (
	"encoding/json"
	"time"
)

// Task is the message a gateway publishes to processing-queue (flow 2) and the
// body posted to processing-backend /process (flow 3).
type Task struct {
	CorrelationID string    `json:"correlation_id"`
	Value         int       `json:"value"`
	CreatedAt     time.Time `json:"created_at"`
}

// EnrichedTask is produced by processing-backend after manipulation/enrichment.
type EnrichedTask struct {
	CorrelationID string    `json:"correlation_id"`
	OriginalValue int       `json:"original_value"`
	Doubled       int       `json:"doubled"`
	Squared       int       `json:"squared"`
	ProcessedBy   string    `json:"processed_by"`
	Note          string    `json:"note"`
	EnrichedAt    time.Time `json:"enriched_at"`
}

// MessageLog is one persisted row in the message_logs table. Payload is kept as
// raw JSON so it serializes back to the client as an embedded object, not a
// quoted string.
type MessageLog struct {
	ID            int64           `json:"id"`
	Flow          string          `json:"flow"`
	CorrelationID string          `json:"correlation_id"`
	Service       string          `json:"service"`
	Stage         string          `json:"stage"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}
