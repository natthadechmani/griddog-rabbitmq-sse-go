package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"griddog/internal/models"
)

// Connect opens a MySQL connection, retrying until the server is reachable.
func Connect(dsn string) (*sql.DB, error) {
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		conn, err := sql.Open("mysql", dsn)
		if err == nil {
			conn.SetConnMaxLifetime(3 * time.Minute)
			conn.SetMaxOpenConns(10)
			conn.SetMaxIdleConns(10)
			if pingErr := conn.Ping(); pingErr == nil {
				log.Printf("connected to MySQL")
				return conn, nil
			} else {
				lastErr = pingErr
				_ = conn.Close()
			}
		} else {
			lastErr = err
		}
		log.Printf("waiting for MySQL (attempt %d/30): %v", attempt, lastErr)
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("could not connect to MySQL after retries: %w", lastErr)
}

const schemaDDL = `
CREATE TABLE IF NOT EXISTS message_logs (
	id             BIGINT NOT NULL AUTO_INCREMENT,
	flow           VARCHAR(32)  NOT NULL,
	correlation_id VARCHAR(64)  NOT NULL,
	service        VARCHAR(32)  NOT NULL,
	stage          VARCHAR(48)  NOT NULL,
	payload        JSON         NOT NULL,
	created_at     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	PRIMARY KEY (id),
	KEY idx_flow (flow),
	KEY idx_correlation (correlation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`

// EnsureSchema creates the message_logs table if it does not already exist.
func EnsureSchema(ctx context.Context, conn *sql.DB) error {
	_, err := conn.ExecContext(ctx, schemaDDL)
	return err
}

// InsertLog persists one hop of a flow. payload is marshaled to JSON.
func InsertLog(ctx context.Context, conn *sql.DB, flow, correlationID, service, stage string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	_, err = conn.ExecContext(ctx,
		`INSERT INTO message_logs (flow, correlation_id, service, stage, payload) VALUES (?, ?, ?, ?, ?)`,
		flow, correlationID, service, stage, string(raw))
	return err
}

// ListLogsByFlow returns logs for a flow ordered chronologically (by id).
func ListLogsByFlow(ctx context.Context, conn *sql.DB, flow string, limit int) ([]models.MessageLog, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	rows, err := conn.QueryContext(ctx,
		`SELECT id, flow, correlation_id, service, stage, payload, created_at
		   FROM message_logs WHERE flow = ? ORDER BY id ASC LIMIT ?`, flow, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.MessageLog, 0, limit)
	for rows.Next() {
		var m models.MessageLog
		var payload []byte
		if err := rows.Scan(&m.ID, &m.Flow, &m.CorrelationID, &m.Service, &m.Stage, &payload, &m.CreatedAt); err != nil {
			return nil, err
		}
		m.Payload = payload
		out = append(out, m)
	}
	return out, rows.Err()
}
