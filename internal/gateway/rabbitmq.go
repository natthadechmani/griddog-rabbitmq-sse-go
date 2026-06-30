package gateway

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/google/uuid"

	"griddog/internal/db"
	"griddog/internal/httpx"
	"griddog/internal/models"
	"griddog/internal/rabbitmq"
)

// pendingRegistry correlates published requests with their completed-queue
// replies so a synchronous HTTP handler can wait for the async round-trip.
type pendingRegistry struct {
	mu sync.Mutex
	m  map[string]chan []byte
}

func newPendingRegistry() *pendingRegistry {
	return &pendingRegistry{m: make(map[string]chan []byte)}
}

func (p *pendingRegistry) register(id string) chan []byte {
	ch := make(chan []byte, 1)
	p.mu.Lock()
	p.m[id] = ch
	p.mu.Unlock()
	return ch
}

func (p *pendingRegistry) deliver(id string, body []byte) bool {
	p.mu.Lock()
	ch, ok := p.m[id]
	if ok {
		delete(p.m, id)
	}
	p.mu.Unlock()
	if ok {
		ch <- body // buffered (cap 1): never blocks
	}
	return ok
}

func (p *pendingRegistry) cancel(id string) {
	p.mu.Lock()
	delete(p.m, id)
	p.mu.Unlock()
}

// StartCompletedConsumer consumes completed-queue and routes each reply to the
// waiting request handler by correlation id.
func (s *Server) StartCompletedConsumer(ctx context.Context) error {
	deliveries, err := rabbitmq.Consume(s.ch, rabbitmq.CompletedQueue)
	if err != nil {
		return err
	}
	go func() {
		log.Printf("gateway-backend consuming %s", rabbitmq.CompletedQueue)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				s.handleCompleted(d)
			}
		}
	}()
	return nil
}

// handleCompleted routes a completed-queue reply to the waiting request handler,
// wrapped in an APM consume span + inbound DSM checkpoint (terminal hop of the
// flow-2 pathway, so the ctx isn't threaded onward).
func (s *Server) handleCompleted(d amqp.Delivery) {
	span, _ := rabbitmq.StartConsumeSpan(d, rabbitmq.CompletedQueue)
	defer span.Finish()
	if !s.pending.deliver(d.CorrelationId, d.Body) {
		log.Printf("no waiter for correlation_id=%s", d.CorrelationId)
	}
	_ = d.Ack(false)
}

// flowRequest is the body for both /rabbitmq-call and /http-call.
type flowRequest struct {
	Value int `json:"value"`
}

func (s *Server) handleRabbitMQCall(w http.ResponseWriter, r *http.Request) {
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

	// log the API request (req)
	if err := db.InsertLog(ctx, s.db, "rabbitmq", corrID, "gateway", "request_in", map[string]any{"value": req.Value}); err != nil {
		log.Printf("request_in log error: %v", err)
	}

	// register the waiter BEFORE publishing to avoid a race with a fast reply
	replyCh := s.pending.register(corrID)

	body, _ := json.Marshal(task)
	if err := rabbitmq.Publish(ctx, s.ch, "", rabbitmq.ProcessingQueue, corrID, body); err != nil {
		s.pending.cancel(corrID)
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "publish failed"})
		return
	}
	// log the queue message
	if err := db.InsertLog(ctx, s.db, "rabbitmq", corrID, "gateway", "queue_published", task); err != nil {
		log.Printf("queue_published log error: %v", err)
	}

	select {
	case replyBody := <-replyCh:
		var enriched models.EnrichedTask
		_ = json.Unmarshal(replyBody, &enriched)
		// message in (consumed from completed-queue)
		if err := db.InsertLog(ctx, s.db, "rabbitmq", corrID, "gateway", "completed_consumed", enriched); err != nil {
			log.Printf("completed_consumed log error: %v", err)
		}
		resp := map[string]any{
			"correlation_id": corrID,
			"input":          map[string]any{"value": req.Value},
			"result":         enriched,
		}
		// message out (response to browser)
		if err := db.InsertLog(ctx, s.db, "rabbitmq", corrID, "gateway", "response_out", resp); err != nil {
			log.Printf("response_out log error: %v", err)
		}
		httpx.WriteJSON(w, http.StatusOK, resp)
	case <-time.After(15 * time.Second):
		s.pending.cancel(corrID)
		httpx.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{
			"error": "timed out waiting for completed-queue", "correlation_id": corrID})
	case <-ctx.Done():
		s.pending.cancel(corrID)
	}
}
