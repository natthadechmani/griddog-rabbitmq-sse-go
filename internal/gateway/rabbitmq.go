package gateway

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	messaging "github.com/natthadechmani/go-rabbitmq-messaging"

	"griddog/internal/db"
	"griddog/internal/httpx"
	"griddog/internal/logx"
	"griddog/internal/models"
	"griddog/internal/queues"
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

// StartCompletedConsumer consumes completed-queue via the shared messaging client and
// routes each reply to the waiting request handler by correlation id. The library creates
// the APM consume span + inbound DSM checkpoint and hands us a ctx that already carries
// both; this handler only resolves the waiter.
func (s *Server) StartCompletedConsumer(ctx context.Context) error {
	go func() {
		log.Printf("gateway-backend consuming %s", queues.Completed)
		if err := s.mq.Consume(ctx, queues.Completed, s.handleCompleted); err != nil {
			log.Printf("completed consumer stopped: %v", err)
		}
	}()
	return nil
}

// handleCompleted is the messaging.Handler for the completed-queue (terminal hop).
// Returning nil Acks the delivery (the library owns ack/nack).
func (s *Server) handleCompleted(ctx context.Context, d messaging.Delivery) error {
	if !s.pending.deliver(d.CorrelationId, d.Body) {
		logx.Printf(ctx, "no waiter for correlation_id=%s", d.CorrelationId)
	}
	return nil
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

	logx.Printf(ctx, "flow2 rabbitmq-call received value=%d correlation_id=%s", req.Value, corrID)

	// log the API request (req)
	if err := db.InsertLog(ctx, s.db, "rabbitmq", corrID, "gateway", "request_in", map[string]any{"value": req.Value}); err != nil {
		logx.Printf(ctx, "request_in log error: %v", err)
	}

	// register the waiter BEFORE publishing to avoid a race with a fast reply
	replyCh := s.pending.register(corrID)

	body, _ := json.Marshal(task)
	// span + DSM checkpoint happen inside the library's Publish.
	if err := s.mq.Publish(ctx, "", queues.Processing, corrID, body); err != nil {
		s.pending.cancel(corrID)
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "publish failed"})
		return
	}
	// log the queue message
	if err := db.InsertLog(ctx, s.db, "rabbitmq", corrID, "gateway", "queue_published", task); err != nil {
		logx.Printf(ctx, "queue_published log error: %v", err)
	}

	select {
	case replyBody := <-replyCh:
		var enriched models.EnrichedTask
		_ = json.Unmarshal(replyBody, &enriched)
		// message in (consumed from completed-queue)
		if err := db.InsertLog(ctx, s.db, "rabbitmq", corrID, "gateway", "completed_consumed", enriched); err != nil {
			logx.Printf(ctx, "completed_consumed log error: %v", err)
		}
		resp := map[string]any{
			"correlation_id": corrID,
			"input":          map[string]any{"value": req.Value},
			"result":         enriched,
		}
		// message out (response to browser)
		if err := db.InsertLog(ctx, s.db, "rabbitmq", corrID, "gateway", "response_out", resp); err != nil {
			logx.Printf(ctx, "response_out log error: %v", err)
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
