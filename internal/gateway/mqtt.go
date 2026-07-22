package gateway

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"

	"griddog/internal/db"
	"griddog/internal/emqx"
	"griddog/internal/httpx"
	"griddog/internal/logx"
	"griddog/internal/models"
)

// OnMQTTConnect (re)establishes the gateway's subscription to the completed topic.
// It is wired as the EMQX OnConnectHandler, so it runs on every (re)connection and
// receives the client from paho — safe to use before SetMQTT has stored the client.
func (s *Server) OnMQTTConnect(c mqtt.Client) {
	tok := c.Subscribe(emqx.CompletedTopic, emqx.QoS, s.handleMQTTCompleted)
	if tok.Wait() && tok.Error() != nil {
		log.Printf("gateway subscribe %s failed: %v", emqx.CompletedTopic, tok.Error())
		return
	}
	log.Printf("gateway-backend subscribed to %s", emqx.CompletedTopic)
}

// handleMQTTCompleted routes a completed-topic reply to the waiting request handler
// by correlation id, which (unlike AMQP's CorrelationId property) is read from the
// JSON payload. This runs directly on paho's router goroutine: it is O(1) and never
// blocks (deliver sends on a cap-1 buffered channel), so no goroutine dispatch.
func (s *Server) handleMQTTCompleted(_ mqtt.Client, m mqtt.Message) {
	var enriched models.EnrichedTask
	if err := json.Unmarshal(m.Payload(), &enriched); err != nil || enriched.CorrelationID == "" {
		log.Printf("mqtt completed: bad/empty payload on %s: %v", m.Topic(), err)
		return
	}
	// Copy the payload: paho may recycle the backing slice once this callback returns,
	// and we hand it to the HTTP handler goroutine via the reply channel.
	if !s.mqttPending.deliver(enriched.CorrelationID, append([]byte(nil), m.Payload()...)) {
		log.Printf("mqtt completed: no waiter for correlation_id=%s", enriched.CorrelationID)
	}
}

// handleMQTTCall mirrors handleRabbitMQCall over MQTT/EMQX: publish the task to the
// requests topic and block (with a 15s timeout) until the completed-topic reply is
// routed back by correlation id. No manual tracing — the surrounding net/http server
// span is auto-instrumented by Orchestrion, and the InsertLog calls below become
// child MySQL spans of that trace.
func (s *Server) handleMQTTCall(w http.ResponseWriter, r *http.Request) {
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

	logx.Printf(ctx, "flow4 mqtt-call received value=%d correlation_id=%s", req.Value, corrID)

	if err := db.InsertLog(ctx, s.db, "mqtt", corrID, "gateway", "request_in", map[string]any{"value": req.Value}); err != nil {
		logx.Printf(ctx, "request_in log error: %v", err)
	}

	// register the waiter BEFORE publishing to avoid a race with a fast reply
	replyCh := s.mqttPending.register(corrID)

	body, _ := json.Marshal(task)
	if err := emqx.Publish(s.mqtt, emqx.RequestTopic, body); err != nil {
		s.mqttPending.cancel(corrID)
		logx.Printf(ctx, "mqtt publish error: %v", err)
		httpx.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "publish failed"})
		return
	}
	if err := db.InsertLog(ctx, s.db, "mqtt", corrID, "gateway", "topic_published", task); err != nil {
		logx.Printf(ctx, "topic_published log error: %v", err)
	}

	select {
	case replyBody := <-replyCh:
		var enriched models.EnrichedTask
		_ = json.Unmarshal(replyBody, &enriched)
		if err := db.InsertLog(ctx, s.db, "mqtt", corrID, "gateway", "completed_consumed", enriched); err != nil {
			logx.Printf(ctx, "completed_consumed log error: %v", err)
		}
		resp := map[string]any{
			"correlation_id": corrID,
			"input":          map[string]any{"value": req.Value},
			"result":         enriched,
		}
		if err := db.InsertLog(ctx, s.db, "mqtt", corrID, "gateway", "response_out", resp); err != nil {
			logx.Printf(ctx, "response_out log error: %v", err)
		}
		httpx.WriteJSON(w, http.StatusOK, resp)
	case <-time.After(15 * time.Second):
		s.mqttPending.cancel(corrID)
		httpx.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{
			"error": "timed out waiting for completed topic", "correlation_id": corrID})
	case <-ctx.Done():
		s.mqttPending.cancel(corrID)
	}
}
