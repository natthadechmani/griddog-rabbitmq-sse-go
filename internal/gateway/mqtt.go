package gateway

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"

	"griddog/internal/db"
	"griddog/internal/emqx"
	"griddog/internal/httpx"
	"griddog/internal/logx"
	"griddog/internal/models"
)

// OnMQTTConnect (re)establishes the gateway's subscription to the completed topic. It
// is wired as autopaho's OnConnectionUp, so it runs on every (re)connection and
// receives the live ConnectionManager — safe to use before SetMQTT has stored it.
func (s *Server) OnMQTTConnect(cm *autopaho.ConnectionManager) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: emqx.CompletedTopic, QoS: emqx.QoS}},
	}); err != nil {
		log.Printf("gateway subscribe %s failed: %v", emqx.CompletedTopic, err)
		return
	}
	log.Printf("gateway-backend subscribed to %s", emqx.CompletedTopic)
}

// handleMQTTCompleted routes a completed-topic reply to the waiting request handler by
// correlation id, which (unlike AMQP's CorrelationId property) is read from the JSON
// payload. It runs on autopaho's inbound router; it is O(1) and never blocks (deliver
// sends on a cap-1 buffered channel), so no goroutine dispatch.
func (s *Server) handleMQTTCompleted(topic string, payload []byte) {
	var enriched models.EnrichedTask
	if err := json.Unmarshal(payload, &enriched); err != nil || enriched.CorrelationID == "" {
		log.Printf("mqtt completed: bad/empty payload on %s: %v", topic, err)
		return
	}
	// Copy the payload before handing it to the HTTP handler goroutine via the channel.
	if !s.mqttPending.deliver(enriched.CorrelationID, append([]byte(nil), payload...)) {
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
	if err := emqx.Publish(ctx, s.mqtt, emqx.RequestTopic, body); err != nil {
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
