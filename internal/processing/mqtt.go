package processing

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"griddog/internal/db"
	"griddog/internal/emqx"
	"griddog/internal/logx"
	"griddog/internal/models"
	"griddog/internal/tracing"
)

// OnMQTTConnect (re)establishes the processing subscription to the requests topic.
// Wired as autopaho's OnConnectionUp, it runs on every (re)connection and receives the
// live ConnectionManager.
func (s *Server) OnMQTTConnect(cm *autopaho.ConnectionManager) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cm.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: emqx.RequestTopic, QoS: emqx.QoS}},
	}); err != nil {
		log.Printf("processing subscribe %s failed: %v", emqx.RequestTopic, err)
		return
	}
	log.Printf("processing-backend subscribed to %s", emqx.RequestTopic)
}

// handleMQTTRequest runs on autopaho's inbound router, so it must return quickly. It
// copies the payload and hands the work to a goroutine — processMQTT does a DB write
// AND a publish, which shouldn't run on the router goroutine.
func (s *Server) handleMQTTRequest(_ string, payload []byte, tr emqx.Trace) {
	p := append([]byte(nil), payload...)
	go s.processMQTT(p, tr)
}

// processMQTT enriches a task and republishes it to the completed topic — the MQTT
// analog of handleDelivery, minus the tracing. There is no ambient span here (the MQTT
// client is not auto-instrumented), so the InsertLog calls below are standalone MySQL
// traces; that is expected for this phase.
func (s *Server) processMQTT(payload []byte, tr emqx.Trace) {
	// Continue the gateway's trace: turn the traceparent EMQX forwarded on delivery into
	// a consume span, so processing's work (DB writes + reply publish) joins the same
	// end-to-end Datadog trace instead of being an orphan.
	span, ctx := tracing.StartConsumeSpan("mqtt.process", "consume "+emqx.RequestTopic, tr)
	defer span.Finish()

	var task models.Task
	if err := json.Unmarshal(payload, &task); err != nil || task.CorrelationID == "" {
		logx.Printf(ctx, "mqtt request: bad/empty task: %v", err)
		return
	}

	logx.Printf(ctx, "flow4 consumed correlation_id=%s value=%d", task.CorrelationID, task.Value)

	if err := db.InsertLog(ctx, s.db, "mqtt", task.CorrelationID, "processing", "topic_consumed", task); err != nil {
		logx.Printf(ctx, "topic_consumed log error: %v", err)
	}

	// enrich / manipulate the message (same math as the RabbitMQ flow)
	enriched := models.EnrichedTask{
		CorrelationID: task.CorrelationID,
		OriginalValue: task.Value,
		Doubled:       task.Value * 2,
		Squared:       task.Value * task.Value,
		ProcessedBy:   "processing-backend",
		Note:          "enriched + doubled",
		EnrichedAt:    time.Now(),
	}
	body, _ := json.Marshal(enriched)

	// Publish the reply carrying THIS consume span's context, so the completed-topic
	// broker spans nest under processing (proper causal ordering within the trace).
	if err := emqx.Publish(ctx, s.mqtt, emqx.CompletedTopic, body, tracing.Inject(ctx)); err != nil {
		logx.Printf(ctx, "publish completed topic error corr=%s: %v", task.CorrelationID, err)
		return
	}

	if err := db.InsertLog(ctx, s.db, "mqtt", task.CorrelationID, "processing", "completed_published", enriched); err != nil {
		logx.Printf(ctx, "completed_published log error: %v", err)
	}
}
