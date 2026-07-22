package processing

import (
	"context"
	"encoding/json"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"griddog/internal/db"
	"griddog/internal/emqx"
	"griddog/internal/logx"
	"griddog/internal/models"
)

// OnMQTTConnect (re)establishes the processing subscription to the requests topic.
// Wired as the EMQX OnConnectHandler, it runs on every (re)connection and receives
// the client from paho.
func (s *Server) OnMQTTConnect(c mqtt.Client) {
	tok := c.Subscribe(emqx.RequestTopic, emqx.QoS, s.handleMQTTRequest)
	if tok.Wait() && tok.Error() != nil {
		log.Printf("processing subscribe %s failed: %v", emqx.RequestTopic, tok.Error())
		return
	}
	log.Printf("processing-backend subscribed to %s", emqx.RequestTopic)
}

// handleMQTTRequest runs on paho's router goroutine, so it must return quickly. It
// copies the payload (paho recycles the buffer after the callback returns) and hands
// the work to a goroutine — processMQTT does a DB write AND a publish+Wait, which
// would block/deadlock the single router goroutine if run inline.
func (s *Server) handleMQTTRequest(_ mqtt.Client, m mqtt.Message) {
	payload := append([]byte(nil), m.Payload()...)
	go s.processMQTT(payload)
}

// processMQTT enriches a task and republishes it to the completed topic — the MQTT
// analog of handleDelivery, minus the tracing. There is no ambient span here (paho
// is not auto-instrumented), so the InsertLog calls below are standalone MySQL
// traces; that is expected for this phase.
func (s *Server) processMQTT(payload []byte) {
	ctx := context.Background()

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

	if err := emqx.Publish(s.mqtt, emqx.CompletedTopic, body); err != nil {
		logx.Printf(ctx, "publish completed topic error corr=%s: %v", task.CorrelationID, err)
		return
	}

	if err := db.InsertLog(ctx, s.db, "mqtt", task.CorrelationID, "processing", "completed_published", enriched); err != nil {
		logx.Printf(ctx, "completed_published log error: %v", err)
	}
}
