package processing

import (
	"context"
	"encoding/json"
	"log"
	"time"

	messaging "github.com/natthadechmani/go-rabbitmq-messaging"

	"griddog/internal/db"
	"griddog/internal/logx"
	"griddog/internal/models"
	"griddog/internal/queues"
)

// StartConsumer consumes processing-queue via the shared messaging client, enriches each
// task, and republishes it to completed-queue. The library owns the delivery loop, the
// consume span + inbound DSM checkpoint, and ack/nack; we just supply the handler.
func (s *Server) StartConsumer(ctx context.Context) error {
	go func() {
		log.Printf("processing-backend consuming %s", queues.Processing)
		if err := s.mq.Consume(ctx, queues.Processing, s.handleDelivery); err != nil {
			log.Printf("consumer stopped: %v", err)
		}
	}()
	return nil
}

// handleDelivery is the messaging.Handler for processing-queue. The ctx already carries
// the consume span AND the DSM pathway, so passing it to mq.Publish continues the same
// trace and pathway. Return nil → Ack; return err → Nack+requeue.
func (s *Server) handleDelivery(ctx context.Context, d messaging.Delivery) error {
	var task models.Task
	if err := json.Unmarshal(d.Body, &task); err != nil {
		logx.Printf(ctx, "bad task message: %v", err)
		return nil // drop malformed message (Ack) — avoid a poison requeue loop
	}
	if task.CorrelationID == "" {
		task.CorrelationID = d.CorrelationId
	}

	logx.Printf(ctx, "flow2 consumed correlation_id=%s value=%d", task.CorrelationID, task.Value)

	// message in
	if err := db.InsertLog(ctx, s.db, "rabbitmq", task.CorrelationID, "processing", "queue_consumed", task); err != nil {
		logx.Printf(ctx, "queue_consumed log error: %v", err)
	}

	// enrich / manipulate the message
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

	// span + DSM checkpoint happen inside Publish; ctx keeps trace + pathway connected.
	if err := s.mq.Publish(ctx, "", queues.Completed, task.CorrelationID, body); err != nil {
		logx.Printf(ctx, "publish completed-queue error: %v", err)
		return err // Nack + requeue for another attempt
	}

	// message out
	if err := db.InsertLog(ctx, s.db, "rabbitmq", task.CorrelationID, "processing", "completed_published", enriched); err != nil {
		logx.Printf(ctx, "completed_published log error: %v", err)
	}
	return nil
}
