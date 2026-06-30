package processing

import (
	"context"
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"griddog/internal/db"
	"griddog/internal/models"
	"griddog/internal/rabbitmq"
)

// StartConsumer consumes processing-queue, enriches each task, and republishes
// it to completed-queue. It runs in a background goroutine until ctx is cancelled.
func (s *Server) StartConsumer(ctx context.Context) error {
	deliveries, err := rabbitmq.Consume(s.ch, rabbitmq.ProcessingQueue)
	if err != nil {
		return err
	}
	go func() {
		log.Printf("processing-backend consuming %s", rabbitmq.ProcessingQueue)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-deliveries:
				if !ok {
					return
				}
				s.handleDelivery(d)
			}
		}
	}()
	return nil
}

func (s *Server) handleDelivery(d amqp.Delivery) {
	// APM consume span + inbound DSM checkpoint; ctx carries both forward so the
	// downstream publish continues the same trace and DSM pathway.
	span, ctx := rabbitmq.StartConsumeSpan(d, rabbitmq.ProcessingQueue)
	defer span.Finish()

	var task models.Task
	if err := json.Unmarshal(d.Body, &task); err != nil {
		log.Printf("bad task message: %v", err)
		_ = d.Nack(false, false) // drop malformed message
		return
	}
	if task.CorrelationID == "" {
		task.CorrelationID = d.CorrelationId
	}

	// message in
	if err := db.InsertLog(ctx, s.db, "rabbitmq", task.CorrelationID, "processing", "queue_consumed", task); err != nil {
		log.Printf("queue_consumed log error: %v", err)
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

	if err := rabbitmq.Publish(ctx, s.ch, "", rabbitmq.CompletedQueue, task.CorrelationID, body); err != nil {
		log.Printf("publish completed-queue error: %v", err)
		_ = d.Nack(false, true) // requeue for another attempt
		return
	}

	// message out
	if err := db.InsertLog(ctx, s.db, "rabbitmq", task.CorrelationID, "processing", "completed_published", enriched); err != nil {
		log.Printf("completed_published log error: %v", err)
	}

	_ = d.Ack(false)
}
