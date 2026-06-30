package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Queue names shared by both services.
const (
	ProcessingQueue = "processing-queue" // gateway -> processing
	CompletedQueue  = "completed-queue"  // processing -> gateway
)

// Connect dials RabbitMQ and opens a channel, retrying until reachable.
func Connect(url string) (*amqp.Connection, *amqp.Channel, error) {
	var lastErr error
	for attempt := 1; attempt <= 30; attempt++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			ch, chErr := conn.Channel()
			if chErr == nil {
				log.Printf("connected to RabbitMQ")
				return conn, ch, nil
			}
			lastErr = chErr
			_ = conn.Close()
		} else {
			lastErr = err
		}
		log.Printf("waiting for RabbitMQ (attempt %d/30): %v", attempt, lastErr)
		time.Sleep(2 * time.Second)
	}
	return nil, nil, fmt.Errorf("could not connect to RabbitMQ after retries: %w", lastErr)
}

// DeclareQueues declares durable queues (idempotent, safe to call from both services).
func DeclareQueues(ch *amqp.Channel, names ...string) error {
	for _, name := range names {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare queue %s: %w", name, err)
		}
	}
	return nil
}

// Publish sends body to a queue through the default exchange
// (routing key = queue name).
func Publish(ctx context.Context, ch *amqp.Channel, queue, correlationID string, body []byte) error {
	return ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationID,
		Body:          body,
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now(),
	})
}

// Consume registers a consumer on a queue with manual acknowledgement.
func Consume(ch *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	return ch.Consume(queue, "", false, false, false, false, nil)
}
