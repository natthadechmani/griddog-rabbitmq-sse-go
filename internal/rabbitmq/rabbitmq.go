package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/DataDog/dd-trace-go/v2/datastreams"
	"github.com/DataDog/dd-trace-go/v2/datastreams/options"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	"griddog/internal/logx"
)

// Queue names shared by both services.
const (
	ProcessingQueue = "processing-queue" // gateway -> processing
	CompletedQueue  = "completed-queue"  // processing -> gateway
)

// brokerHost identifies the RabbitMQ broker, captured once at Connect from the AMQP
// URL. It tags producer spans as out.host so Datadog resolves peer.service and draws
// the broker on the service map — the RabbitMQ analog of Kafka's bootstrap.servers.
var brokerHost string

// Connect dials RabbitMQ and opens a channel, retrying until reachable.
func Connect(url string) (*amqp.Connection, *amqp.Channel, error) {
	if uri, err := amqp.ParseURI(url); err == nil {
		brokerHost = uri.Host
	}
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

// Publish sends body to (exchange, routingKey) with an APM producer span and a
// DSM outbound checkpoint. The trace context and the DSM pathway are injected
// into the message headers so the consumer links back to this produce.
//
// This app uses the default exchange (exchange == ""), so routingKey is the
// destination queue name.
func Publish(ctx context.Context, ch *amqp.Channel, exchange, routingKey, correlationID string, body []byte) (err error) {
	msg := amqp.Publishing{
		Headers:       amqp.Table{}, // non-nil so we can inject headers
		ContentType:   "application/json",
		CorrelationId: correlationID,
		Body:          body,
		DeliveryMode:  amqp.Persistent,
		Timestamp:     time.Now(),
	}

	// 1. APM producer span (child of whatever is in ctx). Tags follow the amqp.*
	//    convention that Datadog's Java/Node auto-instrumentation emits.
	span, ctx := tracer.StartSpanFromContext(ctx, "rabbitmq.publish",
		tracer.ResourceName("publish "+routingKey),
		tracer.SpanType(ext.SpanTypeMessageProducer),
		tracer.Tag(ext.SpanKind, ext.SpanKindProducer),
		tracer.Tag(ext.Component, "rabbitmq"),
		tracer.Tag(ext.MessagingSystem, "rabbitmq"),
		tracer.Tag(ext.MessagingDestinationName, routingKey),
		// out.host -> peer.service, so the broker shows on the service map.
		tracer.Tag(ext.TargetHost, brokerHost),
		tracer.Tag("amqp.exchange", exchangeTag(exchange)),
		tracer.Tag("amqp.routing_key", routingKey),
		tracer.Tag("amqp.command", "basic.publish"),
		tracer.Tag("amqp.delivery_mode", int(msg.DeliveryMode)),
		tracer.Tag("message.size", len(body)),
		tracer.Tag("messaging.message_id", correlationID),
	)
	// The deferred Finish records err (the named return) on the span when non-nil.
	defer func() { span.Finish(tracer.WithError(err)) }()

	// 2. Inject the APM trace context into the headers.
	_ = tracer.Inject(span.Context(), amqpCarrier(msg.Headers))

	// 3. DSM outbound checkpoint, then inject the pathway into the headers. This app
	//    publishes to the default exchange, so the routing key is the destination
	//    queue: tag it topic:<queue> so produce and consume share one DSM edge (the
	//    convention dd-trace-js amqplib uses for the default exchange).
	ctx, ok := tracer.SetDataStreamsCheckpointWithParams(ctx,
		options.CheckpointParams{PayloadSize: amqpMessageSize(msg.Headers, msg.Body)},
		"direction:out", "type:rabbitmq", "topic:"+routingKey,
	)
	if ok {
		datastreams.InjectToBase64Carrier(ctx, amqpCarrier(msg.Headers))
	}

	// 4. Publish; the deferred Finish records any error on the span.
	err = ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg)
	return err
}

// Consume registers a consumer on a queue with manual acknowledgement.
func Consume(ch *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	return ch.Consume(queue, "", false, false, false, false, nil)
}

// StartConsumeSpan begins an APM consumer span (child of the producer's span) and
// an inbound DSM checkpoint for a delivery. The caller MUST Finish the returned
// span. The returned ctx carries BOTH the active span and the DSM pathway: pass
// it to a downstream Publish to continue the trace and the pathway in one go.
//
// Like dd-trace-go's Kafka integration, the consume span records no processing
// error — it is finished with a plain span.Finish(); the handler owns Ack/Nack and
// deals with its own failures. Only the producer records errors (see Publish).
func StartConsumeSpan(d amqp.Delivery, queue string) (*tracer.Span, context.Context) {
	// 1. Extract the upstream APM trace context so this consume is a child span.
	parent, _ := tracer.Extract(amqpCarrier(d.Headers))
	span := tracer.StartSpan("rabbitmq.consume",
		tracer.ChildOf(parent),
		tracer.ResourceName("consume "+queue),
		tracer.SpanType(ext.SpanTypeMessageConsumer),
		tracer.Tag(ext.SpanKind, ext.SpanKindConsumer),
		tracer.Tag(ext.Component, "rabbitmq"),
		tracer.Tag(ext.MessagingSystem, "rabbitmq"),
		tracer.Tag(ext.MessagingDestinationName, queue),
		tracer.Tag("amqp.queue", queue),
		tracer.Tag("amqp.exchange", exchangeTag(d.Exchange)),
		tracer.Tag("amqp.routing_key", d.RoutingKey),
		tracer.Tag("amqp.command", "basic.deliver"),
		tracer.Tag("message.size", len(d.Body)),
		tracer.Measured(),
	)
	ctx := tracer.ContextWithSpan(context.Background(), span)

	// Dump the raw propagated headers, trace-correlated to this consume span.
	logCarrierHeaders(ctx, queue, d.Headers)

	// 2. DSM: extract the upstream pathway from headers, then set the inbound
	//    checkpoint, threaded onto the span's ctx.
	ctx, _ = tracer.SetDataStreamsCheckpointWithParams(
		datastreams.ExtractFromBase64Carrier(ctx, amqpCarrier(d.Headers)),
		options.CheckpointParams{PayloadSize: amqpMessageSize(d.Headers, d.Body)},
		"direction:in", "type:rabbitmq", "topic:"+queue,
	)
	return span, ctx
}

func exchangeTag(exchange string) string {
	if exchange == "" {
		return "default"
	}
	return exchange
}

// amqpMessageSize approximates the DSM payload size like dd-trace-go's Kafka
// getMsgSize and dd-trace-js's getAmqpMessageSize: the body plus each header's
// key/value bytes (the trace-context and pathway headers are strings).
func amqpMessageSize(headers amqp.Table, body []byte) int64 {
	size := int64(len(body))
	for k, v := range headers {
		size += int64(len(k))
		if s, ok := v.(string); ok {
			size += int64(len(s))
		}
	}
	return size
}

// logCarrierHeaders prints every header on an incoming delivery. This surfaces
// the propagated context that travels in the message: the APM trace context
// (x-datadog-* and W3C traceparent/tracestate) and the DSM pathway
// (dd-pathway-ctx-base64). Logging is on by default; set LOG_AMQP_HEADERS=false
// to silence it.
func logCarrierHeaders(ctx context.Context, queue string, headers amqp.Table) {
	if os.Getenv("LOG_AMQP_HEADERS") == "false" {
		return
	}
	// Raw Go-syntax dump of the full headers map (exactly:
	// log.Printf("amqp headers: %#v", d.Headers)), trace-correlated via logx.
	logx.Printf(ctx, "amqp headers (%s): %#v", queue, headers)

	// Readable, sorted key/value view (each line also trace-correlated).
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	logx.Printf(ctx, "[amqp headers] consume %q — %d header(s):", queue, len(headers))
	for _, k := range keys {
		logx.Printf(ctx, "    %s = %v", k, headers[k])
	}
}
