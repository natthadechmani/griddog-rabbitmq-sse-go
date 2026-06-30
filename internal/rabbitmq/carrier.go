package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
)

// amqpCarrier adapts an amqp.Table (map[string]any) to the tracer's TextMap
// interfaces so the SAME message headers carry both the APM trace context and the
// DSM pathway. This mirrors the Kafka contrib's MessageCarrier design.
//
// The method set (Set + ForeachKey) also satisfies datastreams.TextMapReader /
// datastreams.TextMapWriter, which are identical interfaces — so one carrier is
// reused for span propagation (tracer.Inject/Extract) and DSM propagation
// (datastreams.InjectToBase64Carrier/ExtractFromBase64Carrier).
type amqpCarrier amqp.Table

// Compile-time check that amqpCarrier satisfies both tracer TextMap interfaces.
// A missing method becomes a build error, not a runtime one.
var _ interface {
	tracer.TextMapReader
	tracer.TextMapWriter
} = (amqpCarrier)(nil)

// Set implements TextMapWriter (used by tracer.Inject and InjectToBase64Carrier).
func (c amqpCarrier) Set(key, val string) {
	c[key] = val
}

// ForeachKey implements TextMapReader (used by tracer.Extract and ExtractFromBase64Carrier).
func (c amqpCarrier) ForeachKey(handler func(key, val string) error) error {
	for k, v := range c {
		// Span/DSM headers are written as strings; ignore non-string headers.
		if s, ok := v.(string); ok {
			if err := handler(k, s); err != nil {
				return err
			}
		}
	}
	return nil
}
