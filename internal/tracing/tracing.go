// Package tracing bridges dd-trace-go and the transport-only emqx.Trace carrier: it
// injects the active span's W3C traceparent for propagation over MQTT, and continues a
// trace on the consumer side by extracting a forwarded traceparent into a new span.
//
// This is propagation + a single consume span — NOT the full manual produce/consume +
// DSM instrumentation of the RabbitMQ flow. It exists so the processing backend's work
// joins the gateway's end-to-end trace (EMQX forwards the traceparent on delivery, but
// something has to turn it back into a span on the consumer).
package tracing

import (
	"context"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	"griddog/internal/emqx"
)

// Inject returns the active span's W3C traceparent (as an emqx.Trace) so it can ride an
// MQTT publish. Empty if there is no active span. We deliberately carry only traceparent
// (see emqx.Trace.pubProperties for why tracestate is dropped).
func Inject(ctx context.Context) emqx.Trace {
	span, ok := tracer.SpanFromContext(ctx)
	if !ok {
		return emqx.Trace{}
	}
	carrier := tracer.TextMapCarrier{}
	if err := tracer.Inject(span.Context(), carrier); err != nil {
		return emqx.Trace{}
	}
	return emqx.Trace{Traceparent: carrier["traceparent"]}
}

// StartConsumeSpan starts a consumer span that continues the trace carried by tr (if
// present) so the consumer's work joins the producer's trace. It always returns a usable
// span (a fresh root when tr is empty) plus a ctx carrying it; the caller MUST Finish it.
// The span inherits the service from DD_SERVICE (e.g. processing-backend).
func StartConsumeSpan(name, resource string, tr emqx.Trace) (*tracer.Span, context.Context) {
	opts := []tracer.StartSpanOption{
		tracer.ResourceName(resource),
		tracer.SpanType(ext.SpanTypeMessageConsumer),
		tracer.Tag(ext.SpanKind, ext.SpanKindConsumer),
		tracer.Tag(ext.MessagingSystem, "mqtt"),
	}
	if tr.Traceparent != "" {
		if parent, err := tracer.Extract(tracer.TextMapCarrier{"traceparent": tr.Traceparent}); err == nil {
			opts = append(opts, tracer.ChildOf(parent))
		}
	}
	span := tracer.StartSpan(name, opts...)
	return span, tracer.ContextWithSpan(context.Background(), span)
}
