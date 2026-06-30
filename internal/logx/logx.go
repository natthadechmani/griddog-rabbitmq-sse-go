// Package logx provides trace-correlated logging on top of the standard library
// log package, following Datadog's "Correlating Go Logs and Traces" manual
// injection approach: the active span is appended with the %v verb, which emits
// dd.service / dd.env / dd.version / dd.trace_id / dd.span_id so Datadog links the
// log line to its trace.
package logx

import (
	"context"
	"log"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
)

// Printf logs like log.Printf, but when ctx carries an active span it appends the
// span's Datadog correlation fields (dd.trace_id, dd.span_id, dd.service, dd.env,
// dd.version). With no active span it falls back to a plain log line.
func Printf(ctx context.Context, format string, args ...any) {
	if span, ok := tracer.SpanFromContext(ctx); ok {
		withSpan := make([]any, 0, len(args)+1)
		withSpan = append(withSpan, args...)
		withSpan = append(withSpan, span)
		log.Printf(format+" %v", withSpan...)
		return
	}
	log.Printf(format, args...)
}
