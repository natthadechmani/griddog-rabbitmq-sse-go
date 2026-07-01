# Manual APM Spans + DSM for RabbitMQ (Go, dd-trace-go v2)

How this repo instruments RabbitMQ by hand, and how to reproduce it anywhere.

## Why manual?

dd-trace-go has **no contrib package and no Orchestrion auto-instrumentation** for
RabbitMQ/AMQP. Orchestrion here auto-instruments HTTP + `database/sql`, but
the queue hops are invisible until you add spans + Data Streams (DSM) yourself. Two
independent things travel in the **message headers** and are added manually:


| System          | What it gives you                                                            | Header(s) it writes                                                      |
| --------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| **APM span**    | the publish/consume operation in a trace; parent/child links across services | `x-datadog-trace-id`, `x-datadog-parent-id`, `traceparent`, `tracestate` |
| **DSM pathway** | queue-to-queue flow, throughput, end-to-end latency                          | `dd-pathway-ctx-base64`                                                  |


Both are propagated with the **same carrier** and are inject/extract mirror images.

**How the link works:** the trace context travels **inside the message headers** — you
`Inject` it on publish and `Extract` it on consume, so the consumer's span becomes a
child of the producer's and publish → consume shows up as **one trace across services**.

```
producer                          RabbitMQ                       consumer
rabbitmq.publish span ──Inject──▶ [message headers] ──Extract──▶ rabbitmq.consume span
                                                                  (ChildOf producer → same trace)
```

## Prerequisites

- `github.com/DataDog/dd-trace-go/v2` (v2.9.1 here). Import
`.../ddtrace/tracer`, `.../ddtrace/ext`, `.../datastreams`, `.../datastreams/options`.
- The tracer must be **running** — here Orchestrion starts it automatically; otherwise
call `tracer.Start()` / `defer tracer.Stop()` in `main`.
- `DD_DATA_STREAMS_ENABLED=true` (DSM only; spans work without it). Set per service.
- `DD_SERVICE` / `DD_ENV` / `DD_VERSION` for correct service tagging.

---

## 1. The header carrier (required for linking)

To carry the trace context through RabbitMQ, `tracer.Inject`/`tracer.Extract` need a
carrier that can read/write the message headers. dd-trace-go's built-in
`tracer.HTTPHeadersCarrier` only works with `http.Header`; RabbitMQ headers are an
`amqp.Table` (`map[string]any`), so you add this ~8-line adapter (`Set` = write,
`ForeachKey` = read):

`**carrier.go`**

```go
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
```

---

## 2. Publish — start span, then inject into headers

`**rabbitmq.go**`

```go
package rabbitmq

import (
    "context"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"

    "github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
    "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
)

// Publish sends body to (exchange, routingKey) with an APM producer span; the trace
// context is injected into the message headers so the consumer links to it.
// This app uses the default exchange (exchange == ""), so routingKey is the queue name.
func Publish(ctx context.Context, ch *amqp.Channel, exchange, routingKey, correlationID string, body []byte) error {
    msg := amqp.Publishing{
        Headers:       amqp.Table{}, // non-nil so we can inject headers
        ContentType:   "application/json",
        CorrelationId: correlationID,    // ties request/reply together (and used as a span tag)
        Body:          body,
        DeliveryMode:  amqp.Persistent,  // persist to disk so the message survives a broker restart
        Timestamp:     time.Now(),       // when the message was produced
    }

    // 1. Start the producer span (child of whatever span is in ctx, e.g. an HTTP request).
    span, ctx := tracer.StartSpanFromContext(ctx, "rabbitmq.publish",
        tracer.ResourceName("publish "+routingKey),
        tracer.SpanType(ext.SpanTypeMessageProducer),
        tracer.Tag(ext.MessagingSystem, "rabbitmq"),
        tracer.Tag("messaging.destination", routingKey),
        tracer.Tag("messaging.message_id", correlationID),
    )
    defer span.Finish()

    // 2. Inject the trace context into the message headers so the consumer can link to it.
    _ = tracer.Inject(span.Context(), amqpCarrier(msg.Headers))

    // 3. Publish, recording any error on the span.
    if err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg); err != nil {
        span.SetTag(ext.Error, err)
        return err
    }
    return nil
}
```

## 3. Consume — extract from headers, start a child span

`**rabbitmq.go**` (same file, no `time` needed here)

```go
package rabbitmq

import (
    "context"

    amqp "github.com/rabbitmq/amqp091-go"

    "github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
    "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
)

func handleDelivery(d amqp.Delivery, queue string) context.Context {
    // Extract the upstream trace context so this consume is a CHILD of the producer's span.
    parent, _ := tracer.Extract(amqpCarrier(d.Headers)) // err is fine: no headers = new root
    span := tracer.StartSpan("rabbitmq.consume",
        tracer.ChildOf(parent), // <-- this is what links the two services into one trace
        tracer.ResourceName("consume "+queue),
        tracer.SpanType(ext.SpanTypeMessageConsumer),
        tracer.Tag(ext.MessagingSystem, "rabbitmq"),
        tracer.Tag("messaging.destination", queue),
    )
    defer span.Finish()

    // Put the span on a context so downstream work (DB calls, another publish) nests under it.
    // Use Background() — a consumer is a fresh entry point with no inbound ctx to inherit; the
    // cross-service link rides in the message headers (Extract/ChildOf above), not in this ctx.
    ctx := tracer.ContextWithSpan(context.Background(), span)

    // ... process d.Body ...
    return ctx
}
```

`ChildOf(parent)` is the key line — it attaches the consume span to the producer's
trace (same `trace_id`, `parent_id` = the publish span). If there are no headers,
`Extract` returns an error and the consume simply starts a new root trace; don't fail
the consume on it.

---

## 4. Add Data Streams Monitoring (DSM)

DSM is the **same pattern layered on top** — same carrier, same message, same
inject-on-produce / extract-on-consume shape. You add exactly **one checkpoint call
per side** plus one DSM inject/extract. Spans and DSM are independent: spans travel in
the `x-datadog-*` / `traceparent` headers, DSM in a separate `dd-pathway-ctx-base64`
header — but both ride the **same `amqpCarrier`**.

> This is #2's `Publish` and #3's `handleDelivery` with the DSM lines added.
> Everything from #2–3 stays exactly the same — only the `// NEW (DSM)` lines are added.

`**rabbitmq.go` imports** — same as #2/#3 plus the two `datastreams` packages:

```go
package rabbitmq

import (
    "context"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"

    "github.com/DataDog/dd-trace-go/v2/datastreams"         // NEW (DSM)
    "github.com/DataDog/dd-trace-go/v2/datastreams/options" // NEW (DSM)
    "github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
    "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
)
```

### Producer — checkpoint (out), then inject the pathway

```go
func Publish(ctx context.Context, ch *amqp.Channel, exchange, routingKey, correlationID string, body []byte) error {
    msg := amqp.Publishing{
        Headers: amqp.Table{}, ContentType: "application/json",
        CorrelationId: correlationID, Body: body,
        DeliveryMode: amqp.Persistent, Timestamp: time.Now(),
    }

    // ── SAME as #2 (spans): start span + inject trace context ──
    span, ctx := tracer.StartSpanFromContext(ctx, "rabbitmq.publish",
        tracer.ResourceName("publish "+routingKey),
        tracer.SpanType(ext.SpanTypeMessageProducer),
        tracer.Tag(ext.MessagingSystem, "rabbitmq"),
        tracer.Tag("messaging.destination", routingKey),
    )
    defer span.Finish()
    _ = tracer.Inject(span.Context(), amqpCarrier(msg.Headers))

    // ── NEW (DSM): outbound checkpoint, then inject the pathway into the SAME headers ──
    ctx, ok := tracer.SetDataStreamsCheckpointWithParams(ctx,
        options.CheckpointParams{PayloadSize: int64(len(msg.Body))},
        "direction:out", "type:rabbitmq", "exchange:default", // this app always uses the default exchange
    )
    if ok { // false when DD_DATA_STREAMS_ENABLED is off → spans still work, DSM is skipped
        datastreams.InjectToBase64Carrier(ctx, amqpCarrier(msg.Headers))
    }

    // ── SAME as #2: publish ──
    if err := ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg); err != nil {
        span.SetTag(ext.Error, err)
        return err
    }
    return nil
}
```

### Consumer — extract the pathway, then checkpoint (in)

```go
func handleDelivery(d amqp.Delivery, queue string) context.Context {
    // ── SAME as #3 (spans): extract trace context + start child span ──
    parent, _ := tracer.Extract(amqpCarrier(d.Headers))
    span := tracer.StartSpan("rabbitmq.consume",
        tracer.ChildOf(parent),
        tracer.ResourceName("consume "+queue),
        tracer.SpanType(ext.SpanTypeMessageConsumer),
        tracer.Tag(ext.MessagingSystem, "rabbitmq"),
        tracer.Tag("messaging.destination", queue),
    )
    defer span.Finish()
    ctx := tracer.ContextWithSpan(context.Background(), span)

    // ── NEW (DSM): extract the pathway from the SAME headers, then inbound checkpoint ──
    ctx, _ = tracer.SetDataStreamsCheckpointWithParams(
        datastreams.ExtractFromBase64Carrier(ctx, amqpCarrier(d.Headers)),
        options.CheckpointParams{PayloadSize: int64(len(d.Body))},
        "direction:in", "type:rabbitmq", "topic:"+queue,
    )

    // ... process d.Body ...
    return ctx // ctx now carries BOTH the span and the DSM pathway
}
```

### Same vs different


|                     | Spans only (#2–3)            | + DSM (this section)                                                                          |
| ------------------- | ---------------------------- | --------------------------------------------------------------------------------------------- |
| Carrier             | `amqpCarrier`                | **same** `amqpCarrier`, reused unchanged                                                      |
| Header(s) it writes | `x-datadog-`*, `traceparent` | those **plus** `dd-pathway-ctx-base64`                                                        |
| Producer adds       | `tracer.Inject`              | `SetDataStreamsCheckpointWithParams("direction:out")` → `datastreams.InjectToBase64Carrier`   |
| Consumer adds       | `tracer.Extract` + `ChildOf` | `datastreams.ExtractFromBase64Carrier` → `SetDataStreamsCheckpointWithParams("direction:in")` |
| Imports             | `tracer`, `ext`              | **plus** `datastreams`, `datastreams/options`                                                 |
| Config              | none                         | `DD_DATA_STREAMS_ENABLED=true`                                                                |


**Producer ↔ consumer symmetry (same shape, mirrored order)** — exactly like spans
(`Inject` out / `Extract` in):

- Producer: **checkpoint first (`direction:out`), then inject** the pathway.
- Consumer: **extract the pathway first, then checkpoint (`direction:in`)**.

**Edge tags** — keep values low-cardinality (queue/exchange names, never per-message IDs):

- Produce: `direction:out`, `type:rabbitmq`, `exchange:default`
- Consume: `direction:in`, `type:rabbitmq`, `topic:<queue>`

---

## 5. Wire it up: thread `ctx` consume → produce

Go tracing relies on you passing the Go `context.Context` through — and with DSM the
same `ctx` also carries the **pathway**, so threading it keeps **one trace *and* one
DSM pathway** across services. The link only survives if the **consume `ctx` reaches
the next produce**. Never start a downstream publish from `context.Background()`
mid-flow — that begins a disconnected trace and a fresh pathway.

### Before vs after (one produce → consume hop)

**Before — no instrumentation.** The message carries only your payload, so the consumer
can't tell which trace it belongs to and starts a brand-new one:

```
  ┌───────────────┐
  │ PRODUCER      │
  │ Publish(body) │
  └───────────────┘
    │
    ▼  msg = { body, correlationId }   ·   no trace headers
  ┌───────────────────────────────┐
  │ CONSUMER                      │
  │ consume(msg)                  │
  │ (no trace headers to extract) │
  └───────────────────────────────┘

✗  nothing to link back to the producer → a separate root trace, no DSM edge
```

**After — inject on publish, extract on consume.** The trace context + DSM pathway travel
in the message headers, so the consume span becomes a child of the publish span:

```
  ┌────────────────────────────────────┐
  │ PRODUCER                           │
  │ Publish(ctx, body)                 │
  │ • Inject(span.Context())           │
  │ • checkpoint(out) + InjectToBase64 │
  └────────────────────────────────────┘
    │
    ▼  writes trace context + DSM pathway into the headers
  ┌──────────────────────────┐
  │ MESSAGE on the queue     │
  │ body, correlationId      │
  │ x-datadog-*, traceparent │  ◄─ APM  (trace link)
  │ dd-pathway-ctx-base64    │  ◄─ DSM  (pathway)
  └──────────────────────────┘
    │
    ▼  broker delivers
  ┌─────────────────────────────────────────────┐
  │ CONSUMER                                    │
  │ consume(msg)                                │
  │ • Extract(headers) → ChildOf(parent)        │
  │ • ExtractFromBase64Carrier + checkpoint(in) │
  │ • returns ctx  (span + DSM pathway)         │
  └─────────────────────────────────────────────┘

✓  consume span is a CHILD of the publish span → ONE trace + ONE DSM edge
   thread the returned ctx into your next Publish to extend the chain
```

Here's the full gateway → processing → gateway round-trip.

**Gateway — produce (child of the inbound HTTP span):**

```go
// ctx == r.Context(), which already carries the Orchestrion HTTP server span
_ = Publish(ctx, ch, "", ProcessingQueue, corrID, body)
```

**Processing — consume then produce (one continuous trace + pathway):**

```go
// handleDelivery is #3/#4's consumer. Its "process" step enriches and publishes
// onward — done BEFORE the deferred span.Finish() fires, using the consume ctx so the
// trace and DSM pathway carry straight through to the next hop.
func handleDelivery(d amqp.Delivery, queue string) context.Context {
    ctx := /* #3–4: extract → child consume span (defer Finish) → DSM checkpoint(in) */
    // ... enrich d.Body → out ...
    _ = Publish(ctx, ch, "", CompletedQueue, d.CorrelationId, out) // ctx continues trace + pathway
    return ctx
}
```

**Gateway — consume the reply (terminal hop, `ctx` not threaded onward):**

```go
_ = handleDelivery(d, CompletedQueue) // nothing published onward, so the returned ctx is dropped
```

Resulting shape — **one trace, one 4-node DSM pathway across two services:**

```
HTTP → publish(processing-queue) → consume(processing-queue) → publish(completed-queue) → consume(completed-queue)
       [gateway]                    [processing]                [processing]               [gateway]
```

## Verify

1. **Spans linked across services** — fetch the trace in Datadog: the `rabbitmq.consume`
  span's `parent_id` equals the `rabbitmq.publish` span's `span_id`, and both share the
   same `trace_id` — one trace spanning producer and consumer.
2. **DSM flowing** — with `DD_TRACE_DEBUG=true` you'll see
  `datastreams: sending pipeline_stats payload buckets=N` → `pipeline_stats POST status=202`.
   In Datadog, **Data Streams Monitoring** shows the `processing-queue` / `completed-queue` pathway.

---
## References

**Working Implementation Reference**
- [Reference Github Implemetation](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/blob/main/internal/rabbitmq/rabbitmq.go#L65)

**Data Streams Monitoring — manual setup (Go)**
- [Set up DSM for Go — other queuing technologies or protocols](https://docs.datadoghq.com/data_streams/setup/language/go/#other-queuing-technologies-or-protocols) — the official guide for the manual `SetDataStreamsCheckpointWithParams` + inject/extract pattern used in #4.

**Propagation header types (`TextMapWriter` / `TextMapReader`)** — the interfaces the `amqpCarrier` in #1 implements:
- APM: [ddtrace/tracer/propagator.go](https://github.com/DataDog/dd-trace-go/blob/main/ddtrace/tracer/propagator.go#L25)
- DSM: [datastreams/propagation.go](https://github.com/DataDog/dd-trace-go/blob/main/datastreams/propagation.go#L37)

**Kafka auto-instrumentation — reference implementation** — the supported contrib whose carrier design this manual RabbitMQ work mirrors:
- APM: [contrib/segmentio/kafka-go/internal/tracing/tracing.go](https://github.com/DataDog/dd-trace-go/blob/main/contrib/segmentio/kafka-go/internal/tracing/tracing.go)
- DSM: [contrib/segmentio/kafka-go/internal/tracing/dsm.go](https://github.com/DataDog/dd-trace-go/blob/main/contrib/segmentio/kafka-go/internal/tracing/dsm.go)

---

## Debug & troubleshooting

**Turn the switches on first**

- `DD_TRACE_DEBUG=true` — the tracer logs every flush; for DSM you'll see `datastreams: sending pipeline_stats payload buckets=N` → `pipeline_stats POST status=202`. No such lines means DSM isn't actually running.
- `datadog-agent status` — the **APM** section shows traces received; the **Data Streams Monitoring** section shows pathway stats. (DSM requires Agent 7.34+.)
- **Inspect a message on the broker** (e.g. RabbitMQ management UI → *Get messages*) — confirm the propagation headers are present: `x-datadog-*` / `traceparent` (spans) and `dd-pathway-ctx-base64` (DSM). Missing → injection is broken; present but the consumer still starts a new trace → extraction is broken.

**Isolate first — spans and DSM are independent.** Spans ride `x-datadog-*` / `traceparent`; DSM rides `dd-pathway-ctx-base64`. If spans link but DSM doesn't, it's a DSM-side problem (enabled flag / checkpoint / inject). If neither works, it's a shared cause (tracer not started / carrier / headers).

| Symptom | Likely cause | Fix |
|---|---|---|
| No spans at all | Tracer not started, or spans never `Finish()`ed (so never flushed) | Confirm `tracer.Start()` / Orchestrion; `defer span.Finish()` on **every** return path |
| Consumer starts a **new root trace** (not a child of publish) | `Headers` was `nil` on publish → nothing injected; or the delivered header came back as `[]byte`, not `string`, so the carrier skips it | Init `Headers: amqp.Table{}` before `Inject`; log header value types — `ForeachKey` only reads `string` values |
| Publish span has no parent (should nest under the caller) | `ctx` not threaded from the caller (started from `context.Background()`) | Pass the caller's `ctx` into `Publish` |
| DSM UI empty / no pathway | `DD_DATA_STREAMS_ENABLED` not set → `SetDataStreamsCheckpointWithParams` returns `ok=false`, inject skipped | Set `DD_DATA_STREAMS_ENABLED=true` per service; confirm `pipeline_stats` in debug logs |
| DSM nodes disconnected (producer ✗→ consumer) | Consumer didn't `ExtractFromBase64Carrier` before its checkpoint, or the consume `ctx` wasn't threaded into the next publish | Extract → checkpoint on consume; thread the consume `ctx` into the downstream `Publish` |

---

## Checklist

- Tracer is running (`tracer.Start()` or Orchestrion)
- `amqpCarrier` defined (`Set` + `ForeachKey`)
- Publish: `StartSpanFromContext` → `tracer.Inject(span.Context(), amqpCarrier(headers))`
- Headers map non-nil before injecting
- Consume: `tracer.Extract(amqpCarrier(headers))` → `StartSpan(..., tracer.ChildOf(parent))`
- `ctx` threaded through — including consume → next publish (and into goroutines)
- Every span has a matching `Finish()`

**DSM (#4):**

- `DD_DATA_STREAMS_ENABLED=true`
- Produce: `SetDataStreamsCheckpointWithParams("direction:out", …)` → `datastreams.InjectToBase64Carrier`
- Consume: `datastreams.ExtractFromBase64Carrier` → `SetDataStreamsCheckpointWithParams("direction:in", …)`
- Same `amqpCarrier` reused; edge tag values kept low-cardinality

---

## Appendix

### Goroutine / channel hops

Go channels don't carry `context.Context`, so put it in the work item:

```go
type job struct { ctx context.Context; corrID string; body []byte }
jobs <- job{ctx: context.WithoutCancel(ctx), corrID: d.CorrelationId, body: d.Body} // WithoutCancel keeps the pathway alive
// worker:
for j := range jobs { _ = Publish(j.ctx, ch, "", CompletedQueue, j.corrID, j.body) }
```

- **Fan-out** (1 in → N out): pass the same consume `ctx` to each `Publish`.
- **Fan-in** (N in → 1 out): `datastreams.MergeContexts(ctx1, ctx2, ...)` before publishing.

### Two "contexts" — don't mix them

- `span.Context()` → a `*tracer.SpanContext`; used by `tracer.Inject` (writes the trace headers).
- `ctx` → the Go `context.Context`; carries the DSM pathway **and** the active span
(used by `SetDataStreamsCheckpointWithParams` and `InjectToBase64Carrier`).

---