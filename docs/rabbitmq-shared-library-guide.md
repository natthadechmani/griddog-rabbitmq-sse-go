# Scaling RabbitMQ APM + DSM with a Shared Library (Go)

> **Sequel to the [manual instrumentation guide](https://gist.github.com/natthadechmani/5da721cd2a040c9d25d0c2b4ae419d79).**
> That guide shows how to instrument RabbitMQ **by hand** in one service. This guide takes that
> exact code and packages it **once** into a reusable, org-owned Go module that wraps the
> open-source client (`amqp091-go`) and bakes in the Datadog APM spans + DSM checkpoints + context
> propagation — so every other service gets it in ~3 lines instead of re-implementing ~150.

This is **not** a fork of `amqp091-go`. It **depends on** it and adds a thin instrumentation
layer on top (a wrapper / facade). Apps import only the wrapper; the OSS client and dd-trace-go
become hidden, indirect dependencies.

> **Reference implementation — see it end-to-end:**
>
> - 📦 Library: **[github.com/natthadechmani/go-rabbitmq-messaging](https://github.com/natthadechmani/go-rabbitmq-messaging)** (`v0.1.0`)
> - 🔌 App that imports it: **[griddog-rabbitmq-sse-go @ `feature/import-common-lib`](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/tree/feature/import-common-lib)**
> - 🔀 The exact before → after change: **[compare `main…feature/import-common-lib`](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/compare/main...feature/import-common-lib)**
>
> Verified end-to-end with `docker compose`: the RabbitMQ flow returns `200` with enrichment and
> produces **one distributed trace** across both services (gateway `publish` → processing
> `consume` → `publish` → gateway `consume`) plus a DSM pathway — with the services importing only
> the library.

---

## Why a shared library?

The manual guide is correct but its cost scales with the number of services. If a client has
~N Go services each talking to RabbitMQ, copy-pasting the carrier + span + checkpoint +
inject/extract logic into all of them is **O(N)** — and every fix has to land in N repos.

| Approach | Instrument cost | Cost when instrumentation changes / a bug is found | Cost to add service #11 |
|----------|-----------------|----------------------------------------------------|-------------------------|
| Copy-paste the manual code per service | **O(N)** | Fix in N repos | Re-implement everything |
| **Shared library** | **O(1)** (write once) | Bump one version, `go get -u` | ~3 lines |

```
BEFORE — manual, copied into every service        AFTER — one shared library
O(N): fix a bug → edit N repos                    O(1): fix once → bump → go get -u

  app-1  carrier+span+ckpt+inject   (~150 loc)      app-1 ─┐
  app-2  carrier+span+ckpt+inject   (~150 loc)      app-2 ─┤ import messaging
   ···                                              ···   ─┤ New + Publish/Consume
  app-N  carrier+span+ckpt+inject   (~150 loc)      app-N ─┘ (~3 loc each)
                                                           │
  add app #11 → reimplement it all                         ▼
  change a DSM tag → edit 11 places                one module · fix once · bump version
```

**Conclusion:** the shared-library ("golden path" / "paved road") approach is the recommended
way to roll instrumentation across many services.

---

## What the library is

A **normal Go module the org owns** that:

- wraps the third-party RabbitMQ client (`amqp091-go`),
- bakes in the Datadog instrumentation from the manual guide (APM spans + DSM checkpoints +
  header-based context propagation),
- exposes a **small, opinionated API** so app teams never touch the raw client or dd-trace-go.

```
   app-1 · app-2 · … · app-N        each app imports ONLY the module
                   │
                   ▼
   ┌─────────────────────────────────────────────────┐
   │ github.com/natthadechmani/go-rabbitmq-messaging │   the shared library (facade)
   │ New · Publish(ctx, …) · Consume(ctx, handler)   │   ← the only API apps call
   └─────────────────────────────────────────────────┘
                   │  wraps — indirect deps, hidden from apps
         ┌─────────┴──────────┐
         ▼                    ▼
   ┌─────────────────┐   ┌─────────────────┐
   │ amqp091-go      │   │ dd-trace-go/v2  │
   │ RabbitMQ client │   │ APM spans + DSM │
   └─────────────────┘   └─────────────────┘
```

> This repo demonstrated the pattern **locally** first —
> [internal/rabbitmq/](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/tree/main/internal/rabbitmq)
> on `main` is the single integration point, and the two services never import dd-trace-go directly.
> It has now been **promoted** into the standalone module
> [go-rabbitmq-messaging](https://github.com/natthadechmani/go-rabbitmq-messaging); the
> [`feature/import-common-lib`](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/tree/feature/import-common-lib)
> branch `go get`s that module and deletes the local copy.

---

## How it differs from the manual guide

Same instrumentation, different **packaging**. In the manual guide each service writes everything;
with the library the platform team writes it once and each app calls two methods. (Section numbers
below — #1–#5 — refer to the [manual guide](https://gist.github.com/natthadechmani/5da721cd2a040c9d25d0c2b4ae419d79).)

| Concern | Manual guide (per service) | Shared library (this guide) |
|---------|----------------------------|-----------------------------|
| Header carrier | each service writes it — #1 | inside the module, **unexported** |
| Publish span + `Inject` | each service — #2 | `Client.Publish` |
| Consume span + `Extract`/`ChildOf` | each service — #3 | `Client.Consume` loop |
| DSM checkpoints (in/out) | each service — #4 | inside `Publish`/`Consume` |
| Thread `ctx` consume → produce | each service — #5 | handler **receives** the ctx; return it to the next `Publish` |
| Deps the **app** imports | `amqp091-go` **and** `dd-trace-go` | **just the module** (both become indirect) |
| Lines of instrumentation per app | ~150 | **~3** |
| Fix a bug / add a tag | edit N repos | bump one version → `go get -u` |

The key deltas when the code becomes a library:

1. **The carrier is unexported** — apps can't (and needn't) see it.
2. **Service name / env come from config** (Options or `DD_SERVICE`/`DD_ENV`), not hard-coded.
3. **Instrumentation is default-on** — no per-call flag; teams get spans + DSM for free (DSM still
   respects `DD_DATA_STREAMS_ENABLED` at runtime).
4. **`Consume` owns the delivery loop + ack/nack + span lifecycle.** The app supplies only a
   `Handler`, which receives a `ctx` **already carrying the span + DSM pathway** — this *forces*
   the ctx to flow (guide #5) without each team understanding DSM internals.
5. **`amqp.Delivery` is re-exported** as `messaging.Delivery`, so an app imports *only* the module.

---

## The library API

```go
package messaging // github.com/natthadechmani/go-rabbitmq-messaging

// --- configuration (functional options) ---
type Option func(*config)
func WithService(name string) Option          // DD service tag on spans (else DD_SERVICE)
func WithEnv(env string) Option               // DD env (else DD_ENV)
func WithDefaultExchange(name string) Option  // used when Publish exchange == ""

// --- client ---
type Client struct{ /* wraps *amqp.Connection + *amqp.Channel + config */ }
func New(url string, opts ...Option) (*Client, error) // dial with retry
func (c *Client) Close() error
func (c *Client) DeclareQueues(names ...string) error

// --- produce: span + Inject + DSM checkpoint(out) + InjectToBase64Carrier, then publish ---
func (c *Client) Publish(ctx context.Context, exchange, routingKey, correlationID string, body []byte) error

// --- consume: Extract→ChildOf + DSM extract+checkpoint(in), then call the handler ---
type Delivery = amqp.Delivery                 // re-export so apps never import amqp091-go
type Handler func(ctx context.Context, d Delivery) error // return err → Nack+requeue; nil → Ack
func (c *Client) Consume(ctx context.Context, queue string, h Handler) error
```

Instrumentation is **default-on** and lives entirely inside `Publish`/`Consume`; the handler's
`ctx` already carries the active span **and** the DSM pathway.

---

## Inside the library (repackaging the manual code)

This is the same instrumentation from the manual guide, moved behind the API. Only the pieces
that *change* when it becomes a library are shown in full; the rest is a direct lift.

**`carrier.go`** — identical to the guide's
[#1 carrier](https://gist.github.com/natthadechmani/5da721cd2a040c9d25d0c2b4ae419d79), just
**unexported** (`amqpCarrier`) so apps never see it. (Same `Set` + `ForeachKey`.)

**`options.go`** — new: config from Options or `DD_*` env vars.

```go
type config struct {
    service         string // DD service tag (default: DD_SERVICE)
    env             string // DD env         (default: DD_ENV)
    defaultExchange string // used when Publish exchange == ""
}

type Option func(*config)
func WithService(name string) Option        { return func(c *config) { c.service = name } }
func WithEnv(env string) Option             { return func(c *config) { c.env = env } }
func WithDefaultExchange(name string) Option { return func(c *config) { c.defaultExchange = name } }

func newConfig(opts ...Option) config {
    c := config{service: os.Getenv("DD_SERVICE"), env: os.Getenv("DD_ENV")}
    for _, o := range opts { o(&c) }
    return c
}
```

**`publish.go`** — the guide's #2 span + inject and #4 producer DSM, now a method that reads
`ServiceName` from config:

```go
func (c *Client) Publish(ctx context.Context, exchange, routingKey, correlationID string, body []byte) error {
    if exchange == "" {
        exchange = c.cfg.defaultExchange
    }
    msg := amqp.Publishing{
        Headers: amqp.Table{}, ContentType: "application/json",
        CorrelationId: correlationID, Body: body,
        DeliveryMode: amqp.Persistent, Timestamp: time.Now(),
    }

    // APM producer span (child of whatever is in ctx). ServiceName from config.
    span, ctx := tracer.StartSpanFromContext(ctx, "rabbitmq.publish",
        tracer.ServiceName(c.cfg.service),
        tracer.ResourceName("publish "+routingKey),
        tracer.SpanType(ext.SpanTypeMessageProducer),
        tracer.Tag(ext.MessagingSystem, "rabbitmq"),
        tracer.Tag("messaging.destination", routingKey),
    )
    defer span.Finish()
    _ = tracer.Inject(span.Context(), amqpCarrier(msg.Headers))

    // DSM outbound checkpoint, then inject the pathway into the SAME headers.
    ctx, ok := tracer.SetDataStreamsCheckpointWithParams(ctx,
        options.CheckpointParams{PayloadSize: int64(len(msg.Body))},
        "direction:out", "type:rabbitmq", "exchange:"+exchangeTag(exchange),
    )
    if ok {
        datastreams.InjectToBase64Carrier(ctx, amqpCarrier(msg.Headers))
    }

    if err := c.ch.PublishWithContext(ctx, exchange, routingKey, false, false, msg); err != nil {
        span.SetTag(ext.Error, err)
        return err
    }
    return nil
}
```

**`consume.go`** — the guide's #3 extract/ChildOf + #4 consumer DSM, plus the delivery loop and
ack/nack the app used to write. The app now supplies only a `Handler`:

```go
type Delivery = amqp.Delivery // re-export → apps never import amqp091-go
type Handler func(ctx context.Context, d Delivery) error

func (c *Client) Consume(ctx context.Context, queue string, h Handler) error {
    deliveries, err := c.ch.Consume(queue, "", false, false, false, false, nil)
    if err != nil {
        return err
    }
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case d, ok := <-deliveries:
            if !ok {
                return nil
            }
            c.handle(queue, d, h)
        }
    }
}

func (c *Client) handle(queue string, d amqp.Delivery, h Handler) {
    parent, _ := tracer.Extract(amqpCarrier(d.Headers)) // no headers = new root
    span := tracer.StartSpan("rabbitmq.consume",
        tracer.ChildOf(parent),
        tracer.ServiceName(c.cfg.service),
        tracer.ResourceName("consume "+queue),
        tracer.SpanType(ext.SpanTypeMessageConsumer),
        tracer.Tag(ext.MessagingSystem, "rabbitmq"),
        tracer.Tag("messaging.destination", queue),
    )
    defer span.Finish()

    // ctx carries the span; then extract the DSM pathway and set the inbound checkpoint.
    msgCtx := tracer.ContextWithSpan(context.Background(), span)
    msgCtx, _ = tracer.SetDataStreamsCheckpointWithParams(
        datastreams.ExtractFromBase64Carrier(msgCtx, amqpCarrier(d.Headers)),
        options.CheckpointParams{PayloadSize: int64(len(d.Body))},
        "direction:in", "type:rabbitmq", "topic:"+queue,
    )

    if err := h(msgCtx, d); err != nil { // handler gets ctx with span + pathway
        span.SetTag(ext.Error, err)
        _ = d.Nack(false, true)
        return
    }
    _ = d.Ack(false)
}
```

> **Verified:** these files (plus `client.go` = dial-with-retry + `Close` + `DeclareQueues`)
> compile, `go vet` clean, and **pass unit tests** (`go test` — carrier round-trip, config
> defaults/overrides, exchange tag) against `dd-trace-go/v2 v2.9.1` and `amqp091-go v1.10.0`.

---

## Using it in an app

The app imports **only** the module. Producer and consumer are ~3 lines each:

```go
import messaging "github.com/natthadechmani/go-rabbitmq-messaging"

client, _ := messaging.New(rabbitURL, messaging.WithService("orders-api"))
defer client.Close()

// produce — span + DSM happen inside Publish
_ = client.Publish(ctx, "", "orders", corrID, body)

// consume — ctx already carries span + pathway; pass it to the next Publish
_ = client.Consume(ctx, "orders.q", func(ctx context.Context, d messaging.Delivery) error {
    return client.Publish(ctx, "", "orders.done", d.CorrelationId, d.Body) // chain stays connected
})
```

See the real before → after in this repo:

- **Manual** (`main`): [internal/processing/consumer.go](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/blob/main/internal/processing/consumer.go)
  hand-manages `StartConsumeSpan`, `span.Finish()`, ack/nack, and threads `ctx` itself.
- **Library** (`feature/import-common-lib`): [internal/processing/consumer.go](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/blob/feature/import-common-lib/internal/processing/consumer.go)
  just supplies a `Handler` — the module does the rest.
- **Full diff:** [main…feature/import-common-lib](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/compare/main...feature/import-common-lib).

> **Verified end-to-end:** on `feature/import-common-lib` both services import **only** the module
> (`amqp091-go` and `dd-trace-go/v2` are `// indirect` in `go.mod`), and `docker compose up --build`
> runs the flow green — `200` + enrichment, one linked trace + DSM across both services.

---

## Module layout

```
go-rabbitmq-messaging/            module github.com/natthadechmani/go-rabbitmq-messaging
├── go.mod / go.sum               requires amqp091-go + dd-trace-go/v2 (indirect for apps)
├── client.go                     Client, New (retry), Close, DeclareQueues
├── publish.go                    Client.Publish            ← guide #2 + #4 producer
├── consume.go                    Client.Consume, Handler   ← guide #3 + #4 consumer + #5 ctx
├── carrier.go                    amqpCarrier (unexported)  ← guide #1
├── options.go                    Option, config, WithService/WithEnv/WithDefaultExchange
├── carrier_test.go               unit tests — no broker needed
├── options_test.go               unit tests — no broker needed
└── README.md / CHANGELOG.md / LICENSE
```

A library is distributed as **versioned source** you `go get` — there's nothing to build here
(see *Distributing the library* below).

---

## Distributing the library

A Go library is a **versioned source module**, pulled with `go get` at build time. For a private
module the simplest path is a plain private git repo — Go resolves modules directly from git tags,
so no separate package registry is required.

> The reference library [go-rabbitmq-messaging](https://github.com/natthadechmani/go-rabbitmq-messaging)
> is **public**, so `go get` (and the Docker build) need no auth. The steps below also cover the
> **private** case.

### GitHub (private git + semver tag)

**Publish** — a release *is* a tag (there is no separate publish step):

```bash
# go.mod's module line must equal the repo path:
#   module github.com/natthadechmani/go-rabbitmq-messaging
git tag v1.2.0
git push origin v1.2.0
```

For a **breaking change** (v2+), Go's semantic-import-versioning requires the major version as a
path suffix: set `module github.com/natthadechmani/go-rabbitmq-messaging/v2`, and consumers import
`.../v2/...`. `v0`/`v1` take no suffix.

**Consume** the private repo:

```bash
go env -w GOPRIVATE=github.com/natthadechmani/*   # implies GONOPROXY + GONOSUMDB for these paths
go get github.com/natthadechmani/go-rabbitmq-messaging@v1.2.0
```

`GOPRIVATE` tells Go to fetch matching modules **direct from git** (not via the public proxy) and
to **skip the checksum database** (`sum.golang.org` can't see private code). `GONOSUMCHECK` is not
a real variable; the current ones are `GOPRIVATE` / `GONOPROXY` / `GONOSUMDB` / `GOSUMDB`.

**Authenticate** to the private repo (pick one):

```bash
# A) SSH rewrite (developer machines with an SSH key on GitHub)
git config --global url."git@github.com:".insteadOf "https://github.com/"

# B) HTTPS + PAT via ~/.netrc (best for CI)
printf "machine github.com\n  login <user>\n  password <PAT>\n" >> ~/.netrc && chmod 600 ~/.netrc
```

In CI, set `GOPRIVATE` in the job environment and inject the PAT as a secret (write `~/.netrc` at
job start).

---

## Versioning & upgrades

```
platform team: fix a bug / add a tag
        │ git tag v1.3.0   (semver; /v2 path for breaking changes)
        ▼
  git push origin v1.3.0
        │
        │ each service, on its own schedule:
        ▼
  go get -u github.com/natthadechmani/go-rabbitmq-messaging@v1.3.0
        ├─► app-1  rebuild → new instrumentation, no code change
        ├─► app-2  rebuild → new instrumentation, no code change
        └─► app-N  rebuild → new instrumentation, no code change
```

Improve a span tag, fix a DSM edge tag, adopt a new dd-trace-go release — do it once, cut a
version, and services adopt it with a one-line bump. No per-service rework.

---

## End-to-end: two apps, zero app-side instrumentation

```
┌──────────────────────────┐
│ orders-api  (app)        │
│ messaging.Publish(ctx,…) │
│ • span + Inject          │
│ • DSM checkpoint(out)    │
└──────────────────────────┘
     │
     ▼  message headers:  x-datadog-* / traceparent  (APM)
     │                    dd-pathway-ctx-base64       (DSM)
  ═══════════ orders queue ═══════════
     │
     ▼
┌──────────────────────────┐
│ fulfiller  (app)         │
│ messaging.Consume(ctx,h) │
│ • Extract → ChildOf      │
│ • DSM checkpoint(in)     │
│ • h(ctx, d)              │
└──────────────────────────┘
ONE trace + ONE DSM pathway across both apps — neither app wrote any instrumentation.
```

**Proven in this repo:** the
[`feature/import-common-lib`](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/tree/feature/import-common-lib)
branch produces exactly this shape — `POST /api/rabbitmq-call` → `publish(processing-queue)` →
`consume(processing-queue)` → `publish(completed-queue)` → `consume(completed-queue)`, one trace
across `gateway-backend` and `processing-backend`.

---

## Design recommendations (so it actually scales)

1. **Hide the raw client.** Expose only your API (and re-export `amqp.Delivery`), or teams reach
   around it and lose the instrumentation guarantee.
2. **Instrumentation default-on**, not opt-in. No `WithDataStreams()` at every call site.
3. **Force `ctx` to flow.** The handler receives the ctx; passing it to the next `Publish` keeps
   end-to-end pathways intact without each team learning DSM.
4. **Standardize on one RabbitMQ client.** Converging the fleet onto the wrapper is part of the value.
5. **Semver + changelog + deprecation policy.** Breaking change → new major path (`/v2`).
6. **Sensible config defaults** (service/env/tags) from standard `DD_*` env vars.
7. **Clear ownership** (platform / observability team) + a support channel. A golden-path lib
   without an owner rots.

---

## Verify

A consuming app confirms the library works exactly like the manual guide's **Verify** /
**Debug & troubleshooting** sections
([in the gist](https://gist.github.com/natthadechmani/5da721cd2a040c9d25d0c2b4ae419d79)):

1. **Spans linked across services** — the `rabbitmq.consume` span's `parent_id` equals the
   `rabbitmq.publish` span's `span_id`; both share one `trace_id`.
2. **DSM flowing** — with `DD_TRACE_DEBUG=true`, `pipeline_stats` payloads POST `202`; the DSM
   view shows the queue-to-queue pathway. Requires `DD_DATA_STREAMS_ENABLED=true`.
3. **Edge-tag keys stay in the allow-list** (`type`, `direction`, `topic`, `exchange`) — the
   library already does this, so apps can't get it wrong.

---

## Checklist (for the platform team publishing the module)

- [ ] `go.mod` module path == repo path; instrumentation lives in `Publish`/`Consume`, default-on
- [ ] Carrier unexported; `amqp.Delivery` re-exported so apps import only the module
- [ ] `Consume` owns the loop + ack/nack + `span.Finish()`; handler receives ctx (span + pathway)
- [ ] `New` reads `DD_SERVICE`/`DD_ENV`; edge-tag values low-cardinality
- [ ] Released via semver tag (`vMAJOR.MINOR.PATCH`); `/v2` path for breaking changes
- [ ] Consumers documented: `GOPRIVATE` + git auth (SSH rewrite or `~/.netrc` PAT)
- [ ] Owner + changelog + support channel

---

## Related

- [Manual instrumentation guide (gist)](https://gist.github.com/natthadechmani/5da721cd2a040c9d25d0c2b4ae419d79)
  — the per-call manual instrumentation (#1 carrier, #2 publish, #3 consume, #4 DSM, #5 thread
  ctx) that this library centralizes. Read it first; this guide assumes it.
- [griddog-rabbitmq-sse-go on GitHub](https://github.com/natthadechmani/griddog-rabbitmq-sse-go)
  — the in-repo version of the wrapper
  ([internal/rabbitmq/](https://github.com/natthadechmani/griddog-rabbitmq-sse-go/tree/main/internal/rabbitmq):
  `rabbitmq.go`, `carrier.go`) that the two services already share locally.

---

## Appendix: one `dd-trace-go` across the app and the library

Both your app **and** this library import `github.com/DataDog/dd-trace-go/v2`. That's a normal
**diamond dependency** — and Go resolves it to a **single** version, linked **once**, so there is
never a second copy of the tracer. (Unlike npm, Go cannot nest two versions of the same major module
in one build.) Orchestrion is *not* a runtime dependency of the library — it's a build-time tool in
your app — so the only shared runtime dep to reason about is `dd-trace-go/v2` itself.

### How it resolves in practice (MVS)

Go builds one flat module graph, collects every version requirement for a module path, and selects
the **highest** one — Minimum Version Selection ("maximum of the required minimums"). Your app (the
*main module*) decides the final version for the whole build; the library's `require` is only a floor.

```
                    your-app  (main module — its build decides the versions)
                    /                              \
     go-rabbitmq-messaging                   orchestrion (pinned in the app)
     require dd-trace-go/v2 v2.9.1           require dd-trace-go/v2 v2.9.1
                    \                              /
                     ▼                            ▼
             ┌────────────────────────────────────────────────┐
             │ dd-trace-go/v2 — Go collects every requirement │
             │ and links the HIGHEST version once  (MVS)      │
             └────────────────────────────────────────────────┘
                                 │
                                 ▼
          ONE dd-trace-go/v2  →  ONE global tracer
     (Orchestrion HTTP/SQL spans + the library's RabbitMQ spans share it)
```

```
  requirement in the graph          selected for the build
  ────────────────────────          ──────────────────────
  your-app    → v2.10.0      ┐
  commonlib   → v2.9.1       ├──▶    v2.10.0   (one version, compiled once)
  another-dep → v2.8.0       ┘
```

One version → one package → **one process-wide tracer**, so all spans (app, Orchestrion, and this
library) land on the same trace and DSM pathway. No duplication, no conflict.

### The one real concern: major-version mismatch (v2 vs v3)

Go uses *semantic import versioning*: a different **major** version is a different **module path**
(`.../v2` vs `.../v3`), which Go treats as two separate modules — and those **can** coexist, giving
you two tracers.

```
  SAFE — same major                       BROKEN — different majors
  ─────────────────                       ─────────────────────────
  app → dd-trace-go/v2  v2.10.0           app → dd-trace-go/v2   (imports .../v2/...)
  lib → dd-trace-go/v2  v2.9.1            lib → dd-trace-go/v3   (imports .../v3/...)
        │                                       │
   MVS → ONE v2 module                    TWO modules linked in
        │                                       │
   ONE tracer ✅                          TWO tracers ❌
   (spans + DSM unified)                  (spans split; DSM pathway breaks)
```

**Rule:** keep the library and the app on the **same major** of dd-trace-go (everything is `v2.x`
today, so MVS just unifies them). Only act on a major bump (v2 → v3): upgrade the library and the app
together, and keep Orchestrion's pinned dd-trace-go on the same major.

### Managing it

- **Library:** `require` a sensible floor (e.g. `dd-trace-go/v2 v2.9.1`); don't over-pin — any app
  that needs newer bumps it via MVS.
- **App:** `go get github.com/DataDog/dd-trace-go/v2@<version>`; MVS reconciles app + library +
  Orchestrion to one version. Keep `orchestrion pin` on the same major.
- **Inspect what got selected:**
  ```bash
  go list -m github.com/DataDog/dd-trace-go/v2      # the single selected version
  go mod graph | grep dd-trace-go/v2                # who requires what
  go mod why  -m github.com/DataDog/dd-trace-go/v2  # why it's in the build
  ```
- **Force a version if ever needed** (rare): `go get .../v2@vX.Y.Z` in the app, or a `replace`
  directive as a last resort.
