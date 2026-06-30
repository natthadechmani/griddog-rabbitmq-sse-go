# Griddog — RabbitMQ · SSE · MySQL demo

Two small Go services (`gateway-backend` ⇄ `processing-backend`) wired over **RabbitMQ (AMQP)**,
**HTTP**, and **Server-Sent Events**, persisting every hop to **MySQL**, with a **React + Vite**
UI that triggers and visualizes three flows.

```
browser ── React UI ──► gateway-backend ──► processing-backend
                              │  ▲                 │
                              ▼  │                 ▼
                           MySQL │            RabbitMQ
                                 └──── completed-queue ◄──┘
```

## The three flows

| Flow | Endpoint | What happens |
|------|----------|--------------|
| **1 — SSE** | `GET /api/sse-call` | gateway proxies an SSE stream from processing → browser: a counter **1→20, one every 0.5s for 10s**. Live in the UI. Not persisted (pure stream demo). |
| **2 — RabbitMQ** | `POST /api/rabbitmq-call` | gateway publishes to **`processing-queue`** → processing enriches (`value*2`, `squared`, extra fields) → publishes to **`completed-queue`** → gateway consumes the reply and returns it. **6 rows** written to MySQL tracing every hop. |
| **3 — HTTP** | `POST /api/http-call` | gateway calls processing over HTTP; request + response logged on **both** sides. **4 rows** in MySQL. |

Flow 2 is a synchronous round-trip over async queues: the gateway publishes with a `CorrelationId`,
a background consumer on `completed-queue` matches each reply to the waiting request handler (15s timeout).

## Run it

### Option A — everything in Docker (one command)

```bash
docker compose up --build
```

Then open:
- **App UI:** http://localhost:8088
- **Gateway API:** http://localhost:8080/api/health
- **RabbitMQ management:** http://localhost:15672 (guest / guest)
- **MySQL:** localhost:3306 (root / rootpw, db `appdb`)

### Option B — backends in Docker, frontend via Vite (hot reload)

```bash
docker compose up --build mysql rabbitmq gateway-backend processing-backend
# in another terminal:
cd frontend
npm install
npm run dev        # http://localhost:5173  (proxies /api → localhost:8080)
```

The frontend always uses relative `/api` paths, so it works identically whether served by nginx
(Docker) or proxied by Vite (local dev).

### Running the Go services directly (no Docker for the apps)

With `mysql` and `rabbitmq` up (e.g. `docker compose up mysql rabbitmq`):

```bash
go run ./cmd/processing   # :8081
go run ./cmd/gateway      # :8080
```

Defaults point at `localhost` MySQL/RabbitMQ; override via `MYSQL_DSN`, `RABBITMQ_URL`,
`PROCESSING_BASE_URL`, `PORT`.

## Try the flows from the UI

1. **Run SSE** — watch the diagram light up and numbers stream 1→20 over ~10s.
2. **Run RabbitMQ** — see the enriched response, then the 6-step diagram fill in; **Refresh Flow 2**
   to see the matching `message_logs` rows (`request_in`, `queue_published`, `queue_consumed`,
   `completed_published`, `completed_consumed`, `response_out`).
3. **Run HTTP** — see the response, then **Refresh Flow 3** for the 4 rows (gateway + processing,
   each `request_in` / `response_out`).

## Quick API checks (curl)

```bash
curl -N  http://localhost:8080/api/sse-call
curl -s  -X POST http://localhost:8080/api/rabbitmq-call -H 'content-type: application/json' -d '{"value":7}' | jq
curl -s  -X POST http://localhost:8080/api/http-call     -H 'content-type: application/json' -d '{"value":7}' | jq
curl -s 'http://localhost:8080/api/messages?flow=rabbitmq' | jq
curl -s 'http://localhost:8080/api/messages?flow=http'     | jq
```

Inspect MySQL directly:

```bash
docker compose exec mysql mysql -uroot -prootpw appdb \
  -e "SELECT id,flow,service,stage,correlation_id,created_at FROM message_logs ORDER BY id DESC LIMIT 20;"
```

## Layout

```
cmd/{gateway,processing}/main.go   # thin entrypoints
internal/
  config/   models/   httpx/       # config, shared structs, JSON helpers
  db/                              # MySQL connect (retry) + message_logs helpers
  rabbitmq/                        # AMQP connect (retry), declare/publish/consume
  sse/                             # SSE write + proxy-relay helpers
  gateway/                         # routes, CORS, SSE proxy, rabbitmq round-trip, http call, messages
  processing/                      # routes, SSE source, /process, processing-queue consumer
deploy/                            # Go Dockerfile, mysql/init.sql, frontend Dockerfile + nginx.conf
frontend/                          # React + Vite app (flow cards, FlowDiagram, DB viewer)
docker-compose.yml
```

## Notes

- Single-instance demo: the gateway's `completed-queue` consumer correlates replies via an
  in-memory map, so running multiple gateway replicas would split that state.
- Both services declare the queues (durable) and `CREATE TABLE IF NOT EXISTS` at startup, and retry
  their MySQL/RabbitMQ connections, so startup ordering is self-healing.
- AMQP uses the **default exchange** (publish straight to the named queues) — no custom exchange/bindings.
