// Static definitions of each flow's backend steps. For flows 2 & 3 every step
// carries a `match` of {service, stage} that lines up 1:1 with a message_logs
// row, so the diagram lights up as the matching DB rows appear.
export const FLOWS = {
  sse: {
    title: 'Flow 1 — SSE stream',
    endpoint: 'GET /api/sse-call',
    steps: [
      { key: 'browser', service: 'browser', title: 'Browser opens EventSource', detail: 'GET /api/sse-call' },
      { key: 'gw-open', service: 'gateway', title: 'Gateway proxies SSE', detail: 'opens GET /sse-stream on processing' },
      { key: 'pr-emit', service: 'processing', title: 'Processing emits 1..20', detail: 'one event every 0.5s for 10s' },
      { key: 'gw-relay', service: 'gateway', title: 'Gateway relays events', detail: 'streams each event back to the browser' },
    ],
  },
  rabbitmq: {
    title: 'Flow 2 — RabbitMQ round-trip',
    endpoint: 'POST /api/rabbitmq-call',
    steps: [
      { key: 'request_in', service: 'gateway', title: 'Gateway receives request', detail: 'logs request_in → MySQL', match: { service: 'gateway', stage: 'request_in' } },
      { key: 'queue_published', service: 'rabbitmq', title: 'Publish processing-queue', detail: 'gateway → processing-queue, logs queue_published', match: { service: 'gateway', stage: 'queue_published' } },
      { key: 'queue_consumed', service: 'processing', title: 'Processing consumes', detail: 'reads processing-queue, logs queue_consumed', match: { service: 'processing', stage: 'queue_consumed' } },
      { key: 'completed_published', service: 'rabbitmq', title: 'Enrich + publish completed-queue', detail: 'value*2, squared… logs completed_published', match: { service: 'processing', stage: 'completed_published' } },
      { key: 'completed_consumed', service: 'gateway', title: 'Gateway consumes reply', detail: 'reads completed-queue, logs completed_consumed', match: { service: 'gateway', stage: 'completed_consumed' } },
      { key: 'response_out', service: 'gateway', title: 'Gateway responds', detail: 'returns to browser, logs response_out', match: { service: 'gateway', stage: 'response_out' } },
    ],
  },
  mqtt: {
    title: 'Flow 4 — MQTT (EMQX) round-trip',
    endpoint: 'POST /api/mqtt-call',
    steps: [
      { key: 'request_in', service: 'gateway', title: 'Gateway receives request', detail: 'logs request_in → MySQL', match: { service: 'gateway', stage: 'request_in' } },
      { key: 'topic_published', service: 'emqx', title: 'Publish requests topic', detail: 'gateway → griddog/mqtt/requests, logs topic_published', match: { service: 'gateway', stage: 'topic_published' } },
      { key: 'topic_consumed', service: 'processing', title: 'Processing consumes', detail: 'subscribes griddog/mqtt/requests, logs topic_consumed', match: { service: 'processing', stage: 'topic_consumed' } },
      { key: 'completed_published', service: 'emqx', title: 'Enrich + publish completed topic', detail: 'value*2, squared… logs completed_published', match: { service: 'processing', stage: 'completed_published' } },
      { key: 'completed_consumed', service: 'gateway', title: 'Gateway consumes reply', detail: 'subscribes griddog/mqtt/completed, logs completed_consumed', match: { service: 'gateway', stage: 'completed_consumed' } },
      { key: 'response_out', service: 'gateway', title: 'Gateway responds', detail: 'returns to browser, logs response_out', match: { service: 'gateway', stage: 'response_out' } },
    ],
  },
  http: {
    title: 'Flow 3 — HTTP call',
    endpoint: 'POST /api/http-call',
    steps: [
      { key: 'request_in_gw', service: 'gateway', title: 'Gateway receives request', detail: 'logs request_in (gateway)', match: { service: 'gateway', stage: 'request_in' } },
      { key: 'request_in_pr', service: 'processing', title: 'Processing receives call', detail: 'POST /process, logs request_in (processing)', match: { service: 'processing', stage: 'request_in' } },
      { key: 'response_out_pr', service: 'processing', title: 'Processing responds', detail: 'computes result, logs response_out (processing)', match: { service: 'processing', stage: 'response_out' } },
      { key: 'response_out_gw', service: 'gateway', title: 'Gateway responds', detail: 'returns to browser, logs response_out (gateway)', match: { service: 'gateway', stage: 'response_out' } },
    ],
  },
}
