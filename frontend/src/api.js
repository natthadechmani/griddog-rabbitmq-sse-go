const base = '/api'

// postJSON sends a JSON body and returns the parsed response.
export async function postJSON(path, body) {
  const res = await fetch(base + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const data = await res.json()
  return { ok: res.ok, status: res.status, data }
}

// getMessages fetches persisted message_logs rows for a flow.
export async function getMessages(flow) {
  const res = await fetch(`${base}/messages?flow=${encodeURIComponent(flow)}`)
  return res.json()
}

// openSSE opens the flow-1 stream and wires the callbacks.
export function openSSE(onMessage, onDone, onError) {
  const es = new EventSource(`${base}/sse-call`)
  es.onmessage = (e) => {
    try {
      onMessage(JSON.parse(e.data))
    } catch {
      onMessage({ raw: e.data })
    }
  }
  es.addEventListener('done', () => {
    onDone()
    es.close()
  })
  es.onerror = () => {
    onError()
    es.close()
  }
  return es
}
