import { useState } from 'react'
import { FLOWS } from './flows'
import FlowDiagram from './FlowDiagram'
import { postJSON, getMessages, openSSE } from './api'

const delay = (ms) => new Promise((res) => setTimeout(res, ms))

function JsonBlock({ value }) {
  if (value == null) return <p className="muted">—</p>
  return <pre className="json">{JSON.stringify(value, null, 2)}</pre>
}

function MessagesTable({ rows }) {
  if (!rows || rows.length === 0) return <p className="muted">No rows yet — run the flow, then refresh.</p>
  return (
    <div className="table-wrap">
      <table className="msg-table">
        <thead>
          <tr>
            <th>id</th><th>service</th><th>stage</th><th>correlation_id</th><th>payload</th><th>created_at</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.id}>
              <td>{r.id}</td>
              <td><span className={`svc svc-${r.service}`}>{r.service}</span></td>
              <td>{r.stage}</td>
              <td className="mono small">{r.correlation_id.slice(0, 8)}…</td>
              <td>
                <details className="payload-cell">
                  <summary>{JSON.stringify(r.payload)}</summary>
                  <pre className="payload-full">{JSON.stringify(r.payload, null, 2)}</pre>
                </details>
              </td>
              <td className="mono small">{r.created_at}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// State of each step for flows 2 & 3: done if its DB row was revealed, the first
// not-yet-done step is "active" while running, the rest pending.
function computeStepStates(steps, status, doneKeys) {
  const done = new Set(doneKeys)
  const states = {}
  let activeAssigned = false
  for (const s of steps) {
    if (done.has(s.key)) {
      states[s.key] = 'done'
    } else if (status === 'running' && !activeAssigned) {
      states[s.key] = 'active'
      activeAssigned = true
    } else {
      states[s.key] = 'pending'
    }
  }
  return states
}

function computeSSEStates(status, count) {
  const keys = ['browser', 'gw-open', 'pr-emit', 'gw-relay']
  const states = {}
  if (status === 'idle') {
    keys.forEach((k) => (states[k] = 'pending'))
    return states
  }
  if (status === 'done') {
    keys.forEach((k) => (states[k] = 'done'))
    return states
  }
  states.browser = 'done'
  states['gw-open'] = 'done'
  states['pr-emit'] = 'active'
  states['gw-relay'] = count > 0 ? 'active' : 'pending'
  return states
}

export default function App() {
  // Flow 1 (SSE)
  const [sseStatus, setSseStatus] = useState('idle')
  const [sseCounts, setSseCounts] = useState([])

  // Flow 2 (RabbitMQ)
  const [rmqStatus, setRmqStatus] = useState('idle')
  const [rmqReq, setRmqReq] = useState(null)
  const [rmqRes, setRmqRes] = useState(null)
  const [rmqDone, setRmqDone] = useState([])

  // Flow 3 (HTTP)
  const [httpStatus, setHttpStatus] = useState('idle')
  const [httpReq, setHttpReq] = useState(null)
  const [httpRes, setHttpRes] = useState(null)
  const [httpDone, setHttpDone] = useState([])

  // DB viewers
  const [rmqRows, setRmqRows] = useState([])
  const [httpRows, setHttpRows] = useState([])

  function runSSE() {
    setSseStatus('running')
    setSseCounts([])
    openSSE(
      (msg) => { if (msg.count) setSseCounts((c) => [...c, msg.count]) },
      () => setSseStatus('done'),
      () => setSseStatus((s) => (s === 'running' ? 'done' : s)),
    )
  }

  async function runFlow(flowKey, endpoint, setStatus, setReq, setRes, setDone, setRows) {
    setStatus('running')
    setRes(null)
    setDone([])
    const value = Math.floor(Math.random() * 100) + 1
    const req = { value }
    setReq(req)
    try {
      const { data } = await postJSON(endpoint, req)
      setRes(data)
      const corr = data && data.correlation_id
      const rows = await getMessages(flowKey)
      setRows(rows)
      // Reveal each step in order as its matching DB row is found.
      const mine = rows.filter((r) => r.correlation_id === corr)
      for (const step of FLOWS[flowKey].steps) {
        const hit = mine.find((r) => r.service === step.match.service && r.stage === step.match.stage)
        if (hit) {
          setDone((d) => [...d, step.key])
          await delay(300)
        }
      }
      setStatus('done')
      setRows(await getMessages(flowKey)) // final refresh
    } catch {
      setStatus('idle')
    }
  }

  const sseStates = computeSSEStates(sseStatus, sseCounts.length)
  const rmqStates = computeStepStates(FLOWS.rabbitmq.steps, rmqStatus, rmqDone)
  const httpStates = computeStepStates(FLOWS.http.steps, httpStatus, httpDone)

  return (
    <div className="app">
      <header className="app-head">
        <h1>Griddog · RabbitMQ + SSE + MySQL</h1>
        <p className="muted">
          Two Go services — <b>gateway-backend</b> ⇄ <b>processing-backend</b> — over RabbitMQ (AMQP),
          HTTP and SSE, persisting every hop to MySQL.
        </p>
        <div className="legend">
          <span className="svc svc-browser">browser</span>
          <span className="svc svc-gateway">gateway</span>
          <span className="svc svc-processing">processing</span>
          <span className="svc svc-rabbitmq">rabbitmq</span>
        </div>
      </header>

      {/* Flow 1 */}
      <section className="card">
        <div className="card-head">
          <h2>{FLOWS.sse.title}</h2>
          <code className="endpoint">{FLOWS.sse.endpoint}</code>
          <button onClick={runSSE} disabled={sseStatus === 'running'}>Run SSE</button>
          <span className={`badge badge-${sseStatus}`}>{sseStatus}</span>
        </div>
        <FlowDiagram flow={FLOWS.sse} states={sseStates} liveCount={sseCounts[sseCounts.length - 1] || 0} />
        <div className="panel">
          <strong>Live stream</strong>
          <div className="stream">
            {sseCounts.length === 0
              ? <span className="muted">no events yet</span>
              : sseCounts.map((c, i) => <span key={i} className="chip">{c}</span>)}
          </div>
        </div>
      </section>

      {/* Flow 2 */}
      <section className="card">
        <div className="card-head">
          <h2>{FLOWS.rabbitmq.title}</h2>
          <code className="endpoint">{FLOWS.rabbitmq.endpoint}</code>
          <button
            onClick={() => runFlow('rabbitmq', '/rabbitmq-call', setRmqStatus, setRmqReq, setRmqRes, setRmqDone, setRmqRows)}
            disabled={rmqStatus === 'running'}
          >Run RabbitMQ</button>
          <span className={`badge badge-${rmqStatus}`}>{rmqStatus}</span>
        </div>
        <FlowDiagram flow={FLOWS.rabbitmq} states={rmqStates} />
        <div className="cols">
          <div className="panel"><strong>Request</strong><JsonBlock value={rmqReq} /></div>
          <div className="panel"><strong>Response</strong><JsonBlock value={rmqRes} /></div>
        </div>
      </section>

      {/* Flow 3 */}
      <section className="card">
        <div className="card-head">
          <h2>{FLOWS.http.title}</h2>
          <code className="endpoint">{FLOWS.http.endpoint}</code>
          <button
            onClick={() => runFlow('http', '/http-call', setHttpStatus, setHttpReq, setHttpRes, setHttpDone, setHttpRows)}
            disabled={httpStatus === 'running'}
          >Run HTTP</button>
          <span className={`badge badge-${httpStatus}`}>{httpStatus}</span>
        </div>
        <FlowDiagram flow={FLOWS.http} states={httpStates} />
        <div className="cols">
          <div className="panel"><strong>Request</strong><JsonBlock value={httpReq} /></div>
          <div className="panel"><strong>Response</strong><JsonBlock value={httpRes} /></div>
        </div>
      </section>

      {/* DB viewer */}
      <section className="card">
        <div className="card-head"><h2>Database · message_logs</h2></div>
        <p className="muted small">Showing the most recent rows. Click any payload to expand the full value.</p>
        <div className="db-stack">
          <div className="panel">
            <div className="panel-head">
              <strong>Flow 2 (rabbitmq)</strong>
              <button onClick={async () => setRmqRows(await getMessages('rabbitmq'))}>Refresh Flow 2</button>
            </div>
            <MessagesTable rows={rmqRows} />
          </div>
          <div className="panel">
            <div className="panel-head">
              <strong>Flow 3 (http)</strong>
              <button onClick={async () => setHttpRows(await getMessages('http'))}>Refresh Flow 3</button>
            </div>
            <MessagesTable rows={httpRows} />
          </div>
        </div>
      </section>
    </div>
  )
}
