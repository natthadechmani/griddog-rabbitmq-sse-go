// FlowDiagram renders a flow's backend steps as connected boxes that light up
// (pending → active → done) as the request moves through the system.
export default function FlowDiagram({ flow, states, liveCount }) {
  return (
    <div className="diagram">
      {flow.steps.map((step, i) => {
        const state = states[step.key] || 'pending'
        return (
          <div className="diagram-item" key={step.key}>
            <div className={`step step-${state} svc-border-${step.service}`}>
              <div className="step-head">
                <span className={`svc svc-${step.service}`}>{step.service}</span>
                <span className={`step-state state-${state}`}>{state}</span>
              </div>
              <div className="step-title">{step.title}</div>
              <div className="step-detail">{step.detail}</div>
              {step.key === 'pr-emit' && liveCount != null && (
                <div className="step-live">count: {liveCount}</div>
              )}
            </div>
            {i < flow.steps.length - 1 && <div className="arrow">→</div>}
          </div>
        )
      })}
    </div>
  )
}
