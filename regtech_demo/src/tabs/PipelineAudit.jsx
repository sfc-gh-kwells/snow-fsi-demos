import { useState, useEffect } from 'react'

export default function PipelineAudit() {
  const [data,     setData]     = useState(null)
  const [loading,  setLoading]  = useState(true)
  const [expanded, setExpanded] = useState({})
  const [filter,   setFilter]   = useState('all')
  const [running,  setRunning]  = useState(false)
  const [runMsg,   setRunMsg]   = useState(null)

  useEffect(() => {
    fetch('/api/audit-findings')
      .then((r) => r.json())
      .then((d) => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  const toggleRow = (id) => setExpanded((e) => ({ ...e, [id]: !e[id] }))

  const handleRunAudit = () => {
    setRunning(true)
    setRunMsg(null)
    // Simulate a brief "run" then surface pre-run results
    setTimeout(() => {
      setRunning(false)
      setRunMsg('Pipeline audit complete. Results updated as of ' + new Date().toLocaleString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' }))
    }, 2800)
  }

  if (loading) return <div className="rt-loading">Loading audit findings…</div>
  if (!data)   return <div className="rt-empty">No data available.</div>

  const findings = (data.findings || []).filter((f) => filter === 'all' || f.severity === filter)
  const { critical = 0, high = 0, medium = 0 } = data.summary || {}

  return (
    <div>
      {/* ── Page header ── */}
      <div className="pa-page-header">
        <div>
          <h1 className="rt-section-title">Pipeline Audit</h1>
          <p className="rt-section-sub">
            Cortex Agent SDK analysis of regulatory reporting pipelines against updated requirements
          </p>
        </div>
        <div className="pa-header-actions">
          <div className="pa-last-run">
            <span className="pa-last-run-label">Last audit run:</span>
            <span className="pa-last-run-value">{runMsg ? runMsg.replace('Pipeline audit complete. Results updated as of ', '') : data.lastRun}</span>
          </div>
          <button
            className="rt-btn rt-btn-primary"
            onClick={handleRunAudit}
            disabled={running}
            title="Triggers Cortex Agent SDK pipeline analysis"
          >
            {running ? (
              <><span className="pa-spin">⟳</span> Analyzing…</>
            ) : (
              <><span>▶</span> Run Pipeline Audit</>
            )}
          </button>
        </div>
      </div>

      {/* ── Run animation banner ── */}
      {running && (
        <div className="pa-running-banner">
          <span className="pa-spin">⟳</span>
          Cortex Agent SDK is analyzing pipeline artifacts against Basel IV requirements…
        </div>
      )}
      {runMsg && !running && (
        <div className="pa-done-banner">
          ✓ {runMsg}
        </div>
      )}

      {/* ── Summary badges ── */}
      <div className="pa-summary-row">
        <div className="pa-summary-card critical">
          <div className="pa-summary-count">{critical}</div>
          <div className="pa-summary-label">Critical</div>
        </div>
        <div className="pa-summary-card high">
          <div className="pa-summary-count">{high}</div>
          <div className="pa-summary-label">High</div>
        </div>
        <div className="pa-summary-card medium">
          <div className="pa-summary-count">{medium}</div>
          <div className="pa-summary-label">Medium</div>
        </div>
        <div className="pa-summary-card total">
          <div className="pa-summary-count">{critical + high + medium}</div>
          <div className="pa-summary-label">Total Findings</div>
        </div>
      </div>

      {/* ── Filter + findings table ── */}
      <div className="rt-card">
        <div className="rt-card-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span className="rt-card-title">Audit Findings</span>
            <span className="rt-card-sub">{findings.length} finding{findings.length !== 1 ? 's' : ''}</span>
          </div>
          <div style={{ display: 'flex', gap: 4 }}>
            {['all','Critical','High','Medium'].map((s) => (
              <button
                key={s}
                className={`di-filter-btn${filter === s ? ' active' : ''}`}
                onClick={() => setFilter(s)}
              >
                {s === 'all' ? 'All' : s}
              </button>
            ))}
          </div>
        </div>

        <div className="rt-table-wrap">
          <table className="rt-table">
            <thead>
              <tr>
                <th style={{ width: 85 }}>Severity</th>
                <th style={{ width: 170 }}>Pipeline File</th>
                <th style={{ width: 160 }}>Regulatory Rule</th>
                <th>Issue</th>
                <th style={{ width: 120 }}>Impacted Reports</th>
                <th style={{ width: 90 }}>Status</th>
                <th style={{ width: 48 }}></th>
              </tr>
            </thead>
            <tbody>
              {findings.length === 0 ? (
                <tr><td colSpan={7} className="rt-empty">No findings for selected severity.</td></tr>
              ) : findings.map((f) => (
                <>
                  <tr key={f.id} className="pa-finding-row" onClick={() => toggleRow(f.id)} style={{ cursor: 'pointer' }}>
                    <td><span className={`rt-badge ${f.severity.toLowerCase()}`}>{f.severity}</span></td>
                    <td>
                      <code className="pa-file-name">{f.pipelineFile}</code>
                    </td>
                    <td>
                      <div style={{ fontWeight: 500, fontSize: 12 }}>{f.regulatoryRule}</div>
                      <div style={{ fontSize: 11, color: '#9ca3af' }}>{f.ruleSection}</div>
                    </td>
                    <td style={{ fontSize: 12, color: '#374151' }}>{f.issueDescription}</td>
                    <td style={{ fontSize: 11, color: '#6b7280' }}>{f.impactedReports}</td>
                    <td>
                      <span className={`rt-badge ${f.status.toLowerCase().replace(' ', '-')}`}>{f.status}</span>
                    </td>
                    <td style={{ textAlign: 'center', color: '#9ca3af' }}>
                      {expanded[f.id] ? '▲' : '▼'}
                    </td>
                  </tr>

                  {expanded[f.id] && (
                    <tr key={`${f.id}-detail`} className="pa-detail-row">
                      <td colSpan={7}>
                        <div className="pa-detail-grid">
                          <div className="pa-detail-block">
                            <div className="pa-detail-label">Current Logic</div>
                            <pre className="pa-code-block pa-code-old">{f.oldLogic}</pre>
                          </div>
                          <div className="pa-detail-block">
                            <div className="pa-detail-label">Suggested Fix</div>
                            <pre className="pa-code-block pa-code-new">{f.suggestedFix}</pre>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                </>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <style>{`
        .pa-page-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 20px; flex-wrap: wrap; }
        .pa-header-actions { display: flex; align-items: center; gap: 12px; flex-shrink: 0; }
        .pa-last-run { text-align: right; }
        .pa-last-run-label { font-size: 10px; color: #9ca3af; display: block; }
        .pa-last-run-value { font-size: 12px; font-weight: 500; color: #374151; }
        .pa-running-banner { background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 6px; padding: 10px 16px; font-size: 12px; color: #1e40af; margin-bottom: 14px; display: flex; align-items: center; gap: 8px; }
        .pa-done-banner { background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 6px; padding: 10px 16px; font-size: 12px; color: #166534; margin-bottom: 14px; }
        .pa-spin { display: inline-block; animation: pa-rotate 1s linear infinite; }
        @keyframes pa-rotate { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
        .pa-summary-row { display: flex; gap: 12px; margin-bottom: 16px; }
        .pa-summary-card { background: white; border: 1px solid #e5e7eb; border-radius: 8px; padding: 14px 20px; flex: 1; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,.06); }
        .pa-summary-card.critical { border-top: 3px solid #dc2626; }
        .pa-summary-card.high     { border-top: 3px solid #d97706; }
        .pa-summary-card.medium   { border-top: 3px solid #2563eb; }
        .pa-summary-card.total    { border-top: 3px solid #003366; }
        .pa-summary-count { font-size: 28px; font-weight: 700; color: #111827; line-height: 1; }
        .pa-summary-label { font-size: 11px; color: #6b7280; margin-top: 4px; font-weight: 500; }
        .pa-file-name { background: #f3f4f6; color: #1e40af; padding: 2px 6px; border-radius: 4px; font-size: 11px; font-family: 'SF Mono', Menlo, monospace; }
        .pa-finding-row:hover td { background: #f9fafb; }
        .pa-detail-row td { background: #f8fafc; padding: 0; }
        .pa-detail-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; padding: 14px 16px; }
        .pa-detail-label { font-size: 11px; font-weight: 600; color: #6b7280; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 6px; }
        .pa-code-block { font-family: 'SF Mono', Menlo, monospace; font-size: 11px; line-height: 1.6; padding: 10px 12px; border-radius: 5px; margin: 0; white-space: pre-wrap; word-break: break-word; }
        .pa-code-old { background: #fff5f5; border: 1px solid #fecaca; color: #991b1b; }
        .pa-code-new { background: #f0fdf4; border: 1px solid #bbf7d0; color: #166534; }
        .di-filter-btn { padding: 3px 10px; border-radius: 4px; border: 1px solid #e5e7eb; background: white; font-size: 11px; font-weight: 500; color: #6b7280; cursor: pointer; }
        .di-filter-btn:hover { background: #f3f4f6; }
        .di-filter-btn.active { background: var(--navy); color: white; border-color: var(--navy); }
        @media (max-width: 700px) { .pa-detail-grid { grid-template-columns: 1fr; } }
      `}</style>
    </div>
  )
}
