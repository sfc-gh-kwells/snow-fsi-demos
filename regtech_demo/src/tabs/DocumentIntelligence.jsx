import { useState, useEffect } from 'react'
import Chatbot from '../Chatbot'


const DOC_SUGGESTIONS = [
  { icon: '📋', label: 'What are the key capital ratio requirements under 12 CFR Part 3?' },
  { icon: '📊', label: 'How did our RWA perform against Basel III thresholds in Q4 2025?' },
  { icon: '💧', label: 'What HQLA assets qualify under the LCR rules in 12 CFR Part 50?' },
  { icon: '📑', label: 'Summarize the key changes in the latest regulatory document.' },
]

// ── Main component ────────────────────────────────────────────────────────────
export default function DocumentIntelligence() {
  const [data,          setData]          = useState(null)
  const [loading,       setLoading]       = useState(true)
  const [selectedDocId, setSelectedDocId] = useState(null)
  const [filterSev,     setFilterSev]     = useState('all')

  useEffect(() => {
    fetch('/api/documents')
      .then((r) => r.json())
      .then((d) => {
        setData(d)
        const newest = d.documents?.slice(-1)[0]
        if (newest) setSelectedDocId(newest.id)
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [])

  if (loading) return <div className="rt-loading">Loading regulatory documents…</div>
  if (!data)   return <div className="rt-empty">No data available.</div>

  const selectedDoc  = data.documents?.find((d) => d.id === selectedDocId)
  const requirements = (data.requirements || [])
    .filter((r) => r.docId === selectedDocId)
    .filter((r) => filterSev === 'all' || r.severity === filterSev)

  const severityCounts = (data.requirements || [])
    .filter((r) => r.docId === selectedDocId)
    .reduce((acc, r) => { acc[r.severity] = (acc[r.severity] || 0) + 1; return acc }, {})

  return (
    <div>
      <div className="di-page-header">
        <div>
          <h1 className="rt-section-title">Document Intelligence</h1>
          <p className="rt-section-sub">
            AI-extracted regulatory requirements · Cortex Agent for regulatory Q&amp;A
          </p>
        </div>
      </div>

      <div className="di-layout">

        {/* ── Left panel: doc selector + metadata ── */}
        <aside className="di-sidebar">

          <div className="rt-card" style={{ marginBottom: 16 }}>
            <div className="rt-card-header">
              <span className="rt-card-title">Regulatory Documents</span>
            </div>
            <div style={{ padding: '8px 0' }}>
              {(data.documents || []).map((doc) => (
                <div
                  key={doc.id}
                  className={`di-doc-item${doc.id === selectedDocId ? ' active' : ''}`}
                  onClick={() => setSelectedDocId(doc.id)}
                >
                  <div className="di-doc-name">{doc.name}</div>
                  <div className="di-doc-meta">
                    <span>{doc.framework}</span>
                    <span className={`rt-badge ${doc.status === 'Active' ? 'medium' : doc.status === 'Superseded' ? 'removal' : 'change'}`}>
                      {doc.status}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {selectedDoc && (
            <div className="rt-card">
              <div className="rt-card-header">
                <span className="rt-card-title">Document Details</span>
              </div>
              <div className="rt-card-body">
                <div className="di-meta-grid">
                  <div className="di-meta-row">
                    <span className="di-meta-label">Framework</span>
                    <span className="di-meta-value">{selectedDoc.framework}</span>
                  </div>
                  <div className="di-meta-row">
                    <span className="di-meta-label">Version</span>
                    <span className="di-meta-value">{selectedDoc.version}</span>
                  </div>
                  <div className="di-meta-row">
                    <span className="di-meta-label">Effective</span>
                    <span className="di-meta-value">{selectedDoc.effectiveDate || '—'}</span>
                  </div>
                  <div className="di-meta-row">
                    <span className="di-meta-label">Pages</span>
                    <span className="di-meta-value">{selectedDoc.pageCount?.toLocaleString()}</span>
                  </div>
                  <div className="di-meta-row">
                    <span className="di-meta-label">Status</span>
                    <span className={`rt-badge ${selectedDoc.status === 'Active' ? 'medium' : 'removal'}`}>
                      {selectedDoc.status}
                    </span>
                  </div>
                </div>
                <div className="di-summary">{selectedDoc.summary}</div>

                <div className="di-severity-row">
                  {['Critical','High','Medium','Low'].map((s) => (
                    severityCounts[s] ? (
                      <div key={s} className="di-sev-pill">
                        <span className={`rt-badge ${s.toLowerCase()}`}>{s}</span>
                        <span className="di-sev-count">{severityCounts[s]}</span>
                      </div>
                    ) : null
                  ))}
                </div>
              </div>
            </div>
          )}
        </aside>

        {/* ── Right panel: requirements table + agent chat ── */}
        <div className="di-main">

          {/* Extracted requirements */}
          <div className="rt-card">
            <div className="rt-card-header">
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span className="rt-card-title">Extracted Requirements</span>
                <span className="rt-card-sub">via AI_COMPLETE</span>
              </div>
              <div className="di-filter-row">
                {['all','Critical','High','Medium'].map((s) => (
                  <button
                    key={s}
                    className={`di-filter-btn${filterSev === s ? ' active' : ''}`}
                    onClick={() => setFilterSev(s)}
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
                    <th style={{ width: 80 }}>Severity</th>
                    <th style={{ width: 130 }}>Rule / Section</th>
                    <th>Prior Requirement</th>
                    <th>New Requirement</th>
                    <th style={{ width: 150 }}>Impacted Report</th>
                    <th style={{ width: 90 }}>Change Type</th>
                  </tr>
                </thead>
                <tbody>
                  {requirements.length === 0 ? (
                    <tr><td colSpan={6} className="rt-empty">No requirements match the current filter.</td></tr>
                  ) : requirements.map((r) => (
                    <tr key={r.id}>
                      <td><span className={`rt-badge ${r.severity.toLowerCase()}`}>{r.severity}</span></td>
                      <td>
                        <div style={{ fontWeight: 600, fontSize: 12 }}>{r.ruleName}</div>
                        <div style={{ color: '#9ca3af', fontSize: 11 }}>{r.ruleSection}</div>
                      </td>
                      <td style={{ color: '#6b7280', fontSize: 12 }}>{r.oldRequirement || <span style={{ color: '#d1d5db' }}>—</span>}</td>
                      <td style={{ fontSize: 12 }}>{r.newRequirement}</td>
                      <td style={{ fontSize: 12 }}>{r.impactedReport}</td>
                      <td><span className={`rt-badge ${r.changeType?.toLowerCase() || 'change'}`}>{r.changeType}</span></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

        </div>
      </div>

      <Chatbot
        title="Regulatory Document AI"
        suggestions={DOC_SUGGESTIONS}
      />

      <style>{`
        .di-page-header { margin-bottom: 20px; }
        .di-layout { display: grid; grid-template-columns: 280px 1fr; gap: 16px; align-items: start; }
        .di-doc-item { padding: 10px 16px; cursor: pointer; border-left: 3px solid transparent; transition: background 0.12s; }
        .di-doc-item:hover { background: #f9fafb; }
        .di-doc-item.active { background: #f0f4ff; border-left-color: var(--navy); }
        .di-doc-name { font-size: 13px; font-weight: 600; color: #111827; margin-bottom: 4px; }
        .di-doc-meta { display: flex; align-items: center; justify-content: space-between; gap: 8px; font-size: 11px; color: #6b7280; }
        .di-meta-grid { display: flex; flex-direction: column; gap: 8px; margin-bottom: 12px; }
        .di-meta-row { display: flex; align-items: center; justify-content: space-between; }
        .di-meta-label { font-size: 11px; color: #9ca3af; font-weight: 500; }
        .di-meta-value { font-size: 12px; font-weight: 500; color: #111827; }
        .di-summary { font-size: 12px; color: #6b7280; line-height: 1.5; border-top: 1px solid #f3f4f6; padding-top: 10px; margin-top: 4px; }
        .di-severity-row { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; border-top: 1px solid #f3f4f6; padding-top: 10px; }
        .di-sev-pill { display: flex; align-items: center; gap: 4px; }
        .di-sev-count { font-size: 12px; font-weight: 600; color: #374151; }
        .di-filter-row { display: flex; gap: 4px; }
        .di-filter-btn { padding: 3px 10px; border-radius: 4px; border: 1px solid #e5e7eb; background: white; font-size: 11px; font-weight: 500; color: #6b7280; cursor: pointer; }
        .di-filter-btn:hover { background: #f3f4f6; }
        .di-filter-btn.active { background: var(--navy); color: white; border-color: var(--navy); }

        @media (max-width: 900px) { .di-layout { grid-template-columns: 1fr; } }
      `}</style>
    </div>
  )
}
