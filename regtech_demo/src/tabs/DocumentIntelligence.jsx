import { useState, useEffect } from 'react'

const SEVERITY_ORDER = { Critical: 0, High: 1, Medium: 2, Low: 3 }

export default function DocumentIntelligence() {
  const [data,          setData]          = useState(null)
  const [loading,       setLoading]       = useState(true)
  const [selectedDocId, setSelectedDocId] = useState(null)
  const [searchQuery,   setSearchQuery]   = useState('')
  const [searching,     setSearching]     = useState(false)
  const [searchResults, setSearchResults] = useState(null)
  const [filterSev,     setFilterSev]     = useState('all')

  useEffect(() => {
    fetch('/api/documents')
      .then((r) => r.json())
      .then((d) => {
        setData(d)
        // Default to the newest doc
        const newest = d.documents?.slice(-1)[0]
        if (newest) setSelectedDocId(newest.id)
        setLoading(false)
      })
      .catch(() => setLoading(false))
  }, [])

  const handleSearch = async () => {
    if (!searchQuery.trim()) return
    setSearching(true)
    setSearchResults(null)
    try {
      const r = await fetch('/api/search', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ query: searchQuery, docId: selectedDocId }),
      })
      const d = await r.json()
      setSearchResults(d.results || [])
    } catch {
      setSearchResults([])
    } finally {
      setSearching(false)
    }
  }

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
            AI-extracted regulatory requirements · Cortex Search over raw document text
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
                  onClick={() => { setSelectedDocId(doc.id); setSearchResults(null) }}
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

                {/* Severity breakdown */}
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

        {/* ── Right panel: requirements table + search ── */}
        <div className="di-main">

          {/* Requirements table */}
          <div className="rt-card" style={{ marginBottom: 16 }}>
            <div className="rt-card-header">
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span className="rt-card-title">Extracted Requirements</span>
                <span className="rt-card-sub">via AI_EXTRACT</span>
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

          {/* Cortex Search */}
          <div className="rt-card">
            <div className="rt-card-header">
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <span className="rt-card-title">Document Search</span>
                <span className="rt-card-sub">Cortex Search · semantic search over raw document text</span>
              </div>
            </div>
            <div className="rt-card-body">
              <div className="di-search-row">
                <input
                  className="di-search-input"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                  placeholder='e.g. "What is the new Tier 1 capital deduction rule?" or "SA-CCR netting set requirements"'
                />
                <button className="rt-btn rt-btn-primary" onClick={handleSearch} disabled={searching || !searchQuery.trim()}>
                  {searching ? 'Searching…' : 'Search'}
                </button>
              </div>

              <div className="di-search-suggestions">
                {['What changed in the Tier 1 capital deduction rules?',
                  'New FRTB market risk capital requirements',
                  'Liquidity Coverage Ratio HQLA definition changes',
                ].map((q) => (
                  <button key={q} className="di-suggestion" onClick={() => { setSearchQuery(q); }}>
                    {q}
                  </button>
                ))}
              </div>

              {searchResults !== null && (
                <div className="di-search-results">
                  {searchResults.length === 0 ? (
                    <div className="rt-empty">No results found. Try a different query.</div>
                  ) : searchResults.map((r, i) => (
                    <div key={i} className="di-result-card">
                      <div className="di-result-meta">
                        <span className="di-result-doc">{r.docName} · {r.docVersion}</span>
                        {r.sectionTitle && <span className="di-result-section">{r.sectionTitle}</span>}
                        {r.pageNumber  && <span className="di-result-page">p. {r.pageNumber}</span>}
                      </div>
                      <div className="di-result-text">{r.text}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

        </div>
      </div>

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
        .di-search-row { display: flex; gap: 8px; margin-bottom: 10px; }
        .di-search-input { flex: 1; padding: 8px 12px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 13px; outline: none; }
        .di-search-input:focus { border-color: var(--navy); box-shadow: 0 0 0 2px rgba(0,51,102,0.1); }
        .di-search-suggestions { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 14px; }
        .di-suggestion { padding: 4px 10px; background: #f0f4ff; border: 1px solid #c7d7f0; border-radius: 20px; font-size: 11px; color: var(--navy); cursor: pointer; transition: background 0.12s; }
        .di-suggestion:hover { background: #dce8fa; }
        .di-search-results { display: flex; flex-direction: column; gap: 10px; }
        .di-result-card { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 6px; padding: 12px 14px; }
        .di-result-meta { display: flex; align-items: center; gap: 8px; margin-bottom: 6px; flex-wrap: wrap; }
        .di-result-doc { font-size: 11px; font-weight: 600; color: var(--navy); }
        .di-result-section { font-size: 11px; color: #6b7280; }
        .di-result-page { font-size: 11px; color: #9ca3af; }
        .di-result-text { font-size: 13px; color: #374151; line-height: 1.6; }
        @media (max-width: 900px) { .di-layout { grid-template-columns: 1fr; } }
      `}</style>
    </div>
  )
}
