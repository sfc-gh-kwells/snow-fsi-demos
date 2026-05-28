import { useState } from 'react'
import Chatbot from '../Chatbot.jsx'

const SEV_COLORS = { Critical: '#dc2626', High: '#d97706', Medium: '#2563eb', Low: '#6b7280' }
const STATUS_COLORS = { fail: '#dc2626', warn: '#d97706', pass: '#16a34a' }
const STATUS_LABELS = { fail: 'FAIL', warn: 'WARN', pass: 'PASS' }
const STATUS_BG = { fail: '#fef2f2', warn: '#fffbeb', pass: '#f0fdf4' }

export default function CrossReportValidation() {
  const [results,   setResults]   = useState(null)
  const [loading,   setLoading]   = useState(false)
  const [expanded,  setExpanded]  = useState(null)
  const [quarter,   setQuarter]   = useState('Q4 2025')
  const [chatOpen,  setChatOpen]  = useState(false)

  const runValidation = async () => {
    setLoading(true)
    setResults(null)
    setExpanded(null)
    try {
      const res = await fetch('/api/cross-report-validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ quarter }),
      })
      const data = await res.json()
      setResults(data)
    } catch {
      setResults({ error: 'Failed to run validation.' })
    } finally {
      setLoading(false)
    }
  }

  const failures = results?.rules?.filter(r => r.status === 'fail') || []
  const warnings = results?.rules?.filter(r => r.status === 'warn') || []
  const passes   = results?.rules?.filter(r => r.status === 'pass') || []

  return (
    <div>
      {/* ── Header ── */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 12 }}>
          <div>
            <h1 className="rt-section-title">Cross-Report Validation</h1>
            <p className="rt-section-sub">
              MDRM-linked reconciliation across FR Y-9C, LCR, Capital, and Risk reports · AI-powered root cause analysis
            </p>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div className="crv-quarter-selector">
              <span style={{ fontSize: 11, color: '#6b7280', marginRight: 6 }}>Quarter:</span>
              {['Q2 2025', 'Q3 2025', 'Q4 2025'].map(q => (
                <button
                  key={q}
                  className={`bsa-q-btn${quarter === q ? ' active' : ''}${q === 'Q4 2025' ? ' anomaly-q' : ''}`}
                  onClick={() => setQuarter(q)}
                >
                  {q}
                </button>
              ))}
            </div>
            <button
              className="rt-btn rt-btn-primary"
              onClick={runValidation}
              disabled={loading}
              style={{ display: 'flex', alignItems: 'center', gap: 5 }}
            >
              {loading ? '⟳ Running…' : '▶ Run Validation'}
            </button>
            <button className="rt-btn rt-btn-secondary" onClick={() => setChatOpen(true)} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
              ✦ Ask AI
            </button>
          </div>
        </div>
      </div>

      {/* ── Pre-run state ── */}
      {!results && !loading && (
        <div className="rt-card" style={{ textAlign: 'center', padding: '48px 24px' }}>
          <div style={{ fontSize: 40, marginBottom: 12 }}>⚖</div>
          <div style={{ fontSize: 15, fontWeight: 600, color: '#111827', marginBottom: 8 }}>
            Cross-Report Reconciliation Engine
          </div>
          <div style={{ fontSize: 13, color: '#6b7280', maxWidth: 520, margin: '0 auto 20px' }}>
            Validates consistency across FR Y-9C, LCR, FRTB Market Risk, SA-CCR, Leverage, and Capital reports
            using MDRM-linked rules. Identifies which reports are breaking and provides AI root cause analysis.
          </div>
          <button className="rt-btn rt-btn-primary" onClick={runValidation} style={{ fontSize: 13, padding: '8px 20px' }}>
            Run {quarter} Validation
          </button>
          <div className="crv-rule-preview">
            <div style={{ fontSize: 11, fontWeight: 600, color: '#9ca3af', textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 8 }}>
              Sample validation rules
            </div>
            {[
              { r1: 'FR Y-9C HC-R', r2: 'FRTB Market Risk', desc: 'Total RWA reconciliation', mdrm: 'BHCK3545 ↔ MR-RWA-TOTAL' },
              { r1: 'LCR Report', r2: 'FR Y-9C HC-H', desc: 'HQLA stock consistency', mdrm: 'LCR-HQLA ↔ BHCK1754' },
              { r1: 'Capital Report', r2: 'FR Y-9C HC-R', desc: 'CET1 numerator reconciliation', mdrm: 'CAP-CET1-NET ↔ BHCK8274' },
              { r1: 'SA-CCR Report', r2: 'FR Y-9C HC-R', desc: 'Derivatives EAD consistency', mdrm: 'CCR-EAD ↔ BHCK3529' },
            ].map((rule, i) => (
              <div key={i} className="crv-rule-row">
                <span className="crv-mdrm-tag">{rule.mdrm}</span>
                <span style={{ fontSize: 12, color: '#374151', fontWeight: 500 }}>{rule.desc}</span>
                <span style={{ fontSize: 11, color: '#9ca3af' }}>{rule.r1} ↔ {rule.r2}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {loading && (
        <div className="rt-card" style={{ textAlign: 'center', padding: '48px 24px' }}>
          <div className="crv-spinner" />
          <div style={{ fontSize: 13, color: '#6b7280', marginTop: 12 }}>
            Running {quarter} cross-report validation across 11 rules…
          </div>
        </div>
      )}

      {results && !results.error && (
        <>
          {/* ── Summary scorecard ── */}
          <div className="crv-summary-row">
            <div className="crv-stat-card fail">
              <div className="crv-stat-num">{results.summary.fail}</div>
              <div className="crv-stat-label">Failures</div>
            </div>
            <div className="crv-stat-card warn">
              <div className="crv-stat-num">{results.summary.warn}</div>
              <div className="crv-stat-label">Warnings</div>
            </div>
            <div className="crv-stat-card pass">
              <div className="crv-stat-num">{results.summary.pass}</div>
              <div className="crv-stat-label">Passing</div>
            </div>
            <div className="crv-stat-card total">
              <div className="crv-stat-num">{results.summary.total}</div>
              <div className="crv-stat-label">Total Rules</div>
            </div>
            <div className="crv-stat-meta">
              <div style={{ fontSize: 11, color: '#6b7280' }}>Last run: {results.lastRun}</div>
              <div style={{ fontSize: 11, color: '#6b7280' }}>Quarter: {results.quarter}</div>
              {results.summary.fail > 0 && (
                <div style={{ fontSize: 11, color: '#dc2626', fontWeight: 600, marginTop: 4 }}>
                  ⚠ {results.summary.fail} rule(s) require attention before submission
                </div>
              )}
            </div>
          </div>

          {/* ── Failures first ── */}
          {failures.length > 0 && (
            <div className="rt-card" style={{ marginBottom: 16 }}>
              <div className="rt-card-header">
                <span className="rt-card-title" style={{ color: '#dc2626' }}>Reconciliation Failures</span>
                <span className="rt-card-sub">{failures.length} rule(s) — action required before regulatory submission</span>
              </div>
              <div style={{ padding: '0 0 8px' }}>
                {failures.map(rule => (
                  <RuleRow key={rule.id} rule={rule} expanded={expanded} setExpanded={setExpanded} />
                ))}
              </div>
            </div>
          )}

          {/* ── Warnings ── */}
          {warnings.length > 0 && (
            <div className="rt-card" style={{ marginBottom: 16 }}>
              <div className="rt-card-header">
                <span className="rt-card-title" style={{ color: '#d97706' }}>Warnings</span>
                <span className="rt-card-sub">{warnings.length} rule(s) — within tolerance but trending toward breach</span>
              </div>
              <div style={{ padding: '0 0 8px' }}>
                {warnings.map(rule => (
                  <RuleRow key={rule.id} rule={rule} expanded={expanded} setExpanded={setExpanded} />
                ))}
              </div>
            </div>
          )}

          {/* ── Passing ── */}
          {passes.length > 0 && (
            <div className="rt-card">
              <div className="rt-card-header">
                <span className="rt-card-title" style={{ color: '#16a34a' }}>Passing Rules</span>
                <span className="rt-card-sub">{passes.length} rule(s) reconcile within tolerance</span>
              </div>
              <div style={{ padding: '0 0 8px' }}>
                {passes.map(rule => (
                  <RuleRow key={rule.id} rule={rule} expanded={expanded} setExpanded={setExpanded} />
                ))}
              </div>
            </div>
          )}
        </>
      )}

      {results?.error && (
        <div className="rt-card" style={{ padding: 24, color: '#dc2626' }}>
          Error: {results.error}
        </div>
      )}

      <Chatbot
        open={chatOpen}
        onClose={() => setChatOpen(false)}
        title="Cross-Report Validation AI"
        suggestions={[
          { icon: '⚖', label: 'Which reports have MDRM reconciliation failures in Q4 2025?' },
          { icon: '🔍', label: 'Explain the FR Y-9C vs FRTB RWA discrepancy' },
          { icon: '📋', label: 'What actions are needed before the FR Y-9C submission?' },
          { icon: '🔗', label: 'How does the FRTB recalibration flow through to other reports?' },
        ]}
      />

      <style>{`
        .crv-quarter-selector { display: flex; align-items: center; gap: 4px; }
        .crv-summary-row { display: flex; gap: 12px; margin-bottom: 16px; align-items: stretch; flex-wrap: wrap; }
        .crv-stat-card { background: white; border: 1px solid #dde3ed; border-radius: 8px; padding: 14px 20px; min-width: 100px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,.06); }
        .crv-stat-card.fail  { border-color: #fca5a5; background: #fef2f2; }
        .crv-stat-card.warn  { border-color: #fcd34d; background: #fffbeb; }
        .crv-stat-card.pass  { border-color: #86efac; background: #f0fdf4; }
        .crv-stat-card.total { border-color: #bfdbfe; background: #eff6ff; }
        .crv-stat-num   { font-size: 28px; font-weight: 700; color: #111827; line-height: 1; }
        .crv-stat-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #6b7280; margin-top: 4px; }
        .crv-stat-meta  { flex: 1; background: white; border: 1px solid #dde3ed; border-radius: 8px; padding: 14px 16px; display: flex; flex-direction: column; justify-content: center; }
        .crv-rule-row-wrap { border-bottom: 1px solid #f3f4f6; }
        .crv-rule-row-wrap:last-child { border-bottom: none; }
        .crv-rule-header { display: flex; align-items: center; gap: 10px; padding: 12px 16px; cursor: pointer; transition: background 0.1s; }
        .crv-rule-header:hover { background: #f9fafb; }
        .crv-status-pill { font-size: 10px; font-weight: 700; padding: 2px 8px; border-radius: 10px; letter-spacing: 0.06em; min-width: 40px; text-align: center; flex-shrink: 0; }
        .crv-rule-desc { flex: 1; font-size: 12px; font-weight: 600; color: #111827; }
        .crv-rule-reports { font-size: 11px; color: #6b7280; white-space: nowrap; }
        .crv-mdrm-chips { display: flex; gap: 4px; flex-shrink: 0; }
        .crv-mdrm-chip { font-size: 10px; background: #f3f4f6; color: #374151; border-radius: 4px; padding: 2px 6px; font-family: 'SF Mono', Menlo, monospace; border: 1px solid #e5e7eb; }
        .crv-sev-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
        .crv-expand-toggle { font-size: 12px; color: #9ca3af; flex-shrink: 0; }
        .crv-rule-detail { background: #f9fafb; border-top: 1px solid #f3f4f6; padding: 14px 16px; }
        .crv-values-row { display: flex; gap: 16px; margin-bottom: 12px; flex-wrap: wrap; }
        .crv-value-block { background: white; border: 1px solid #e5e7eb; border-radius: 6px; padding: 10px 14px; min-width: 140px; }
        .crv-value-label { font-size: 10px; color: #9ca3af; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 4px; }
        .crv-value-num { font-size: 15px; font-weight: 700; color: #111827; }
        .crv-value-mdrm { font-size: 10px; color: #6b7280; margin-top: 2px; font-family: 'SF Mono', Menlo, monospace; }
        .crv-ai-box { background: #eff6ff; border: 1px solid #bfdbfe; border-radius: 6px; padding: 10px 14px; }
        .crv-ai-label { font-size: 10px; font-weight: 700; color: #2563eb; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 6px; }
        .crv-ai-text { font-size: 12px; color: #1e3a5f; line-height: 1.6; }
        .crv-rule-preview { margin-top: 24px; background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 14px 16px; max-width: 560px; margin-left: auto; margin-right: auto; text-align: left; }
        .crv-rule-row { display: flex; align-items: center; gap: 10px; padding: 6px 0; border-bottom: 1px solid #f3f4f6; }
        .crv-rule-row:last-child { border-bottom: none; }
        .crv-mdrm-tag { font-size: 10px; background: #e0e7ff; color: #3730a3; border-radius: 4px; padding: 2px 6px; font-family: 'SF Mono', Menlo, monospace; flex-shrink: 0; min-width: 180px; }
        .crv-spinner { width: 32px; height: 32px; border: 3px solid #e5e7eb; border-top-color: var(--navy,#003366); border-radius: 50%; animation: crv-spin 0.8s linear infinite; margin: 0 auto; }
        @keyframes crv-spin { to { transform: rotate(360deg); } }
        .rt-btn-secondary { background: white; color: var(--navy,#003366); border: 1px solid #dde3ed; border-radius: 6px; padding: 6px 14px; font-size: 12px; font-weight: 600; cursor: pointer; transition: background 0.1s; }
        .rt-btn-secondary:hover { background: #f3f4f6; }
      `}</style>
    </div>
  )
}

function RuleRow({ rule, expanded, setExpanded }) {
  const isOpen = expanded === rule.id
  const statusColor = STATUS_COLORS[rule.status]
  const statusBg    = STATUS_BG[rule.status]
  const sevColor    = SEV_COLORS[rule.severity] || '#6b7280'

  return (
    <div className="crv-rule-row-wrap">
      <div className="crv-rule-header" onClick={() => setExpanded(isOpen ? null : rule.id)}>
        <span
          className="crv-status-pill"
          style={{ background: statusBg, color: statusColor, border: `1px solid ${statusColor}33` }}
        >
          {STATUS_LABELS[rule.status]}
        </span>
        <span className="crv-sev-dot" style={{ background: sevColor }} title={`${rule.severity} severity`} />
        <span className="crv-rule-desc">{rule.description}</span>
        {rule.value1 && rule.value2 && (
          <span style={{ fontSize: 11, color: statusColor, fontWeight: 600, whiteSpace: 'nowrap' }}>
            {rule.delta}
          </span>
        )}
        <div className="crv-mdrm-chips">
          {rule.mdrm1 && <span className="crv-mdrm-chip">{rule.mdrm1}</span>}
          {rule.mdrm2 && <span className="crv-mdrm-chip">{rule.mdrm2}</span>}
        </div>
        <span className="crv-rule-reports">{rule.report1} ↔ {rule.report2}</span>
        {(rule.value1 || rule.explanation) && (
          <span className="crv-expand-toggle">{isOpen ? '▲' : '▼'}</span>
        )}
      </div>

      {isOpen && (rule.value1 || rule.explanation) && (
        <div className="crv-rule-detail">
          {(rule.value1 || rule.value2 || rule.delta) && (
            <div className="crv-values-row">
              <div className="crv-value-block">
                <div className="crv-value-label">{rule.report1}</div>
                <div className="crv-value-num">{rule.value1 || '—'}</div>
                <div className="crv-value-mdrm">{rule.mdrm1}</div>
              </div>
              <div className="crv-value-block">
                <div className="crv-value-label">{rule.report2}</div>
                <div className="crv-value-num">{rule.value2 || '—'}</div>
                <div className="crv-value-mdrm">{rule.mdrm2}</div>
              </div>
              {rule.delta && (
                <div className="crv-value-block" style={{ borderColor: `${statusColor}66` }}>
                  <div className="crv-value-label">Discrepancy</div>
                  <div className="crv-value-num" style={{ color: statusColor }}>{rule.delta}</div>
                  <div className="crv-value-mdrm">BL: {rule.businessLine || 'All'}</div>
                </div>
              )}
            </div>
          )}
          {rule.explanation && (
            <div className="crv-ai-box">
              <div className="crv-ai-label">✦ AI Root Cause Analysis</div>
              <div className="crv-ai-text">{rule.explanation}</div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
