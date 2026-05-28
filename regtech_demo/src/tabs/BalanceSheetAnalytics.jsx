import { useState, useEffect, useRef } from 'react'
import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell,
} from 'recharts'
import Chatbot from '../Chatbot.jsx'

const BIZ_COLORS = {
  'Institutional Securities': '#003366',
  'Wealth Management':        '#2563eb',
  'Investment Management':    '#7c3aed',
}

const QUARTERS_ORDERED = ['Q1 2024','Q2 2024','Q3 2024','Q4 2024','Q1 2025','Q2 2025','Q3 2025','Q4 2025']
const ANOMALY_QUARTER  = 'Q4 2025'

function KpiCard({ label, value, unit, change, anomaly, sub, tooltip }) {
  const [tipPos, setTipPos] = useState(null)
  const subRef = useRef(null)

  const showTip = (e) => {
    if (!tooltip) return
    const rect = e.currentTarget.getBoundingClientRect()
    setTipPos({ top: rect.bottom + 8, left: rect.left })
  }

  return (
    <div className={`bsa-kpi-card${anomaly ? ' anomaly' : ''}`}>
      {anomaly && (
        <div className="bsa-anomaly-flag">
          <span className="rt-badge anomaly">⚠ Anomaly Detected</span>
        </div>
      )}
      <div className="bsa-kpi-label">{label}</div>
      <div className="bsa-kpi-value">{value}<span className="bsa-kpi-unit">{unit}</span></div>
      {change !== undefined && (
        <div className={`bsa-kpi-change ${change >= 0 ? 'up' : 'down'}`}>
          {change >= 0 ? '▲' : '▼'} {Math.abs(change).toFixed(1)}% vs prior quarter
        </div>
      )}
      {sub && (
        <div
          ref={subRef}
          className={`bsa-kpi-sub${tooltip ? ' has-tooltip' : ''}`}
          onMouseEnter={showTip}
          onMouseLeave={() => setTipPos(null)}
        >
          {sub}
          {tipPos && tooltip && (
            <div className="bsa-tooltip" style={{ top: tipPos.top, left: tipPos.left }}>
              {tooltip}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function AnomalyNote({ anomaly }) {
  const [tipPos, setTipPos] = useState(null)

  const showTip = (e) => {
    if (!anomaly.descriptionFull) return
    const rect = e.currentTarget.getBoundingClientRect()
    setTipPos({ top: rect.bottom + 8, left: Math.min(rect.left, window.innerWidth - 420) })
  }

  const label = anomaly.severity === 'Critical' ? '⚠ Q4 2025 critical anomaly:'
               : anomaly.severity === 'High'     ? '⚠ Q4 2025 anomaly:'
               :                                   '○ Q4 2025 watch item:'

  return (
    <div
      className={`bsa-anomaly-note has-tooltip${anomaly.severity === 'Watch' ? ' watch' : ''}`}
      onMouseEnter={showTip}
      onMouseLeave={() => setTipPos(null)}
    >
      <strong>{label}</strong> {anomaly.description}
      {tipPos && anomaly.descriptionFull && (
        <div className="bsa-tooltip" style={{ top: tipPos.top, left: tipPos.left }}>
          {anomaly.descriptionFull}
        </div>
      )}
    </div>
  )
}

function CustomRwaTooltip({ active, payload, label, anomaly }) {
  if (!active || !payload?.length) return null
  const total = payload.reduce((s, p) => s + (Number(p.value) || 0), 0)
  const isWatch = label === ANOMALY_QUARTER && anomaly

  return (
    <div style={{
      background: '#fff', border: `1px solid ${isWatch ? '#d97706' : '#e2e8f0'}`,
      borderRadius: 8, padding: '10px 14px', fontSize: 11,
      boxShadow: '0 4px 12px rgba(0,0,0,0.1)', maxWidth: 360,
    }}>
      <div style={{ fontWeight: 700, marginBottom: 6, color: '#111827' }}>{label}</div>
      {payload.map((p) => (
        <div key={p.dataKey} style={{ display: 'flex', justifyContent: 'space-between', gap: 16, color: '#374151', marginBottom: 2 }}>
          <span style={{ color: p.fill ?? p.color }}>{p.dataKey}</span>
          <span style={{ fontVariantNumeric: 'tabular-nums' }}>${Number(p.value).toFixed(0)}B</span>
        </div>
      ))}
      <div style={{ borderTop: '1px solid #f3f4f6', marginTop: 6, paddingTop: 6, fontWeight: 600, display: 'flex', justifyContent: 'space-between' }}>
        <span>Total RWA</span>
        <span>${total.toFixed(0)}B</span>
      </div>
      {isWatch && (
        <div style={{
          marginTop: 8, paddingTop: 8, borderTop: '1px solid #fde68a',
          background: '#fffbeb', borderRadius: 4, padding: '6px 8px',
          color: '#92400e', fontSize: 10.5, lineHeight: 1.5,
        }}>
          <div style={{ fontWeight: 700, marginBottom: 3 }}>
            {anomaly.severity === 'Critical' ? '⚠ Anomaly' : anomaly.severity === 'High' ? '⚠ Anomaly' : '○ Watch item'} · ML score {(anomaly.pctChange > 0 ? '+' : '')}{anomaly.pctChange.toFixed(1)}% vs forecast
          </div>
          {anomaly.description}
          {anomaly.descriptionFull && anomaly.descriptionFull !== anomaly.description && (
            <div style={{ marginTop: 4, color: '#b45309', fontSize: 10 }}>Hover the banner below for full ML analysis ↓</div>
          )}
        </div>
      )}
    </div>
  )
}

export default function BalanceSheetAnalytics() {
  const [data,      setData]      = useState(null)
  const [loading,   setLoading]   = useState(true)
  const [quarter,   setQuarter]   = useState('Q4 2025')
  const [chatOpen,  setChatOpen]  = useState(false)
  const [reportChatOpen, setReportChatOpen] = useState(false)
  const [reportInitMsg,  setReportInitMsg]  = useState('')
  const [reportMenuOpen, setReportMenuOpen] = useState(false)
  const [dqOpen,    setDqOpen]    = useState(true)
  const [dqData,    setDqData]    = useState(null)
  const [dqLoading, setDqLoading] = useState(true)

  const openReport = (template) => {
    setReportInitMsg(template)
    setReportChatOpen(true)
  }

  const loadDQ = () => {
    setDqOpen(o => !o)
  }

  useEffect(() => {
    fetch('/api/balance-sheet')
      .then((r) => r.json())
      .then((d) => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
    // Pre-fetch DQ (served from server cache — instant)
    fetch('/api/data-quality')
      .then((r) => r.json())
      .then((d) => { setDqData(d); setDqLoading(false) })
      .catch(() => { setDqData({ error: 'Failed to load data quality checks.' }); setDqLoading(false) })
  }, [])

  if (loading) return <div className="rt-loading">Loading balance sheet data…</div>
  if (!data)   return <div className="rt-empty">No data available.</div>

  // Aggregate across business lines for selected quarter KPIs
  // Consolidated overrides: use firm-wide ratios from 10-K/8-K when available
  // (segment averages would give wrong consolidated LCR/Tier1/CET1 due to pool-level calculation)
  const qMetrics = (data.metrics || []).filter((m) => m.quarter === quarter)
  const consol    = (data.consolidated || {})[quarter] || {}
  const totalRwa   = consol.totalRwa ?? qMetrics.reduce((s, m) => s + m.rwa, 0)
  const avgCet1    = consol.cet1  ?? (qMetrics.length ? qMetrics.reduce((s, m) => s + m.cet1Ratio, 0) / qMetrics.length : 0)
  const avgTier1   = consol.tier1 ?? (qMetrics.length ? qMetrics.reduce((s, m) => s + m.tier1Ratio, 0) / qMetrics.length : 0)
  const avgLcr     = consol.lcr   ?? (qMetrics.length ? qMetrics.reduce((s, m) => s + m.lcr, 0) / qMetrics.length : 0)
  const totalRev   = qMetrics.reduce((s, m) => s + m.netRevenue, 0)

  // Prior quarter for delta
  const priorQ     = QUARTERS_ORDERED[QUARTERS_ORDERED.indexOf(quarter) - 1]
  const priorQMet  = (data.metrics || []).filter((m) => m.quarter === priorQ)
  const priorRwa   = priorQMet.reduce((s, m) => s + m.rwa, 0)
  const rwaChg     = priorRwa ? ((totalRwa - priorRwa) / priorRwa) * 100 : 0

  // RWA stacked bar data (per quarter, by business line)
  const rwaChartData = QUARTERS_ORDERED.map((q) => {
    const row = { quarter: q }
    ;(data.metrics || []).filter((m) => m.quarter === q).forEach((m) => {
      row[m.businessLine] = m.rwa
    })
    return row
  })

  // CET1 line data (avg across BLs per quarter)
  const cet1ChartData = QUARTERS_ORDERED.map((q) => {
    const qm = (data.metrics || []).filter((m) => m.quarter === q)
    const avg = qm.length ? qm.reduce((s, m) => s + m.cet1Ratio, 0) / qm.length : 0
    return { quarter: q, cet1: +avg.toFixed(2) }
  })

  // Anomaly flag — includes Watch items (ML score > 0.5) not just Critical/High
  const anomaly = (data.anomalies || []).find((a) => ['Critical','High','Watch'].includes(a.severity))

  // Variance table (for chatbot context; show top rows)
  const variance = (data.variance || []).filter((v) => v.currentQuarter === 'Q4 2025').slice(0, 8)

  const TOOLTIP_STYLE = {
    contentStyle: { background: '#fff', border: '1px solid #e2e8f0', borderRadius: 6, fontSize: 11 },
    labelStyle:   { color: '#6b7280', fontWeight: 600 },
  }

  return (
    <div>
      <div style={{ marginBottom: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 12 }}>
          <div>
            <h1 className="rt-section-title">Balance Sheet Analytics</h1>
            <p className="rt-section-sub">Capital ratios · Variance analysis · Anomaly detection</p>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
            <div className="bsa-quarter-selector">
              <span style={{ fontSize: 11, color: '#6b7280', marginRight: 6 }}>Quarter:</span>
              {QUARTERS_ORDERED.slice(-4).map((q) => (
                <button
                  key={q}
                  className={`bsa-q-btn${quarter === q ? ' active' : ''}${q === ANOMALY_QUARTER ? ' anomaly-q' : ''}`}
                  onClick={() => setQuarter(q)}
                >
                  {q}
                </button>
              ))}
            </div>
            <div style={{ position: 'relative' }}>
              <button
                className="rt-btn crv-report-menu-btn"
                onClick={() => setReportMenuOpen(o => !o)}
                style={{ display: 'flex', alignItems: 'center', gap: 5 }}
              >
                📋 Generate Report ▾
              </button>
              {reportMenuOpen && (
                <div className="crv-report-dropdown">
                  {[
                    { label: 'FR Y-9C Capital Variance Commentary', msg: 'Generate a formal FR Y-9C regulatory capital variance commentary for Q4 2025. Include: (1) Executive Summary of capital position, (2) CET1 ratio change analysis with QoQ drivers, (3) RWA movement explanation referencing FRTB recalibration, (4) LCR variance footnote, (5) Forward-looking disclosures. Use formal regulatory language suitable for Federal Reserve submission.' },
                    { label: 'LCR Stress Report Narrative', msg: 'Generate a formal LCR (Liquidity Coverage Ratio) stress report narrative for Q4 2025 for Morgan Stanley Institutional Securities. Include: (1) HQLA stock composition and changes, (2) Net cash outflow drivers, (3) LCR ratio trend vs regulatory minimum, (4) Management actions and forward guidance. Format as a structured regulatory narrative.' },
                    { label: 'Anomaly Root Cause Memo', msg: 'Draft a formal internal memo explaining the Q4 2025 RWA anomaly for Morgan Stanley Institutional Securities. Address: (1) Nature and magnitude of the anomaly ($68.9B RWA increase), (2) Root cause analysis (FRTB Expected Shortfall recalibration + SA-CCR), (3) Impact on CET1 ratio, (4) Regulatory disclosure requirements, (5) Remediation timeline. Suitable for review by senior management and regulators.' },
                  ].map((r, i) => (
                    <button key={i} className="crv-report-option" onClick={() => { openReport(r.msg); setReportMenuOpen(false) }}>
                      {r.label}
                    </button>
                  ))}
                </div>
              )}
            </div>
            <button className="rt-btn rt-btn-primary" onClick={() => setChatOpen(true)} style={{ display: 'flex', alignItems: 'center', gap: 5 }}>
              ✦ Ask AI
            </button>
          </div>
        </div>
      </div>

      {/* ── KPI row ── */}
      <div className="bsa-kpi-row">
        <KpiCard
          label="CET1 Ratio"
          value={avgCet1.toFixed(2)}
          unit="%"
          sub="Common Equity Tier 1"
        />
        <KpiCard
          label="Tier 1 Capital Ratio"
          value={avgTier1.toFixed(2)}
          unit="%"
          sub="Regulatory minimum: 8.5%"
        />
        <KpiCard
          label="Risk-Weighted Assets"
          value={totalRwa.toFixed(0)}
          unit="B"
          change={rwaChg}
          anomaly={quarter === ANOMALY_QUARTER && !!anomaly}
          sub={anomaly && quarter === ANOMALY_QUARTER ? anomaly.description : 'Total across business lines'}
          tooltip={anomaly && quarter === ANOMALY_QUARTER ? anomaly.descriptionFull : null}
        />
        <KpiCard
          label="Liquidity Coverage Ratio"
          value={avgLcr.toFixed(1)}
          unit="%"
          sub="Target: ≥ 100%"
        />
        <KpiCard
          label="Net Revenue"
          value={`$${(totalRev / 1000).toFixed(1)}`}
          unit="B"
          sub={`${quarter} across all BUs`}
        />
      </div>

      {/* ── Charts ── */}
      <div className="bsa-charts-row">

        {/* RWA stacked bar */}
        <div className="rt-card" style={{ flex: '1 1 55%' }}>
          <div className="rt-card-header">
            <span className="rt-card-title">Risk-Weighted Assets by Business Line</span>
            <span className="rt-card-sub">$B · anomalous quarter highlighted</span>
          </div>
          <div className="rt-card-body" style={{ paddingTop: 8 }}>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={rwaChartData} barSize={22}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
                <XAxis dataKey="quarter" tick={{ fontSize: 10, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 10, fill: '#9ca3af' }} axisLine={false} tickLine={false} unit="B" />
                <Tooltip content={<CustomRwaTooltip anomaly={anomaly} />} />
                <Legend iconSize={10} wrapperStyle={{ fontSize: 11 }} />
                {Object.entries(BIZ_COLORS).map(([bl, color]) => (
                  <Bar key={bl} dataKey={bl} stackId="a" fill={color}>
                    {rwaChartData.map((entry, i) => (
                      <Cell
                        key={i}
                        fill={entry.quarter === ANOMALY_QUARTER ? '#b45309' : color}
                        opacity={entry.quarter === ANOMALY_QUARTER ? 1 : 0.85}
                      />
                    ))}
                  </Bar>
                ))}
              </BarChart>
            </ResponsiveContainer>
            {anomaly && (
              <AnomalyNote anomaly={anomaly} />
            )}
          </div>
        </div>

        {/* CET1 line chart */}
        <div className="rt-card" style={{ flex: '1 1 40%' }}>
          <div className="rt-card-header">
            <span className="rt-card-title">CET1 Ratio Trend</span>
            <span className="rt-card-sub">% · 8-quarter view</span>
          </div>
          <div className="rt-card-body" style={{ paddingTop: 8 }}>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={cet1ChartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
                <XAxis dataKey="quarter" tick={{ fontSize: 10, fill: '#9ca3af' }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 10, fill: '#9ca3af' }} axisLine={false} tickLine={false} domain={['auto','auto']} unit="%" />
                <Tooltip {...TOOLTIP_STYLE} formatter={(v) => [`${v}%`, 'CET1 Ratio']} />
                <Line
                  type="monotone" dataKey="cet1"
                  stroke="#003366" strokeWidth={2.5}
                  dot={(props) => {
                    const { cx, cy, payload } = props
                    const isAnomaly = payload.quarter === ANOMALY_QUARTER
                    return <circle key={props.key} cx={cx} cy={cy} r={isAnomaly ? 6 : 3.5} fill={isAnomaly ? '#d97706' : '#003366'} stroke="white" strokeWidth={1.5} />
                  }}
                  activeDot={{ r: 5 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* ── Variance table ── */}
      {variance.length > 0 && (
        <div className="rt-card" style={{ marginTop: 16 }}>
          <div className="rt-card-header">
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <span className="rt-card-title">Q3 → Q4 2025 Variance Analysis</span>
              <span className="rt-card-sub">QoQ change by business line and metric</span>
            </div>
          </div>
          <div className="rt-table-wrap">
            <table className="rt-table">
              <thead>
                <tr>
                  <th>Business Line</th>
                  <th>Metric</th>
                  <th>Q3 2025</th>
                  <th>Q4 2025</th>
                  <th>Change</th>
                  <th>Change %</th>
                  <th>Primary Driver</th>
                </tr>
              </thead>
              <tbody>
                {variance.map((v, i) => (
                  <tr key={i}>
                    <td style={{ fontWeight: 500 }}>{v.businessLine}</td>
                    <td>{v.metricName}</td>
                    <td>{v.priorValue?.toLocaleString()}</td>
                    <td>{v.currentValue?.toLocaleString()}</td>
                    <td style={{ color: v.changeAbs >= 0 ? '#16a34a' : '#dc2626', fontWeight: 600 }}>
                      {v.changeAbs >= 0 ? '+' : ''}{v.changeAbs?.toLocaleString()}
                    </td>
                    <td style={{ color: v.changePct >= 0 ? '#16a34a' : '#dc2626', fontWeight: 600 }}>
                      {v.changePct >= 0 ? '+' : ''}{v.changePct?.toFixed(1)}%
                    </td>
                    <td style={{ fontSize: 11, color: '#6b7280' }}>{v.driver}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* ── Data Quality panel ── */}
      <div className="rt-card" style={{ marginTop: 16 }}>
        <div
          className="rt-card-header dq-toggle-header"
          onClick={loadDQ}
          style={{ cursor: 'pointer' }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span className="rt-card-title">Data Quality Checks</span>
            {dqData && !dqData.error && (
              <div style={{ display: 'flex', gap: 5 }}>
                {dqData.summary.fail > 0 && <span className="rt-badge critical">{dqData.summary.fail} Fail</span>}
                {dqData.summary.warn > 0 && <span className="rt-badge high">{dqData.summary.warn} Warn</span>}
                {dqData.summary.pass > 0 && <span className="rt-badge medium" style={{ background: '#f0fdf4', color: '#166534', borderColor: '#86efac' }}>{dqData.summary.pass} Pass</span>}
              </div>
            )}
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span className="rt-card-sub">Native Snowflake Data Metric Functions · AI root cause analysis</span>
            <span style={{ fontSize: 13, color: '#9ca3af' }}>{dqLoading ? '⟳' : (dqOpen ? '▲' : '▼')}</span>
          </div>
        </div>

        {dqOpen && (
          <div>
            {dqLoading && (
              <div style={{ padding: '24px', textAlign: 'center', color: '#6b7280', fontSize: 13 }}>
                Running data quality checks…
              </div>
            )}
            {dqData?.error && (
              <div style={{ padding: 16, color: '#dc2626', fontSize: 13 }}>{dqData.error}</div>
            )}
            {dqData && !dqData.error && !dqLoading && (
              <div className="rt-table-wrap">
                <table className="rt-table">
                  <thead>
                    <tr>
                      <th style={{ width: 70 }}>Status</th>
                      <th style={{ width: 120 }}>Category</th>
                      <th>Check</th>
                      <th style={{ width: 110 }}>Value</th>
                      <th style={{ width: 180 }}>Threshold</th>
                      <th>AI Root Cause</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(dqData.checks || []).map((c) => (
                      <tr key={c.id} style={{ background: c.status === 'fail' ? '#fef2f2' : c.status === 'warn' ? '#fffbeb' : 'white' }}>
                        <td>
                          <span style={{
                            display: 'inline-block', padding: '2px 8px', borderRadius: 10,
                            fontSize: 10, fontWeight: 700, letterSpacing: '0.06em',
                            background: c.status === 'fail' ? '#fef2f2' : c.status === 'warn' ? '#fffbeb' : '#f0fdf4',
                            color: c.status === 'fail' ? '#dc2626' : c.status === 'warn' ? '#d97706' : '#16a34a',
                            border: `1px solid ${c.status === 'fail' ? '#fca5a5' : c.status === 'warn' ? '#fcd34d' : '#86efac'}`,
                          }}>
                            {c.status === 'fail' ? 'FAIL' : c.status === 'warn' ? 'WARN' : 'PASS'}
                          </span>
                        </td>
                        <td style={{ fontSize: 11, color: '#6b7280' }}>{c.category}</td>
                        <td style={{ fontSize: 12, fontWeight: 500 }}>
                          {c.name}
                          {c.dmf && (
                            <div style={{ fontSize: 9.5, color: '#6366f1', fontFamily: 'monospace', marginTop: 2, fontWeight: 400, letterSpacing: '0.01em' }}>
                              {c.dmf}
                            </div>
                          )}
                          {c.quarter && <div style={{ fontSize: 10, color: '#9ca3af' }}>{c.quarter} · {c.businessLine || 'All'}</div>}
                        </td>
                        <td style={{ fontSize: 12, fontWeight: 600, color: c.status === 'fail' ? '#dc2626' : c.status === 'warn' ? '#d97706' : '#374151' }}>
                          {c.value || '✓'}
                        </td>
                        <td style={{ fontSize: 11, color: '#6b7280' }}>{c.threshold}</td>
                        <td style={{ fontSize: 11, color: '#374151', lineHeight: 1.5 }}>
                          {c.rootCause
                            ? <span style={{ color: '#1e3a5f' }}>{c.rootCause}</span>
                            : <span style={{ color: '#d1d5db' }}>—</span>
                          }
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>

      <Chatbot
        open={chatOpen}
        onClose={() => setChatOpen(false)}
        title="Balance Sheet AI"
        suggestions={[
          { icon: '📊', label: 'What drove the RWA spike in Q4 2025?' },
          { icon: '⚠', label: 'Any capital threshold breaches this quarter?' },
          { icon: '📉', label: 'Show CET1 trend vs prior 4 quarters' },
          { icon: '📋', label: 'Draft regulatory commentary for Tier 1 change' },
        ]}
      />

      <Chatbot
        open={reportChatOpen}
        onClose={() => { setReportChatOpen(false); setReportInitMsg('') }}
        title="Regulatory Report Generator"
        initialMessage={reportInitMsg}
        suggestions={[
          { icon: '📋', label: 'FR Y-9C Q4 2025 capital variance commentary' },
          { icon: '💧', label: 'LCR stress narrative for Institutional Securities' },
          { icon: '⚠', label: 'Anomaly root cause memo for Q4 2025 RWA spike' },
          { icon: '📊', label: 'Tier 1 capital trend narrative for last 4 quarters' },
        ]}
      />

      <style>{`
        .bsa-quarter-selector { display: flex; align-items: center; gap: 4px; }
        .bsa-q-btn { padding: 4px 10px; border-radius: 4px; border: 1px solid #e5e7eb; background: white; font-size: 11px; font-weight: 500; color: #6b7280; cursor: pointer; }
        .bsa-q-btn:hover { background: #f3f4f6; }
        .bsa-q-btn.active { background: var(--navy); color: white; border-color: var(--navy); }
        .bsa-q-btn.anomaly-q { border-color: #d97706; }
        .bsa-q-btn.active.anomaly-q { background: #d97706; border-color: #d97706; }
        .bsa-kpi-row { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 16px; }
        .bsa-kpi-card { background: white; border: 1px solid #dde3ed; border-radius: 8px; padding: 14px 16px; box-shadow: 0 1px 3px rgba(0,0,0,.06); position: relative; }
        .bsa-kpi-card.anomaly { border-color: #d97706; box-shadow: 0 0 0 2px rgba(217,119,6,0.15); }
        .bsa-anomaly-flag { margin-bottom: 6px; }
        .bsa-kpi-label { font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #9ca3af; margin-bottom: 4px; }
        .bsa-kpi-value { font-size: 26px; font-weight: 700; color: #111827; line-height: 1.1; }
        .bsa-kpi-unit { font-size: 14px; font-weight: 500; color: #6b7280; margin-left: 2px; }
        .bsa-kpi-change { font-size: 11px; font-weight: 600; margin-top: 4px; }
        .bsa-kpi-change.up { color: #16a34a; }
        .bsa-kpi-change.down { color: #dc2626; }
        .bsa-kpi-sub { font-size: 10px; color: #9ca3af; margin-top: 4px; position: relative; }
        .bsa-charts-row { display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 0; }
        .bsa-anomaly-note { margin-top: 10px; font-size: 12px; color: #92400e; background: #fffbeb; border: 1px solid #fcd34d; border-radius: 5px; padding: 8px 12px; position: relative; }
        .bsa-anomaly-note.watch { color: #6b7280; background: #f9fafb; border: 1px solid #d1d5db; }
        .has-tooltip { cursor: default; }
        .bsa-kpi-sub.has-tooltip { text-decoration: underline dotted #9ca3af; cursor: help; }
        .bsa-tooltip { position: fixed; z-index: 9999; min-width: 280px; max-width: 420px; background: #1e293b; color: #f1f5f9; font-size: 12px; line-height: 1.6; padding: 10px 14px; border-radius: 6px; box-shadow: 0 4px 16px rgba(0,0,0,0.3); pointer-events: none; white-space: normal; }
        @media (max-width: 1100px) { .bsa-kpi-row { grid-template-columns: repeat(3, 1fr); } }
        @media (max-width: 700px)  { .bsa-kpi-row { grid-template-columns: 1fr 1fr; } }
        .crv-report-menu-btn { background: white; color: var(--navy,#003366); border: 1px solid #dde3ed; border-radius: 6px; padding: 6px 14px; font-size: 12px; font-weight: 600; cursor: pointer; }
        .crv-report-menu-btn:hover { background: #f3f4f6; }
        .crv-report-dropdown { position: absolute; top: calc(100% + 4px); right: 0; background: white; border: 1px solid #e5e7eb; border-radius: 8px; box-shadow: 0 4px 16px rgba(0,0,0,0.12); min-width: 260px; z-index: 100; overflow: hidden; }
        .crv-report-option { display: block; width: 100%; text-align: left; background: none; border: none; border-bottom: 1px solid #f3f4f6; padding: 10px 14px; font-size: 12px; color: #111827; cursor: pointer; }
        .crv-report-option:last-child { border-bottom: none; }
        .crv-report-option:hover { background: #f9fafb; }
        .dq-toggle-header:hover { background: #f9fafb; border-radius: 8px 8px 0 0; }
      `}</style>
    </div>
  )
}
