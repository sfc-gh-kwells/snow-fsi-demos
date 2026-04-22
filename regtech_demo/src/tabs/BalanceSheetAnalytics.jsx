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
    setTipPos({ top: rect.bottom + 8, left: rect.left })
  }

  return (
    <div
      className="bsa-anomaly-note has-tooltip"
      onMouseEnter={showTip}
      onMouseLeave={() => setTipPos(null)}
    >
      ⚠ <strong>Q4 2025 anomaly:</strong> {anomaly.description} — RWA {anomaly.pctChange > 0 ? '+' : ''}{anomaly.pctChange.toFixed(1)}% vs expected range
      {tipPos && anomaly.descriptionFull && (
        <div className="bsa-tooltip" style={{ top: tipPos.top, left: tipPos.left }}>
          {anomaly.descriptionFull}
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

  useEffect(() => {
    fetch('/api/balance-sheet')
      .then((r) => r.json())
      .then((d) => { setData(d); setLoading(false) })
      .catch(() => setLoading(false))
  }, [])

  if (loading) return <div className="rt-loading">Loading balance sheet data…</div>
  if (!data)   return <div className="rt-empty">No data available.</div>

  // Aggregate across business lines for selected quarter KPIs
  const qMetrics = (data.metrics || []).filter((m) => m.quarter === quarter)
  const totalRwa   = qMetrics.reduce((s, m) => s + m.rwa, 0)
  const avgCet1    = qMetrics.length ? qMetrics.reduce((s, m) => s + m.cet1Ratio, 0) / qMetrics.length : 0
  const avgTier1   = qMetrics.length ? qMetrics.reduce((s, m) => s + m.tier1Ratio, 0) / qMetrics.length : 0
  const avgLcr     = qMetrics.length ? qMetrics.reduce((s, m) => s + m.lcr, 0) / qMetrics.length : 0
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

  // Anomaly flag
  const anomaly = (data.anomalies || []).find((a) => a.severity === 'High' || a.severity === 'Critical')

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
                <Tooltip {...TOOLTIP_STYLE} formatter={(v) => [`$${v.toFixed(0)}B`]} />
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
        .has-tooltip { cursor: default; }
        .bsa-kpi-sub.has-tooltip { text-decoration: underline dotted #9ca3af; cursor: help; }
        .bsa-tooltip { position: fixed; z-index: 9999; min-width: 280px; max-width: 420px; background: #1e293b; color: #f1f5f9; font-size: 12px; line-height: 1.6; padding: 10px 14px; border-radius: 6px; box-shadow: 0 4px 16px rgba(0,0,0,0.3); pointer-events: none; white-space: normal; }
        @media (max-width: 1100px) { .bsa-kpi-row { grid-template-columns: repeat(3, 1fr); } }
        @media (max-width: 700px)  { .bsa-kpi-row { grid-template-columns: 1fr 1fr; } }
      `}</style>
    </div>
  )
}
