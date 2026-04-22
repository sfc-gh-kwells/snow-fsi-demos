import { useState, useRef, useEffect, useMemo } from 'react'
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'

const CHART_COLORS = ['#003366','#2563eb','#7c3aed','#0891b2','#16a34a','#d97706','#dc2626','#6366f1']

const STATUS_LABELS = {
  planning:              'Planning next steps',
  extracting_tool_calls: 'Selecting data sources',
  reasoning_agent_stop:  'Reviewing results',
  proceeding_to_answer:  'Forming answer',
  tool_execution:        'Executing query',
}
function getStatusLabel(s) { return STATUS_LABELS[s] || s }

// ── Lightweight inline markdown ───────────────────────────────────────────────
function parseInline(text) {
  const tokens = []; let i = 0, buf = ''
  while (i < text.length) {
    if (text[i] === '`') {
      const end = text.indexOf('`', i + 1)
      if (end !== -1) { if (buf) { tokens.push(buf); buf = '' }; tokens.push({ type: 'code', text: text.slice(i + 1, end) }); i = end + 1; continue }
    }
    if (text[i] === '*' && text[i + 1] === '*') {
      const end = text.indexOf('**', i + 2)
      if (end !== -1) { if (buf) { tokens.push(buf); buf = '' }; tokens.push({ type: 'bold', text: text.slice(i + 2, end) }); i = end + 2; continue }
    }
    if (text[i] === '*') {
      const end = text.indexOf('*', i + 1)
      if (end !== -1 && end > i + 1) { if (buf) { tokens.push(buf); buf = '' }; tokens.push({ type: 'em', text: text.slice(i + 1, end) }); i = end + 1; continue }
    }
    buf += text[i]; i++
  }
  if (buf) tokens.push(buf)
  return tokens.map((t, idx) => {
    if (typeof t === 'string') return t
    if (t.type === 'bold') return <strong key={idx}>{t.text}</strong>
    if (t.type === 'em')   return <em key={idx}>{t.text}</em>
    if (t.type === 'code') return <code key={idx} className="md-code-inline">{t.text}</code>
    return null
  })
}

// ── Block markdown renderer ───────────────────────────────────────────────────
function SimpleMarkdown({ text }) {
  if (!text) return null
  const lines = text.split('\n')
  const out = []; let i = 0
  while (i < lines.length) {
    const line = lines[i]
    if (line.startsWith('```')) {
      const cl = []; i++
      while (i < lines.length && !lines[i].startsWith('```')) { cl.push(lines[i]); i++ }
      out.push(<pre key={i} className="md-code-block"><code>{cl.join('\n')}</code></pre>)
      i++; continue
    }
    if (line.trim().startsWith('|') && lines[i + 1]?.trim().match(/^\|?[\s\-:|]+\|/)) {
      const tl = []
      while (i < lines.length && lines[i].trim().startsWith('|')) { tl.push(lines[i]); i++ }
      const rows = tl
        .filter(l => !l.trim().match(/^\|?[\s\-:|]+\|/))
        .map(l => l.trim().replace(/^\||\|$/g, '').split('|').map(c => c.trim()))
      if (rows.length > 0) {
        const [hdr, ...body] = rows
        out.push(
          <table key={i} className="md-table">
            <thead><tr>{hdr.map((c, j) => <th key={j}>{parseInline(c)}</th>)}</tr></thead>
            <tbody>{body.map((r, ri) => <tr key={ri}>{r.map((c, ci) => <td key={ci}>{parseInline(c)}</td>)}</tr>)}</tbody>
          </table>
        )
      }
      continue
    }
    if (line.startsWith('### ')) { out.push(<h3 key={i} className="md-h3">{parseInline(line.slice(4))}</h3>); i++; continue }
    if (line.startsWith('## '))  { out.push(<h2 key={i} className="md-h2">{parseInline(line.slice(3))}</h2>); i++; continue }
    if (line.startsWith('# '))   { out.push(<h1 key={i} className="md-h1">{parseInline(line.slice(2))}</h1>); i++; continue }
    if (line.match(/^[-*] /)) {
      const items = []
      while (i < lines.length && lines[i].match(/^[-*] /)) { items.push(lines[i].slice(2)); i++ }
      out.push(<ul key={i} className="md-ul">{items.map((it, j) => <li key={j}>{parseInline(it)}</li>)}</ul>)
      continue
    }
    if (line.match(/^\d+\. /)) {
      const items = []
      while (i < lines.length && lines[i].match(/^\d+\. /)) { items.push(lines[i].replace(/^\d+\. /, '')); i++ }
      out.push(<ol key={i} className="md-ol">{items.map((it, j) => <li key={j}>{parseInline(it)}</li>)}</ol>)
      continue
    }
    if (!line.trim()) { out.push(<div key={i} className="md-spacer" />); i++; continue }
    out.push(<p key={i} className="md-p">{parseInline(line)}</p>)
    i++
  }
  return <div className="md-content">{out}</div>
}

// ── Chart visualization ───────────────────────────────────────────────────────
function ChartVisualization({ spec }) {
  const [view, setView] = useState('chart')
  const parsed = useMemo(() => {
    try {
      const s = typeof spec === 'string' ? JSON.parse(spec) : spec
      const values = s.data?.values || []
      if (!values.length) return null
      const mark = typeof s.mark === 'string' ? s.mark : (s.mark?.type || 'bar')
      const keys  = Object.keys(values[0] || {})
      const xKey  = keys.find(k =>
        typeof values[0][k] === 'string' ||
        k.toLowerCase().includes('name') || k.toLowerCase().includes('month') ||
        k.toLowerCase().includes('date') || k.toLowerCase().includes('category') ||
        k.toLowerCase().includes('label') || k.toLowerCase().includes('quarter')
      ) || keys[0]
      const yKeys = keys.filter(k => k !== xKey && (typeof values[0][k] === 'number' || !isNaN(Number(values[0][k]))))
      const data  = values.map((item, idx) => ({ ...item, name: item[xKey] ?? `Item ${idx + 1}` }))
      return { mark, data, xKey, yKeys, title: s.title || '' }
    } catch { return null }
  }, [spec])

  if (!parsed?.data?.length) return null
  const { mark, data, yKeys, title } = parsed
  const tt = { contentStyle: { background: '#fff', border: '1px solid #e5e7eb', borderRadius: 8, fontSize: 12 } }

  let chart
  if (mark === 'line') {
    chart = (
      <LineChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
        <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#6b7280' }} stroke="#e5e7eb" />
        <YAxis tick={{ fontSize: 11, fill: '#6b7280' }} stroke="#e5e7eb" />
        <Tooltip {...tt} /><Legend />
        {yKeys.map((k, i) => <Line key={k} type="monotone" dataKey={k} stroke={CHART_COLORS[i % CHART_COLORS.length]} strokeWidth={2} dot={{ r: 3 }} />)}
      </LineChart>
    )
  } else if (mark === 'arc' || mark === 'pie') {
    chart = (
      <PieChart>
        <Pie data={data} dataKey={yKeys[0] || 'value'} nameKey="name" cx="50%" cy="50%" outerRadius={72}
          label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`} labelLine={false}>
          {data.map((_, i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
        </Pie>
        <Tooltip />
      </PieChart>
    )
  } else {
    chart = (
      <BarChart data={data}>
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
        <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#6b7280' }} stroke="#e5e7eb" />
        <YAxis tick={{ fontSize: 11, fill: '#6b7280' }} stroke="#e5e7eb" />
        <Tooltip {...tt} /><Legend />
        {yKeys.map((k, i) => <Bar key={k} dataKey={k} fill={CHART_COLORS[i % CHART_COLORS.length]} radius={[4, 4, 0, 0]} />)}
      </BarChart>
    )
  }

  return (
    <div className="cv-wrap">
      <div className="cv-header">
        {title && <span className="cv-title">📊 {title}</span>}
        <div className="cv-tabs">
          <button className={`cv-tab${view === 'chart' ? ' active' : ''}`} onClick={() => setView('chart')}>Chart</button>
          <button className={`cv-tab${view === 'table' ? ' active' : ''}`} onClick={() => setView('table')}>Table</button>
        </div>
      </div>
      <div className="cv-body">
        {view === 'table' ? (
          <div className="cv-table-wrap">
            <table className="cv-table">
              <thead><tr>{Object.keys(data[0]).filter(k => k !== 'name').map(k => <th key={k}>{k}</th>)}</tr></thead>
              <tbody>
                {data.map((row, ri) => (
                  <tr key={ri}>
                    {Object.entries(row).filter(([k]) => k !== 'name').map(([k, v]) => (
                      <td key={k} style={{ textAlign: typeof v === 'number' ? 'right' : 'left' }}>
                        {typeof v === 'number' ? v.toLocaleString() : String(v ?? '')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={180}>{chart}</ResponsiveContainer>
        )}
      </div>
    </div>
  )
}

// ── Main Chatbot component ────────────────────────────────────────────────────
// Props:
//   open / onClose  – controlled mode (drawer triggered externally)
//   title           – panel header title
//   suggestions     – array of 4 {icon, label} or plain strings
export default function Chatbot({
  open: openProp,
  onClose,
  title       = 'RegTech AI Assistant',
  suggestions = [
    { icon: '📊', label: 'What drove the RWA spike in Q4 2025?' },
    { icon: '⚠', label: 'Any threshold breaches this quarter?' },
    { icon: '📋', label: 'Explain our CET1 and Tier 1 ratios' },
    { icon: '🔍', label: 'What needs immediate attention?' },
  ],
}) {
  const controlled = openProp !== undefined
  const [open,             setOpen]             = useState(false)
  const [messages,         setMessages]         = useState([])
  const [input,            setInput]            = useState('')
  const [loading,          setLoading]          = useState(false)
  const [expandedThinking, setExpandedThinking] = useState({})
  const [expandedSql,      setExpandedSql]      = useState({})
  const [copiedId,         setCopiedId]         = useState(null)
  const listRef = useRef(null)

  useEffect(() => {
    if (listRef.current) listRef.current.scrollTop = listRef.current.scrollHeight
  }, [messages, loading])

  const copySql = async (sql, id) => {
    await navigator.clipboard.writeText(sql)
    setCopiedId(id)
    setTimeout(() => setCopiedId(null), 2000)
  }

  const send = async (text) => {
    text = (text || input).trim()
    if (!text || loading) return
    setInput('')
    setLoading(true)

    const msgId  = Date.now().toString()
    const aId    = `${msgId}_a`
    const history = messages.map(m => ({ role: m.role, text: m.text }))

    const updateMsg = (patch) =>
      setMessages(m => m.map(msg => msg.id === aId ? { ...msg, ...patch } : msg))

    setMessages(m => [...m,
      { id: `${msgId}_u`, role: 'user', text },
      { id: aId, role: 'assistant', text: '', isStreaming: true,
        streamingStatus: 'Connecting…', thinkingSteps: [], thinkingContent: '', charts: [], sql: null },
    ])
    setExpandedThinking(e => ({ ...e, [aId]: true }))

    try {
      const res = await fetch('/api/chat', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ message: text, history }),
      })

      if (!res.ok || !res.body) {
        const err = await res.text().catch(() => 'Unknown error')
        updateMsg({ text: `Error: ${err}`, isStreaming: false })
        return
      }

      const reader  = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ''
      let accText = '', accThinking = '', accSql = ''
      let accSteps = [], accCharts = []

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const events = buffer.split('\n\n')
        buffer = events.pop() || ''

        for (const block of events) {
          if (!block.trim()) continue
          let etype = '', dstr = ''
          for (const line of block.split('\n')) {
            if (line.startsWith('event:'))     etype = line.slice(6).trim()
            else if (line.startsWith('data:')) dstr  = line.slice(5).trim()
          }
          if (!dstr || dstr === '{}') continue
          try {
            const data = JSON.parse(dstr)
            switch (etype) {
              case 'thinking':
                if (data.text) { accThinking += data.text; updateMsg({ thinkingContent: accThinking, streamingStatus: 'Analyzing…' }) }
                break
              case 'status': {
                const label = getStatusLabel(data.message || data.status || '')
                if (label && !accSteps.includes(label)) { accSteps = [...accSteps, label]; updateMsg({ thinkingSteps: accSteps, streamingStatus: label }) }
                break
              }
              case 'text':
                if (data.text) { accText += data.text; updateMsg({ text: accText, streamingStatus: undefined }) }
                break
              case 'sql':
                if (data.sql) {
                  accSql = data.sql
                  if (!accSteps.includes('Query executed')) accSteps = [...accSteps, 'Query executed']
                  updateMsg({ sql: accSql, thinkingSteps: accSteps })
                }
                break
              case 'chart':
                if (data.chart_spec) { accCharts = [...accCharts, { spec: data.chart_spec }]; updateMsg({ charts: accCharts }) }
                break
            }
          } catch { /* skip unparseable */ }
        }
      }

      updateMsg({ isStreaming: false, streamingStatus: undefined, text: accText || 'No response.',
        thinkingSteps: accSteps, thinkingContent: accThinking || undefined, sql: accSql || null, charts: accCharts })
    } catch {
      updateMsg({ text: 'Failed to reach the assistant.', isStreaming: false })
    } finally {
      setLoading(false)
    }
  }

  const isOpen   = controlled ? openProp : open
  const closePanel = () => { controlled ? onClose?.() : setOpen(false) }

  const getSuggestionLabel = (s) => (typeof s === 'string' ? s : s.label)

  return (
    <>
      {/* Standalone trigger button (non-controlled mode only) */}
      {!controlled && (
        <button className="chat-toggle" onClick={() => setOpen(o => !o)} aria-label="Toggle chat">
          💬
        </button>
      )}

      {/* Backdrop */}
      {isOpen && <div className="chat-backdrop" onClick={closePanel} />}

      <div className={`chat-panel${isOpen ? ' open' : ''}`}>
          <div className="chat-header">
            <div>
              <div className="chat-header-title">
                <span className="chat-header-dot" />
                {title}
              </div>
              <span className="chat-header-sub">Powered by Snowflake Cortex Agents</span>
            </div>
            <button className="chat-header-close" onClick={closePanel}>✕</button>
          </div>

          <div className="chat-messages" ref={listRef}>
            {messages.length === 0 && (
              <div className="chat-empty">
                <div className="chat-intro">
                  <p style={{ color: '#374151', marginBottom: 8, fontSize: 13 }}>
                    Hello! I'm your AI regulatory analyst — I have access to your balance sheet metrics, RWA anomaly flags, and audit findings. I can help you:
                  </p>
                  <ul className="chat-intro-list">
                    <li><strong>Analyze capital trends</strong> — spot deteriorating or improving metrics</li>
                    <li><strong>Identify threshold breaches</strong> — flag indicators at risk</li>
                    <li><strong>Compare reporting periods</strong> — quarter-over-quarter analysis</li>
                    <li><strong>Explain anomalies</strong> — RWA spikes, CET1 drops, LCR changes</li>
                  </ul>
                  <p style={{ color: '#6b7280', fontSize: 12, marginTop: 8 }}>What would you like to know?</p>
                </div>
                <div className="chat-suggestion-grid">
                  {suggestions.map((s, i) => {
                    const label = getSuggestionLabel(s)
                    const icon  = typeof s === 'string' ? '💬' : s.icon
                    return (
                      <button key={i} className="chat-suggestion-card" onClick={() => send(label)}>
                        <span className="chat-sug-icon">{icon}</span>
                        <span className="chat-sug-label">{label}</span>
                      </button>
                    )
                  })}
                </div>
              </div>
            )}

            {messages.map((m) => (
              <div key={m.id} className={`chat-msg ${m.role}`}>
                {m.role === 'user' ? (
                  <div className="chat-bubble user-bubble">{m.text}</div>
                ) : (
                  <div className="assistant-msg">
                    {(m.thinkingSteps?.length > 0 || m.isStreaming) && (
                      <div className="thinking-panel">
                        <button className="thinking-header" onClick={() => setExpandedThinking(e => ({ ...e, [m.id]: !e[m.id] }))}>
                          <span className={`thinking-icon${m.isStreaming && !m.text ? ' spin' : ''}`}>
                            {m.isStreaming && !m.text ? '⟳' : '🧠'}
                          </span>
                          <span className="thinking-label">
                            {m.isStreaming && !m.text
                              ? (m.streamingStatus || 'Thinking…')
                              : `Thinking & Planning${m.thinkingSteps?.length ? ` (${m.thinkingSteps.length} steps)` : ''}`}
                          </span>
                          <span className="thinking-chevron">{expandedThinking[m.id] ? '▲' : '▼'}</span>
                        </button>
                        {expandedThinking[m.id] && (
                          <div className="thinking-body">
                            {m.thinkingSteps?.map((step, i) => <div key={i} className="thinking-step">✓ {step}</div>)}
                            {m.isStreaming && m.streamingStatus && (
                              <div className="thinking-step thinking-current">⟳ {m.streamingStatus}</div>
                            )}
                          </div>
                        )}
                      </div>
                    )}

                    {m.sql && (
                      <div className="sql-panel">
                        <button className="sql-header" onClick={() => setExpandedSql(e => ({ ...e, [m.id]: !e[m.id] }))}>
                          <span>🗄 SQL executed</span>
                          <div className="sql-header-actions">
                            <button className="sql-copy" onClick={e => { e.stopPropagation(); copySql(m.sql, m.id) }}>
                              {copiedId === m.id ? '✓ Copied' : 'Copy'}
                            </button>
                            <span>{expandedSql[m.id] ? '▲' : '▼'}</span>
                          </div>
                        </button>
                        {expandedSql[m.id] && <pre className="sql-body">{m.sql}</pre>}
                      </div>
                    )}

                    {m.charts?.map((c, i) => <ChartVisualization key={i} spec={c.spec} />)}

                    {m.text ? (
                      <div className="chat-bubble assistant-bubble"><SimpleMarkdown text={m.text} /></div>
                    ) : m.isStreaming ? (
                      <div className="chat-bubble assistant-bubble chat-typing">Thinking…</div>
                    ) : null}
                  </div>
                )}
              </div>
            ))}
          </div>

          <div className="chat-input-row">
            <input
              className="chat-input"
              value={input}
              onChange={e => setInput(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && !e.shiftKey && (e.preventDefault(), send())}
              placeholder="Ask about capital ratios, variance, regulatory changes…"
              disabled={loading}
            />
            <button className="chat-send" onClick={() => send()} disabled={loading || !input.trim()}>➤</button>
          </div>
      </div>

      <style>{`
        /* ── Standalone trigger (non-controlled mode) ── */
        .chat-toggle { position: fixed; bottom: 24px; right: 24px; width: 52px; height: 52px; border-radius: 50%; background: var(--navy); color: white; border: none; font-size: 22px; cursor: pointer; box-shadow: 0 4px 12px rgba(0,0,0,0.2); z-index: 1100; display: flex; align-items: center; justify-content: center; }
        .chat-toggle:hover { opacity: 0.88; }

        /* ── Backdrop ── */
        .chat-backdrop { position: fixed; inset: 0; background: rgba(0,0,0,0.28); z-index: 1099; animation: bd-in 0.2s ease; }
        @keyframes bd-in { from { opacity: 0; } to { opacity: 1; } }

        /* ── Full-height drawer panel ── */
        .chat-panel {
          position: fixed; top: 0; right: 0; height: 100vh; width: 440px;
          background: white; box-shadow: -4px 0 32px rgba(0,0,0,0.18);
          display: flex; flex-direction: column; z-index: 1100;
          transform: translateX(100%); transition: transform 0.25s cubic-bezier(0.4,0,0.2,1);
          overflow: hidden;
        }
        .chat-panel.open { transform: translateX(0); }

        .chat-header { background: var(--navy); color: white; padding: 14px 18px; display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }
        .chat-header-icon { font-size: 18px; }
        .chat-header > div > div { font-size: 14px; font-weight: 600; }
        .chat-header-sub { display: block; font-size: 10px; opacity: 0.6; font-weight: 400; margin-top: 1px; }
        .chat-header-close { background: none; border: none; color: white; font-size: 18px; cursor: pointer; opacity: 0.7; padding: 0; line-height: 1; }
        .chat-header-close:hover { opacity: 1; }

        .chat-messages { flex: 1; overflow-y: auto; padding: 16px; display: flex; flex-direction: column; gap: 10px; }
        .chat-empty { display: flex; flex-direction: column; gap: 14px; }

        /* Intro block */
        .chat-intro { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 14px 16px; }
        .chat-intro-greeting { font-size: 13px; color: #374151; margin: 0 0 10px; line-height: 1.5; }
        .chat-intro-list { margin: 0 0 10px 16px; padding: 0; font-size: 12px; color: #4b5563; line-height: 1.7; }
        .chat-intro-list li { margin-bottom: 2px; }
        .chat-intro-prompt { font-size: 12px; color: #6b7280; margin: 8px 0 0; border-top: 1px solid #e5e7eb; padding-top: 8px; }

        /* 2×2 suggestion grid */
        .chat-suggestion-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
        .chat-suggestion-card { display: flex; flex-direction: column; align-items: flex-start; gap: 6px; background: white; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; font-size: 12px; color: #374151; cursor: pointer; text-align: left; transition: border-color 0.12s, box-shadow 0.12s; }
        .chat-suggestion-card:hover { border-color: var(--navy); box-shadow: 0 0 0 2px rgba(0,51,102,0.08); }
        .chat-sug-icon { font-size: 18px; line-height: 1; }
        .chat-sug-label { font-size: 11px; color: #374151; line-height: 1.4; }

        /* Messages */
        .chat-msg { display: flex; flex-direction: column; }
        .chat-msg.user { align-items: flex-end; }
        .chat-msg.assistant { align-items: flex-start; }
        .chat-bubble { padding: 9px 13px; border-radius: 10px; font-size: 13px; line-height: 1.5; max-width: 95%; }
        .user-bubble { background: var(--navy); color: white; border-radius: 10px 10px 2px 10px; }
        .assistant-bubble { background: #f3f4f6; color: #111827; border-radius: 2px 10px 10px 10px; }
        .assistant-msg { width: 100%; display: flex; flex-direction: column; gap: 6px; }
        .chat-typing { color: #9ca3af; font-style: italic; }

        /* Thinking / SQL panels */
        .thinking-panel { background: #fafafa; border: 1px solid #e5e7eb; border-radius: 7px; overflow: hidden; }
        .thinking-header { width: 100%; display: flex; align-items: center; gap: 8px; padding: 7px 10px; background: none; border: none; cursor: pointer; font-size: 12px; color: #6b7280; }
        .thinking-icon { font-size: 14px; }
        .thinking-icon.spin { animation: ch-spin 1s linear infinite; display: inline-block; }
        @keyframes ch-spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
        .thinking-label { flex: 1; text-align: left; font-weight: 500; }
        .thinking-chevron { font-size: 10px; }
        .thinking-body { padding: 6px 10px 8px; display: flex; flex-direction: column; gap: 3px; }
        .thinking-step { font-size: 11px; color: #6b7280; padding: 1px 0; }
        .thinking-current { color: #2563eb; }
        .sql-panel { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 7px; overflow: hidden; }
        .sql-header { width: 100%; display: flex; align-items: center; justify-content: space-between; padding: 7px 10px; background: none; border: none; cursor: pointer; font-size: 12px; color: #475569; font-weight: 500; }
        .sql-header-actions { display: flex; align-items: center; gap: 8px; }
        .sql-copy { background: none; border: 1px solid #cbd5e1; border-radius: 4px; padding: 2px 8px; font-size: 11px; color: #475569; cursor: pointer; }
        .sql-copy:hover { background: #f1f5f9; }
        .sql-body { margin: 0; padding: 8px 10px; font-family: 'SF Mono', Menlo, monospace; font-size: 11px; color: #1e293b; white-space: pre-wrap; word-break: break-word; border-top: 1px solid #e2e8f0; max-height: 180px; overflow-y: auto; }

        /* Chart */
        .cv-wrap { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden; }
        .cv-header { display: flex; align-items: center; justify-content: space-between; padding: 8px 12px; border-bottom: 1px solid #e5e7eb; }
        .cv-title { font-size: 12px; font-weight: 600; color: #374151; }
        .cv-tabs { display: flex; gap: 4px; }
        .cv-tab { padding: 3px 10px; border-radius: 4px; border: 1px solid #e5e7eb; background: white; font-size: 11px; font-weight: 500; color: #6b7280; cursor: pointer; }
        .cv-tab.active { background: var(--navy); color: white; border-color: var(--navy); }
        .cv-body { padding: 8px 12px; }
        .cv-table-wrap { overflow-x: auto; }
        .cv-table { width: 100%; border-collapse: collapse; font-size: 11px; }
        .cv-table th { background: #f3f4f6; padding: 5px 8px; text-align: left; font-size: 10px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: #6b7280; border-bottom: 1px solid #e5e7eb; }
        .cv-table td { padding: 5px 8px; border-bottom: 1px solid #f3f4f6; color: #374151; }

        /* Input bar */
        .chat-input-row { display: flex; gap: 6px; padding: 12px 16px; border-top: 1px solid #e5e7eb; flex-shrink: 0; background: white; }
        .chat-input { flex: 1; padding: 8px 12px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 13px; outline: none; }
        .chat-input:focus { border-color: var(--navy); box-shadow: 0 0 0 2px rgba(0,51,102,0.1); }
        .chat-send { padding: 8px 14px; background: var(--navy); color: white; border: none; border-radius: 6px; font-size: 14px; cursor: pointer; }
        .chat-send:hover { opacity: 0.88; }
        .chat-send:disabled { opacity: 0.4; cursor: not-allowed; }

        /* Markdown */
        .md-content { font-size: 13px; line-height: 1.6; }
        .md-p { margin: 0 0 6px; }
        .md-h1, .md-h2, .md-h3 { margin: 8px 0 4px; font-weight: 700; }
        .md-h1 { font-size: 15px; } .md-h2 { font-size: 14px; } .md-h3 { font-size: 13px; }
        .md-ul, .md-ol { margin: 4px 0 6px 16px; padding: 0; }
        .md-ul li, .md-ol li { margin-bottom: 2px; }
        .md-code-inline { background: #f1f5f9; color: #0f172a; padding: 1px 5px; border-radius: 4px; font-family: 'SF Mono', Menlo, monospace; font-size: 12px; }
        .md-code-block { background: #1e293b; color: #e2e8f0; padding: 10px 12px; border-radius: 6px; font-family: 'SF Mono', Menlo, monospace; font-size: 11px; overflow-x: auto; white-space: pre-wrap; margin: 4px 0; }
        .md-spacer { height: 4px; }
        .md-table { border-collapse: collapse; font-size: 11px; margin: 6px 0; width: 100%; }
        .md-table th { background: #f3f4f6; padding: 5px 8px; text-align: left; font-weight: 600; border: 1px solid #e5e7eb; }
        .md-table td { padding: 5px 8px; border: 1px solid #e5e7eb; }

        @media (max-width: 500px) { .chat-panel { width: 100vw; } }
      `}</style>
    </>
  )
}
