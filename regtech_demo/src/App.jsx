import { useState } from 'react'
import DocumentIntelligence from './tabs/DocumentIntelligence.jsx'
import BalanceSheetAnalytics from './tabs/BalanceSheetAnalytics.jsx'
import PipelineAudit from './tabs/PipelineAudit.jsx'
import './App.css'

const TABS = [
  { id: 'documents',  label: 'Document Intelligence',   icon: '📄' },
  { id: 'analytics',  label: 'Balance Sheet Analytics', icon: '📊' },
  { id: 'audit',      label: 'Pipeline Audit',          icon: '🔍' },
]

export default function App() {
  const [activeTab, setActiveTab] = useState('documents')

  return (
    <div className="rt-app">

      {/* ── Header ── */}
      <header className="rt-header">
        <div className="rt-header-left">
          <div className="rt-logo">
            <span className="rt-logo-mark">RT</span>
            <div className="rt-logo-text">
              <span className="rt-logo-main">RegTech AI Platform</span>
              <span className="rt-logo-sub">Powered by Snowflake Cortex</span>
            </div>
          </div>
        </div>
        <div className="rt-header-right">
          <span className="rt-env-badge">REGULATORY REPORTING</span>
          <div className="rt-avatar">KW</div>
        </div>
      </header>

      {/* ── Tab Bar ── */}
      <nav className="rt-tabbar">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            className={`rt-tab${activeTab === tab.id ? ' active' : ''}`}
            onClick={() => setActiveTab(tab.id)}
          >
            <span className="rt-tab-icon">{tab.icon}</span>
            {tab.label}
          </button>
        ))}
      </nav>

      {/* ── Content ── */}
      <main className="rt-content">
        {activeTab === 'documents' && <DocumentIntelligence />}
        {activeTab === 'analytics' && <BalanceSheetAnalytics />}
        {activeTab === 'audit'     && <PipelineAudit />}
      </main>

    </div>
  )
}
