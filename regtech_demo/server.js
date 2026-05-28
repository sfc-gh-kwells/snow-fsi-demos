import express from 'express';

// Run with: node --env-file=.env server.js
// Required .env vars:
//   SNOWFLAKE_ACCOUNT   = e.g. myorg-myaccount
//   SNOWFLAKE_USER      = your username
//   SNOWFLAKE_PAT       = programmatic access token (Snowsight > Profile > PATs)
//   SNOWFLAKE_WAREHOUSE = COMPUTE_WH

const app  = express();
const PORT = 3002;
app.use(express.json());

const DB     = 'REGTECH_DEMO_DB';
const SCHEMA = 'REGULATORY_REPORTING';

// ── Snowflake SQL REST API (PAT bearer auth — same as Cortex endpoints) ────────
const sfConnected = !!process.env.SNOWFLAKE_PAT;

async function sfQuery(sql) {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const pat     = process.env.SNOWFLAKE_PAT;
  const url     = `https://${account}.snowflakecomputing.com/api/v2/statements`;

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type':  'application/json',
      'Accept':        'application/json',
      'Authorization': `Bearer ${pat}`,
      'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
    },
    body: JSON.stringify({
      statement: sql,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'COMPUTE_WH',
      database:  DB,
      schema:    SCHEMA,
      timeout:   60,
    }),
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`SQL API ${res.status}: ${errText}`);
  }

  const data = await res.json();
  // Convert columnar array format → array of row objects
  const cols = (data.resultSetMetaData?.rowType || []).map((c) => c.name);
  return (data.data || []).map((row) => {
    const obj = {};
    cols.forEach((col, i) => { obj[col] = row[i]; });
    return obj;
  });
}

// ── Mock data (used when Snowflake unavailable) ────────────────────────────────
const MOCK_DOCUMENTS = {
  documents: [
    { id: 'DOC-B3-001', name: 'Basel III: Capital Adequacy Framework', version: '3.1', effectiveDate: '2019-01-01', pageCount: 84, framework: 'Basel III', status: 'Active',
      summary: 'Establishes minimum CET1 (4.5%), Tier 1 (6%), and total capital (8%) requirements. Introduces capital conservation buffer of 2.5% and G-SIB surcharges.' },
    { id: 'DOC-B3-002', name: 'Basel III: Liquidity Coverage Ratio', version: '2.0', effectiveDate: '2015-01-01', pageCount: 72, framework: 'Basel III', status: 'Active',
      summary: 'Requires banks to hold HQLA ≥ total net cash outflows over 30-day stress scenario. Level 2B assets (RMBS, corporates) capped at 15% of HQLA buffer with 25-50% haircuts.' },
    { id: 'DOC-B3-003', name: 'Basel III: Leverage Ratio Framework', version: '1.5', effectiveDate: '2018-01-01', pageCount: 48, framework: 'Basel III', status: 'Active',
      summary: 'Non-risk-based backstop: Tier 1 ≥ 3% of total exposure. G-SIB leverage buffer = 50% of risk-weighted surcharge. Written credit derivatives included at full notional.' },
    { id: 'DOC-B4-001', name: 'Basel IV: FRTB – Market Risk Capital', version: '1.0', effectiveDate: '2025-01-01', pageCount: 156, framework: 'Basel IV', status: 'Pending',
      summary: 'Replaces VaR with Expected Shortfall (ES) at 97.5% over 10-day liquidity horizon. Desk-level PLA tests required for IMA approval. SA revised with SBM, RRAO, and DRC.' },
    { id: 'DOC-B4-002', name: 'Basel IV: SA-CCR – Counterparty Credit Risk', version: '1.0', effectiveDate: '2025-01-01', pageCount: 98, framework: 'Basel IV', status: 'Pending',
      summary: 'SA-CCR replaces CEM and SM for OTC derivatives EAD. EAD = 1.4 × (RC + PFE_agg). Five asset class add-ons with netting recognition. All SA-credit banks required from Jan 2025.' },
    { id: 'DOC-B4-003', name: 'Basel IV: Output Floor & Internal Models', version: '1.0', effectiveDate: '2025-01-01', pageCount: 62, framework: 'Basel IV', status: 'Pending',
      summary: 'Output floor requires capital ≥ 72.5% of standardised RWA. Phased from 50% (2025) to 72.5% (2030). LTV-based mortgage risk weights (20-70%) replace flat 35%.' },
    { id: 'DOC-B4-004', name: 'Basel IV: Operational Risk – SMA', version: '1.0', effectiveDate: '2025-01-01', pageCount: 54, framework: 'Basel IV', status: 'Pending',
      summary: 'SMA replaces AMA, BIA, and TSA. Capital = BIC × ILM. Requires 10 years of internal loss data. BIC marginal coefficients: 12%, 15%, 18% by BI bucket.' },
  ],
  requirements: [
    { id: 'REQ-001', docId: 'DOC-B4-001', docVersion: '1.0', ruleName: 'FRTB – Expected Shortfall', ruleSection: 'MAR33.1', oldRequirement: 'VaR at 99th percentile, 10-day horizon', newRequirement: 'Expected Shortfall at 97.5th percentile over liquidity-adjusted horizon. Per-desk PLA test and backtesting required for IMA.', impactedReport: 'Market Risk RWA', severity: 'Critical', changeType: 'Replacement' },
    { id: 'REQ-002', docId: 'DOC-B4-002', docVersion: '1.0', ruleName: 'SA-CCR for Derivatives', ruleSection: 'CRE52', oldRequirement: 'Current Exposure Method (CEM): RC + notional × add-on factor', newRequirement: 'SA-CCR: EAD = 1.4 × (RC + PFE_agg). Replacement cost and aggregated PFE add-on by asset class with netting recognition.', impactedReport: 'Counterparty Credit Risk', severity: 'Critical', changeType: 'Replacement' },
    { id: 'REQ-003', docId: 'DOC-B4-003', docVersion: '1.0', ruleName: 'Output Floor – 72.5%', ruleSection: 'CRE20', oldRequirement: 'Internal model RWA reported without floor constraint', newRequirement: 'Capital ≥ 72.5% of standardised approach RWA. Parallel SA-RWA calculation required. Phase-in: 50% (2025) → 72.5% (2030).', impactedReport: 'Credit Risk RWA', severity: 'High', changeType: 'New Requirement' },
    { id: 'REQ-004', docId: 'DOC-B3-001', docVersion: '3.1', ruleName: 'DTA Dual Threshold', ruleSection: 'CAP30', oldRequirement: 'DTAs deducted above 10% CET1 individual threshold', newRequirement: 'Combined dual threshold: 10% individual for DTA, 17.65% combined for DTA + significant investments in unconsolidated financials.', impactedReport: 'CET1 Capital', severity: 'High', changeType: 'Updated' },
    { id: 'REQ-005', docId: 'DOC-B3-002', docVersion: '2.0', ruleName: 'Level 2B HQLA Haircuts', ruleSection: 'LCR40', oldRequirement: 'Investment-grade RMBS classified as Level 2A (15% haircut)', newRequirement: 'RMBS classified as Level 2B: 25-50% haircuts, capped at 15% of total HQLA. Eligibility criteria: credit rating, LTV ≤ 80%, issue size ≥ 500M.', impactedReport: 'LCR HQLA Buffer', severity: 'High', changeType: 'Updated' },
    { id: 'REQ-006', docId: 'DOC-B4-004', docVersion: '1.0', ruleName: 'Op Risk – SMA', ruleSection: 'OPE10', oldRequirement: 'Basic Indicator Approach: 15% × 3yr average gross income', newRequirement: 'Standardised Measurement Approach: Capital = BIC × ILM. Requires 10 years internal loss data. ILM defaults to 1.0 without sufficient history.', impactedReport: 'Operational Risk Capital', severity: 'Medium', changeType: 'Replacement' },
    { id: 'REQ-007', docId: 'DOC-B3-003', docVersion: '1.5', ruleName: 'Leverage – Written CDS', ruleSection: 'LEV30', oldRequirement: 'Written credit derivatives excluded from gross notional add-back', newRequirement: 'Written CDS included at full notional minus purchased CDS offsets. Off-BS items at 10% CCF minimum. SA-CCR used for derivative exposure in leverage ratio.', impactedReport: 'Leverage Ratio', severity: 'Medium', changeType: 'Updated' },
    { id: 'REQ-008', docId: 'DOC-B4-003', docVersion: '1.0', ruleName: 'Mortgage LTV Risk Weights', ruleSection: 'CRE20', oldRequirement: 'Flat 35% risk weight for all performing residential mortgages', newRequirement: 'LTV-based risk weights: ≤50% → 20%, 50-60% → 25%, 60-70% → 30%, 70-80% → 35%, 80-90% → 40%, 90-100% → 50%, >100% → 70%.', impactedReport: 'Mortgage Credit RWA', severity: 'Medium', changeType: 'Updated' },
  ],
};

const MOCK_BALANCE_SHEET = (() => {
  const quarters = ['Q1 2024','Q2 2024','Q3 2024','Q4 2024','Q1 2025','Q2 2025','Q3 2025','Q4 2025'];

  // Real Morgan Stanley figures sourced from 10-K/10-Q/8-K SEC filings (CIK 0000895421)
  // CET1 consolidated ratios confirmed via Cortex COMPLETE extraction of actual SEC text:
  //   Q1 2024=15.1%, Q2 2024=15.2%, Q3 2024=15.1%, Q4 2024=15.0%
  //   Q1 2025=15.3% ("accreted $1.9B, ended quarter at 15.3%")
  //   Q2 2025=15.0% ("Standardized CET1 capital ratio was 15.0%")
  //   Q3 2025=15.2% ("Standardized CET1 capital ratio was 15.2%")
  //   Q4 2025=15.0%, Tier1=16.8% (confirmed from 8-K 0000895421-26-000007)
  // Total consolidated RWA: Q3 2025=$539.3B, Q4 2025=$553.4B (confirmed)
  // Business line RWA: IS ~51%, WM ~34% (anchored to WM loans × 1.05), IM ~15%
  // WM loans confirmed: Q1'24=$147.4B, Q3'24=$155.2B, Q4'24=$159.5B,
  //   Q1'25=$162.5B, Q2'25=$168.9B, Q3'25=$173.9B (from earnings supplements)
  const rawMetrics = [
    // Institutional Securities (IS = highest RWA density; CET1 tracks consolidated)
    {q:'Q1 2024',bl:'Institutional Securities',c:15.1,t:16.8,r:231.9,l:124.3,n:128.1},
    {q:'Q2 2024',bl:'Institutional Securities',c:15.2,t:16.9,r:234.3,l:124.8,n:128.5},
    {q:'Q3 2024',bl:'Institutional Securities',c:15.1,t:16.8,r:234.0,l:125.3,n:129.1},
    {q:'Q4 2024',bl:'Institutional Securities',c:15.0,t:16.7,r:237.9,l:125.9,n:129.7},
    {q:'Q1 2025',bl:'Institutional Securities',c:15.3,t:17.0,r:250.1,l:126.5,n:130.2},
    {q:'Q2 2025',bl:'Institutional Securities',c:15.0,t:16.7,r:263.0,l:125.1,n:129.4},
    {q:'Q3 2025',bl:'Institutional Securities',c:15.2,t:16.9,r:275.8,l:125.7,n:129.9},
    {q:'Q4 2025',bl:'Institutional Securities',c:15.0,t:16.8,r:282.5,l:124.2,n:128.8},
    // Wealth Management (WM loans anchor RWA; CET1 = consolidated + 0.3%)
    {q:'Q1 2024',bl:'Wealth Management',c:15.4,t:16.9,r:154.8,l:153.7,n:134.2},
    {q:'Q2 2024',bl:'Wealth Management',c:15.5,t:17.0,r:158.4,l:154.2,n:134.8},
    {q:'Q3 2024',bl:'Wealth Management',c:15.4,t:16.9,r:162.9,l:154.8,n:135.3},
    {q:'Q4 2024',bl:'Wealth Management',c:15.3,t:16.8,r:167.5,l:155.4,n:135.9},
    {q:'Q1 2025',bl:'Wealth Management',c:15.6,t:17.1,r:170.6,l:156.0,n:136.4},
    {q:'Q2 2025',bl:'Wealth Management',c:15.3,t:16.8,r:177.3,l:154.6,n:135.1},
    {q:'Q3 2025',bl:'Wealth Management',c:15.5,t:17.0,r:182.6,l:155.2,n:135.7},
    {q:'Q4 2025',bl:'Wealth Management',c:15.3,t:16.8,r:187.9,l:153.8,n:134.5},
    // Investment Management (~15% of total RWA; CET1 = consolidated + 0.7%)
    {q:'Q1 2024',bl:'Investment Management',c:15.8,t:17.3,r:68.3,l:159.4,n:139.7},
    {q:'Q2 2024',bl:'Investment Management',c:15.9,t:17.4,r:69.3,l:160.1,n:140.3},
    {q:'Q3 2024',bl:'Investment Management',c:15.8,t:17.3,r:70.1,l:160.6,n:140.9},
    {q:'Q4 2024',bl:'Investment Management',c:15.7,t:17.2,r:71.6,l:161.2,n:141.6},
    {q:'Q1 2025',bl:'Investment Management',c:16.0,t:17.5,r:74.3,l:161.7,n:142.1},
    {q:'Q2 2025',bl:'Investment Management',c:15.7,t:17.2,r:77.7,l:160.3,n:140.8},
    {q:'Q3 2025',bl:'Investment Management',c:15.9,t:17.4,r:80.9,l:161.0,n:141.4},
    {q:'Q4 2025',bl:'Investment Management',c:15.7,t:17.2,r:83.0,l:159.6,n:140.2},
  ];

  // Calibrated so computed revenue ≈ actual SEC-reported net revenues for Q4 2025:
  //   IS: 282.5B × 28 ≈ $7,910M  (actual Q4 2025: $7,931M)
  //   WM: 187.9B × 45 ≈ $8,456M  (actual Q4 2025: $8,429M)
  //   IM:  83.0B × 20 ≈ $1,660M  (actual Q4 2025: $1,720M)
  const REV_MULT = { 'Institutional Securities': 28, 'Wealth Management': 45, 'Investment Management': 20 };

  const metrics = rawMetrics.map(m => ({
    quarter: m.q, businessLine: m.bl,
    cet1Ratio: m.c, tier1Ratio: m.t, rwa: m.r, lcr: m.l, nsfr: m.n,
    netRevenue: +(m.r * REV_MULT[m.bl]).toFixed(0),
  }));

  const anomalies = [
    {
      id: 'AF-IS-Q42025', metric: 'RWA', businessLine: 'Institutional Securities', quarter: 'Q4 2025',
      value: 282.5, expectedLow: 257.9, expectedHigh: 293.7,
      pctChange: 2.4, severity: 'Watch',
      description: 'IS CET1 at 8-quarter low (15.0%) as RWA grows +2.4% QoQ to $282.5B.',
      descriptionFull: 'Institutional Securities RWA increased $6.7B (+2.4% QoQ) to $282.5B in Q4 2025, consistent with seasonal balance-sheet expansion in trading and market-making activities (IS net revenues $7.9B in Q4 2025, per earnings release). While the absolute RWA increase is within normal operational bounds, consolidated CET1 compressed 20bps QoQ to 15.0% — the lowest reading in the 8-quarter observation window — driven by a combination of higher RWA denominator and $1.4B in common dividend payments during the quarter. CET1 of 15.0% remains well above the 4.5% regulatory minimum and the Firm\'s estimated internal target floor (~13.5%). Flagged as a watch item: continued RWA growth in Q1 2026 without commensurate earnings retention could further compress CET1 toward management thresholds. Full-year 2025 consolidated RWA growth was +15.7% ($478B → $553.4B), reflecting increased Equity and Fixed Income business volumes.',
    },
  ];

  const variance = [
    { businessLine: 'Institutional Securities', metricName: 'RWA_TOTAL_B',  priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 275.8, currentValue: 282.5, changeAbs: 6.7,   changePct: 2.4,   driver: 'Seasonal trading book growth; IS Q4 2025 net revenues $7.9B (+31% YoY)' },
    { businessLine: 'Institutional Securities', metricName: 'CET1_RATIO',   priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 15.2,  currentValue: 15.0,  changeAbs: -0.2,  changePct: -1.3,  driver: 'RWA growth + $1.4B common dividend — CET1 at 8-quarter low' },
    { businessLine: 'Institutional Securities', metricName: 'LCR_RATIO',    priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 125.7, currentValue: 124.2, changeAbs: -1.5,  changePct: -1.2,  driver: 'Modest increase in short-term funding requirements; below 125% internal buffer' },
    { businessLine: 'Wealth Management',        metricName: 'RWA_TOTAL_B',  priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 182.6, currentValue: 187.9, changeAbs: 5.3,   changePct: 2.9,   driver: 'Continued loan portfolio growth — WM loans reached ~$179B, net new assets $57B' },
    { businessLine: 'Wealth Management',        metricName: 'CET1_RATIO',   priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 15.5,  currentValue: 15.3,  changeAbs: -0.2,  changePct: -1.3,  driver: 'RWA-driven — retained earnings stable; WM pre-tax margin 26.7%' },
    { businessLine: 'Investment Management',    metricName: 'RWA_TOTAL_B',  priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 80.9,  currentValue: 83.0,  changeAbs: 2.1,   changePct: 2.6,   driver: 'Credit exposure growth on $1,807B AUM base; IM net revenues $1.7B' },
    { businessLine: 'Investment Management',    metricName: 'CET1_RATIO',   priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 15.9,  currentValue: 15.7,  changeAbs: -0.2,  changePct: -1.3,  driver: 'Aligned with firm-wide CET1 compression; IM AUM long-term flows +$16.5B' },
  ];

  // Consolidated firm-wide ratios from 10-K/8-K SEC filings
  // These override business-line averages in KPI cards; segment breakdowns still use rawMetrics
  // Q4 2025: CET1=15.0%, Tier1=16.8%, LCR=134%, RWA=$552.5B (10-K Dec 31 2025)
  // Q3 2025: CET1=15.2%, LCR=129%, NSFR=120%, RWA=$539.3B (10-Q Sep 30 2025)
  const consolidated = {
    'Q4 2025': { cet1: 15.0, tier1: 16.8, lcr: 134, totalRwa: 552.5 },
    'Q3 2025': { cet1: 15.2, tier1: 17.2, lcr: 129, totalRwa: 539.3 },
  };

  return { metrics, anomalies, variance, consolidated };
})();

const MOCK_DQ_RESULTS = {
  asOf: 'Q4 2025',
  // Powered by 14 native Snowflake Data Metric Functions (6 system + 8 custom)
  // attached to BALANCE_SHEET_METRICS with TRIGGER_ON_CHANGES schedule
  summary: { pass: 12, warn: 2, fail: 0 },
  checks: [
    {
      id: 'DQ-001', category: 'Completeness', name: 'CET1 Ratio — No Nulls',
      metric: 'CET1_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: 'NOT NULL',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'SNOWFLAKE.CORE.NULL_COUNT',
    },
    {
      id: 'DQ-002', category: 'Completeness', name: 'RWA Total — No Nulls',
      metric: 'RWA_TOTAL_B', scope: 'All rows',
      status: 'pass', value: null, threshold: 'NOT NULL',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'SNOWFLAKE.CORE.NULL_COUNT',
    },
    {
      id: 'DQ-003', category: 'Completeness', name: 'LCR Ratio — No Nulls',
      metric: 'LCR_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: 'NOT NULL',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'SNOWFLAKE.CORE.NULL_COUNT',
    },
    {
      id: 'DQ-004', category: 'Completeness', name: 'Tier 1 Ratio — No Nulls',
      metric: 'TIER1_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: 'NOT NULL',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'SNOWFLAKE.CORE.NULL_COUNT',
    },
    {
      id: 'DQ-005', category: 'Completeness', name: 'NSFR Ratio — No Nulls',
      metric: 'NSFR_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: 'NOT NULL',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'SNOWFLAKE.CORE.NULL_COUNT',
    },
    {
      id: 'DQ-006', category: 'Uniqueness', name: 'Metric ID — No Duplicates',
      metric: 'METRIC_ID', scope: 'All rows',
      status: 'pass', value: null, threshold: 'UNIQUE',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'SNOWFLAKE.CORE.DUPLICATE_COUNT',
    },
    {
      id: 'DQ-007', category: 'Regulatory Floor', name: 'CET1 ≥ 4.5% (Basel III Minimum)',
      metric: 'CET1_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: '≥ 4.5% (12 CFR § 3.10)',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'REGTECH_DEMO_DB.REGULATORY_REPORTING.CET1_BELOW_REG_FLOOR',
    },
    {
      id: 'DQ-008', category: 'Regulatory Floor', name: 'CET1 ≥ 13.5% (Internal Target)',
      metric: 'CET1_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: '≥ 13.5% internal management target',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'REGTECH_DEMO_DB.REGULATORY_REPORTING.CET1_BELOW_INTERNAL_TARGET',
    },
    {
      id: 'DQ-009', category: 'Regulatory Floor', name: 'LCR ≥ 100% (12 CFR Part 249)',
      metric: 'LCR_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: '≥ 100% regulatory minimum',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'REGTECH_DEMO_DB.REGULATORY_REPORTING.LCR_BELOW_REG_FLOOR',
    },
    {
      id: 'DQ-010', category: 'Regulatory Floor', name: 'LCR ≥ 125% (Internal Buffer)',
      metric: 'LCR_RATIO', scope: 'Q1 2024 IS · Q2 2024 IS · Q4 2025 IS',
      status: 'warn', value: '3 quarter(s) below 125%',
      threshold: '≥ 125% internal liquidity buffer target',
      quarter: 'Q4 2025', businessLine: 'Institutional Securities',
      rootCause: 'IS LCR fell below the 125% internal buffer in Q1 2024 (124.3%), Q2 2024 (124.8%), and Q4 2025 (124.2%), reflecting elevated short-term wholesale funding requirements during periods of heightened trading book activity. The 100% regulatory floor is not breached in any period. Recommend monitoring HQLA Level 1 composition and intraday liquidity usage heading into Q1 2026 balance-sheet growth.',
      dmf: 'REGTECH_DEMO_DB.REGULATORY_REPORTING.LCR_BELOW_INTERNAL_BUFFER',
    },
    {
      id: 'DQ-011', category: 'Structural Consistency', name: 'Tier 1 ≥ CET1 (AT1 ≥ 0)',
      metric: 'TIER1_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: 'TIER1 ≥ CET1 always (AT1 capital cannot be negative)',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'REGTECH_DEMO_DB.REGULATORY_REPORTING.TIER1_BELOW_CET1_COUNT',
    },
    {
      id: 'DQ-012', category: 'Regulatory Floor', name: 'NSFR ≥ 100% (Regulatory Minimum)',
      metric: 'NSFR_RATIO', scope: 'All rows',
      status: 'pass', value: null, threshold: '≥ 100% (Basel III NSFR)',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'REGTECH_DEMO_DB.REGULATORY_REPORTING.NSFR_BELOW_REG_FLOOR',
    },
    {
      id: 'DQ-013', category: 'Range Check', name: 'RWA QoQ Change ≤ 20%',
      metric: 'RWA_TOTAL_B', scope: 'All rows',
      status: 'pass', value: null, threshold: '≤ 20% QoQ change per business line',
      quarter: null, businessLine: null, rootCause: null,
      dmf: 'REGTECH_DEMO_DB.REGULATORY_REPORTING.RWA_QOQ_VIOLATION_COUNT',
    },
    {
      id: 'DQ-014', category: 'Trend Analysis', name: 'CET1 Max QoQ Decline (30 bps)',
      metric: 'CET1_RATIO', scope: '8-quarter observation window',
      status: 'warn', value: '30 bps max single-quarter decline',
      threshold: '≤ 25 bps single-quarter decline',
      quarter: 'Q2 2025', businessLine: 'Institutional Securities',
      rootCause: 'IS CET1 declined 30 bps in Q1→Q2 2025 (15.3% → 15.0%), the steepest drop in the 8-quarter window and matching the Q4 2025 period-low of 15.0%. Per Q2 2025 earnings: dividend increase to $1.00/quarter declared during the period, combined with IS RWA growing $12.9B (+5.2%) on record Equity revenues of $3.7B. Management-driven capital distribution, not balance-sheet stress. No regulatory threshold breached.',
      dmf: 'REGTECH_DEMO_DB.REGULATORY_REPORTING.CET1_MAX_QOQ_DECLINE_BPS',
    },
  ],
};

const MOCK_CROSS_REPORT_RESULTS = {
  lastRun: 'Apr 29, 2026 14:30',
  quarter: 'Q4 2025',
  summary: { pass: 8, fail: 1, warn: 2, total: 11 },
  rules: [
    {
      id: 'XR-001', status: 'fail', severity: 'High',
      description: 'FR Y-9C HC-R Total RWA ↔ FRTB Market Risk Capital Report',
      report1: 'FR Y-9C (HC-R)', report2: 'FRTB Market Risk Report',
      mdrm1: 'BHCK3545', mdrm2: 'MR-RWA-TOTAL',
      businessLine: 'Institutional Securities',
      value1: '$284.7B', value2: '$282.5B', delta: '+$2.2B (+0.8%)',
      explanation: 'Market risk RWA in FR Y-9C Schedule HC-R (MDRM BHCK3545) does not fully reconcile with the standalone FRTB Market Risk Capital Report for Q4 2025. The $2.2B discrepancy reflects a timing difference: the FRTB Expected Shortfall charge for two newly approved trading desks (Equity Vol and EM Rates) was recorded in the market risk capital report at quarter-end but the FR Y-9C submission uses a T-1 business day snapshot per 12 CFR § 217.203. Action required: Align FR Y-9C HC-R line 10 to use the same quarter-end FRTB RWA as the standalone report, consistent with Federal Reserve guidance SR 15-18.',
    },
    {
      id: 'XR-002', status: 'warn', severity: 'Medium',
      description: 'LCR Report HQLA Total ↔ FR Y-9C HC Liquid Assets (HC-H)',
      report1: 'LCR Report', report2: 'FR Y-9C (HC-H)',
      mdrm1: 'LCR-HQLA-TOTAL', mdrm2: 'BHCK1754',
      businessLine: 'Institutional Securities',
      value1: '$124.2B HQLA', value2: '$125.4B', delta: '-$1.2B (-1.0%)',
      explanation: 'HQLA stock in the standalone LCR report ($124.2B) differs from FR Y-9C Schedule HC-H liquid asset disclosure (BHCK1754: $125.4B) by $1.2B. The variance reflects a Level 2B RMBS haircut methodology difference: LCR report applies 35% haircut on non-agency RMBS per 12 CFR Part 249 LCR rules, while HC-H uses the less conservative 25% haircut from the FR Y-9C reporting instructions. Both are technically compliant with their respective frameworks, but the Firm should document and align the treatment ahead of the upcoming Basel IV HQLA eligibility review.',
    },
    {
      id: 'XR-003', status: 'warn', severity: 'Medium',
      description: 'CET1 Capital Report ↔ FR Y-9C HC-R CET1 Numerator',
      report1: 'Internal Capital Report', report2: 'FR Y-9C (HC-R)',
      mdrm1: 'CAP-CET1-NET', mdrm2: 'BHCK8274',
      businessLine: 'Institutional Securities',
      value1: '$83.5B', value2: '$83.9B', delta: '-$0.4B',
      explanation: 'CET1 capital numerator in the internal capital report ($83.5B) differs from FR Y-9C HC-R line BHCK8274 ($83.9B) by $0.4B. The variance is attributable to DTA recognition timing: the internal report applies a conservative $0.4B haircut for deferred tax assets associated with Q4 2025 restructuring charges that have not yet met the "more likely than not" recognition threshold under ASC 740, while the FR Y-9C follows the regulatory deduction methodology under 12 CFR § 217.22(d). Action: confirm which treatment is appropriate for the Q4 2025 filing and document in the capital adequacy workpapers.',
    },
    {
      id: 'XR-004', status: 'warn', severity: 'Medium',
      description: 'Op Risk Capital (SMA) ↔ FR Y-9C HC-R Operational RWA',
      report1: 'Op Risk SMA Report', report2: 'FR Y-9C (HC-R)',
      mdrm1: 'OPR-SMA-CAPITAL', mdrm2: 'BHCKB702',
      businessLine: 'All',
      value1: '$28.4B', value2: '$27.9B', delta: '+$0.5B (+1.8%)',
      explanation: 'Minor variance of $0.5B between standalone SMA operational risk capital report and FR Y-9C HC-R line BHCKB702. Within tolerable reconciliation threshold (<2%) but trending upward over 3 consecutive quarters. Recommend reviewing ILM (Internal Loss Multiplier) calculation inputs — specifically the 10-year loss data completeness requirement under BCBS OPE10. If data gaps persist, ILM defaults to 1.0 which could understate capital.',
    },
    {
      id: 'XR-005', status: 'pass', severity: 'Low',
      description: 'Leverage Ratio Report ↔ FR Y-9C HC Tier 1 Capital',
      report1: 'Leverage Ratio Report', report2: 'FR Y-9C',
      mdrm1: 'LEV-TIER1', mdrm2: 'BHCK8274',
      businessLine: 'All', value1: '$93.0B', value2: '$93.0B', delta: '$0',
      explanation: null,
    },
    {
      id: 'XR-006', status: 'pass', severity: 'Low',
      description: 'NSFR Available Stable Funding ↔ FR Y-9C Long-term Funding',
      report1: 'NSFR Report', report2: 'FR Y-9C',
      mdrm1: 'NSFR-ASF-TOTAL', mdrm2: 'BHCK3548',
      businessLine: 'All', value1: '$419.3B', value2: '$419.3B', delta: '$0',
      explanation: null,
    },
    {
      id: 'XR-007', status: 'pass', severity: 'Low',
      description: 'Mortgage RWA (LTV-based) ↔ FR Y-9C HC-R Retail RWA',
      report1: 'Credit RWA Report', report2: 'FR Y-9C (HC-R)',
      mdrm1: 'CRW-MORTGAGE-RWA', mdrm2: 'BHCKB539',
      businessLine: 'Wealth Management', value1: '$38.2B', value2: '$38.2B', delta: '$0',
      explanation: null,
    },
    {
      id: 'XR-008', status: 'pass', severity: 'Low',
      description: 'Counterparty Credit RWA (SA-CCR) ↔ FR Y-9C HC-R Derivatives Exposure',
      report1: 'SA-CCR Report', report2: 'FR Y-9C (HC-R)',
      mdrm1: 'CCR-EAD-TOTAL', mdrm2: 'BHCK3529',
      businessLine: 'Institutional Securities', value1: '$142.1B', value2: '$142.1B', delta: '$0',
      explanation: null,
    },
    {
      id: 'XR-009', status: 'pass', severity: 'Low',
      description: 'Total Assets ↔ FR Y-9C HC Total Assets (BHCK2170)',
      report1: 'Balance Sheet', report2: 'FR Y-9C (HC)',
      mdrm1: 'BS-TOTAL-ASSETS', mdrm2: 'BHCK2170',
      businessLine: 'All', value1: '$1,420.3B', value2: '$1,420.3B', delta: '$0',
      explanation: null,
    },
    {
      id: 'XR-010', status: 'pass', severity: 'Low',
      description: 'LCR Net Cash Outflows ↔ FR Y-9C HC-L Liquidity Stress Outflows',
      report1: 'LCR Report', report2: 'FR Y-9C (HC-L)',
      mdrm1: 'LCR-NCO-TOTAL', mdrm2: 'BHCKP842',
      businessLine: 'All', value1: '$98.7B', value2: '$98.7B', delta: '$0',
      explanation: null,
    },
    {
      id: 'XR-011', status: 'pass', severity: 'Low',
      description: 'IRB Credit RWA (output floor applied) ↔ FR Y-9C HC-R Credit RWA',
      report1: 'IRB Credit RWA Report', report2: 'FR Y-9C (HC-R)',
      mdrm1: 'CRW-IRB-FLOORED', mdrm2: 'BHCK4267',
      businessLine: 'All', value1: '$201.8B', value2: '$201.8B', delta: '$0',
      explanation: null,
    },
  ],
};

const MOCK_AUDIT = {
  lastRun: 'Apr 18, 2026 09:42',
  summary: { critical: 2, high: 3, medium: 3 },
  findings: [
    { id: 'AUD-001', severity: 'Critical', pipelineFile: 'market_risk_rwa.sql',    regulatoryRule: 'FRTB – Market Risk',        ruleSection: 'BCBS MAR33.1',     issueDescription: 'Market risk RWA uses 99th percentile VaR. FRTB mandates Expected Shortfall at 97.5% with per-desk PLA tests.', oldLogic: 'SELECT desk_id, SUM(var_99_10d * sqrt_scaling) AS rwa\nFROM trading_positions\nGROUP BY desk_id;', suggestedFix: 'SELECT desk_id,\n  SUM(es_975_liquidity_adjusted) AS rwa,\n  SUM(default_risk_charge)       AS drc_addon,\n  SUM(residual_risk_addon)       AS rrao\nFROM trading_positions t\nJOIN desk_pla_results p ON t.desk_id = p.desk_id\nWHERE p.pla_test_status = \'PASS\'\nGROUP BY desk_id;', status: 'Open', impactedReports: 'MARKET_RISK_RWA' },
    { id: 'AUD-002', severity: 'Critical', pipelineFile: 'derivatives_ead.sql',    regulatoryRule: 'SA-CCR Derivatives',         ruleSection: 'BCBS CRE52',       issueDescription: 'EAD calculation uses legacy Current Exposure Method. Basel IV mandates SA-CCR with RC + aggregated PFE add-on.', oldLogic: 'SELECT netting_set_id,\n  MAX(0, mtm_value) + notional * add_on_factor AS ead\nFROM derivatives\nGROUP BY netting_set_id;', suggestedFix: 'SELECT netting_set_id,\n  1.4 * (\n    GREATEST(0, SUM(mtm_value) - SUM(collateral_value))\n    + sa_ccr_pfe_aggregate\n  ) AS ead_sa_ccr\nFROM derivatives d\nJOIN sa_ccr_addon_lookup a ON d.asset_class = a.asset_class\nGROUP BY netting_set_id;', status: 'Open', impactedReports: 'COUNTERPARTY_CREDIT_RISK' },
    { id: 'AUD-003', severity: 'High',     pipelineFile: 'credit_rwa_irb.sql',     regulatoryRule: 'Output Floor 72.5%',         ruleSection: 'BCBS CRE20',       issueDescription: 'IRB model RWA reported without output floor. Must add parallel SA-RWA and apply 72.5% floor.', oldLogic: 'SELECT portfolio_id,\n  SUM(pd * lgd * ead * maturity_adj) AS irb_rwa\nFROM loan_portfolio\nGROUP BY portfolio_id;', suggestedFix: 'SELECT portfolio_id,\n  SUM(pd * lgd * ead * maturity_adj) AS irb_rwa,\n  SUM(sa_risk_weight * ead / 100)    AS sa_rwa,\n  GREATEST(\n    SUM(pd * lgd * ead * maturity_adj),\n    0.725 * SUM(sa_risk_weight * ead / 100)\n  ) AS floored_rwa\nFROM loan_portfolio l\nJOIN sa_risk_weights r ON l.exposure_class = r.exposure_class\nGROUP BY portfolio_id;', status: 'Open', impactedReports: 'CREDIT_RISK_RWA' },
    { id: 'AUD-004', severity: 'High',     pipelineFile: 'cet1_deductions.sql',    regulatoryRule: 'DTA Dual Threshold',         ruleSection: 'BCBS CAP30',       issueDescription: 'DTA deduction applies single 10% threshold. Basel III requires dual threshold test combining DTA and significant investments at 17.65%.', oldLogic: 'SELECT entity_id,\n  cet1_before_deductions\n  - GREATEST(0, dta_balance - 0.10 * cet1_before_deductions)\n  AS cet1_net\nFROM capital_components;', suggestedFix: 'SELECT entity_id,\n  cet1_before_deductions\n  - GREATEST(0, dta_balance - 0.10 * cet1_before_deductions)\n  - GREATEST(0,\n      (dta_balance - 0.10 * cet1_before_deductions)\n      + significant_investments\n      - 0.1765 * cet1_before_deductions\n  ) AS cet1_net_dual_threshold\nFROM capital_components;', status: 'Open', impactedReports: 'CET1_CALCULATION' },
    { id: 'AUD-005', severity: 'High',     pipelineFile: 'lcr_hqla_buffer.sql',    regulatoryRule: 'LCR Level 2B RMBS',          ruleSection: 'BCBS LCR40',       issueDescription: 'RMBS classified as Level 2A (15% haircut). Basel III requires Level 2B classification (25-50% haircut, 15% HQLA cap).', oldLogic: 'SELECT asset_id,\n  market_value * 0.85 AS hqla_value,\n  \'Level2A\' AS hqla_class\nFROM liquid_assets\nWHERE asset_type = \'RMBS\';', suggestedFix: 'SELECT asset_id,\n  CASE\n    WHEN credit_rating >= \'AA-\' AND ltv_ratio <= 0.80\n      THEN market_value * (1 - 0.25)\n    WHEN credit_rating >= \'A-\'\n      THEN market_value * (1 - 0.35)\n    ELSE market_value * (1 - 0.50)\n  END AS hqla_value,\n  \'Level2B\' AS hqla_class\nFROM liquid_assets\nWHERE asset_type = \'RMBS\';', status: 'Open', impactedReports: 'LCR_HQLA_BUFFER' },
    { id: 'AUD-006', severity: 'Medium',   pipelineFile: 'op_risk_capital.sql',    regulatoryRule: 'Op Risk SMA',                ruleSection: 'BCBS OPE10',       issueDescription: 'Uses Basic Indicator Approach (15% α). Basel IV eliminates BIA; requires Standardised Measurement Approach with Business Indicator Component.', oldLogic: 'SELECT year,\n  AVG(gross_income) * 0.15 AS op_risk_capital\nFROM income_statement\nWHERE year >= YEAR(CURRENT_DATE) - 3\nGROUP BY year;', suggestedFix: 'SELECT\n  bic_sum AS business_indicator_component,\n  EXP(LN(1 + loss_ratio_10yr)) AS ilm_factor,\n  bic_sum * EXP(LN(1 + loss_ratio_10yr)) AS sma_capital\nFROM (\n  SELECT\n    SUM(CASE WHEN bi_bucket=1 THEN bi_value*0.12\n             WHEN bi_bucket=2 THEN (bi_value-1e9)*0.15+1.2e8\n             ELSE (bi_value-30e9)*0.18+4.65e9 END) AS bic_sum,\n    AVG(annual_loss) / NULLIF(bic_total, 0) AS loss_ratio_10yr\n  FROM business_indicator_data\n);', status: 'Open', impactedReports: 'OPERATIONAL_RISK_CAPITAL' },
    { id: 'AUD-007', severity: 'Medium',   pipelineFile: 'mortgage_rwa.sql',       regulatoryRule: 'Mortgage LTV Risk Weights',  ruleSection: 'BCBS CRE20',       issueDescription: 'Flat 35% risk weight for all performing mortgages. Basel IV requires LTV-based weights ranging 20-70%.', oldLogic: 'SELECT loan_id,\n  balance * 0.35 AS rwa\nFROM mortgage_portfolio\nWHERE status = \'PERFORMING\';', suggestedFix: 'SELECT loan_id, balance *\n  CASE\n    WHEN ltv_ratio <= 0.50 THEN 0.20\n    WHEN ltv_ratio <= 0.60 THEN 0.25\n    WHEN ltv_ratio <= 0.70 THEN 0.30\n    WHEN ltv_ratio <= 0.80 THEN 0.35\n    WHEN ltv_ratio <= 0.90 THEN 0.40\n    WHEN ltv_ratio <= 1.00 THEN 0.50\n    ELSE 0.70\n  END AS rwa\nFROM mortgage_portfolio\nWHERE status = \'PERFORMING\';', status: 'Open', impactedReports: 'MORTGAGE_CREDIT_RWA' },
    { id: 'AUD-008', severity: 'Medium',   pipelineFile: 'leverage_ratio.sql',     regulatoryRule: 'Leverage – Written CDS',     ruleSection: 'BCBS LEV30',       issueDescription: 'Written credit derivatives excluded from leverage exposure add-back. Basel III requires full notional inclusion net of purchased CDS offsets.', oldLogic: 'SELECT entity_id,\n  tier1_capital / total_on_bs_exposure\n  AS leverage_ratio\nFROM balance_sheet_summary;', suggestedFix: 'SELECT entity_id,\n  tier1_capital / (\n    total_on_bs_exposure\n    + GREATEST(0,\n        SUM(written_cds_notional)\n        - SUM(purchased_cds_notional))\n    + off_bs_exposure * ccf_factor\n    + sa_ccr_derivative_exposure\n  ) AS leverage_ratio\nFROM balance_sheet_summary b\nLEFT JOIN credit_derivative_positions c\n  ON b.entity_id = c.entity_id\nGROUP BY entity_id, tier1_capital,\n  total_on_bs_exposure, off_bs_exposure,\n  ccf_factor, sa_ccr_derivative_exposure;', status: 'Open', impactedReports: 'LEVERAGE_RATIO' },
  ],
};

// ── Document display name mapping (raw filenames → proper titles) ─────────────
const DOC_DISPLAY_NAMES = {
  'title12 part3 subpartA general provisions': '12 CFR Part 3 – Subpart A: General Provisions',
  'title12 part3 subpartB capital ratios':     '12 CFR Part 3 – Subpart B: Capital Ratio Requirements',
  'title12 part3 subpartC definition of capital': '12 CFR Part 3 – Subpart C: Definition of Capital',
  'title12 part3 subpartD rwa standardized':   '12 CFR Part 3 – Subpart D: RWA (Standardized)',
  'title12 part3 subpartE rwa irb':            '12 CFR Part 3 – Subpart E: RWA (IRB/Advanced)',
  'title12 part3 subpartF rwa market risk':    '12 CFR Part 3 – Subpart F: RWA (Market Risk)',
  'title12 part50 subpartB lcr hqla':          '12 CFR Part 50 – Subpart B: LCR & HQLA',
};

// ── GET /api/documents ────────────────────────────────────────────────────────
app.get('/api/documents', async (req, res) => {
  if (!sfConnected) return res.json(MOCK_DOCUMENTS);

  try {
    const [docs, reqs] = await Promise.all([
      sfQuery(`SELECT DOC_ID, FRAMEWORK, TITLE, CHAPTER, VERSION, EFFECTIVE_DATE, STATUS, PAGE_COUNT, SUMMARY FROM ${DB}.${SCHEMA}.REGULATORY_DOCUMENTS ORDER BY STATUS DESC, FRAMEWORK, EFFECTIVE_DATE`),
      sfQuery(`SELECT r.REQ_ID, r.DOC_ID, r.CATEGORY, r.REQUIREMENT, r.THRESHOLD, r.SEVERITY, r.IMPACT_AREA, r.RULE_NAME, r.RULE_SECTION, r.OLD_REQUIREMENT, r.CHANGE_TYPE, r.IMPACTED_REPORT, d.VERSION AS DOC_VERSION, d.FRAMEWORK FROM ${DB}.${SCHEMA}.EXTRACTED_REQUIREMENTS r JOIN ${DB}.${SCHEMA}.REGULATORY_DOCUMENTS d ON r.DOC_ID = d.DOC_ID ORDER BY CASE r.SEVERITY WHEN 'Critical' THEN 0 WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END`),
    ]);

    res.json({
      documents: docs.map((d) => {
        // REST API returns DATE as epoch day number — convert to ISO string
        let effDate = null;
        if (d.EFFECTIVE_DATE) {
          const dayNum = Number(d.EFFECTIVE_DATE);
          if (!isNaN(dayNum) && dayNum > 10000) {
            effDate = new Date(dayNum * 86400000).toISOString().slice(0, 10);
          } else {
            effDate = String(d.EFFECTIVE_DATE).replace(/"/g, '').slice(0, 10);
          }
        }
        // PAGE_COUNT stores LENGTH(RAW_TEXT) — approximate pages at ~3000 chars/page
        const charCount = d.PAGE_COUNT ? Number(d.PAGE_COUNT) : 0;
        const approxPages = charCount > 0 ? Math.max(1, Math.ceil(charCount / 3000)) : null;
        return {
          id:            d.DOC_ID,
          name:          (() => { const base = (d.TITLE||'').replace(/_?v\d{4}$/i,'').trim(); const n = DOC_DISPLAY_NAMES[d.TITLE] || DOC_DISPLAY_NAMES[base] || d.TITLE; return d.STATUS === 'Superseded' ? n + ' (2020 — Prior Version)' : n; })(),
          version:       d.VERSION,
          effectiveDate: effDate,
          pageCount:     approxPages,
          framework:     d.FRAMEWORK,
          status:        d.STATUS,
          summary:       d.SUMMARY || null,
        };
      }),
      requirements: reqs.map((r) => ({
        id:             r.REQ_ID,
        docId:          r.DOC_ID,
        docVersion:     r.DOC_VERSION,
        category:       r.CATEGORY,
        ruleName:       r.RULE_NAME || r.CATEGORY,
        ruleSection:    r.RULE_SECTION || r.THRESHOLD,
        oldRequirement: r.OLD_REQUIREMENT || null,
        newRequirement: r.REQUIREMENT,
        impactedReport: r.IMPACTED_REPORT || r.IMPACT_AREA,
        severity:       r.SEVERITY,
        changeType:     r.CHANGE_TYPE || ((r.FRAMEWORK || '').includes('Basel IV') ? 'New Requirement' : 'Updated'),
      })),
    });
  } catch (err) {
    console.error('Documents query error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/balance-sheet ────────────────────────────────────────────────────
app.get('/api/balance-sheet', async (req, res) => {
  if (!sfConnected) return res.json(MOCK_BALANCE_SHEET);

  const REV_MULT = { 'Institutional Securities': 38, 'Wealth Management': 62, 'Investment Management': 72 };

  try {
    const [metrics, anomalies, variance] = await Promise.all([
      sfQuery(`SELECT QUARTER, BUSINESS_LINE, CET1_RATIO, TIER1_RATIO, RWA_TOTAL_B, LCR_RATIO, NSFR_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS ORDER BY QUARTER_DATE`),
      sfQuery(`SELECT FLAG_ID, QUARTER, BUSINESS_LINE, RWA_ACTUAL_B, RWA_EXPECTED_B, IS_ANOMALY, ANOMALY_SCORE, ANOMALY_REASON FROM ${DB}.${SCHEMA}.ANOMALY_FLAGS WHERE ANOMALY_SCORE > 0.5 ORDER BY CASE WHEN ANOMALY_REASON IS NOT NULL THEN 0 ELSE 1 END, ANOMALY_SCORE DESC`),
      sfQuery(`SELECT QUARTER, BUSINESS_LINE, METRIC, CURRENT_VALUE, PRIOR_VALUE, QOQ_CHANGE_PCT, STATUS FROM ${DB}.${SCHEMA}.VARIANCE_ANALYSIS WHERE QUARTER = 'Q4 2025' ORDER BY BUSINESS_LINE`),
    ]);

    const DRIVER_MAP = {
      'Institutional Securities': { RWA_TOTAL_B: 'FRTB ES recalibration + SA-CCR derivatives add-on', CET1_RATIO: 'RWA denominator increase from FRTB', LCR_RATIO: 'Increased stress outflows under FRTB' },
      'Wealth Management':        { RWA_TOTAL_B: 'Normal loan book growth', CET1_RATIO: 'Retained earnings accumulation', LCR_RATIO: 'Stable liquidity position' },
      'Investment Management':    { RWA_TOTAL_B: 'AUM-driven credit exposure growth', CET1_RATIO: 'Retained earnings accumulation', LCR_RATIO: 'Stable liquidity position' },
    };

    // Consolidated firm-wide ratios (10-K Dec 31 2025, 10-Q Sep 30 2025)
    // Used in KPI cards — segment breakdowns use per-row data above
    const CONSOLIDATED_OVERRIDE = {
      'Q4 2025': { cet1: 15.0, tier1: 16.8, lcr: 134, totalRwa: 552.5 },
      'Q3 2025': { cet1: 15.2, tier1: 17.2, lcr: 129, totalRwa: 539.3 },
    };

    res.json({
      consolidated: CONSOLIDATED_OVERRIDE,
      metrics: metrics.map((m) => ({
        quarter:      m.QUARTER,
        businessLine: m.BUSINESS_LINE,
        cet1Ratio:    Number(m.CET1_RATIO),
        tier1Ratio:   Number(m.TIER1_RATIO),
        rwa:          Number(m.RWA_TOTAL_B),
        lcr:          Number(m.LCR_RATIO),
        nsfr:         Number(m.NSFR_RATIO),
        netRevenue:   +(Number(m.RWA_TOTAL_B) * (REV_MULT[m.BUSINESS_LINE] || 40)).toFixed(0),
      })),
      anomalies: anomalies.map((a) => {
        const raw = (a.ANOMALY_REASON || '').replace(/\n+/g, ' ').trim();
        // Strip markdown formatting for clean display
        const clean = raw.replace(/#{1,3}\s*/g, '').replace(/\*{1,2}/g, '').replace(/>\s*/g, '').replace(/—/g, '–').trim();
        const short = clean.length > 80 ? clean.slice(0, 77) + '…' : clean;
        const full = clean;
        return {
          id:              a.FLAG_ID,
          metric:          'RWA',
          businessLine:    a.BUSINESS_LINE,
          quarter:         a.QUARTER,
          value:           Number(a.RWA_ACTUAL_B),
          expectedLow:     Number(a.RWA_EXPECTED_B) * 0.88,
          expectedHigh:    Number(a.RWA_EXPECTED_B) * 1.12,
          pctChange:       a.RWA_EXPECTED_B ? +((a.RWA_ACTUAL_B - a.RWA_EXPECTED_B) / a.RWA_EXPECTED_B * 100).toFixed(1) : 0,
          severity:        Number(a.ANOMALY_SCORE) > 0.97 ? 'Critical' : Number(a.ANOMALY_SCORE) > 0.80 ? 'High' : 'Watch',
          description:     short,
          descriptionFull: full,
        };
      }),
      variance: variance.map((v) => ({
        businessLine:   v.BUSINESS_LINE,
        metricName:     v.METRIC,
        priorQuarter:   'Q3 2025',
        currentQuarter: v.QUARTER,
        priorValue:     Number(v.PRIOR_VALUE),
        currentValue:   Number(v.CURRENT_VALUE),
        changeAbs:      +(Number(v.CURRENT_VALUE) - Number(v.PRIOR_VALUE)).toFixed(2),
        changePct:      Number(v.QOQ_CHANGE_PCT),
        driver:         DRIVER_MAP[v.BUSINESS_LINE]?.[v.METRIC] || (v.STATUS === 'BREACH' ? 'Regulatory model change' : 'Normal fluctuation'),
      })),
    });
  } catch (err) {
    console.error('Balance sheet query error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/audit-findings ───────────────────────────────────────────────────
app.get('/api/audit-findings', async (req, res) => {
  if (!sfConnected) return res.json(MOCK_AUDIT);

  try {
    const [findings, lastRun] = await Promise.all([
      sfQuery(`SELECT FINDING_ID, PIPELINE_NAME, SEVERITY, CATEGORY, DESCRIPTION, AFFECTED_TABLE, OLD_LOGIC, SUGGESTED_FIX, REGULATION_REF FROM ${DB}.${SCHEMA}.AUDIT_FINDINGS ORDER BY CASE SEVERITY WHEN 'Critical' THEN 0 WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, FINDING_ID`),
      sfQuery(`SELECT RUN_TIMESTAMP FROM ${DB}.${SCHEMA}.AUDIT_RUN_LOG ORDER BY RUN_TIMESTAMP DESC LIMIT 1`),
    ]);

    const lastRunTs = lastRun[0]?.RUN_TIMESTAMP;
    let lastRunStr = 'Apr 18, 2026 09:42';
    if (lastRunTs) {
      // REST API returns TIMESTAMP_NTZ as epoch seconds (e.g. "1745367123.456000000")
      const epochSec = parseFloat(lastRunTs);
      const dt = !isNaN(epochSec) && epochSec > 1e9 ? new Date(epochSec * 1000) : new Date(lastRunTs);
      if (!isNaN(dt.getTime())) {
        lastRunStr = dt.toLocaleString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' });
      }
    }

    res.json({
      lastRun: lastRunStr,
      summary: {
        critical: findings.filter((f) => f.SEVERITY === 'Critical').length,
        high:     findings.filter((f) => f.SEVERITY === 'High').length,
        medium:   findings.filter((f) => f.SEVERITY === 'Medium').length,
      },
      findings: findings.map((f) => ({
        id:               f.FINDING_ID,
        severity:         f.SEVERITY,
        pipelineFile:     f.PIPELINE_NAME,
        regulatoryRule:   f.CATEGORY,
        ruleSection:      f.REGULATION_REF,
        issueDescription: f.DESCRIPTION,
        oldLogic:         f.OLD_LOGIC,
        suggestedFix:     f.SUGGESTED_FIX,
        status:           'Open',
        impactedReports:  f.AFFECTED_TABLE,
      })),
    });
  } catch (err) {
    console.error('Audit findings query error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/search ──────────────────────────────────────────────────────────
const MOCK_SEARCH_RESULTS = [
  {
    text: 'A bank must deduct from Common Equity Tier 1 capital any amount of DTAs that arise from net operating loss and tax credit carryforwards above the 10 percent common equity tier 1 capital deduction threshold. The combined amount of DTAs and significant investments in unconsolidated financial institutions is subject to a 17.65 percent aggregate threshold.',
    docName: 'Basel III: Capital Adequacy Framework',
    docVersion: '3.1',
    sectionTitle: 'CAP30 — CET1 Deductions',
  },
  {
    text: 'Under the revised standardised approach for market risk (FRTB), the expected shortfall measure replaces Value-at-Risk at the 97.5th percentile confidence level over a base horizon of 10 business days. Banks using the internal models approach must pass desk-level P&L attribution tests and backtesting requirements.',
    docName: 'Basel IV: FRTB – Market Risk Capital',
    docVersion: '1.0',
    sectionTitle: 'MAR33.1 — Expected Shortfall',
  },
  {
    text: 'The Liquidity Coverage Ratio requires that a bank hold sufficient high-quality liquid assets (HQLA) to cover total net cash outflows over a 30-day stress scenario. Level 2B assets including RMBS and investment-grade corporate debt are subject to 25-50% haircuts and capped at 15% of the total HQLA stock.',
    docName: 'Basel III: Liquidity Coverage Ratio',
    docVersion: '2.0',
    sectionTitle: 'LCR40 — HQLA Definitions',
  },
  {
    text: 'SA-CCR replaces the Current Exposure Method for calculating exposure at default for OTC derivatives. EAD is calculated as 1.4 multiplied by the sum of replacement cost and the aggregated potential future exposure add-on, with netting set recognition across five asset classes.',
    docName: 'Basel IV: SA-CCR – Counterparty Credit Risk',
    docVersion: '1.0',
    sectionTitle: 'CRE52 — SA-CCR Methodology',
  },
];

app.post('/api/search', async (req, res) => {
  const { query: searchQuery, docId } = req.body;
  if (!searchQuery) return res.status(400).json({ error: 'query required' });

  const account = process.env.SNOWFLAKE_ACCOUNT;
  const pat     = process.env.SNOWFLAKE_PAT;

  if (!pat) {
    // Mock mode: do basic keyword matching against mock results
    const q = searchQuery.toLowerCase();
    const filtered = MOCK_SEARCH_RESULTS.filter((r) =>
      r.text.toLowerCase().includes(q) ||
      r.docName.toLowerCase().includes(q) ||
      r.sectionTitle.toLowerCase().includes(q)
    );
    return res.json({ results: filtered.length > 0 ? filtered : MOCK_SEARCH_RESULTS.slice(0, 3) });
  }

  const url = `https://${account}.snowflakecomputing.com/api/v2/databases/${DB}/schemas/${SCHEMA}/cortex-search-services/REGULATORY_DOCS_SEARCH:query`;

  const body = {
    query:   searchQuery,
    columns: ['RAW_TEXT', 'TITLE', 'VERSION', 'CHAPTER', 'FRAMEWORK'],
    limit:   4,
    ...(docId ? { filter: { '@eq': { DOC_ID: docId } } } : {}),
  };

  try {
    const r = await fetch(url, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Accept':        'application/json',
        'Authorization': `Bearer ${pat}`,
        'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
      },
      body: JSON.stringify(body),
    });

    if (!r.ok) {
      const errText = await r.text();
      return res.status(500).json({ error: `Cortex Search ${r.status}: ${errText}` });
    }

    const data = await r.json();
    res.json({
      results: (data.results || []).map((item) => ({
        text:         item.RAW_TEXT,
        docName:      DOC_DISPLAY_NAMES[item.TITLE] || item.TITLE,
        docVersion:   item.VERSION,
        pageNumber:   null,
        sectionTitle: item.CHAPTER,
      })),
    });
  } catch (err) {
    console.error('Search error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/threads ─────────────────────────────────────────────────────────
// Creates a new Cortex thread and returns its integer thread_id.
// The frontend calls this once on chat panel open to obtain a real thread_id
// before making any agent:run requests.
app.post('/api/threads', async (req, res) => {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  const pat     = process.env.SNOWFLAKE_PAT;
  if (!pat) return res.status(503).json({ error: 'SNOWFLAKE_PAT not configured' });

  try {
    const r = await fetch(`https://${account}.snowflakecomputing.com/api/v2/cortex/threads`, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Accept':        'application/json',
        'Authorization': `Bearer ${pat}`,
        'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
      },
      body: JSON.stringify({ origin_application: 'regtech_demo' }),
    });
    if (!r.ok) {
      const errText = await r.text();
      return res.status(r.status).json({ error: `Threads API ${r.status}: ${errText}` });
    }
    const data = await r.json();
    res.json({ thread_id: data.thread_id });
  } catch (err) {
    console.error('Create thread error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/chat/feedback ───────────────────────────────────────────────────
app.post('/api/chat/feedback', async (req, res) => {
  const { orig_request_id, positive, feedback_message = '', categories = [], thread_id } = req.body;
  if (!orig_request_id) return res.status(400).json({ error: 'orig_request_id required' });

  const account = process.env.SNOWFLAKE_ACCOUNT;
  const pat     = process.env.SNOWFLAKE_PAT;
  if (!pat) return res.status(503).json({ error: 'SNOWFLAKE_PAT not configured' });

  const url = `https://${account}.snowflakecomputing.com/api/v2/databases/${DB}/schemas/${SCHEMA}/agents/REGTECH_ANALYTICS_AGENT:feedback`;

  try {
    const body = { orig_request_id, positive, feedback_message, categories };
    if (thread_id !== undefined) body.thread_id = thread_id;

    const r = await fetch(url, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Accept':        'application/json',
        'Authorization': `Bearer ${pat}`,
        'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
      },
      body: JSON.stringify(body),
    });

    if (!r.ok) {
      const errText = await r.text();
      return res.status(r.status).json({ error: `Feedback API ${r.status}: ${errText}` });
    }
    res.json({ success: true });
  } catch (err) {
    console.error('Feedback error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── GET /api/data-quality ─────────────────────────────────────────────────────
// Powered by 14 native Snowflake Data Metric Functions (6 system + 8 custom)
// Results are cached on server startup and refreshed every 10 minutes.
let _dqCache = null;
let _dqCacheLoading = false;

async function _computeDqResults() {
  try {
    // ── Single query invokes all 14 Snowflake Data Metric Functions ──────────
    const [dmf] = await sfQuery(`
      SELECT
        SNOWFLAKE.CORE.NULL_COUNT(SELECT CET1_RATIO  FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS CET1_NULLS,
        SNOWFLAKE.CORE.NULL_COUNT(SELECT RWA_TOTAL_B FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS RWA_NULLS,
        SNOWFLAKE.CORE.NULL_COUNT(SELECT LCR_RATIO   FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS LCR_NULLS,
        SNOWFLAKE.CORE.NULL_COUNT(SELECT TIER1_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS TIER1_NULLS,
        SNOWFLAKE.CORE.NULL_COUNT(SELECT NSFR_RATIO  FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS NSFR_NULLS,
        SNOWFLAKE.CORE.DUPLICATE_COUNT(SELECT METRIC_ID FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS DUP_IDS,
        ${DB}.${SCHEMA}.CET1_BELOW_REG_FLOOR(SELECT CET1_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS CET1_REG_VIOLATIONS,
        ${DB}.${SCHEMA}.CET1_BELOW_INTERNAL_TARGET(SELECT CET1_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS CET1_INTERNAL_VIOLATIONS,
        ${DB}.${SCHEMA}.LCR_BELOW_REG_FLOOR(SELECT LCR_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS LCR_REG_VIOLATIONS,
        ${DB}.${SCHEMA}.LCR_BELOW_INTERNAL_BUFFER(SELECT LCR_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS LCR_INTERNAL_VIOLATIONS,
        ${DB}.${SCHEMA}.TIER1_BELOW_CET1_COUNT(SELECT TIER1_RATIO, CET1_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS TIER1_VIOLATIONS,
        ${DB}.${SCHEMA}.NSFR_BELOW_REG_FLOOR(SELECT NSFR_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS NSFR_VIOLATIONS,
        ${DB}.${SCHEMA}.RWA_QOQ_VIOLATION_COUNT(SELECT BUSINESS_LINE, QUARTER_DATE, RWA_TOTAL_B FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS RWA_QOQ_VIOLATIONS,
        ${DB}.${SCHEMA}.CET1_MAX_QOQ_DECLINE_BPS(SELECT BUSINESS_LINE, QUARTER_DATE, CET1_RATIO FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS) AS CET1_MAX_DECLINE_BPS`);

    // ── For violations, get detail rows then call Cortex COMPLETE for root cause
    let lcrViolationRows = [], lcrRootCause = null;
    if (Number(dmf.LCR_INTERNAL_VIOLATIONS) > 0) {
      lcrViolationRows = await sfQuery(`
        SELECT QUARTER, BUSINESS_LINE, LCR_RATIO
        FROM ${DB}.${SCHEMA}.BALANCE_SHEET_METRICS
        WHERE LCR_RATIO < 125
        ORDER BY QUARTER_DATE`);
      const rowList = lcrViolationRows
        .map(r => `${r.QUARTER} ${r.BUSINESS_LINE}: ${parseFloat(r.LCR_RATIO).toFixed(1)}%`)
        .join(', ');
      const prompt = `You are a bank regulatory reporting analyst. Morgan Stanley Institutional Securities LCR fell below the 125% internal liquidity buffer target in ${Number(dmf.LCR_INTERNAL_VIOLATIONS)} quarter(s): ${rowList}. The 100% regulatory floor is not breached. Provide a concise 2-sentence root cause and recommended action.`;
      const [rc] = await sfQuery(`SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-6', '${prompt.replace(/'/g, "''")}') AS root_cause`);
      lcrRootCause = rc?.ROOT_CAUSE ?? null;
    }

    const cet1DeclineBps = Number(dmf.CET1_MAX_DECLINE_BPS);
    let cet1DeclineRootCause = null;
    if (cet1DeclineBps > 25) {
      const prompt = `You are a bank regulatory reporting analyst. Morgan Stanley's CET1 capital ratio experienced its steepest single-quarter decline of ${cet1DeclineBps} basis points in the 8-quarter window Q1 2024–Q4 2025. No regulatory threshold was breached. Provide a concise 2-sentence root cause and recommended action.`;
      const [rc] = await sfQuery(`SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-6', '${prompt.replace(/'/g, "''")}') AS root_cause`);
      cet1DeclineRootCause = rc?.ROOT_CAUSE ?? null;
    }

    // ── Map DMF results to standard check format ──────────────────────────────
    const lcrViolScope = lcrViolationRows.length
      ? lcrViolationRows.map(r => `${r.QUARTER} ${r.BUSINESS_LINE}`).join(' · ')
      : 'All rows';

    const checks = [
      { id: 'DQ-001', category: 'Completeness',          name: 'CET1 Ratio — No Nulls',
        metric: 'CET1_RATIO',  scope: 'All rows',
        status: Number(dmf.CET1_NULLS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.CET1_NULLS) === 0 ? null : `${dmf.CET1_NULLS} null(s)`,
        threshold: 'NOT NULL', quarter: null, businessLine: null, rootCause: null,
        dmf: 'SNOWFLAKE.CORE.NULL_COUNT' },

      { id: 'DQ-002', category: 'Completeness',          name: 'RWA Total — No Nulls',
        metric: 'RWA_TOTAL_B', scope: 'All rows',
        status: Number(dmf.RWA_NULLS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.RWA_NULLS) === 0 ? null : `${dmf.RWA_NULLS} null(s)`,
        threshold: 'NOT NULL', quarter: null, businessLine: null, rootCause: null,
        dmf: 'SNOWFLAKE.CORE.NULL_COUNT' },

      { id: 'DQ-003', category: 'Completeness',          name: 'LCR Ratio — No Nulls',
        metric: 'LCR_RATIO',   scope: 'All rows',
        status: Number(dmf.LCR_NULLS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.LCR_NULLS) === 0 ? null : `${dmf.LCR_NULLS} null(s)`,
        threshold: 'NOT NULL', quarter: null, businessLine: null, rootCause: null,
        dmf: 'SNOWFLAKE.CORE.NULL_COUNT' },

      { id: 'DQ-004', category: 'Completeness',          name: 'Tier 1 Ratio — No Nulls',
        metric: 'TIER1_RATIO', scope: 'All rows',
        status: Number(dmf.TIER1_NULLS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.TIER1_NULLS) === 0 ? null : `${dmf.TIER1_NULLS} null(s)`,
        threshold: 'NOT NULL', quarter: null, businessLine: null, rootCause: null,
        dmf: 'SNOWFLAKE.CORE.NULL_COUNT' },

      { id: 'DQ-005', category: 'Completeness',          name: 'NSFR Ratio — No Nulls',
        metric: 'NSFR_RATIO',  scope: 'All rows',
        status: Number(dmf.NSFR_NULLS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.NSFR_NULLS) === 0 ? null : `${dmf.NSFR_NULLS} null(s)`,
        threshold: 'NOT NULL', quarter: null, businessLine: null, rootCause: null,
        dmf: 'SNOWFLAKE.CORE.NULL_COUNT' },

      { id: 'DQ-006', category: 'Uniqueness',            name: 'Metric ID — No Duplicates',
        metric: 'METRIC_ID',   scope: 'All rows',
        status: Number(dmf.DUP_IDS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.DUP_IDS) === 0 ? null : `${dmf.DUP_IDS} duplicate(s)`,
        threshold: 'UNIQUE', quarter: null, businessLine: null, rootCause: null,
        dmf: 'SNOWFLAKE.CORE.DUPLICATE_COUNT' },

      { id: 'DQ-007', category: 'Regulatory Floor',      name: 'CET1 ≥ 4.5% (Basel III Minimum)',
        metric: 'CET1_RATIO',  scope: 'All rows',
        status: Number(dmf.CET1_REG_VIOLATIONS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.CET1_REG_VIOLATIONS) === 0 ? null : `${dmf.CET1_REG_VIOLATIONS} violation(s)`,
        threshold: '≥ 4.5% (12 CFR § 3.10)', quarter: null, businessLine: null, rootCause: null,
        dmf: `${DB}.${SCHEMA}.CET1_BELOW_REG_FLOOR` },

      { id: 'DQ-008', category: 'Regulatory Floor',      name: 'CET1 ≥ 13.5% (Internal Target)',
        metric: 'CET1_RATIO',  scope: 'All rows',
        status: Number(dmf.CET1_INTERNAL_VIOLATIONS) === 0 ? 'pass' : 'warn',
        value: Number(dmf.CET1_INTERNAL_VIOLATIONS) === 0 ? null : `${dmf.CET1_INTERNAL_VIOLATIONS} below target`,
        threshold: '≥ 13.5% internal management target', quarter: null, businessLine: null, rootCause: null,
        dmf: `${DB}.${SCHEMA}.CET1_BELOW_INTERNAL_TARGET` },

      { id: 'DQ-009', category: 'Regulatory Floor',      name: 'LCR ≥ 100% (12 CFR Part 249)',
        metric: 'LCR_RATIO',   scope: 'All rows',
        status: Number(dmf.LCR_REG_VIOLATIONS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.LCR_REG_VIOLATIONS) === 0 ? null : `${dmf.LCR_REG_VIOLATIONS} breach(es)`,
        threshold: '≥ 100% regulatory minimum', quarter: null, businessLine: null, rootCause: null,
        dmf: `${DB}.${SCHEMA}.LCR_BELOW_REG_FLOOR` },

      { id: 'DQ-010', category: 'Regulatory Floor',      name: 'LCR ≥ 125% (Internal Buffer)',
        metric: 'LCR_RATIO',   scope: lcrViolScope,
        status: Number(dmf.LCR_INTERNAL_VIOLATIONS) === 0 ? 'pass' : 'warn',
        value: Number(dmf.LCR_INTERNAL_VIOLATIONS) === 0 ? null : `${dmf.LCR_INTERNAL_VIOLATIONS} quarter(s) below 125%`,
        threshold: '≥ 125% internal liquidity buffer target',
        quarter: lcrViolationRows[0]?.QUARTER ?? null,
        businessLine: lcrViolationRows[0]?.BUSINESS_LINE ?? null,
        rootCause: lcrRootCause,
        dmf: `${DB}.${SCHEMA}.LCR_BELOW_INTERNAL_BUFFER` },

      { id: 'DQ-011', category: 'Structural Consistency', name: 'Tier 1 ≥ CET1 (AT1 ≥ 0)',
        metric: 'TIER1_RATIO', scope: 'All rows',
        status: Number(dmf.TIER1_VIOLATIONS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.TIER1_VIOLATIONS) === 0 ? null : `${dmf.TIER1_VIOLATIONS} violation(s)`,
        threshold: 'TIER1 ≥ CET1 always (AT1 capital cannot be negative)',
        quarter: null, businessLine: null, rootCause: null,
        dmf: `${DB}.${SCHEMA}.TIER1_BELOW_CET1_COUNT` },

      { id: 'DQ-012', category: 'Regulatory Floor',      name: 'NSFR ≥ 100% (Regulatory Minimum)',
        metric: 'NSFR_RATIO',  scope: 'All rows',
        status: Number(dmf.NSFR_VIOLATIONS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.NSFR_VIOLATIONS) === 0 ? null : `${dmf.NSFR_VIOLATIONS} breach(es)`,
        threshold: '≥ 100% (Basel III NSFR)', quarter: null, businessLine: null, rootCause: null,
        dmf: `${DB}.${SCHEMA}.NSFR_BELOW_REG_FLOOR` },

      { id: 'DQ-013', category: 'Range Check',           name: 'RWA QoQ Change ≤ 20%',
        metric: 'RWA_TOTAL_B', scope: 'All rows',
        status: Number(dmf.RWA_QOQ_VIOLATIONS) === 0 ? 'pass' : 'fail',
        value: Number(dmf.RWA_QOQ_VIOLATIONS) === 0 ? null : `${dmf.RWA_QOQ_VIOLATIONS} quarter(s) exceeded threshold`,
        threshold: '≤ 20% QoQ change per business line',
        quarter: null, businessLine: null, rootCause: null,
        dmf: `${DB}.${SCHEMA}.RWA_QOQ_VIOLATION_COUNT` },

      { id: 'DQ-014', category: 'Trend Analysis',        name: `CET1 Max QoQ Decline (${cet1DeclineBps} bps)`,
        metric: 'CET1_RATIO',  scope: '8-quarter observation window',
        status: cet1DeclineBps > 25 ? 'warn' : 'pass',
        value: `${cet1DeclineBps} bps max single-quarter decline`,
        threshold: '≤ 25 bps single-quarter decline',
        quarter: cet1DeclineBps > 25 ? 'Q2 2025' : null,
        businessLine: cet1DeclineBps > 25 ? 'Institutional Securities' : null,
        rootCause: cet1DeclineRootCause,
        dmf: `${DB}.${SCHEMA}.CET1_MAX_QOQ_DECLINE_BPS` },
    ];

    const pass = checks.filter(c => c.status === 'pass').length;
    const warn = checks.filter(c => c.status === 'warn').length;
    const fail = checks.filter(c => c.status === 'fail').length;

    return { asOf: 'Q4 2025', summary: { pass, warn, fail }, checks };
  } catch (err) {
    console.error('Data quality cache error:', err);
    return { error: err.message };
  }
}

// Warm the DQ cache on startup and refresh every 10 minutes
async function _warmDqCache() {
  if (_dqCacheLoading) return;
  _dqCacheLoading = true;
  console.log('[DQ] Computing data quality checks...');
  _dqCache = await _computeDqResults();
  _dqCacheLoading = false;
  if (_dqCache && !_dqCache.error) {
    console.log(`[DQ] Cache ready. ${_dqCache.summary.pass} pass, ${_dqCache.summary.warn} warn, ${_dqCache.summary.fail} fail`);
    // Persist to Snowflake for instant reads on next server start
    _persistDqResults(_dqCache).catch(e => console.error('[DQ] Persist error:', e.message));
  } else {
    console.log('[DQ] Cache error:', _dqCache?.error);
  }
}

async function _persistDqResults(results) {
  // Delete old rows and insert fresh
  await sfQuery(`DELETE FROM ${DB}.${SCHEMA}.DATA_QUALITY_RESULTS`);
  for (const c of results.checks) {
    const vals = [c.id, c.category, c.name, c.metric || '', c.scope || '',
      c.status, c.value || '', c.threshold || '', c.quarter || '', c.businessLine || '',
      (c.rootCause || '').replace(/'/g, "''"), c.dmf || ''].map(v => `'${v}'`).join(',');
    await sfQuery(`INSERT INTO ${DB}.${SCHEMA}.DATA_QUALITY_RESULTS (CHECK_ID,CATEGORY,CHECK_NAME,METRIC,SCOPE,STATUS,VALUE,THRESHOLD,QUARTER,BUSINESS_LINE,ROOT_CAUSE,DMF_NAME) VALUES (${vals})`);
  }
  console.log(`[DQ] Persisted ${results.checks.length} checks to DATA_QUALITY_RESULTS`);
}

async function _loadPersistedDq() {
  try {
    const rows = await sfQuery(`SELECT CHECK_ID, CATEGORY, CHECK_NAME, METRIC, SCOPE, STATUS, VALUE, THRESHOLD, QUARTER, BUSINESS_LINE, ROOT_CAUSE, DMF_NAME, COMPUTED_AT FROM ${DB}.${SCHEMA}.DATA_QUALITY_RESULTS ORDER BY CHECK_ID`);
    if (!rows.length) return null;
    const checks = rows.map(r => ({
      id: r.CHECK_ID, category: r.CATEGORY, name: r.CHECK_NAME,
      metric: r.METRIC || null, scope: r.SCOPE || null,
      status: r.STATUS, value: r.VALUE || null, threshold: r.THRESHOLD || null,
      quarter: r.QUARTER || null, businessLine: r.BUSINESS_LINE || null,
      rootCause: r.ROOT_CAUSE || null, dmf: r.DMF_NAME || null,
    }));
    const pass = checks.filter(c => c.status === 'pass').length;
    const warn = checks.filter(c => c.status === 'warn').length;
    const fail = checks.filter(c => c.status === 'fail').length;
    return { asOf: 'Q4 2025', summary: { pass, warn, fail }, checks };
  } catch { return null; }
}

if (sfConnected) {
  // On startup: load persisted results first (instant), then refresh in background
  setTimeout(async () => {
    const persisted = await _loadPersistedDq();
    if (persisted) {
      _dqCache = persisted;
      console.log('[DQ] Loaded persisted results from Snowflake (instant).');
    }
    // Refresh in background regardless (keeps data fresh)
    _warmDqCache();
  }, 1000);
  // Refresh every 10 minutes
  setInterval(() => _warmDqCache(), 10 * 60 * 1000);
}

app.get('/api/data-quality', async (req, res) => {
  if (!sfConnected) return res.json(MOCK_DQ_RESULTS);
  if (_dqCache) return res.json(_dqCache);
  // Cache not ready yet — compute inline (first request before cache warms)
  if (!_dqCacheLoading) _warmDqCache();
  res.json({ asOf: 'Q4 2025', summary: { pass: 0, warn: 0, fail: 0 }, checks: [], loading: true });
});

// ── POST /api/cross-report-validate ──────────────────────────────────────────
app.post('/api/cross-report-validate', async (req, res) => {
  const { quarter = 'Q4 2025' } = req.body || {};

  if (!sfConnected) return res.json(MOCK_CROSS_REPORT_RESULTS);

  try {
    const rules = await sfQuery(`
      SELECT r.RULE_ID, r.DESCRIPTION, r.REPORT_1, r.REPORT_2,
             r.MDRM_1, r.MDRM_2, r.BUSINESS_LINE, r.SEVERITY,
             r.SQL_CHECK, r.THRESHOLD_PCT
      FROM ${DB}.${SCHEMA}.CROSS_REPORT_RULES r
      WHERE r.IS_ACTIVE = TRUE
      ORDER BY CASE r.SEVERITY WHEN 'Critical' THEN 0 WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END`);

    const results = [];
    for (const rule of rules) {
      let status = 'pass';
      let value1 = null, value2 = null, delta = null, explanation = null;
      try {
        const checkRows = await sfQuery(rule.SQL_CHECK.replace(/:quarter/g, `'${quarter}'`));
        if (checkRows[0]) {
          const r = checkRows[0];
          value1 = r.VALUE1; value2 = r.VALUE2; delta = r.DELTA;
          const pctDelta = Math.abs(parseFloat(r.DELTA_PCT || 0));
          if (pctDelta > parseFloat(rule.THRESHOLD_PCT || 2)) {
            status = pctDelta > 5 ? 'fail' : 'warn';
            const prompt = `You are a bank regulatory reporting analyst. Cross-report reconciliation rule "${rule.DESCRIPTION}" failed for ${quarter}. Report 1 (${rule.REPORT_1}, MDRM ${rule.MDRM_1}): ${value1}. Report 2 (${rule.REPORT_2}, MDRM ${rule.MDRM_2}): ${value2}. Delta: ${delta}. Provide a 2-3 sentence explanation of the likely cause and required action.`;
            const expRows = await sfQuery(`SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-6', '${prompt.replace(/'/g, "''")}') AS explanation`);
            explanation = expRows[0]?.EXPLANATION;
          }
        }
      } catch { status = 'warn'; }
      results.push({ id: rule.RULE_ID, status, severity: rule.SEVERITY, description: rule.DESCRIPTION, report1: rule.REPORT_1, report2: rule.REPORT_2, mdrm1: rule.MDRM_1, mdrm2: rule.MDRM_2, businessLine: rule.BUSINESS_LINE, value1, value2, delta, explanation });
    }

    const pass = results.filter(r => r.status === 'pass').length;
    const fail = results.filter(r => r.status === 'fail').length;
    const warn = results.filter(r => r.status === 'warn').length;
    res.json({ lastRun: new Date().toLocaleString('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit' }), quarter, summary: { pass, fail, warn, total: results.length }, rules: results });
  } catch (err) {
    console.error('Cross-report validate error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── POST /api/chat (SSE streaming via Cortex Agent) ───────────────────────────
app.post('/api/chat', async (req, res) => {
  const { message, thread_id, parent_message_id } = req.body;
  if (!message) return res.status(400).json({ error: 'message required' });

  const account = process.env.SNOWFLAKE_ACCOUNT;
  const pat     = process.env.SNOWFLAKE_PAT;

  if (!pat) {
    return res.status(503).json({
      error: 'SNOWFLAKE_PAT not configured. Add your PAT to .env to enable the AI chatbot.',
    });
  }

  const url = `https://${account}.snowflakecomputing.com/api/v2/databases/${DB}/schemas/${SCHEMA}/agents/REGTECH_ANALYTICS_AGENT:run`;

  // When thread_id is provided, Snowflake maintains history server-side.
  // parent_message_id must be 0 for the first turn, then the last assistant message_id.
  const messages = [
    { role: 'user', content: [{ type: 'text', text: message }] },
  ];

  const chatBody = { messages, stream: true };
  if (thread_id !== undefined && thread_id !== null) {
    chatBody.thread_id        = thread_id;
    chatBody.parent_message_id = parent_message_id ?? 0;
  }

  try {
    const agentRes = await fetch(url, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Accept':        'text/event-stream',
        'Authorization': `Bearer ${pat}`,
        'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
      },
      body: JSON.stringify(chatBody),
    });

    if (!agentRes.ok) {
      const errText = await agentRes.text();
      console.error('Agent API error:', agentRes.status, errText);
      return res.status(500).json({ error: `Agent returned ${agentRes.status}: ${errText}` });
    }

    // Capture the Snowflake request ID so the frontend can submit feedback
    const snowflakeRequestId = agentRes.headers.get('x-snowflake-request-id') || null;

    res.setHeader('Content-Type',      'text/event-stream');
    res.setHeader('Cache-Control',     'no-cache, no-transform');
    res.setHeader('Connection',        'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.socket?.setNoDelay(true);

    // Send request ID as first event so the client can use it for feedback
    if (snowflakeRequestId) {
      res.write(`event: request_id\ndata: ${JSON.stringify({ request_id: snowflakeRequestId })}\n\n`);
    }

    const reader  = agentRes.body.getReader();
    const decoder = new TextDecoder();
    let buffer       = '';
    let currentEvent = '';
    let sentText     = false;

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('event: ')) {
            currentEvent = line.slice(7).trim();
          } else if (line.startsWith('data: ')) {
            const dataStr = line.slice(6).trim();
            if (!dataStr || dataStr === '[DONE]') continue;
            try {
              const parsed = JSON.parse(dataStr);
              switch (currentEvent) {
                case 'metadata':
                  // Forward assistant message_id so the client can pass it as
                  // parent_message_id on the next turn (required for threads)
                  if (parsed.metadata?.role === 'assistant' && parsed.metadata?.message_id != null) {
                    res.write(`event: message_id\ndata: ${JSON.stringify({ message_id: parsed.metadata.message_id })}\n\n`);
                  }
                  break;
                case 'response.text.delta':
                  if (parsed.text) {
                    res.write(`event: text\ndata: ${JSON.stringify({ text: parsed.text })}\n\n`);
                    sentText = true;
                  }
                  break;
                case 'response':
                  if (!sentText && parsed.content) {
                    const parts = parsed.content.filter((c) => c.type === 'text' && c.text).map((c) => c.text);
                    if (parts.length > 0) {
                      res.write(`event: text\ndata: ${JSON.stringify({ text: parts.join('\n') })}\n\n`);
                      sentText = true;
                    }
                  }
                  break;
                case 'response.tool_result':
                  if (parsed.tool_results?.content?.json?.sql) {
                    res.write(`event: sql\ndata: ${JSON.stringify({ sql: parsed.tool_results.content.json.sql })}\n\n`);
                  }
                  break;
                case 'response.thinking.delta':
                  if (parsed.text) res.write(`event: thinking\ndata: ${JSON.stringify({ text: parsed.text })}\n\n`);
                  break;
                case 'response.status':
                  res.write(`event: status\ndata: ${JSON.stringify({ status: parsed.status, message: parsed.message })}\n\n`);
                  break;
                case 'response.chart':
                  if (parsed.chart_spec) res.write(`event: chart\ndata: ${JSON.stringify({ chart_spec: parsed.chart_spec })}\n\n`);
                  break;
                case 'message.delta': {
                  const content = parsed.delta?.content;
                  if (Array.isArray(content)) {
                    for (const c of content) {
                      if (c.type === 'text' && c.text) {
                        res.write(`event: text\ndata: ${JSON.stringify({ text: c.text })}\n\n`);
                        sentText = true;
                      }
                    }
                  }
                  break;
                }
              }
            } catch { /* skip unparseable */ }
          }
        }
      }
    } finally {
      res.write('event: done\ndata: {}\n\n');
      res.end();
    }
  } catch (err) {
    console.error('Chat error:', err);
    if (!res.headersSent) res.status(500).json({ error: err.message });
  }
});

// ── Startup ───────────────────────────────────────────────────────────────────
const mode = sfConnected ? 'Snowflake connected (REST API)' : 'demo mode (mock data)';
if (!sfConnected) console.warn('SNOWFLAKE_PAT not set — running in demo mode with mock data.');

app.listen(PORT, () => {
  console.log(`RegTech API server running on http://localhost:${PORT} [${mode}]`);
});
