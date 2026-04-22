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
  const rawMetrics = [
    // IS
    {q:'Q1 2024',bl:'Institutional Securities',c:13.8,t:15.2,r:168.4,l:128.5,n:112.3},
    {q:'Q2 2024',bl:'Institutional Securities',c:14.1,t:15.6,r:172.1,l:131.2,n:114.7},
    {q:'Q3 2024',bl:'Institutional Securities',c:14.3,t:15.8,r:176.8,l:133.8,n:116.1},
    {q:'Q4 2024',bl:'Institutional Securities',c:14.6,t:16.1,r:184.7,l:135.4,n:118.3},
    {q:'Q1 2025',bl:'Institutional Securities',c:14.4,t:15.9,r:182.3,l:134.1,n:117.2},
    {q:'Q2 2025',bl:'Institutional Securities',c:14.2,t:15.7,r:179.5,l:132.7,n:115.8},
    {q:'Q3 2025',bl:'Institutional Securities',c:14.5,t:16.0,r:185.2,l:136.9,n:119.4},
    {q:'Q4 2025',bl:'Institutional Securities',c:12.1,t:13.8,r:243.6,l:118.3,n:103.7},
    // WM
    {q:'Q1 2024',bl:'Wealth Management',c:16.2,t:17.8,r:82.4,l:142.3,n:124.6},
    {q:'Q2 2024',bl:'Wealth Management',c:16.5,t:18.1,r:84.7,l:144.8,n:126.3},
    {q:'Q3 2024',bl:'Wealth Management',c:16.8,t:18.4,r:86.2,l:146.1,n:127.8},
    {q:'Q4 2024',bl:'Wealth Management',c:17.1,t:18.7,r:88.5,l:148.3,n:129.5},
    {q:'Q1 2025',bl:'Wealth Management',c:17.0,t:18.6,r:87.8,l:147.6,n:128.9},
    {q:'Q2 2025',bl:'Wealth Management',c:17.3,t:18.9,r:89.4,l:149.2,n:130.7},
    {q:'Q3 2025',bl:'Wealth Management',c:17.5,t:19.1,r:91.2,l:151.4,n:132.1},
    {q:'Q4 2025',bl:'Wealth Management',c:17.6,t:19.2,r:92.8,l:152.7,n:133.4},
    // IM
    {q:'Q1 2024',bl:'Investment Management',c:18.4,t:20.1,r:41.3,l:156.7,n:138.4},
    {q:'Q2 2024',bl:'Investment Management',c:18.7,t:20.4,r:42.1,l:158.3,n:140.1},
    {q:'Q3 2024',bl:'Investment Management',c:18.9,t:20.6,r:43.0,l:159.8,n:141.6},
    {q:'Q4 2024',bl:'Investment Management',c:19.2,t:20.9,r:44.2,l:161.5,n:143.2},
    {q:'Q1 2025',bl:'Investment Management',c:19.1,t:20.8,r:43.8,l:160.9,n:142.7},
    {q:'Q2 2025',bl:'Investment Management',c:19.4,t:21.1,r:44.7,l:162.4,n:144.3},
    {q:'Q3 2025',bl:'Investment Management',c:19.6,t:21.3,r:45.6,l:164.1,n:145.8},
    {q:'Q4 2025',bl:'Investment Management',c:19.8,t:21.5,r:46.4,l:165.3,n:147.2},
  ];

  const REV_MULT = { 'Institutional Securities': 38, 'Wealth Management': 62, 'Investment Management': 72 };

  const metrics = rawMetrics.map(m => ({
    quarter: m.q, businessLine: m.bl,
    cet1Ratio: m.c, tier1Ratio: m.t, rwa: m.r, lcr: m.l, nsfr: m.n,
    netRevenue: +(m.r * REV_MULT[m.bl]).toFixed(0),
  }));

  const anomalies = [
    {
      id: 'AF-IS-Q42025', metric: 'RWA', businessLine: 'Institutional Securities', quarter: 'Q4 2025',
      value: 243.6, expectedLow: 175.0, expectedHigh: 200.0,
      pctChange: 31.8, severity: 'Critical',
      description: 'RWA spike +31.8% QoQ: FRTB ES recalibration with equity vol regime shift.',
      descriptionFull: 'RWA spike +31.8% QoQ driven by FRTB Expected Shortfall recalibration combined with equity volatility regime shift. SA-CCR add-on increase on derivatives book due to higher replacement cost and PFE aggregation under new netting set rules. Institutional Securities desk-level capital requirement breached internal threshold of $200B.',
    },
  ];

  const variance = [
    { businessLine: 'Institutional Securities', metricName: 'RWA_TOTAL_B',  priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 185.2, currentValue: 243.6, changeAbs: 58.4, changePct: 31.5, driver: 'FRTB ES recalibration + SA-CCR derivatives add-on' },
    { businessLine: 'Institutional Securities', metricName: 'CET1_RATIO',   priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 14.5,  currentValue: 12.1,  changeAbs: -2.4,  changePct: -16.6, driver: 'RWA denominator increase from FRTB' },
    { businessLine: 'Institutional Securities', metricName: 'LCR_RATIO',    priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 136.9, currentValue: 118.3, changeAbs: -18.6, changePct: -13.6, driver: 'Increased net outflows under FRTB stress scenario' },
    { businessLine: 'Wealth Management',        metricName: 'RWA_TOTAL_B',  priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 91.2,  currentValue: 92.8,  changeAbs: 1.6,   changePct: 1.8,   driver: 'Normal loan book growth' },
    { businessLine: 'Wealth Management',        metricName: 'CET1_RATIO',   priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 17.5,  currentValue: 17.6,  changeAbs: 0.1,   changePct: 0.6,   driver: 'Retained earnings accumulation' },
    { businessLine: 'Investment Management',    metricName: 'RWA_TOTAL_B',  priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 45.6,  currentValue: 46.4,  changeAbs: 0.8,   changePct: 1.8,   driver: 'AUM-driven credit exposure growth' },
    { businessLine: 'Investment Management',    metricName: 'CET1_RATIO',   priorQuarter: 'Q3 2025', currentQuarter: 'Q4 2025', priorValue: 19.6,  currentValue: 19.8,  changeAbs: 0.2,   changePct: 1.0,   driver: 'Retained earnings accumulation' },
  ];

  return { metrics, anomalies, variance };
})();

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
      sfQuery(`SELECT DOC_ID, FRAMEWORK, TITLE, CHAPTER, VERSION, EFFECTIVE_DATE, STATUS, PAGE_COUNT FROM ${DB}.${SCHEMA}.REGULATORY_DOCUMENTS ORDER BY FRAMEWORK, EFFECTIVE_DATE`),
      sfQuery(`SELECT r.REQ_ID, r.DOC_ID, r.CATEGORY, r.REQUIREMENT, r.THRESHOLD, r.SEVERITY, r.IMPACT_AREA, d.VERSION AS DOC_VERSION, d.FRAMEWORK FROM ${DB}.${SCHEMA}.EXTRACTED_REQUIREMENTS r JOIN ${DB}.${SCHEMA}.REGULATORY_DOCUMENTS d ON r.DOC_ID = d.DOC_ID ORDER BY CASE r.SEVERITY WHEN 'Critical' THEN 0 WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END`),
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
          name:          DOC_DISPLAY_NAMES[d.TITLE] || d.TITLE,
          version:       d.VERSION,
          effectiveDate: effDate,
          pageCount:     approxPages,
          framework:     d.FRAMEWORK,
          status:        d.STATUS,
          summary:       null,
        };
      }),
      requirements: reqs.map((r) => ({
        id:             r.REQ_ID,
        docId:          r.DOC_ID,
        docVersion:     r.DOC_VERSION,
        ruleName:       r.CATEGORY,
        ruleSection:    r.THRESHOLD,
        oldRequirement: null,
        newRequirement: r.REQUIREMENT,
        impactedReport: r.IMPACT_AREA,
        severity:       r.SEVERITY,
        changeType:     (r.FRAMEWORK || '').includes('Basel IV') ? 'New Requirement' : 'Updated',
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
      sfQuery(`SELECT FLAG_ID, QUARTER, BUSINESS_LINE, RWA_ACTUAL_B, RWA_EXPECTED_B, IS_ANOMALY, ANOMALY_SCORE, ANOMALY_REASON FROM ${DB}.${SCHEMA}.ANOMALY_FLAGS WHERE IS_ANOMALY = TRUE ORDER BY ANOMALY_SCORE DESC`),
      sfQuery(`SELECT QUARTER, BUSINESS_LINE, METRIC, CURRENT_VALUE, PRIOR_VALUE, QOQ_CHANGE_PCT, STATUS FROM ${DB}.${SCHEMA}.VARIANCE_ANALYSIS WHERE QUARTER = 'Q4 2025' ORDER BY BUSINESS_LINE`),
    ]);

    const DRIVER_MAP = {
      'Institutional Securities': { RWA_TOTAL_B: 'FRTB ES recalibration + SA-CCR derivatives add-on', CET1_RATIO: 'RWA denominator increase from FRTB', LCR_RATIO: 'Increased stress outflows under FRTB' },
      'Wealth Management':        { RWA_TOTAL_B: 'Normal loan book growth', CET1_RATIO: 'Retained earnings accumulation', LCR_RATIO: 'Stable liquidity position' },
      'Investment Management':    { RWA_TOTAL_B: 'AUM-driven credit exposure growth', CET1_RATIO: 'Retained earnings accumulation', LCR_RATIO: 'Stable liquidity position' },
    };

    res.json({
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
        const full = clean.length > 300 ? clean.slice(0, 297) + '…' : clean;
        return {
          id:              a.FLAG_ID,
          metric:          'RWA',
          businessLine:    a.BUSINESS_LINE,
          quarter:         a.QUARTER,
          value:           Number(a.RWA_ACTUAL_B),
          expectedLow:     Number(a.RWA_EXPECTED_B) * 0.88,
          expectedHigh:    Number(a.RWA_EXPECTED_B) * 1.12,
          pctChange:       a.RWA_EXPECTED_B ? +((a.RWA_ACTUAL_B - a.RWA_EXPECTED_B) / a.RWA_EXPECTED_B * 100).toFixed(1) : 0,
          severity:        Number(a.ANOMALY_SCORE) > 0.97 ? 'Critical' : 'High',
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
        docName:      item.TITLE,
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

// ── POST /api/chat (SSE streaming via Cortex Agent) ───────────────────────────
app.post('/api/chat', async (req, res) => {
  const { message, history = [] } = req.body;
  if (!message) return res.status(400).json({ error: 'message required' });

  const account = process.env.SNOWFLAKE_ACCOUNT;
  const pat     = process.env.SNOWFLAKE_PAT;

  if (!pat) {
    return res.status(503).json({
      error: 'SNOWFLAKE_PAT not configured. Add your PAT to .env to enable the AI chatbot.',
    });
  }

  const url = `https://${account}.snowflakecomputing.com/api/v2/databases/${DB}/schemas/${SCHEMA}/agents/REGTECH_ANALYTICS_AGENT:run`;

  const messages = [
    ...history.map((m) => ({
      role:    m.role,
      content: [{ type: 'text', text: m.text }],
    })),
    { role: 'user', content: [{ type: 'text', text: message }] },
  ];

  try {
    const agentRes = await fetch(url, {
      method:  'POST',
      headers: {
        'Content-Type':  'application/json',
        'Accept':        'text/event-stream',
        'Authorization': `Bearer ${pat}`,
        'X-Snowflake-Authorization-Token-Type': 'PROGRAMMATIC_ACCESS_TOKEN',
      },
      body: JSON.stringify({ messages, stream: true }),
    });

    if (!agentRes.ok) {
      const errText = await agentRes.text();
      console.error('Agent API error:', agentRes.status, errText);
      return res.status(500).json({ error: `Agent returned ${agentRes.status}: ${errText}` });
    }

    res.setHeader('Content-Type',      'text/event-stream');
    res.setHeader('Cache-Control',     'no-cache, no-transform');
    res.setHeader('Connection',        'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.socket?.setNoDelay(true);

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
