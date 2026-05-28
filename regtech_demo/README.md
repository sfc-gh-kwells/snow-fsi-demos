# RegTech AI Platform Demo

An AI-powered regulatory technology platform built on Snowflake Cortex. Demonstrates how Cortex AI services (COMPLETE, Search, Agents, ML Anomaly Detection) can automate balance sheet analytics, regulatory document intelligence, and pipeline compliance auditing for banking institutions.

## Architecture

```
React UI (Vite)  <-->  Express API (server.js)  <-->  Snowflake REST APIs
                                                        ├─ SQL API (data queries)
                                                        ├─ Cortex Search (regulation search)
                                                        └─ Cortex Agent (AI chatbot)
```

**Snowflake services used:**
- `SNOWFLAKE.CORTEX.COMPLETE()` — requirement extraction and anomaly explanation
- `SNOWFLAKE.ML.ANOMALY_DETECTION` — RWA anomaly detection across business lines
- Cortex Search Service — semantic search over 12 CFR banking regulations
- Semantic View — structured access to balance sheet and audit data
- Cortex Agent — orchestrated AI analyst combining text-to-SQL and search

## Prerequisites

- Snowflake account with Cortex AI enabled
- [Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (`snow`) installed
- Node.js 18+
- Python 3.10+
- A Snowflake [programmatic access token](https://docs.snowflake.com/en/user-guide/admin-programmatic-access-token) (PAT)

## Setup

### 1. Configure Snowflake connection

Add a connection to `~/.snowflake/connections.toml`:

```toml
[MY_DEMO]
account = "myorg-myaccount"
user = "myuser"
role = "SYSADMIN"
warehouse = "COMPUTE_WH"
database = "REGTECH_DEMO_DB"
schema = "REGULATORY_REPORTING"
authenticator = "snowflake"
```

### 2. Provision Snowflake objects (notebook)

Open `regtech_demo_setup.ipynb` in Snowsight and run cells in order:

1. **Steps 1-2**: Create database, schema, and tables
2. **Step 3 (Cell 6)**: Create stage and file format
3. **Step 3a (local)**: Fetch regulations and upload to stage (see below)
4. **Steps 3b-7**: Load data, extract requirements, train ML model, compute variance
5. **Step 8 (local)**: Run audit pipeline (see below)
6. **Steps 9-11**: Create Cortex Search, Semantic View, and Agent
7. **Step 12**: Verify row counts

### 3. Run local setup steps

```bash
cd regtech_demo

# Fetch eCFR regulations and upload to Snowflake stage
bash local_setup.sh
```

After running Steps 3b-7 in the notebook:

```bash
# Run the agent-powered audit pipeline
python3 audit_pipeline.py --connection MY_DEMO --force
```

The audit pipeline requires `cortex_code_agent_sdk`:

```bash
python3 -m venv venv && source venv/bin/activate
pip install cortex-code-agent-sdk
```

### 4. Start the web application

```bash
# Install dependencies
npm install

# Create .env from template
cp .env.example .env
# Edit .env with your Snowflake account, user, and PAT

# Start the dev server (Express API + Vite React)
npm run server
# In a separate terminal:
npm run dev
```

Open the URL printed by Vite (default: `http://localhost:5173`).

## Tabs

| Tab | Description |
|---|---|
| **Balance Sheet Analytics** | KPI cards, RWA trend chart, QoQ variance, ML anomaly flags with AI-generated explanations |
| **Document Intelligence** | Cortex Search over 12 CFR regulations, extracted requirements table |
| **Pipeline Audit** | Agent-powered compliance findings across 8 SQL pipelines, severity breakdown |
| **AI Chatbot** | Cortex Agent combining text-to-SQL (Analyst) and regulation search |

## Project Structure

```
regtech_demo/
├── server.js                  # Express API backend (Snowflake REST + Cortex APIs)
├── src/
│   ├── App.jsx                # Tab layout
│   ├── Chatbot.jsx            # Agent chatbot component
│   └── tabs/
│       ├── BalanceSheetAnalytics.jsx
│       ├── DocumentIntelligence.jsx
│       └── PipelineAudit.jsx
├── pipelines/                 # 8 SQL pipelines with intentional compliance gaps
├── regulations/               # eCFR text files (fetched by fetch_regulations.py)
├── regtech_demo_setup.ipynb   # Snowflake provisioning notebook
├── fetch_regulations.py       # Downloads regulations from eCFR API
├── audit_pipeline.py          # Agent-powered audit (cortex_code_agent_sdk)
├── local_setup.sh             # Stage upload helper
├── .env.example               # Environment variable template
└── package.json
```

## Mock Mode

When `SNOWFLAKE_PAT` is not set in `.env`, the server returns synthetic mock data for all endpoints. This allows UI development without a live Snowflake connection.

## Data

All data is synthetic. The balance sheet metrics, anomaly thresholds, and audit findings are illustrative and do not represent any real institution.

Regulations are fetched from the public [eCFR API](https://www.ecfr.gov/developer/documentation/api/v1) (Title 12, Parts 3 and 50).
