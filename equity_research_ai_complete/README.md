# Equity Research — AI_COMPLETE Document Intelligence

## Overview

This demo shows how to use Snowflake's **AI_COMPLETE** function to process equity research PDFs directly from a Snowflake stage — no parsing pipelines, no text extraction, no intermediate tables.

With AI_COMPLETE's document intelligence capability, you point a model directly at PDF files and ask questions, extract structured data, compare documents, and analyze charts.

## Key Capabilities

| Capability | How |
|-----------|-----|
| Direct PDF processing | `TO_FILE('@stage', 'file.pdf')` inside `PROMPT()` |
| Chart/graph analysis | Model reads visual elements natively from PDFs |
| Guaranteed JSON output | `response_format` with JSON schema — no regex, no `TRY_PARSE_JSON` |
| Multi-document reasoning | Up to 5 docs per prompt (Claude) or 20 (Gemini) |
| Batch extraction at scale | `DIRECTORY(@stage)` + `AI_COMPLETE` over all files |
| PDF → relational table | One `CREATE TABLE AS` query, zero ETL pipelines |

## Prerequisites

1. **Snowflake Account** with Cortex AI enabled
2. **Role** with the `SNOWFLAKE.CORTEX_USER` database role granted
3. **Warehouse** available for compute (the notebook uses `AICOLLEGE` — change to yours)

## Quick Start

### Step 1: Run Setup SQL

Open `setup.sql` in a Snowflake worksheet and execute it. This creates:
- Database `FSI_DEMO_DB`
- Schema `EQUITY_RESEARCH`
- Stage `REPORTS` with server-side encryption (required for AI_COMPLETE)

```sql
-- Run in a Snowflake worksheet
@setup.sql
```

### Step 2: Upload Research PDFs

Upload equity research PDFs to the `@REPORTS` stage. See [`sample_data/README.md`](sample_data/README.md) for sample files.

**Option A — Snowsight UI:**
Navigate to Data → Databases → FSI_DEMO_DB → EQUITY_RESEARCH → Stages → REPORTS → click "+ Files"

**Option B — SnowSQL / PUT:**
```sql
PUT file:///path/to/your/pdfs/*.pdf @FSI_DEMO_DB.EQUITY_RESEARCH.REPORTS AUTO_COMPRESS=FALSE;
```

**Option C — Snowflake CLI:**
```bash
snow stage copy "your_report.pdf" @FSI_DEMO_DB.EQUITY_RESEARCH.REPORTS --overwrite
```

### Step 3: Refresh Stage Directory
```sql
ALTER STAGE FSI_DEMO_DB.EQUITY_RESEARCH.REPORTS REFRESH;
```

### Step 4: Open the Notebook

Import `equity_research.ipynb` into Snowflake Notebooks (Snowsight → Projects → Notebooks → Import .ipynb) and run all cells.

The notebook walks through:
1. Exploring research reports on stage
2. Single-document Q&A directly from PDFs
3. Chart and visualization analysis
4. Structured data extraction with guaranteed JSON output
5. Multi-document comparison and cross-referencing
6. Batch processing all documents at scale
7. Building an analytics-ready table

## Architecture

```
┌─────────────────┐     ┌──────────────────────────┐     ┌─────────────────┐
│  Research PDFs  │────▶│  @REPORTS Stage (SSE)    │────▶│  AI_COMPLETE    │
│  (uploaded)     │     │  DIRECTORY() metadata    │     │  + TO_FILE()    │
└─────────────────┘     └──────────────────────────┘     └────────┬────────┘
                                                                   │
                                                                   ▼
                                                         ┌─────────────────┐
                                                         │  Structured     │
                                                         │  JSON / Table   │
                                                         └─────────────────┘
```

## Key Requirements

- Stage **must** use server-side encryption: `ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')`
- Supported file types: `.pdf`, `.txt`, `.md` (all models) + `.doc`, `.docx`, `.xls`, `.xlsx`, `.csv`, `.xhtml` (Claude models)
- User must have the `SNOWFLAKE.CORTEX_USER` database role and READ access to the stage
