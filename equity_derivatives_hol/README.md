# Equity Derivatives — RAG Hands-On Lab

End-to-end RAG pipeline for an equity derivatives sales desk, combining ISDA legal document intelligence with multimodal equity research and a 6-tool Cortex Agent.

## Overview

This hands-on lab covers two document pipelines and a Cortex Agent:

| Document Type | Processing | Search Strategy |
|---|---|---|
| **ISDA Legal** (master agreements + amendments) | `AI_EXTRACT` → structured table + `AI_PARSE_DOCUMENT` (page-split) → Cortex Search | Attribute filtering (party, doc type, governing law, page number) |
| **Equity Research** (individual equity + macro/thematic reports) | `AI_COMPLETE` with `TO_FILE()` → text + charts + tables + voyage-multimodal-3 image embeddings | Multi-index CSS (text + user-provided vectors), numeric boost, time decay, stored procedure custom tool |

## Key Capabilities

| Capability | How |
|---|---|
| Structured clause extraction | `AI_EXTRACT` with 15 ISDA-specific questions + confidence scores |
| Page-level document search | `AI_PARSE_DOCUMENT` with `page_split: true` → per-page Cortex Search |
| Direct PDF multimodal processing | `AI_COMPLETE` reads charts, tables, and visual elements natively |
| Works for both equity and macro reports | Enriched JSON schema with `report_type`, `covered_tickers`, `key_themes` |
| True cross-modal retrieval | `AI_EMBED('voyage-multimodal-3', ...)` aligns text and image vectors in the same space |
| Custom tool stored procedure | Stored procedure embeds query text with voyage-multimodal-3 → `multi_index_query` |
| Numeric boost + time decay | `view_count` multiplier + `publish_date` decay in Cortex Search |
| 6-tool Cortex Agent | ISDA structured, ISDA text, equity research (stored proc), stock prices, SEC filings, FX rates |
| Agent evaluation | Ground truth dataset + 3 custom metrics (`tool_selection`, `citation_quality`, `financial_accuracy`) |

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     EQ_DERIVATIVES_AGENT                             │
│            (claude-sonnet-4-5, 180s budget, 100k tokens)            │
└──────┬──────────┬──────────────┬────────────┬──────────┬────────────┘
       │          │              │            │          │
  AgreementTerms  DocumentSearch  EquityResearch MarketData SECFilings + FXRates
  (Analyst)      (Search)        (Stored Proc) (Analyst) (Analyst)
       │          │              │            │          │
  Semantic View  CSS w/          voyage-mul-3 Semantic   Semantic
  on ISDA table  page-split attrs multi_index  View on    Views on
                                 CSS + boost   V_STOCK    V_SEC_FIN
                                 + decay       _PRICES    ANCIALS

Part 1: Document Intelligence                Part 2: Agent + Evaluation
─────────────────────────────────────────    ──────────────────────────
@ISDA_DOCS ─→ AI_EXTRACT ─→ ISDA_TERMS      Marketplace data → views
           ─→ AI_PARSE_DOCUMENT ─→ CSS       → Semantic Views
                                             → Stored Procedure (voyage)
@EQUITY_RESEARCH ─→ AI_COMPLETE ─→ table    → CREATE AGENT
              ─→ PyMuPDF ─→ PNG images       → RBAC grants
              ─→ AI_EMBED(voyage) ─→ vectors → Ground truth dataset
              ─→ Cortex Search Services       → EXECUTE_AI_EVALUATION
```

## Prerequisites

1. **Snowflake account** with Cortex AI access
2. **Role** with `SNOWFLAKE.CORTEX_USER` database role granted
3. **ACCOUNTADMIN or SYSADMIN** for warehouse creation and RBAC grants
4. **Warehouse** — the notebooks create `COMPUTE_WH (MEDIUM)` if it doesn't exist
5. **Marketplace listing** — [Snowflake Public Data (Free)](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255) installed
6. **PDF documents** to upload — see [Document Requirements](#document-requirements) below

## Quick Start

### Part 1 — Run `01_document_intelligence.ipynb`

1. Import the notebook into Snowflake Notebooks (Snowsight → Workspaces → Import .ipynb)
2. **Set up PyPI access** — in Edit Service, select Artifact Repository: `snowflake.snowpark.pypi_shared_repository` (needed for `!pip install pymupdf`)
3. Upload documents to the stages created in Cell 1 (see [Document Requirements](#document-requirements))
4. Run all cells sequentially — each cell builds on the previous

**What gets built:**

| Cell | Output |
|---|---|
| `isda_extract` | `ISDA_AGREEMENT_TERMS` table — 15 structured fields per agreement |
| `isda_parse_search` | `ISDA_PARSED_DOCUMENTS` table — page-level text |
| `create_cortex_search` | `ISDA_DOCUMENT_SEARCH` CSS — attribute-filtered search |
| `equity_extract` | `EQUITY_RESEARCH_EXTRACTED` table — ratings, themes, chart descriptions |
| `equity_readership` | `EQUITY_RESEARCH_READERSHIP` table — synthetic view counts for boosting |
| `render_chart_images` | PNG page renders uploaded to `@CHART_IMAGES` |
| `image_embeddings` | `CHART_IMAGE_EMBEDDINGS` — voyage-multimodal-3 vectors |
| `create_equity_search` | `EQUITY_RESEARCH_SEARCH` CSS — user-provided voyage vectors |
| `equity_voyage_base` | `EQUITY_RESEARCH_VOYAGE_BASE` — report-level voyage embeddings |
| `create_agent_search` | `EQUITY_RESEARCH_SEARCH_AGENT` CSS — managed embeddings for agent |

### Part 2 — Run `02_agent_and_evaluation.ipynb`

1. Import into Snowflake Notebooks
2. Run cells in order: marketplace views → semantic views → stored procedure → agent → RBAC → evaluation

**Steps:**

| Step | What it does |
|---|---|
| Step 1 | Create `V_STOCK_PRICES`, `V_SEC_FINANCIALS`, `V_FX_RATES` from marketplace |
| Step 2 | Create 4 semantic views (ISDA, stock, SEC, FX) via `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` |
| Step 3a | Create `EQUITY_RESEARCH_MULTIMODAL_SEARCH` stored procedure (voyage-multimodal-3 multi_index_query) |
| Step 3 | Deploy `EQ_DERIVATIVES_AGENT` with 6 tools |
| Step 4a | RBAC grants — PUBLIC access to all objects + PyPI EAI setup |
| Steps 5–7 | Build evaluation dataset → register → run `EXECUTE_AI_EVALUATION` |

## Document Requirements

### ISDA Documents (`@ISDA_DOCS` stage)

Upload ISDA Master Agreement PDFs and any amendments. The pipeline handles:
- 1992 and 2002 ISDA Master Agreements
- Credit Support Annexes (CSAs)
- Amendments (any filename containing "amendment")

### Equity Research PDFs (`@EQUITY_RESEARCH` stage)

Upload any combination of:
- **Individual equity research** — earnings reviews, initiations, risk alerts (filename convention: `YYYY-MM-DD_TICKER_description.pdf` enables auto-extraction of ticker and date)
- **Thematic/macro research** — Goldman Sachs-style sector reports, economic analysis, industry surveys (AI extracts topic, themes, and charts automatically)

Upload via Snowsight: Data → Databases → `CORTEX_AI_HOL` → `RAG_PIPELINE` → Stages → click the stage → "+ Files"

## Agent Sample Queries

```
-- ISDA structured
"What is the cross-default threshold for the Barclays agreement and what currency?"

-- ISDA text search
"What does the agreement say about automatic early termination?"

-- Equity research (multimodal — finds charts too)
"What is the rating and price target for NVIDIA?"

-- Cross-tool: price vs target
"Is NVIDIA trading above or below its analyst price target?"

-- Thematic research
"What does Goldman Sachs say about autonomous vehicles?"

-- FX conversion
"Convert the $50M cross-default threshold in our Bank of America agreement to GBP"
```

## Cortex Search Scoring Features Demonstrated

| Feature | Parameter | Use case |
|---|---|---|
| Numeric boost | `"boosts": {"view_count": {"boost_by": "multiplier"}}` | Popular reports rank higher |
| Time decay | `"decays": {"publish_date": {"decay_speed": "fast"}}` | Recent research prioritized |
| Component weights | `"scoring": {"component_weights": {"text_weight": 0.3, "vector_weight": 0.7}}` | Visual queries prefer image vectors |
| Attribute filter | `"filter": {"@eq": {"rating": "Overweight"}}` | Filter by rating, ticker, report type |
| Multi-index query | `"multi_index_query": {"searchable_text": {...}, "voyage_embedding": {...}}` | Text + image vector combined search |

## Key Technical Notes

- Stage **must** use `ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')` for all AI functions
- `voyage-multimodal-3` embeds both text and images in the same 1024-dim vector space — a text query can semantically retrieve visually similar chart content
- The stored procedure pattern (`generic` tool type) is required when Cortex Search uses user-provided vectors — the agent needs to embed the query at call time
- `GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE SYSADMIN` (or your active role) is required before running AI functions
- The `execution_environment: {type: warehouse, warehouse: "..."}` block is required for all `cortex_analyst_text_to_sql` tools in the agent spec
