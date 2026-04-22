# ISDA Document Intelligence POC

## Overview

This POC demonstrates using **Snowflake Cortex AI** to extract structured information from ISDA (International Swaps and Derivatives Association) Master Agreements and build a knowledge graph for document intelligence.
ZOOM CLIP DEMO: https://snowflake.zoom.us/clips/share/beoOeLeRS-ioGl5gXUqOVg
### Key Capabilities

- **PARSE_DOCUMENT**: Extract full text from PDF documents
- **AI_EXTRACT**: Structured extraction of ISDA fields including:
  - Core agreement info (version, dates, parties, governing law)
  - Events of Default with trigger types and unwinding mechanisms
  - Termination Events with affected parties and waiting periods
  - Early termination provisions (automatic vs optional, payment method)
- **Knowledge Graph**: Track relationships between documents, parties, and clauses
- **Cortex Agent**: Natural language queries with semantic models and RAG search
- **Streamlit App**: Middle office portal for document upload and chat interface

## Document Types Supported

| Document Type | Description |
|--------------|-------------|
| ISDA Master Agreement (1992) | Standard 1992 version with Market Quotation/Loss |
| ISDA Master Agreement (2002) | Updated 2002 version with Close-out Amount |
| Credit Support Annex (CSA) | Collateral and margin provisions |
| Amendments | Modifications to base agreements |

## Prerequisites

1. **Snowflake Account** with Cortex AI enabled
2. **Role** with privileges to:
   - CREATE DATABASE, SCHEMA, STAGE, TABLE, VIEW, PROCEDURE
   - Use SNOWFLAKE.CORTEX functions (PARSE_DOCUMENT, AI_EXTRACT, COMPLETE)
   - CREATE CORTEX SEARCH SERVICE
   - CREATE AGENT
3. **Snowflake CLI** (`snow`) for uploading documents

## Quick Start

### Step 1: Setup Infrastructure
```sql
-- Run the setup script to create database, schema, stage, and tables
@sql/01_setup_infrastructure.sql
```

### Step 2: Upload Documents
```bash
# Upload PDFs using Snowflake CLI
snow stage copy "ISDA Master Agreement - example 1.pdf" \
    @ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS \
    --connection YOUR_CONNECTION --overwrite

# Or upload the real_docs folder contents
cd real_docs
for f in *.pdf; do
    snow stage copy "$f" @ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS --overwrite
done
```

### Step 3: Load Document Metadata
```sql
@sql/02_load_documents.sql
```

### Step 4: Parse Documents
```sql
-- This creates procedures and parses documents using PARSE_DOCUMENT
@sql/03_parse_documents.sql

-- Or call the batch procedure:
CALL PARSE_ALL_PENDING_DOCUMENTS();
```

### Step 5: Extract ISDA Fields
```sql
-- This uses AI_EXTRACT for structured extraction
@sql/04_extract_isda_fields.sql

-- Or call the batch procedure:
CALL EXTRACT_ALL_PENDING_DOCUMENTS();
```

### Step 6: Build Knowledge Graph
```sql
@sql/05_populate_knowledge_graph.sql
```

### Step 7: Create Views
```sql
@sql/06_create_views.sql
```

### Step 8: Run Demo Queries
```sql
@sql/07_demo_queries.sql
```

### Step 9: Add Synthetic Documents & Resolution Logic
```sql
-- Creates CSA and amendments to demonstrate multi-document resolution
@sql/08_synthetic_docs_and_resolution.sql
```

### Step 10: Create Cortex Agent
```sql
-- Creates semantic views, search service, and Cortex Agent
@sql/09_create_cortex_agent.sql
```

### Step 11: Create Party Name Search Service
```sql
-- Creates search service for fuzzy party name matching
@sql/11_party_name_search_service.sql
```

### Step 12: Run Data Quality Checks
```sql
@sql/10_data_quality_checks.sql
```

## Stored Procedures

The POC creates reusable stored procedures for the document processing pipeline:

### Parsing Procedures
| Procedure | Purpose |
|-----------|---------|
| `PARSE_SINGLE_DOCUMENT(document_id)` | Parse one document using PARSE_DOCUMENT |
| `PARSE_ALL_PENDING_DOCUMENTS()` | Batch parse all documents with status 'UPLOADED' |

### Extraction Procedures
| Procedure | Purpose |
|-----------|---------|
| `EXTRACT_ISDA_FIELDS_FOR_DOCUMENT(document_id)` | Extract core fields using AI_EXTRACT |
| `EXTRACT_EVENTS_OF_DEFAULT(document_id)` | Extract Section 5(a) events |
| `EXTRACT_TERMINATION_EVENTS(document_id)` | Extract Section 5(b) events |
| `EXTRACT_SINGLE_DOCUMENT(document_id)` | Full extraction pipeline for one document |
| `EXTRACT_ALL_PENDING_DOCUMENTS()` | Batch extract all parsed documents |

### Usage Example
```sql
-- Process a new document
INSERT INTO RAW_DOCUMENT_METADATA (DOCUMENT_ID, FILE_NAME, FILE_PATH, DOCUMENT_TYPE, PROCESSING_STATUS)
VALUES (UUID_STRING(), 'new_agreement.pdf', '@ISDA_DOCUMENTS/new_agreement.pdf', 'MASTER_AGREEMENT', 'UPLOADED');

-- Parse it
CALL PARSE_SINGLE_DOCUMENT('<document_id>');

-- Extract ISDA fields
CALL EXTRACT_SINGLE_DOCUMENT('<document_id>');
```

## Schema Overview

### Core Tables

| Table | Purpose |
|-------|---------|
| `RAW_DOCUMENT_METADATA` | Tracks uploaded documents and processing status |
| `DOCUMENT_FULL_TEXT` | Stores parsed text from PARSE_DOCUMENT |
| `EXTRACTED_ISDA_MASTER` | Structured extraction results |
| `EXTRACTED_CSA` | Credit Support Annex details |
| `EXTRACTED_AMENDMENTS` | Amendment history with supersession tracking |

### Knowledge Graph Tables

| Table | Purpose |
|-------|---------|
| `DOCUMENT_NODES` | Nodes: documents, parties, events, clauses |
| `DOCUMENT_EDGES` | Relationships: PARTY_TO, CONTAINS, AMENDS |
| `CLAUSE_VERSIONS` | Track clause changes across amendments |
| `COUNTERPARTY_RELATIONSHIPS` | Party-to-party agreement tracking |

### Views

| View | Purpose |
|------|---------|
| `V_EVENT_TERMINATION_SUMMARY` | What happens for each event type |
| `V_COUNTERPARTY_EXPOSURE` | All agreements by counterparty pair |
| `V_CROSS_DEFAULT_COMPARISON` | Cross-default thresholds comparison |
| `V_AUTOMATIC_EARLY_TERMINATION` | AET status by party |
| `V_DOCUMENT_GRAPH` | Knowledge graph traversal |
| `V_PARTY_PORTFOLIO` | All agreements for a party |
| `V_CURRENT_AGREEMENT_TERMS` | Amendment-resolved current values |

## Extracted Fields

### Core Agreement Info
- Agreement Version (1992 vs 2002)
- Effective Date
- Party A/B Name and Type
- Governing Law

### Events of Default (Section 5(a))
For each event:
- Event Type (Bankruptcy, Cross Default, Failure to Pay, etc.)
- Trigger Type (automatic vs optional)
- Grace Period
- Applicable To (Party A, Party B, or both)
- Unwinding Mechanism

### Termination Events (Section 5(b))
For each event:
- Event Type (Illegality, Tax Event, Force Majeure, etc.)
- Trigger Type
- Affected Party
- Waiting Period
- Transfer Option
- Unwinding Mechanism

### Early Termination Provisions
- Automatic Early Termination (by party)
- Payment Method (First Method vs Second Method)
- Close-out Calculation (Market Quotation/Loss vs Close-out Amount)
- Set-off Rights

## AI_EXTRACT vs AI_COMPLETE

This POC uses **AI_EXTRACT** instead of AI_COMPLETE for structured extraction:

```sql
-- AI_EXTRACT approach (cleaner, more reliable)
SELECT SNOWFLAKE.CORTEX.AI_EXTRACT(
    document_text,
    [
        'agreement_version: Is this 1992 or 2002?',
        'party_a_name: What is Party A name?',
        'cross_default_applicable: Is cross-default applicable?'
    ]
);

-- Returns structured JSON:
-- {"response": {"agreement_version": "2002", "party_a_name": "Bank of America", ...}}
```

**Benefits of AI_EXTRACT:**
- Purpose-built for document extraction
- Structured questions → structured answers
- More consistent JSON output
- Better handling of missing fields

## Files in this Repository

```
finance-otc-docs/
├── sql/
│   ├── 01_setup_infrastructure.sql      # Database, schema, stage, tables
│   ├── 02_load_documents.sql            # Stage refresh, metadata insert
│   ├── 03_parse_documents.sql           # PARSE_DOCUMENT procedures
│   ├── 04_extract_isda_fields.sql       # AI_EXTRACT procedures
│   ├── 05_populate_knowledge_graph.sql  # Nodes, edges, relationships
│   ├── 06_create_views.sql              # Analytical views
│   ├── 07_demo_queries.sql              # Sample queries and demos
│   ├── 08_synthetic_docs_and_resolution.sql # Amendment resolution logic
│   ├── 09_create_cortex_agent.sql       # Cortex Agent with semantic views
│   ├── 10_data_quality_checks.sql       # Data integrity validation
│   └── 11_party_name_search_service.sql # Fuzzy party name matching
├── semantic_models/
│   ├── isda_agreement_terms.yaml        # Semantic model for agreement queries
│   └── isda_knowledge_graph.yaml        # Semantic model for graph navigation
├── real_docs/                           # Actual ISDA PDFs for processing
│   └── *.pdf
├── synthetic_docs/                      # Generated test documents
│   ├── CSA_BofA_LKQ_2024.txt
│   ├── Amendment_1_Barclays_WorldOmni_2024.txt
│   └── Amendment_2_Barclays_WorldOmni_2024.txt
├── README.md                            # This file
└── DATA_CATALOG.md                      # Complete schema documentation & ERD
```

## Amendment Resolution Logic

The key complexity this POC demonstrates is resolving conflicting terms across multiple agreements. The principle is **"later supersedes earlier"**.

### How It Works

1. **EXTRACTED_AMENDMENTS** table tracks:
   - Which master agreement the amendment modifies
   - Effective date for temporal ordering
   - Which prior amendment it supersedes (if any)
   - New values for modified terms

2. **V_CURRENT_AGREEMENT_TERMS** view resolves conflicts:
   ```sql
   ROW_NUMBER() OVER (
       PARTITION BY PARENT_MASTER_AGREEMENT_ID 
       ORDER BY EFFECTIVE_DATE DESC
   ) as AMENDMENT_RANK
   ```

3. **COALESCE pattern** picks the right value:
   ```sql
   COALESCE(
       latest_amendment.NEW_CROSS_DEFAULT_THRESHOLD,
       original_master.CROSS_DEFAULT_THRESHOLD
   ) as CURRENT_CROSS_DEFAULT_THRESHOLD
   ```

### Example Resolution

For Barclays/World Omni:
- **Original Agreement**: Cross-default threshold = $1,000,000
- **Amendment 1 (2024-03-01)**: Changes to $10,000,000
- **Amendment 2 (2024-09-15)**: Supersedes Amendment 1, changes to $25,000,000
- **Current Value**: $25,000,000 (from Amendment 2)

## Cortex Agent Architecture

The agent (script 09) combines three tools:

| Tool | Type | Purpose |
|------|------|---------|
| `AgreementTerms` | Cortex Analyst | Query structured agreement data with amendment resolution |
| `KnowledgeGraph` | Cortex Analyst | Navigate document/party/event relationships |
| `DocumentSearch` | Cortex Search | RAG over raw document text |

### Party Name Search Integration

The semantic model uses **PARTY_NAME_SEARCH** Cortex Search Service for fuzzy party matching. This enables queries like "Barclays" to correctly match "BARCLAYS BANK PLC".

```yaml
# In isda_agreement_terms.yaml
- name: party_a
  expr: PARTY_A
  cortex_search_service:
    service: PARTY_NAME_SEARCH
    literal_column: PARTY_NAME
    database: ISDA_DOCUMENT_POC
    schema: SEMANTIC_VIEWS
```

### Sample Agent Queries

```
# Structured queries (routes to AgreementTerms)
"What is the current cross-default threshold for Barclays?"
"Which agreements have automatic early termination enabled?"

# Relationship queries (routes to KnowledgeGraph)
"Which documents is Bank of America party to?"
"Show all amendments for the Barclays agreement"

# Document search (routes to DocumentSearch)
"Find the definition of Early Termination Date"
"Search for bankruptcy provisions in Section 5"
```

## Key Differences: 1992 vs 2002 ISDA

| Feature | 1992 ISDA | 2002 ISDA |
|---------|-----------|-----------|
| Close-out | Market Quotation or Loss | Close-out Amount |
| Force Majeure | Not included | Section 5(b)(ii) |
| Calculation | Two-step (quotation then fallback) | Single method |
| Set-off | Limited | Enhanced |

## Troubleshooting

### "Function not found" errors
Ensure Cortex AI is enabled in your account and you have access to SNOWFLAKE.CORTEX functions.

### Parsing failures
- Check document is a valid PDF
- Ensure stage has DIRECTORY enabled
- Verify file was uploaded successfully with `LIST @ISDA_DOCUMENTS`

### Extraction quality issues
- AI_EXTRACT returns `{"response": {...}, "error": null}` - check the response field
- Ensure document text was fully extracted (check FULL_TEXT in DOCUMENT_FULL_TEXT)
- For complex events, consider extracting separately then combining

### Procedure errors
```sql
-- Check processing status
SELECT FILE_NAME, PROCESSING_STATUS, ERROR_MESSAGE 
FROM RAW_DOCUMENT_METADATA 
WHERE PROCESSING_STATUS LIKE '%FAILED%';
```

## Next Steps / Extensions

1. ✅ **AI_EXTRACT Integration**: Replaced AI_COMPLETE with purpose-built AI_EXTRACT
2. ✅ **Stored Procedures**: Created reusable procedures for parsing and extraction
3. ✅ **Amendment Resolution**: Implemented "later supersedes earlier" logic
4. ✅ **Cortex Agent**: Created agent with semantic views and search service
5. 🔄 **Streamlit App**: Build interactive UI for document upload and chat (in progress)
6. ⏳ **Alerts**: Set up monitoring for cross-default threshold breaches
7. ⏳ **Batch Scheduling**: Create Snowflake Tasks for automated processing
