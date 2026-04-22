# ISDA Document Intelligence - Data Catalog

## Database: ISDA_DOCUMENT_POC

This document provides a complete catalog of all tables, views, and relationships in the ISDA Document Intelligence POC.

---

## Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              ISDA DOCUMENT INTELLIGENCE - ERD                                │
└─────────────────────────────────────────────────────────────────────────────────────────────┘

                                    DOCUMENT_INTELLIGENCE SCHEMA
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                              │
│  ┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐    │
│  │ RAW_DOCUMENT_METADATA│         │  DOCUMENT_FULL_TEXT │         │ EXTRACTED_ISDA_MASTER│    │
│  ├─────────────────────┤         ├─────────────────────┤         ├─────────────────────┤    │
│  │ PK: DOCUMENT_ID     │────────▶│ PK: DOCUMENT_ID     │◀────────│ PK: DOCUMENT_ID     │    │
│  │ FILE_NAME           │         │ FULL_TEXT           │         │ AGREEMENT_VERSION   │    │
│  │ DOCUMENT_TYPE       │         │ PAGE_COUNT          │         │ PARTY_A_NAME        │    │
│  │ UPLOAD_TIMESTAMP    │         │ PARSED_CONTENT      │         │ PARTY_B_NAME        │    │
│  │ PROCESSING_STATUS   │         │ PARSE_TIMESTAMP     │         │ EVENTS_OF_DEFAULT   │    │
│  │ METADATA (VARIANT)  │         └─────────────────────┘         │ TERMINATION_EVENTS  │    │
│  └─────────────────────┘                                         │ CROSS_DEFAULT_*     │    │
│           │                                                      └─────────────────────┘    │
│           │                                                               │                 │
│           │                                                               │                 │
│           ▼                                                               ▼                 │
│  ┌─────────────────────┐                                        ┌─────────────────────┐    │
│  │    DOCUMENT_NODES   │                                        │    EXTRACTED_CSA    │    │
│  ├─────────────────────┤                                        ├─────────────────────┤    │
│  │ PK: NODE_ID         │                                        │ PK: DOCUMENT_ID     │    │
│  │ NODE_TYPE           │                                        │ FK: PARENT_MASTER_  │    │
│  │ NODE_LABEL          │                                        │     AGREEMENT_ID    │────┤
│  │ PROPERTIES          │                                        │ PARTY_A/B_THRESHOLD │    │
│  └─────────────────────┘                                        │ ELIGIBLE_COLLATERAL │    │
│           │                                                     └─────────────────────┘    │
│           │                                                               │                 │
│           ▼                                                               │                 │
│  ┌─────────────────────┐                                        ┌─────────────────────┐    │
│  │    DOCUMENT_EDGES   │                                        │ EXTRACTED_AMENDMENTS│    │
│  ├─────────────────────┤                                        ├─────────────────────┤    │
│  │ PK: EDGE_ID         │                                        │ PK: DOCUMENT_ID     │    │
│  │ EDGE_TYPE           │                                        │ FK: PARENT_MASTER_  │    │
│  │ FK: SOURCE_NODE_ID  │────┐                                   │     AGREEMENT_ID    │────┤
│  │ FK: TARGET_NODE_ID  │────┤                                   │ FK: SUPERSEDES_     │    │
│  │ PROPERTIES          │    │                                   │     DOCUMENT_ID     │◀───┤
│  └─────────────────────┘    │                                   │ AMENDMENT_NUMBER    │    │
│                             │                                   │ EFFECTIVE_DATE      │    │
│                             ▼                                   │ NEW_CROSS_DEFAULT_* │    │
│                    (self-referential                            └─────────────────────┘    │
│                     via NODE_ID)                                                           │
│                                                                                            │
│  ┌─────────────────────┐         ┌─────────────────────┐                                   │
│  │   CLAUSE_VERSIONS   │         │COUNTERPARTY_RELATIONS│                                  │
│  ├─────────────────────┤         ├─────────────────────┤                                   │
│  │ PK: VERSION_ID      │         │ PK: RELATIONSHIP_ID │                                   │
│  │ FK: DOCUMENT_ID     │         │ PARTY_A/B_NAME      │                                   │
│  │ FK: CLAUSE_NODE_ID  │         │ PARTY_A/B_NORMALIZED│                                   │
│  │ CLAUSE_TEXT         │         │ FK: BASE_ISDA_DOC_ID│◀──────────────────────────────────┤
│  │ VERSION_NUMBER      │         │ FK: ACTIVE_CSA_ID   │◀──────────────────────────────────┤
│  └─────────────────────┘         │ FK: LATEST_AMEND_ID │◀──────────────────────────────────┤
│                                  │ RELATIONSHIP_STATUS │                                   │
│                                  └─────────────────────┘                                   │
└──────────────────────────────────────────────────────────────────────────────────────────────┘

                                      SEMANTIC_VIEWS SCHEMA
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                              │
│  ┌─────────────────────┐         ┌─────────────────────┐         ┌─────────────────────┐    │
│  │   V_AGREEMENT_TERMS │         │   V_KNOWLEDGE_GRAPH │         │ ISDA_DOCUMENT_SEARCH│    │
│  ├─────────────────────┤         ├─────────────────────┤         ├─────────────────────┤    │
│  │ (View joining:)     │         │ (View joining:)     │         │ (Cortex Search on:) │    │
│  │ • EXTRACTED_ISDA_   │         │ • DOCUMENT_NODES    │         │ • DOCUMENT_FULL_TEXT│    │
│  │   MASTER            │         │ • DOCUMENT_EDGES    │         │ • RAW_DOCUMENT_     │    │
│  │ • V_CURRENT_AGREE-  │         │                     │         │   METADATA          │    │
│  │   MENT_TERMS        │         │                     │         │ • EXTRACTED_ISDA_   │    │
│  │ • EXTRACTED_CSA     │         │                     │         │   MASTER            │    │
│  └─────────────────────┘         └─────────────────────┘         └─────────────────────┘    │
│           │                               │                               │                 │
│           ▼                               ▼                               ▼                 │
│  ┌─────────────────────┐         ┌─────────────────────┐                                   │
│  │ ISDA_AGREEMENT_TERMS│         │ ISDA_KNOWLEDGE_GRAPH│         ┌─────────────────────┐    │
│  │   (Semantic View)   │         │   (Semantic View)   │         │ ISDA_DOCUMENT_AGENT │    │
│  └─────────────────────┘         └─────────────────────┘         │   (Cortex Agent)    │    │
│           │                               │                      ├─────────────────────┤    │
│           └───────────────┬───────────────┘                      │ Tools:              │    │
│                           │                                      │ • AgreementTerms    │◀───┤
│                           └─────────────────────────────────────▶│ • KnowledgeGraph    │◀───┤
│                                                                  │ • DocumentSearch    │◀───┤
│                                                                  └─────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Schema: DOCUMENT_INTELLIGENCE

### Core Document Tables

#### RAW_DOCUMENT_METADATA
**Purpose**: Tracks uploaded documents and their processing status.

| Column | Type | Description |
|--------|------|-------------|
| `DOCUMENT_ID` | VARCHAR(50) | **PK** - UUID generated on insert |
| `FILE_NAME` | VARCHAR(500) | Original filename |
| `FILE_PATH` | VARCHAR(1000) | Path in stage |
| `FILE_SIZE` | NUMBER | File size in bytes |
| `DOCUMENT_TYPE` | VARCHAR(50) | Type: ISDA_MASTER_AGREEMENT, CSA, AMENDMENT |
| `UPLOAD_TIMESTAMP` | TIMESTAMP_NTZ | When document was uploaded |
| `PROCESSING_STATUS` | VARCHAR(20) | PENDING, PARSED, EXTRACTED, FAILED |
| `METADATA` | VARIANT | Additional document metadata (JSON) |

**Relationships**:
- Parent to `DOCUMENT_FULL_TEXT` (1:1)
- Parent to `EXTRACTED_ISDA_MASTER` (1:1)
- Referenced by `DOCUMENT_NODES` (1:N)

---

#### DOCUMENT_FULL_TEXT
**Purpose**: Stores parsed text extracted via AI_PARSE_DOCUMENT.

| Column | Type | Description |
|--------|------|-------------|
| `DOCUMENT_ID` | VARCHAR(50) | **PK/FK** → RAW_DOCUMENT_METADATA |
| `FULL_TEXT` | VARCHAR(16MB) | Complete extracted text |
| `PAGE_COUNT` | NUMBER | Number of pages (from PARSED_CONTENT.metadata.pageCount) |
| `PARSED_CONTENT` | VARIANT | Full AI_PARSE_DOCUMENT output (JSON) |
| `PARSE_TIMESTAMP` | TIMESTAMP_NTZ | When parsing completed |

**Relationships**:
- Child of `RAW_DOCUMENT_METADATA` (1:1)
- Source for `ISDA_DOCUMENT_SEARCH` Cortex Search Service

---

#### EXTRACTED_ISDA_MASTER
**Purpose**: Structured extraction results from AI_COMPLETE.

| Column | Type | Description |
|--------|------|-------------|
| `DOCUMENT_ID` | VARCHAR(50) | **PK/FK** → RAW_DOCUMENT_METADATA |
| `AGREEMENT_VERSION` | VARCHAR(10) | "1992" or "2002" |
| `EFFECTIVE_DATE` | DATE | Agreement execution date |
| `PARTY_A_NAME` | VARCHAR(500) | First party (typically dealer) |
| `PARTY_A_TYPE` | VARCHAR(100) | Entity type of Party A |
| `PARTY_B_NAME` | VARCHAR(500) | Second party (typically client) |
| `PARTY_B_TYPE` | VARCHAR(100) | Entity type of Party B |
| `GOVERNING_LAW` | VARCHAR(100) | Jurisdiction (New York, English, etc.) |
| `EVENTS_OF_DEFAULT` | ARRAY | Section 5(a) events (JSON array) |
| `TERMINATION_EVENTS` | ARRAY | Section 5(b) events (JSON array) |
| `CROSS_DEFAULT_APPLICABLE` | BOOLEAN | Whether cross-default applies |
| `CROSS_DEFAULT_THRESHOLD_AMOUNT` | NUMBER(20,2) | Original threshold amount |
| `CROSS_DEFAULT_THRESHOLD_CURRENCY` | VARCHAR(3) | Currency code (USD, etc.) |
| `AUTOMATIC_EARLY_TERMINATION_PARTY_A` | BOOLEAN | AET for Party A |
| `AUTOMATIC_EARLY_TERMINATION_PARTY_B` | BOOLEAN | AET for Party B |
| `PAYMENT_METHOD` | VARCHAR(50) | Payment netting method |
| `CLOSE_OUT_CALCULATION` | VARCHAR(100) | Close-out Amount, Market Quotation, Loss |
| `EXTRACTION_TIMESTAMP` | TIMESTAMP_NTZ | When extraction completed |
| `EXTRACTION_CONFIDENCE` | FLOAT | AI confidence score |
| `RAW_EXTRACTION_JSON` | VARIANT | Full AI response |

**Relationships**:
- Child of `RAW_DOCUMENT_METADATA` (1:1)
- Parent to `EXTRACTED_CSA` (1:N)
- Parent to `EXTRACTED_AMENDMENTS` (1:N)
- Source for `COUNTERPARTY_RELATIONSHIPS`

---

### Supporting Document Tables

#### EXTRACTED_CSA
**Purpose**: Credit Support Annex details linked to master agreements.

| Column | Type | Description |
|--------|------|-------------|
| `DOCUMENT_ID` | VARCHAR(50) | **PK** - Synthetic ID for CSA |
| `PARENT_MASTER_AGREEMENT_ID` | VARCHAR(50) | **FK** → EXTRACTED_ISDA_MASTER |
| `EFFECTIVE_DATE` | DATE | CSA effective date |
| `PARTY_A_NAME` | VARCHAR(500) | Party A name |
| `PARTY_B_NAME` | VARCHAR(500) | Party B name |
| `PARTY_A_THRESHOLD_AMOUNT` | NUMBER(20,2) | Threshold for Party A |
| `PARTY_B_THRESHOLD_AMOUNT` | NUMBER(20,2) | Threshold for Party B |
| `ELIGIBLE_COLLATERAL` | ARRAY | Accepted collateral types |
| `MINIMUM_TRANSFER_AMOUNT` | NUMBER(20,2) | MTA amount |
| `VALUATION_AGENT` | VARCHAR(100) | Who performs valuations |

**Relationships**:
- Child of `EXTRACTED_ISDA_MASTER` via `PARENT_MASTER_AGREEMENT_ID`

---

#### EXTRACTED_AMENDMENTS
**Purpose**: Amendment history with supersession tracking for resolution logic.

| Column | Type | Description |
|--------|------|-------------|
| `DOCUMENT_ID` | VARCHAR(50) | **PK** - Synthetic ID for amendment |
| `PARENT_MASTER_AGREEMENT_ID` | VARCHAR(50) | **FK** → EXTRACTED_ISDA_MASTER |
| `SUPERSEDES_DOCUMENT_ID` | VARCHAR(50) | **FK** → EXTRACTED_AMENDMENTS (self-ref) |
| `AMENDMENT_NUMBER` | NUMBER | Sequential amendment number |
| `EFFECTIVE_DATE` | DATE | **Critical for resolution** - later supersedes earlier |
| `NEW_CROSS_DEFAULT_THRESHOLD_PARTY_A` | NUMBER(20,2) | Updated threshold for Party A |
| `NEW_CROSS_DEFAULT_THRESHOLD_PARTY_B` | NUMBER(20,2) | Updated threshold for Party B |
| `NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_A` | BOOLEAN | Updated AET for Party A |
| `NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_B` | BOOLEAN | Updated AET for Party B |
| `ADDITIONAL_TERMINATION_EVENTS` | ARRAY | New termination events added |
| `AMENDMENT_SUMMARY` | VARCHAR(4000) | Description of changes |

**Relationships**:
- Child of `EXTRACTED_ISDA_MASTER` via `PARENT_MASTER_AGREEMENT_ID`
- Self-referential via `SUPERSEDES_DOCUMENT_ID`

**Resolution Logic**:
```sql
ROW_NUMBER() OVER (
    PARTITION BY PARENT_MASTER_AGREEMENT_ID 
    ORDER BY EFFECTIVE_DATE DESC
) as AMENDMENT_RANK
-- AMENDMENT_RANK = 1 is the current/latest amendment
```

---

### Knowledge Graph Tables

#### DOCUMENT_NODES
**Purpose**: Nodes in the document knowledge graph.

| Column | Type | Description |
|--------|------|-------------|
| `NODE_ID` | VARCHAR(100) | **PK** - Unique node identifier |
| `NODE_TYPE` | VARCHAR(50) | DOCUMENT, PARTY, EVENT_OF_DEFAULT, TERMINATION_EVENT, CLAUSE, CSA, AMENDMENT |
| `NODE_LABEL` | VARCHAR(500) | Human-readable label |
| `PROPERTIES` | VARIANT | Additional node properties (JSON) |
| `CREATED_AT` | TIMESTAMP_NTZ | Node creation time |

**Node Types**:
- `DOCUMENT` - Master agreements
- `PARTY` - Counterparties (banks, corporations)
- `EVENT_OF_DEFAULT` - Section 5(a) events
- `TERMINATION_EVENT` - Section 5(b) events
- `CLAUSE` - Specific contractual provisions
- `CSA` - Credit Support Annexes
- `AMENDMENT` - Amendment documents

---

#### DOCUMENT_EDGES
**Purpose**: Relationships between nodes in the knowledge graph.

| Column | Type | Description |
|--------|------|-------------|
| `EDGE_ID` | VARCHAR(100) | **PK** - Unique edge identifier |
| `EDGE_TYPE` | VARCHAR(50) | Relationship type |
| `SOURCE_NODE_ID` | VARCHAR(100) | **FK** → DOCUMENT_NODES |
| `TARGET_NODE_ID` | VARCHAR(100) | **FK** → DOCUMENT_NODES |
| `PROPERTIES` | VARIANT | Edge properties (JSON) |
| `CREATED_AT` | TIMESTAMP_NTZ | Edge creation time |

**Edge Types**:
| Edge Type | From → To | Description |
|-----------|-----------|-------------|
| `PARTY_TO` | PARTY → DOCUMENT | Party is signatory to agreement |
| `CONTAINS` | DOCUMENT → EVENT/CLAUSE | Document contains this element |
| `AMENDS` | AMENDMENT → DOCUMENT | Amendment modifies master |
| `SUPERSEDES` | AMENDMENT → AMENDMENT | Later amendment replaces earlier |
| `SUPPLEMENTS` | CSA → DOCUMENT | CSA supplements master |
| `HAS_CSA` | DOCUMENT → CSA | Master has this CSA |

---

### Relationship Table

#### COUNTERPARTY_RELATIONSHIPS
**Purpose**: Aggregated view of party-to-party relationships across all documents.

| Column | Type | Description |
|--------|------|-------------|
| `RELATIONSHIP_ID` | VARCHAR(50) | **PK** - Unique relationship ID |
| `PARTY_A_NAME` | VARCHAR(500) | Original Party A name |
| `PARTY_A_NORMALIZED` | VARCHAR(500) | Normalized for matching |
| `PARTY_B_NAME` | VARCHAR(500) | Original Party B name |
| `PARTY_B_NORMALIZED` | VARCHAR(500) | Normalized for matching |
| `BASE_ISDA_DOCUMENT_ID` | VARCHAR(50) | **FK** → EXTRACTED_ISDA_MASTER |
| `ACTIVE_CSA_DOCUMENT_ID` | VARCHAR(50) | **FK** → EXTRACTED_CSA |
| `ACTIVE_MSA_DOCUMENT_ID` | VARCHAR(50) | **FK** (reserved for future) |
| `LATEST_AMENDMENT_DOCUMENT_ID` | VARCHAR(50) | **FK** → EXTRACTED_AMENDMENTS |
| `RELATIONSHIP_START_DATE` | DATE | Earliest agreement date |
| `RELATIONSHIP_STATUS` | VARCHAR(20) | ACTIVE, TERMINATED |

---

## Schema: SEMANTIC_VIEWS

### Views

#### V_AGREEMENT_TERMS
**Purpose**: Flattened view with amendment resolution applied.

**Source Tables**: 
- `EXTRACTED_ISDA_MASTER`
- `RAW_DOCUMENT_METADATA`
- `V_CURRENT_AGREEMENT_TERMS`
- `EXTRACTED_CSA`

**Key Columns**:
- `CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A/B` - Resolved threshold (amendment or original)
- `CROSS_DEFAULT_SOURCE` - "Original Agreement" or "Amendment N (date)"
- `HAS_AMENDMENTS` - Boolean flag

---

#### V_KNOWLEDGE_GRAPH
**Purpose**: Joined view of nodes and edges for graph navigation.

**Source Tables**:
- `DOCUMENT_NODES`
- `DOCUMENT_EDGES`

**Key Columns**:
- `SOURCE_LABEL`, `TARGET_LABEL` - Human-readable node names
- `SOURCE_TYPE`, `TARGET_TYPE` - Node types
- `EDGE_TYPE` - Relationship type

---

### Semantic Views (Cortex Analyst)

#### ISDA_AGREEMENT_TERMS
**Purpose**: Semantic model for natural language queries about agreements.

**Dimensions**: document_id, party_a, party_b, agreement_version, governing_law, cross_default_source, has_amendments, closeout_method

**Facts**: original_cross_default_threshold, current_cross_default_threshold_party_a/b, csa_threshold_party_a/b

**Metrics**: agreement_count, avg_cross_default_threshold

---

#### ISDA_KNOWLEDGE_GRAPH
**Purpose**: Semantic model for navigating document relationships.

**Dimensions**: node_id, node_type, node_label, edge_type, source_label, target_label

**Metrics**: node_count, edge_count

---

### Cortex Search Service

#### ISDA_DOCUMENT_SEARCH
**Purpose**: RAG search over raw document text.

**Search Column**: `FULL_TEXT`

**Attributes**: `DOCUMENT_ID`, `FILENAME`, `PARTY_A`, `PARTY_B`

**Source Query**:
```sql
SELECT ft.DOCUMENT_ID, ft.FULL_TEXT, m.FILE_NAME as FILENAME,
       COALESCE(im.PARTY_A_NAME, 'Unknown') as PARTY_A,
       COALESCE(im.PARTY_B_NAME, 'Unknown') as PARTY_B
FROM DOCUMENT_FULL_TEXT ft
LEFT JOIN RAW_DOCUMENT_METADATA m ON ft.DOCUMENT_ID = m.DOCUMENT_ID
LEFT JOIN EXTRACTED_ISDA_MASTER im ON ft.DOCUMENT_ID = im.DOCUMENT_ID
```

---

### Cortex Agent

#### ISDA_DOCUMENT_AGENT
**Purpose**: Orchestrates queries across structured data, graph, and search.

**Tools**:
| Tool Name | Type | Resource |
|-----------|------|----------|
| `AgreementTerms` | cortex_analyst_text_to_sql | ISDA_AGREEMENT_TERMS |
| `KnowledgeGraph` | cortex_analyst_text_to_sql | ISDA_KNOWLEDGE_GRAPH |
| `DocumentSearch` | cortex_search | ISDA_DOCUMENT_SEARCH |

---

## Common Join Patterns

### 1. Document → Extraction → Current Terms
```sql
SELECT m.FILE_NAME, im.PARTY_A_NAME, im.PARTY_B_NAME,
       cat.CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A,
       cat.CROSS_DEFAULT_SOURCE
FROM RAW_DOCUMENT_METADATA m
JOIN EXTRACTED_ISDA_MASTER im ON m.DOCUMENT_ID = im.DOCUMENT_ID
LEFT JOIN V_CURRENT_AGREEMENT_TERMS cat ON m.DOCUMENT_ID = cat.MASTER_AGREEMENT_ID;
```

### 2. Party → Documents → Events
```sql
SELECT p.NODE_LABEL as PARTY, 
       d.NODE_LABEL as DOCUMENT,
       e.NODE_LABEL as EVENT
FROM DOCUMENT_NODES p
JOIN DOCUMENT_EDGES pe ON p.NODE_ID = pe.SOURCE_NODE_ID AND pe.EDGE_TYPE = 'PARTY_TO'
JOIN DOCUMENT_NODES d ON pe.TARGET_NODE_ID = d.NODE_ID
JOIN DOCUMENT_EDGES de ON d.NODE_ID = de.SOURCE_NODE_ID AND de.EDGE_TYPE = 'CONTAINS'
JOIN DOCUMENT_NODES e ON de.TARGET_NODE_ID = e.NODE_ID
WHERE p.NODE_TYPE = 'PARTY';
```

### 3. Amendment Chain Resolution
```sql
WITH amendment_hierarchy AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY PARENT_MASTER_AGREEMENT_ID 
        ORDER BY EFFECTIVE_DATE DESC
    ) as AMENDMENT_RANK
    FROM EXTRACTED_AMENDMENTS
)
SELECT * FROM amendment_hierarchy WHERE AMENDMENT_RANK = 1;  -- Latest only
```

### 4. Full Counterparty View
```sql
SELECT cr.PARTY_A_NAME, cr.PARTY_B_NAME,
       im.AGREEMENT_VERSION,
       csa.MINIMUM_TRANSFER_AMOUNT,
       am.NEW_CROSS_DEFAULT_THRESHOLD_PARTY_A as CURRENT_THRESHOLD
FROM COUNTERPARTY_RELATIONSHIPS cr
JOIN EXTRACTED_ISDA_MASTER im ON cr.BASE_ISDA_DOCUMENT_ID = im.DOCUMENT_ID
LEFT JOIN EXTRACTED_CSA csa ON cr.ACTIVE_CSA_DOCUMENT_ID = csa.DOCUMENT_ID
LEFT JOIN EXTRACTED_AMENDMENTS am ON cr.LATEST_AMENDMENT_DOCUMENT_ID = am.DOCUMENT_ID;
```

---

## Stored Procedures

The document processing pipeline is implemented through stored procedures in the `DOCUMENT_INTELLIGENCE` schema.

### Parsing Procedures

| Procedure | Parameters | Description |
|-----------|------------|-------------|
| `PARSE_SINGLE_DOCUMENT` | `document_id VARCHAR` | Parses a single document using `PARSE_DOCUMENT()` and stores results in `DOCUMENT_FULL_TEXT` |
| `PARSE_ALL_PENDING_DOCUMENTS` | None | Batch processes all documents in `RAW_DOCUMENT_METADATA` that haven't been parsed yet |

### Extraction Procedures

| Procedure | Parameters | Description |
|-----------|------------|-------------|
| `EXTRACT_SINGLE_DOCUMENT` | `document_id VARCHAR` | Orchestrates full extraction for a document (ISDA fields + Events of Default + Termination Events) |
| `EXTRACT_ALL_PENDING_DOCUMENTS` | None | Batch processes all parsed documents that haven't been extracted yet |
| `EXTRACT_ISDA_FIELDS_FOR_DOCUMENT` | `document_id VARCHAR` | Extracts core ISDA Master Agreement fields using `AI_EXTRACT()` |
| `EXTRACT_EVENTS_OF_DEFAULT` | `document_id VARCHAR` | Extracts Events of Default provisions using `AI_EXTRACT()` |
| `EXTRACT_TERMINATION_EVENTS` | `document_id VARCHAR` | Extracts Termination Event provisions using `AI_EXTRACT()` |

### Procedure Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Document Processing Pipeline                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. Upload PDF to Stage: @ISDA_DOCUMENTS                            │
│                           │                                          │
│                           ▼                                          │
│  2. Register Metadata:  RAW_DOCUMENT_METADATA                       │
│                           │                                          │
│                           ▼                                          │
│  3. Parse Document:     PARSE_SINGLE_DOCUMENT()                     │
│                           │                                          │
│                           ├──► Uses PARSE_DOCUMENT()                │
│                           │                                          │
│                           ▼                                          │
│                       DOCUMENT_FULL_TEXT                            │
│                           │                                          │
│                           ▼                                          │
│  4. Extract Fields:     EXTRACT_SINGLE_DOCUMENT()                   │
│                           │                                          │
│         ┌─────────────────┼─────────────────┐                       │
│         ▼                 ▼                 ▼                       │
│   EXTRACT_ISDA_     EXTRACT_EVENTS_   EXTRACT_TERMINATION_         │
│   FIELDS_FOR_       OF_DEFAULT()      EVENTS()                     │
│   DOCUMENT()              │                 │                       │
│         │                 │                 │                       │
│         ▼                 ▼                 ▼                       │
│   ISDA_EXTRACTIONS  EVENTS_OF_      TERMINATION_                   │
│                     DEFAULT         EVENTS                          │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Example Usage

```sql
-- Process a single new document
CALL PARSE_SINGLE_DOCUMENT('doc_abc123');
CALL EXTRACT_SINGLE_DOCUMENT('doc_abc123');

-- Batch process all pending documents
CALL PARSE_ALL_PENDING_DOCUMENTS();
CALL EXTRACT_ALL_PENDING_DOCUMENTS();
```

---

## Cortex Agent Infrastructure

The project includes a Cortex Agent for natural language querying of ISDA documents.

### Components (SEMANTIC_VIEWS Schema)

| Object | Type | Description |
|--------|------|-------------|
| `ISDA_DOCUMENT_AGENT` | Cortex Agent | Natural language interface for querying ISDA documents |
| `ISDA_DOCUMENT_SEARCH` | Cortex Search Service | Full-text search over parsed document content |
| `PARTY_NAME_SEARCH` | Cortex Search Service | Fuzzy/semantic search for party name matching |
| `V_AGREEMENT_TERMS` | View | Consolidated view of agreement terms with amendment resolution |

### Party Name Search Service

The `PARTY_NAME_SEARCH` service enables fuzzy matching on party names. When a user asks about "Barclays", it matches "BARCLAYS BANK PLC" accurately. This service is integrated into the semantic model for both `party_a` and `party_b` dimensions.

```sql
-- Test the search service
SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.PARTY_NAME_SEARCH',
    '{"query": "Barclays", "columns": ["PARTY_NAME"], "limit": 3}'
);
-- Returns: BARCLAYS BANK PLC (top match)
```

### Agent API Endpoint

```
POST /api/v2/databases/ISDA_DOCUMENT_POC/schemas/SEMANTIC_VIEWS/agents/ISDA_DOCUMENT_AGENT:run
```

### Sample Agent Queries

- "What is the cross-default threshold for Barclays?"
- "List all Events of Default in the BofA master agreement"
- "What termination events apply to Party A in the Citi agreement?"
- "Compare the governing law across all master agreements"

---

## Data Quality Checks

```sql
-- Check for orphaned documents (parsed but not extracted)
SELECT m.DOCUMENT_ID, m.FILE_NAME
FROM RAW_DOCUMENT_METADATA m
LEFT JOIN EXTRACTED_ISDA_MASTER im ON m.DOCUMENT_ID = im.DOCUMENT_ID
WHERE im.DOCUMENT_ID IS NULL AND m.DOCUMENT_TYPE LIKE '%ISDA%';

-- Check for missing page counts
SELECT DOCUMENT_ID FROM DOCUMENT_FULL_TEXT 
WHERE PAGE_COUNT IS NULL AND PARSED_CONTENT:metadata:pageCount IS NOT NULL;

-- Check for empty counterparty relationships
SELECT COUNT(*) as RELATIONSHIP_COUNT FROM COUNTERPARTY_RELATIONSHIPS;

-- Verify amendment resolution
SELECT PARTY_A, PARTY_B, 
       ORIGINAL_CROSS_DEFAULT_THRESHOLD,
       CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A,
       CROSS_DEFAULT_SOURCE
FROM SEMANTIC_VIEWS.V_AGREEMENT_TERMS
WHERE HAS_AMENDMENTS = TRUE;
```

---

## Current Data Summary

| Table | Row Count | Notes |
|-------|-----------|-------|
| RAW_DOCUMENT_METADATA | 7 | 4 real PDFs + 3 synthetic |
| DOCUMENT_FULL_TEXT | 7 | All parsed |
| EXTRACTED_ISDA_MASTER | 4 | Master agreements only |
| EXTRACTED_CSA | 1 | BofA/LKQ CSA |
| EXTRACTED_AMENDMENTS | 2 | Barclays amendments |
| DOCUMENT_NODES | 55 | All node types |
| DOCUMENT_EDGES | 52 | All relationship types |
| COUNTERPARTY_RELATIONSHIPS | 4 | One per master agreement |
