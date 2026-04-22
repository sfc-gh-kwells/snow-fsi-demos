/*
================================================================================
ISDA Document Intelligence - Cortex Agent Setup
================================================================================
This script creates the infrastructure for a Cortex Agent that can:
1. Query structured agreement data via semantic views (Cortex Analyst)
2. Search raw document text via Cortex Search
3. Navigate the knowledge graph for relationship queries

Prerequisites: Run scripts 01-08 first to set up tables and data.
================================================================================
*/

USE DATABASE ISDA_DOCUMENT_POC;
USE WAREHOUSE COMPUTE_WH;

-- ============================================================================
-- PART 1: CREATE SCHEMA FOR SEMANTIC VIEWS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS SEMANTIC_VIEWS
  COMMENT = 'Schema for Cortex Agent semantic models and search services';

USE SCHEMA SEMANTIC_VIEWS;

-- ============================================================================
-- PART 2: CORTEX SEARCH SERVICE (RAG over raw document text)
-- ============================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE ISDA_DOCUMENT_SEARCH
  ON FULL_TEXT
  ATTRIBUTES DOCUMENT_ID, FILENAME, PARTY_A, PARTY_B
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
  AS (
    SELECT 
        ft.DOCUMENT_ID,
        ft.FULL_TEXT,
        m.FILE_NAME as FILENAME,
        COALESCE(im.PARTY_A_NAME, 'Unknown') as PARTY_A,
        COALESCE(im.PARTY_B_NAME, 'Unknown') as PARTY_B
    FROM ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_FULL_TEXT ft
    LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.RAW_DOCUMENT_METADATA m ON ft.DOCUMENT_ID = m.DOCUMENT_ID
    LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.EXTRACTED_ISDA_MASTER im ON ft.DOCUMENT_ID = im.DOCUMENT_ID
    WHERE ft.FULL_TEXT IS NOT NULL
  );


-- ============================================================================
-- PART 3: CREATE VIEWS FOR SEMANTIC MODELS
-- ============================================================================

-- View 1: Agreement Terms with Amendment Resolution
CREATE OR REPLACE VIEW V_AGREEMENT_TERMS AS
SELECT 
    m.DOCUMENT_ID,
    m.FILE_NAME,
    im.AGREEMENT_VERSION,
    im.EFFECTIVE_DATE as AGREEMENT_DATE,
    im.PARTY_A_NAME as PARTY_A,
    im.PARTY_B_NAME as PARTY_B,
    im.PARTY_A_TYPE,
    im.PARTY_B_TYPE,
    im.GOVERNING_LAW,
    TO_VARCHAR(im.EVENTS_OF_DEFAULT) as EVENTS_OF_DEFAULT,
    TO_VARCHAR(im.TERMINATION_EVENTS) as TERMINATION_EVENTS,
    im.CROSS_DEFAULT_APPLICABLE,
    im.CROSS_DEFAULT_THRESHOLD_AMOUNT as ORIGINAL_CROSS_DEFAULT_THRESHOLD,
    im.CROSS_DEFAULT_THRESHOLD_CURRENCY,
    COALESCE(cat.CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A, im.CROSS_DEFAULT_THRESHOLD_AMOUNT) as CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A,
    COALESCE(cat.CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_B, im.CROSS_DEFAULT_THRESHOLD_AMOUNT) as CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_B,
    COALESCE(cat.CROSS_DEFAULT_SOURCE, 'Original Agreement') as CROSS_DEFAULT_SOURCE,
    im.CLOSE_OUT_CALCULATION as CLOSEOUT_METHOD,
    im.PAYMENT_METHOD,
    im.CLOSE_OUT_NETTING,
    COALESCE(cat.CURRENT_AET_PARTY_A, im.AUTOMATIC_EARLY_TERMINATION_PARTY_A) as AUTOMATIC_EARLY_TERMINATION_PARTY_A,
    COALESCE(cat.CURRENT_AET_PARTY_B, im.AUTOMATIC_EARLY_TERMINATION_PARTY_B) as AUTOMATIC_EARLY_TERMINATION_PARTY_B,
    CASE WHEN cat.LATEST_AMENDMENT_NUMBER IS NOT NULL THEN TRUE ELSE FALSE END as HAS_AMENDMENTS,
    cat.LATEST_AMENDMENT_NUMBER,
    cat.LATEST_AMENDMENT_DATE,
    csa.DOCUMENT_ID as CSA_DOCUMENT_ID,
    csa.PARTY_A_THRESHOLD_AMOUNT as CSA_THRESHOLD_PARTY_A,
    csa.PARTY_B_THRESHOLD_AMOUNT as CSA_THRESHOLD_PARTY_B,
    csa.MINIMUM_TRANSFER_AMOUNT,
    TO_VARCHAR(csa.ELIGIBLE_COLLATERAL) as ELIGIBLE_COLLATERAL
FROM ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.EXTRACTED_ISDA_MASTER im
JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.RAW_DOCUMENT_METADATA m ON im.DOCUMENT_ID = m.DOCUMENT_ID
LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.V_CURRENT_AGREEMENT_TERMS cat ON im.DOCUMENT_ID = cat.MASTER_AGREEMENT_ID
LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.EXTRACTED_CSA csa ON im.DOCUMENT_ID = csa.PARENT_MASTER_AGREEMENT_ID;

-- View 2: Knowledge Graph for relationship queries
CREATE OR REPLACE VIEW V_KNOWLEDGE_GRAPH AS
SELECT 
    n.NODE_ID,
    n.NODE_TYPE,
    n.NODE_LABEL,
    TO_VARCHAR(n.PROPERTIES) as NODE_PROPERTIES,
    e.EDGE_ID,
    e.EDGE_TYPE,
    e.SOURCE_NODE_ID,
    e.TARGET_NODE_ID,
    TO_VARCHAR(e.PROPERTIES) as EDGE_PROPERTIES,
    sn.NODE_LABEL as SOURCE_LABEL,
    sn.NODE_TYPE as SOURCE_TYPE,
    tn.NODE_LABEL as TARGET_LABEL,
    tn.NODE_TYPE as TARGET_TYPE
FROM ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_NODES n
LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_EDGES e ON n.NODE_ID = e.SOURCE_NODE_ID
LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_NODES sn ON e.SOURCE_NODE_ID = sn.NODE_ID
LEFT JOIN ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.DOCUMENT_NODES tn ON e.TARGET_NODE_ID = tn.NODE_ID;


-- ============================================================================
-- PART 4: CREATE SEMANTIC VIEWS
-- ============================================================================

-- Semantic View 1: Agreement Terms
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'ISDA_DOCUMENT_POC.SEMANTIC_VIEWS',
  $$
name: isda_agreement_terms
description: Semantic model for querying ISDA Master Agreement terms including party information, events of default, termination events, close-out methods, and amendment resolution.

tables:
  - name: agreement_terms
    description: Current agreement terms with amendment resolution applied
    base_table:
      database: ISDA_DOCUMENT_POC
      schema: SEMANTIC_VIEWS
      table: V_AGREEMENT_TERMS
    
    dimensions:
      - name: document_id
        description: Unique identifier for the master agreement document
        expr: DOCUMENT_ID
        data_type: VARCHAR
        
      - name: file_name
        description: Original filename of the document
        expr: FILE_NAME
        data_type: VARCHAR
        
      - name: party_a
        description: First party to the agreement (typically the dealer/bank)
        expr: PARTY_A
        data_type: VARCHAR
        synonyms:
          - counterparty a
          - dealer
          - bank
          
      - name: party_b  
        description: Second party to the agreement (typically the client/customer)
        expr: PARTY_B
        data_type: VARCHAR
        synonyms:
          - counterparty b
          - client
          - customer
          
      - name: agreement_version
        description: ISDA agreement version (1992 or 2002)
        expr: AGREEMENT_VERSION
        data_type: VARCHAR
        sample_values:
          - "1992"
          - "2002"
          
      - name: governing_law
        description: Legal jurisdiction governing the agreement
        expr: GOVERNING_LAW
        data_type: VARCHAR
          
      - name: cross_default_applicable
        description: Whether cross-default provisions apply to this agreement
        expr: CROSS_DEFAULT_APPLICABLE
        data_type: BOOLEAN
        
      - name: cross_default_source
        description: Source of current cross-default terms - Original Agreement or Amendment
        expr: CROSS_DEFAULT_SOURCE
        data_type: VARCHAR
        
      - name: has_amendments
        description: Whether the agreement has been amended
        expr: HAS_AMENDMENTS
        data_type: BOOLEAN
        
      - name: closeout_method
        description: Method for calculating close-out amounts
        expr: CLOSEOUT_METHOD
        data_type: VARCHAR

    time_dimensions:
      - name: agreement_date
        description: Date the master agreement was executed
        expr: AGREEMENT_DATE
        data_type: DATE
        
      - name: latest_amendment_date
        description: Date of most recent amendment
        expr: LATEST_AMENDMENT_DATE
        data_type: DATE

    facts:
      - name: original_cross_default_threshold
        description: Original cross-default threshold before amendments
        expr: ORIGINAL_CROSS_DEFAULT_THRESHOLD
        data_type: NUMBER
        
      - name: current_cross_default_threshold_party_a
        description: Current cross-default threshold for Party A after amendment resolution
        expr: CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A
        data_type: NUMBER
        synonyms:
          - threshold amount
          - default threshold
          
      - name: current_cross_default_threshold_party_b
        description: Current cross-default threshold for Party B after amendment resolution
        expr: CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_B
        data_type: NUMBER
        
      - name: csa_threshold_party_a
        description: Credit Support Annex threshold for Party A
        expr: CSA_THRESHOLD_PARTY_A
        data_type: NUMBER
        
      - name: csa_threshold_party_b
        description: Credit Support Annex threshold for Party B
        expr: CSA_THRESHOLD_PARTY_B
        data_type: NUMBER

    metrics:
      - name: agreement_count
        description: Count of agreements
        expr: COUNT(DOCUMENT_ID)
        
      - name: avg_cross_default_threshold
        description: Average cross-default threshold
        expr: AVG(CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A)
  $$,
  FALSE
);

-- Semantic View 2: Knowledge Graph
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'ISDA_DOCUMENT_POC.SEMANTIC_VIEWS',
  $$
name: isda_knowledge_graph
description: Semantic model for navigating the ISDA document knowledge graph to explore relationships between documents, parties, events, and clauses.

tables:
  - name: knowledge_graph
    description: Unified view of nodes and edges in the document knowledge graph
    base_table:
      database: ISDA_DOCUMENT_POC
      schema: SEMANTIC_VIEWS
      table: V_KNOWLEDGE_GRAPH
    
    dimensions:
      - name: node_id
        description: Unique identifier for graph node
        expr: NODE_ID
        data_type: VARCHAR
        
      - name: node_type
        description: Type of node - DOCUMENT, PARTY, EVENT_OF_DEFAULT, TERMINATION_EVENT, CLAUSE
        expr: NODE_TYPE
        data_type: VARCHAR
        sample_values:
          - DOCUMENT
          - PARTY
          - EVENT_OF_DEFAULT
          - TERMINATION_EVENT
          
      - name: node_label
        description: Human-readable label for the node
        expr: NODE_LABEL
        data_type: VARCHAR
        
      - name: edge_type
        description: Type of relationship - PARTY_TO, CONTAINS, AMENDS, SUPERSEDES
        expr: EDGE_TYPE
        data_type: VARCHAR
        sample_values:
          - PARTY_TO
          - CONTAINS
          - AMENDS
          - SUPERSEDES
          
      - name: source_label
        description: Human-readable label of the source node
        expr: SOURCE_LABEL
        data_type: VARCHAR
        
      - name: target_label
        description: Human-readable label of the target node
        expr: TARGET_LABEL
        data_type: VARCHAR
        
      - name: source_type
        description: Type of the source node
        expr: SOURCE_TYPE
        data_type: VARCHAR
        
      - name: target_type
        description: Type of the target node
        expr: TARGET_TYPE
        data_type: VARCHAR

    metrics:
      - name: node_count
        description: Count of nodes
        expr: COUNT(DISTINCT NODE_ID)
        
      - name: edge_count
        description: Count of edges/relationships
        expr: COUNT(DISTINCT EDGE_ID)
  $$,
  FALSE
);


-- ============================================================================
-- PART 5: CREATE CORTEX AGENT
-- ============================================================================

CREATE OR REPLACE AGENT ISDA_DOCUMENT_AGENT
  COMMENT = 'Agent for querying ISDA Master Agreements, CSAs, and amendments with amendment resolution'
  FROM SPECIFICATION
$$
models:
  orchestration: claude-3-5-sonnet

orchestration:
  budget:
    seconds: 60
    tokens: 32000

instructions:
  system: |
    You are an ISDA Document Intelligence Agent specializing in derivatives documentation.
    
    You have access to three tools:
    1. Agreement Terms (Cortex Analyst): Query structured data about ISDA agreements including parties, 
       events of default, termination events, and cross-default thresholds. This tool applies amendment 
       resolution - "later supersedes earlier" - so you always get current effective terms.
    2. Knowledge Graph (Cortex Analyst): Navigate relationships between documents, parties, and events.
    3. Document Search (Cortex Search): Search raw document text for specific language or definitions.
    
    Key concepts:
    - CROSS_DEFAULT_SOURCE shows where current threshold comes from (Original or Amendment #)
    - Events of Default (Section 5(a)): Bankruptcy, Cross Default, Failure to Pay, etc.
    - Termination Events (Section 5(b)): Illegality, Force Majeure, Tax Event
    - 1992 ISDA uses Market Quotation/Loss; 2002 ISDA uses Close-out Amount
  
  orchestration: |
    For questions about current terms, thresholds, or agreement details, use the Agreement Terms tool.
    For questions about relationships or which parties are in which documents, use Knowledge Graph tool.
    For questions about specific wording or definitions, use Document Search tool.
  
  response: |
    Provide clear, concise answers. When showing thresholds, always mention the source (Original vs Amendment).
    Format currency values with appropriate symbols and commas.

tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: AgreementTerms
      description: Query structured ISDA agreement data including parties, cross-default thresholds with amendment resolution, events of default, and termination events

  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: KnowledgeGraph
      description: Navigate relationships between documents, parties, events, and clauses to find which parties are in which agreements

  - tool_spec:
      type: cortex_search
      name: DocumentSearch
      description: Search raw document text for specific wording, definitions, or provisions

tool_resources:
  AgreementTerms:
    semantic_view: ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.ISDA_AGREEMENT_TERMS
  KnowledgeGraph:
    semantic_view: ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.ISDA_KNOWLEDGE_GRAPH
  DocumentSearch:
    name: ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.ISDA_DOCUMENT_SEARCH
    max_results: 5
$$;


-- ============================================================================
-- PART 6: VERIFICATION
-- ============================================================================

-- Verify all components
SHOW CORTEX SEARCH SERVICES IN ISDA_DOCUMENT_POC.SEMANTIC_VIEWS;
SHOW SEMANTIC VIEWS IN ISDA_DOCUMENT_POC.SEMANTIC_VIEWS;
SHOW AGENTS IN ISDA_DOCUMENT_POC.SEMANTIC_VIEWS;

-- Test the Agreement Terms view
SELECT PARTY_A, PARTY_B, CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A, CROSS_DEFAULT_SOURCE
FROM V_AGREEMENT_TERMS;


-- ============================================================================
-- PART 7: SAMPLE AGENT QUERIES
-- ============================================================================

/*
Test the agent with these questions:

1. Structured Data (Agreement Terms):
   "What is the current cross-default threshold for Barclays?"
   "Which agreements have been amended?"
   "List all ISDA agreements with their counterparties"

2. Relationship Queries (Knowledge Graph):
   "Which documents is Bank of America party to?"
   "Show the amendment history for agreements"
   "What events of default are defined in the system?"

3. Document Search (Raw Text):
   "Find the definition of Early Termination Date"
   "What does the agreement say about Automatic Early Termination?"
   "Search for bankruptcy provisions"
*/
