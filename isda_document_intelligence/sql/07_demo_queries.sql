/*
================================================================================
ISDA Document Intelligence POC - Demo Queries
================================================================================
Sample queries demonstrating the POC capabilities:
1. Basic extraction queries
2. Event analysis queries
3. Agentic "double-click" queries using AI_COMPLETE
4. Knowledge graph traversal
5. Cross-document comparison

Prerequisites:
- Run scripts 01-06 first
================================================================================
*/

USE SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

-- =============================================================================
-- DEMO 1: Basic Extraction Summary
-- =============================================================================
-- What documents do we have and what are the key terms?

SELECT 
    FILE_NAME,
    AGREEMENT_VERSION as VERSION,
    EFFECTIVE_DATE,
    PARTY_A_NAME,
    PARTY_B_NAME,
    GOVERNING_LAW,
    PAYMENT_METHOD,
    CLOSE_OUT_CALCULATION
FROM EXTRACTED_ISDA_MASTER ex
JOIN RAW_DOCUMENT_METADATA m ON ex.DOCUMENT_ID = m.DOCUMENT_ID
ORDER BY EFFECTIVE_DATE;

-- =============================================================================
-- DEMO 2: "What happens if Bankruptcy occurs?"
-- =============================================================================
-- Query the event termination view for a specific event type

SELECT 
    FILE_NAME,
    AGREEMENT_VERSION,
    TRIGGER_TYPE,
    GRACE_PERIOD,
    UNWINDING_MECHANISM,
    PAYMENT_METHOD
FROM V_EVENT_TERMINATION_SUMMARY
WHERE EVENT_TYPE ILIKE '%Bankruptcy%'
ORDER BY FILE_NAME;

-- =============================================================================
-- DEMO 3: "What triggers automatic early termination?"
-- =============================================================================
-- Find events that trigger automatic (not optional) termination

SELECT 
    FILE_NAME,
    AGREEMENT_VERSION,
    EVENT_CATEGORY,
    EVENT_TYPE,
    UNWINDING_MECHANISM
FROM V_EVENT_TERMINATION_SUMMARY
WHERE TRIGGER_TYPE = 'automatic'
ORDER BY FILE_NAME, EVENT_CATEGORY, EVENT_TYPE;

-- =============================================================================
-- DEMO 4: Compare Cross-Default Provisions
-- =============================================================================

SELECT 
    FILE_NAME,
    AGREEMENT_VERSION,
    PARTY_A_NAME || ' / ' || PARTY_B_NAME as COUNTERPARTIES,
    CROSS_DEFAULT_APPLICABLE,
    THRESHOLD_DISPLAY,
    UNWINDING_IF_TRIGGERED
FROM V_CROSS_DEFAULT_COMPARISON
ORDER BY FILE_NAME;

-- =============================================================================
-- DEMO 5: Agentic "Double-Click" - Deep Dive into a Clause
-- =============================================================================
-- Use AI_COMPLETE to interpret specific clauses in natural language
-- This demonstrates the "agentic reasoning" capability

-- Example: Explain the bankruptcy provisions in detail for a specific document
SELECT 
    m.FILE_NAME,
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'You are a legal analyst specializing in ISDA derivatives documentation. ',
            'Based on the following ISDA Master Agreement text, explain in detail: ',
            '1. What specific events constitute a Bankruptcy Event of Default? ',
            '2. What is the grace period before the event triggers? ',
            '3. Is early termination automatic or does the non-defaulting party have to elect it? ',
            '4. How are positions unwound (what calculation method is used)? ',
            '5. Any special provisions or modifications in the Schedule? ',
            
            'Document text: ',
            SUBSTRING(ft.FULL_TEXT, 1, 50000)
        )
    ) as BANKRUPTCY_ANALYSIS
FROM DOCUMENT_FULL_TEXT ft
JOIN RAW_DOCUMENT_METADATA m ON ft.DOCUMENT_ID = m.DOCUMENT_ID
WHERE m.FILE_NAME LIKE '%example 1%'
LIMIT 1;

-- =============================================================================
-- DEMO 6: Compare Unwinding Mechanisms Across Versions
-- =============================================================================
-- How do 1992 vs 2002 ISDA handle position close-out?

SELECT 
    AGREEMENT_VERSION,
    EVENT_CATEGORY,
    EVENT_TYPE,
    LISTAGG(DISTINCT UNWINDING_MECHANISM, ' | ') as UNWINDING_METHODS,
    LISTAGG(DISTINCT CLOSE_OUT_CALCULATION, ' | ') as CLOSE_OUT_CALCS
FROM V_EVENT_TERMINATION_SUMMARY
GROUP BY AGREEMENT_VERSION, EVENT_CATEGORY, EVENT_TYPE
ORDER BY AGREEMENT_VERSION, EVENT_CATEGORY, EVENT_TYPE;

-- =============================================================================
-- DEMO 7: Knowledge Graph - Find All Agreements for a Party
-- =============================================================================

SELECT 
    PARTY_NAME,
    ROLE_IN_AGREEMENT,
    AGREEMENT,
    VERSION,
    EFFECTIVE_DATE,
    GOVERNING_LAW
FROM V_PARTY_PORTFOLIO
WHERE PARTY_NAME ILIKE '%Bank%'
ORDER BY PARTY_NAME, EFFECTIVE_DATE;

-- =============================================================================
-- DEMO 8: Knowledge Graph - Traverse from Party to Events
-- =============================================================================
-- Find all events of default that apply to a specific party

SELECT 
    p.NODE_LABEL as PARTY,
    d.NODE_LABEL as AGREEMENT,
    eod.NODE_LABEL as EVENT_TYPE,
    eod.PROPERTIES:triggers_early_termination::STRING as TRIGGER,
    eod.PROPERTIES:grace_period::STRING as GRACE_PERIOD,
    eod.PROPERTIES:unwinding_mechanism::STRING as UNWINDING
FROM DOCUMENT_NODES p
JOIN DOCUMENT_EDGES e1 ON p.NODE_ID = e1.SOURCE_NODE_ID AND e1.EDGE_TYPE = 'PARTY_TO'
JOIN DOCUMENT_NODES d ON e1.TARGET_NODE_ID = d.NODE_ID
JOIN DOCUMENT_EDGES e2 ON d.NODE_ID = e2.SOURCE_NODE_ID AND e2.EDGE_TYPE = 'CONTAINS'
JOIN DOCUMENT_NODES eod ON e2.TARGET_NODE_ID = eod.NODE_ID AND eod.NODE_TYPE = 'EVENT_OF_DEFAULT'
WHERE p.NODE_LABEL ILIKE '%Barclays%'
ORDER BY eod.NODE_LABEL;

-- =============================================================================
-- DEMO 9: Agentic Query - Natural Language Question
-- =============================================================================
-- Ask a complex question about the documents

SELECT 
    SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        CONCAT(
            'Based on the following ISDA agreement summaries, answer this question: ',
            '"Which agreements have the most protective provisions for the bank counterparty ',
            'in terms of automatic early termination and cross-default thresholds?" ',
            
            'Agreement summaries: ',
            (SELECT LISTAGG(
                CONCAT(
                    'Agreement: ', PARTY_A_NAME, ' / ', PARTY_B_NAME, 
                    ', Version: ', AGREEMENT_VERSION,
                    ', AET Party A: ', AUTOMATIC_EARLY_TERMINATION_PARTY_A,
                    ', AET Party B: ', AUTOMATIC_EARLY_TERMINATION_PARTY_B,
                    ', Cross Default: ', CROSS_DEFAULT_APPLICABLE,
                    ', Payment Method: ', PAYMENT_METHOD
                ), 
                ' | '
            ) FROM EXTRACTED_ISDA_MASTER)
        )
    ) as ANALYSIS;

-- =============================================================================
-- DEMO 10: Termination Event Waiting Periods
-- =============================================================================
-- For Termination Events, what waiting periods apply before termination?

SELECT 
    FILE_NAME,
    EVENT_TYPE,
    AFFECTED_PARTY,
    WAITING_PERIOD,
    TRIGGER_TYPE,
    UNWINDING_MECHANISM
FROM V_EVENT_TERMINATION_SUMMARY
WHERE EVENT_CATEGORY = 'Termination Event'
  AND WAITING_PERIOD IS NOT NULL
ORDER BY FILE_NAME, EVENT_TYPE;

-- =============================================================================
-- DEMO 11: Full Document Text Search
-- =============================================================================
-- Search across all documents for specific terms

SELECT 
    m.FILE_NAME,
    m.DOCUMENT_TYPE,
    POSITION('cross default' IN LOWER(ft.FULL_TEXT)) as CROSS_DEFAULT_POS,
    POSITION('bankruptcy' IN LOWER(ft.FULL_TEXT)) as BANKRUPTCY_POS,
    POSITION('force majeure' IN LOWER(ft.FULL_TEXT)) as FORCE_MAJEURE_POS
FROM DOCUMENT_FULL_TEXT ft
JOIN RAW_DOCUMENT_METADATA m ON ft.DOCUMENT_ID = m.DOCUMENT_ID;

-- =============================================================================
-- DEMO 12: Processing Status Dashboard
-- =============================================================================

SELECT 
    PROCESSING_STATUS,
    COUNT(*) as DOCUMENT_COUNT,
    SUM(FILE_SIZE_BYTES) / 1024 as TOTAL_SIZE_KB
FROM RAW_DOCUMENT_METADATA
GROUP BY PROCESSING_STATUS;

SELECT 'Demo queries complete!' as STATUS;
