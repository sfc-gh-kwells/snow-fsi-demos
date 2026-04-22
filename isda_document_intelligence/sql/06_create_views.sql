/*
================================================================================
ISDA Document Intelligence POC - Create Views
================================================================================
This script creates analytical views for querying ISDA data, including:
- Event termination summary (what happens for each event type)
- Counterparty exposure analysis
- Cross-default threshold comparison
- Amendment resolution (later supersedes earlier)

Prerequisites:
- Run scripts 01-05 first
================================================================================
*/

USE SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

-- =============================================================================
-- VIEW 1: Event Termination Summary
-- =============================================================================
-- "What happens when X event occurs?" - Shows termination type and unwinding
-- mechanism for each event type across all documents

CREATE OR REPLACE VIEW V_EVENT_TERMINATION_SUMMARY AS
SELECT 
    m.FILE_NAME,
    ex.AGREEMENT_VERSION,
    ex.PARTY_A_NAME,
    ex.PARTY_B_NAME,
    'Event of Default' as EVENT_CATEGORY,
    eod.value:event_type::STRING as EVENT_TYPE,
    eod.value:triggers_early_termination::STRING as TRIGGER_TYPE,
    eod.value:grace_period::STRING as GRACE_PERIOD,
    NULL as AFFECTED_PARTY,
    NULL as WAITING_PERIOD,
    eod.value:unwinding_mechanism::STRING as UNWINDING_MECHANISM,
    ex.PAYMENT_METHOD,
    ex.CLOSE_OUT_CALCULATION
FROM EXTRACTED_ISDA_MASTER ex
JOIN RAW_DOCUMENT_METADATA m ON ex.DOCUMENT_ID = m.DOCUMENT_ID,
LATERAL FLATTEN(input => ex.EVENTS_OF_DEFAULT) eod

UNION ALL

SELECT 
    m.FILE_NAME,
    ex.AGREEMENT_VERSION,
    ex.PARTY_A_NAME,
    ex.PARTY_B_NAME,
    'Termination Event' as EVENT_CATEGORY,
    te.value:event_type::STRING as EVENT_TYPE,
    te.value:triggers_early_termination::STRING as TRIGGER_TYPE,
    NULL as GRACE_PERIOD,
    te.value:affected_party::STRING as AFFECTED_PARTY,
    te.value:waiting_period::STRING as WAITING_PERIOD,
    te.value:unwinding_mechanism::STRING as UNWINDING_MECHANISM,
    ex.PAYMENT_METHOD,
    ex.CLOSE_OUT_CALCULATION
FROM EXTRACTED_ISDA_MASTER ex
JOIN RAW_DOCUMENT_METADATA m ON ex.DOCUMENT_ID = m.DOCUMENT_ID,
LATERAL FLATTEN(input => ex.TERMINATION_EVENTS) te;

COMMENT ON VIEW V_EVENT_TERMINATION_SUMMARY IS 
'Shows what happens for each termination event type - trigger mechanism, grace periods, and how positions are unwound';

-- =============================================================================
-- VIEW 2: Counterparty Exposure Summary
-- =============================================================================
-- Shows all agreements for each counterparty pair

CREATE OR REPLACE VIEW V_COUNTERPARTY_EXPOSURE AS
SELECT 
    PARTY_A_NAME,
    PARTY_B_NAME,
    COUNT(*) as AGREEMENT_COUNT,
    LISTAGG(DISTINCT AGREEMENT_VERSION, ', ') WITHIN GROUP (ORDER BY AGREEMENT_VERSION) as AGREEMENT_VERSIONS,
    MIN(EFFECTIVE_DATE) as EARLIEST_AGREEMENT,
    MAX(EFFECTIVE_DATE) as LATEST_AGREEMENT,
    SUM(CASE WHEN CROSS_DEFAULT_APPLICABLE THEN 1 ELSE 0 END) as CROSS_DEFAULT_COUNT,
    SUM(CASE WHEN AUTOMATIC_EARLY_TERMINATION_PARTY_A OR AUTOMATIC_EARLY_TERMINATION_PARTY_B THEN 1 ELSE 0 END) as AET_ENABLED_COUNT
FROM EXTRACTED_ISDA_MASTER
GROUP BY PARTY_A_NAME, PARTY_B_NAME;

COMMENT ON VIEW V_COUNTERPARTY_EXPOSURE IS 
'Summary of all ISDA agreements grouped by counterparty pair';

-- =============================================================================
-- VIEW 3: Cross-Default Comparison
-- =============================================================================
-- Compare cross-default provisions across all agreements

CREATE OR REPLACE VIEW V_CROSS_DEFAULT_COMPARISON AS
SELECT 
    m.FILE_NAME,
    ex.AGREEMENT_VERSION,
    ex.PARTY_A_NAME,
    ex.PARTY_B_NAME,
    ex.CROSS_DEFAULT_APPLICABLE,
    ex.CROSS_DEFAULT_THRESHOLD_AMOUNT,
    ex.CROSS_DEFAULT_THRESHOLD_CURRENCY,
    CASE 
        WHEN ex.CROSS_DEFAULT_THRESHOLD_AMOUNT IS NOT NULL 
        THEN CONCAT('$', TO_CHAR(ex.CROSS_DEFAULT_THRESHOLD_AMOUNT, '999,999,999'), ' ', COALESCE(ex.CROSS_DEFAULT_THRESHOLD_CURRENCY, 'USD'))
        ELSE 'Not specified'
    END as THRESHOLD_DISPLAY,
    CASE 
        WHEN ex.CROSS_DEFAULT_APPLICABLE = TRUE 
        THEN COALESCE(ex.CLOSE_OUT_CALCULATION, 'Close-out Amount')
        ELSE 'N/A - Cross Default not applicable'
    END as UNWINDING_IF_TRIGGERED
FROM EXTRACTED_ISDA_MASTER ex
JOIN RAW_DOCUMENT_METADATA m ON ex.DOCUMENT_ID = m.DOCUMENT_ID;

COMMENT ON VIEW V_CROSS_DEFAULT_COMPARISON IS 
'Compares cross-default provisions and thresholds across all ISDA agreements';

-- =============================================================================
-- VIEW 4: Automatic Early Termination Analysis
-- =============================================================================
-- Which agreements have AET enabled and for which parties

CREATE OR REPLACE VIEW V_AUTOMATIC_EARLY_TERMINATION AS
SELECT 
    m.FILE_NAME,
    ex.AGREEMENT_VERSION,
    ex.PARTY_A_NAME,
    ex.AUTOMATIC_EARLY_TERMINATION_PARTY_A as AET_PARTY_A,
    ex.PARTY_B_NAME,
    ex.AUTOMATIC_EARLY_TERMINATION_PARTY_B as AET_PARTY_B,
    CASE 
        WHEN ex.AUTOMATIC_EARLY_TERMINATION_PARTY_A AND ex.AUTOMATIC_EARLY_TERMINATION_PARTY_B THEN 'Both Parties'
        WHEN ex.AUTOMATIC_EARLY_TERMINATION_PARTY_A THEN 'Party A Only'
        WHEN ex.AUTOMATIC_EARLY_TERMINATION_PARTY_B THEN 'Party B Only'
        ELSE 'Neither Party'
    END as AET_STATUS,
    ex.CLOSE_OUT_CALCULATION,
    ex.PAYMENT_METHOD
FROM EXTRACTED_ISDA_MASTER ex
JOIN RAW_DOCUMENT_METADATA m ON ex.DOCUMENT_ID = m.DOCUMENT_ID;

COMMENT ON VIEW V_AUTOMATIC_EARLY_TERMINATION IS 
'Shows which agreements have Automatic Early Termination enabled for each party';

-- =============================================================================
-- VIEW 5: Document Graph Traversal
-- =============================================================================
-- Shows all relationships in the knowledge graph

CREATE OR REPLACE VIEW V_DOCUMENT_GRAPH AS
SELECT 
    sn.NODE_LABEL as SOURCE_LABEL,
    sn.NODE_TYPE as SOURCE_TYPE,
    e.EDGE_TYPE,
    tn.NODE_LABEL as TARGET_LABEL,
    tn.NODE_TYPE as TARGET_TYPE,
    e.EFFECTIVE_DATE,
    e.PROPERTIES as EDGE_PROPERTIES
FROM DOCUMENT_EDGES e
JOIN DOCUMENT_NODES sn ON e.SOURCE_NODE_ID = sn.NODE_ID
JOIN DOCUMENT_NODES tn ON e.TARGET_NODE_ID = tn.NODE_ID;

COMMENT ON VIEW V_DOCUMENT_GRAPH IS 
'Knowledge graph view showing all node relationships';

-- =============================================================================
-- VIEW 6: Party Portfolio
-- =============================================================================
-- All agreements for a specific party (as either Party A or B)

CREATE OR REPLACE VIEW V_PARTY_PORTFOLIO AS
SELECT 
    ex.PARTY_A_NAME as PARTY_NAME,
    'Party A' as ROLE_IN_AGREEMENT,
    m.FILE_NAME as AGREEMENT,
    ex.AGREEMENT_VERSION as VERSION,
    ex.EFFECTIVE_DATE,
    ex.GOVERNING_LAW,
    ex.DOCUMENT_ID
FROM EXTRACTED_ISDA_MASTER ex
JOIN RAW_DOCUMENT_METADATA m ON ex.DOCUMENT_ID = m.DOCUMENT_ID
UNION ALL
SELECT 
    ex.PARTY_B_NAME as PARTY_NAME,
    'Party B' as ROLE_IN_AGREEMENT,
    m.FILE_NAME as AGREEMENT,
    ex.AGREEMENT_VERSION as VERSION,
    ex.EFFECTIVE_DATE,
    ex.GOVERNING_LAW,
    ex.DOCUMENT_ID
FROM EXTRACTED_ISDA_MASTER ex
JOIN RAW_DOCUMENT_METADATA m ON ex.DOCUMENT_ID = m.DOCUMENT_ID;

COMMENT ON VIEW V_PARTY_PORTFOLIO IS 
'Shows all agreements for each party with their role (Party A or B)';

-- =============================================================================
-- VIEW 7: Amendment Resolution (Latest Values)
-- =============================================================================
-- For clause versioning - later effective dates supersede earlier

CREATE OR REPLACE VIEW V_CURRENT_CLAUSE_VALUES AS
SELECT 
    cv.*,
    m.FILE_NAME as SOURCE_DOCUMENT
FROM CLAUSE_VERSIONS cv
JOIN RAW_DOCUMENT_METADATA m ON cv.DOCUMENT_ID = m.DOCUMENT_ID
WHERE cv.IS_CURRENT = TRUE;

COMMENT ON VIEW V_CURRENT_CLAUSE_VALUES IS 
'Shows current (non-superseded) clause values after amendment resolution';

-- =============================================================================
-- Verify Views
-- =============================================================================
SHOW VIEWS IN SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

SELECT 'Views created successfully!' as STATUS;
