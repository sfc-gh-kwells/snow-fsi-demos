/*
================================================================================
ISDA Document Intelligence POC - Populate Knowledge Graph
================================================================================
This script populates the knowledge graph tables (DOCUMENT_NODES, DOCUMENT_EDGES,
COUNTERPARTY_RELATIONSHIPS) from extracted ISDA data.

The knowledge graph enables:
- Traversing relationships between documents
- Finding all agreements for a counterparty
- Tracking amendment hierarchies
- Resolution logic (later supersedes earlier)

Prerequisites:
- Run scripts 01-04 first
================================================================================
*/

USE SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

-- =============================================================================
-- STEP 1: Create Document Nodes
-- =============================================================================
-- Each document becomes a node in the graph

INSERT INTO DOCUMENT_NODES (NODE_ID, NODE_TYPE, NODE_LABEL, DOCUMENT_ID, PROPERTIES)
SELECT 
    CONCAT('DOC_', DOCUMENT_ID) as NODE_ID,
    'DOCUMENT' as NODE_TYPE,
    CONCAT(AGREEMENT_VERSION, ' ISDA - ', PARTY_A_NAME, ' / ', PARTY_B_NAME) as NODE_LABEL,
    DOCUMENT_ID,
    OBJECT_CONSTRUCT(
        'agreement_version', AGREEMENT_VERSION,
        'effective_date', EFFECTIVE_DATE,
        'governing_law', GOVERNING_LAW,
        'payment_method', PAYMENT_METHOD,
        'close_out_calculation', CLOSE_OUT_CALCULATION
    ) as PROPERTIES
FROM EXTRACTED_ISDA_MASTER;

-- =============================================================================
-- STEP 2: Create Party Nodes
-- =============================================================================
-- Each unique party becomes a node

-- Party A nodes
INSERT INTO DOCUMENT_NODES (NODE_ID, NODE_TYPE, NODE_LABEL, PROPERTIES)
SELECT DISTINCT
    CONCAT('PARTY_', MD5(UPPER(PARTY_A_NAME))) as NODE_ID,
    'PARTY' as NODE_TYPE,
    PARTY_A_NAME as NODE_LABEL,
    OBJECT_CONSTRUCT(
        'party_type', PARTY_A_TYPE,
        'name_normalized', UPPER(PARTY_A_NAME)
    ) as PROPERTIES
FROM EXTRACTED_ISDA_MASTER
WHERE NOT EXISTS (
    SELECT 1 FROM DOCUMENT_NODES 
    WHERE NODE_ID = CONCAT('PARTY_', MD5(UPPER(EXTRACTED_ISDA_MASTER.PARTY_A_NAME)))
);

-- Party B nodes
INSERT INTO DOCUMENT_NODES (NODE_ID, NODE_TYPE, NODE_LABEL, PROPERTIES)
SELECT DISTINCT
    CONCAT('PARTY_', MD5(UPPER(PARTY_B_NAME))) as NODE_ID,
    'PARTY' as NODE_TYPE,
    PARTY_B_NAME as NODE_LABEL,
    OBJECT_CONSTRUCT(
        'party_type', PARTY_B_TYPE,
        'name_normalized', UPPER(PARTY_B_NAME)
    ) as PROPERTIES
FROM EXTRACTED_ISDA_MASTER
WHERE NOT EXISTS (
    SELECT 1 FROM DOCUMENT_NODES 
    WHERE NODE_ID = CONCAT('PARTY_', MD5(UPPER(EXTRACTED_ISDA_MASTER.PARTY_B_NAME)))
);

-- =============================================================================
-- STEP 3: Create Event Nodes (Events of Default)
-- =============================================================================
INSERT INTO DOCUMENT_NODES (NODE_ID, NODE_TYPE, NODE_LABEL, DOCUMENT_ID, PROPERTIES)
SELECT 
    CONCAT('EOD_', ex.DOCUMENT_ID, '_', eod.index) as NODE_ID,
    'EVENT_OF_DEFAULT' as NODE_TYPE,
    eod.value:event_type::STRING as NODE_LABEL,
    ex.DOCUMENT_ID,
    OBJECT_CONSTRUCT(
        'event_type', eod.value:event_type::STRING,
        'triggers_early_termination', eod.value:triggers_early_termination::STRING,
        'grace_period', eod.value:grace_period::STRING,
        'unwinding_mechanism', eod.value:unwinding_mechanism::STRING
    ) as PROPERTIES
FROM EXTRACTED_ISDA_MASTER ex,
LATERAL FLATTEN(input => ex.EVENTS_OF_DEFAULT) eod;

-- =============================================================================
-- STEP 4: Create Termination Event Nodes
-- =============================================================================
INSERT INTO DOCUMENT_NODES (NODE_ID, NODE_TYPE, NODE_LABEL, DOCUMENT_ID, PROPERTIES)
SELECT 
    CONCAT('TE_', ex.DOCUMENT_ID, '_', te.index) as NODE_ID,
    'TERMINATION_EVENT' as NODE_TYPE,
    te.value:event_type::STRING as NODE_LABEL,
    ex.DOCUMENT_ID,
    OBJECT_CONSTRUCT(
        'event_type', te.value:event_type::STRING,
        'triggers_early_termination', te.value:triggers_early_termination::STRING,
        'affected_party', te.value:affected_party::STRING,
        'waiting_period', te.value:waiting_period::STRING,
        'unwinding_mechanism', te.value:unwinding_mechanism::STRING
    ) as PROPERTIES
FROM EXTRACTED_ISDA_MASTER ex,
LATERAL FLATTEN(input => ex.TERMINATION_EVENTS) te;

-- =============================================================================
-- STEP 5: Create Edges - Party to Document Relationships
-- =============================================================================
-- Party A -> Document (as Party A)
INSERT INTO DOCUMENT_EDGES (EDGE_ID, SOURCE_NODE_ID, TARGET_NODE_ID, EDGE_TYPE, EFFECTIVE_DATE, PROPERTIES)
SELECT 
    CONCAT('EDGE_PA_', DOCUMENT_ID) as EDGE_ID,
    CONCAT('PARTY_', MD5(UPPER(PARTY_A_NAME))) as SOURCE_NODE_ID,
    CONCAT('DOC_', DOCUMENT_ID) as TARGET_NODE_ID,
    'PARTY_TO' as EDGE_TYPE,
    EFFECTIVE_DATE,
    OBJECT_CONSTRUCT('role', 'Party A') as PROPERTIES
FROM EXTRACTED_ISDA_MASTER;

-- Party B -> Document (as Party B)
INSERT INTO DOCUMENT_EDGES (EDGE_ID, SOURCE_NODE_ID, TARGET_NODE_ID, EDGE_TYPE, EFFECTIVE_DATE, PROPERTIES)
SELECT 
    CONCAT('EDGE_PB_', DOCUMENT_ID) as EDGE_ID,
    CONCAT('PARTY_', MD5(UPPER(PARTY_B_NAME))) as SOURCE_NODE_ID,
    CONCAT('DOC_', DOCUMENT_ID) as TARGET_NODE_ID,
    'PARTY_TO' as EDGE_TYPE,
    EFFECTIVE_DATE,
    OBJECT_CONSTRUCT('role', 'Party B') as PROPERTIES
FROM EXTRACTED_ISDA_MASTER;

-- =============================================================================
-- STEP 6: Create Edges - Document Contains Events
-- =============================================================================
-- Document -> Events of Default
INSERT INTO DOCUMENT_EDGES (EDGE_ID, SOURCE_NODE_ID, TARGET_NODE_ID, EDGE_TYPE, PROPERTIES)
SELECT 
    CONCAT('EDGE_DOC_EOD_', ex.DOCUMENT_ID, '_', eod.index) as EDGE_ID,
    CONCAT('DOC_', ex.DOCUMENT_ID) as SOURCE_NODE_ID,
    CONCAT('EOD_', ex.DOCUMENT_ID, '_', eod.index) as TARGET_NODE_ID,
    'CONTAINS' as EDGE_TYPE,
    OBJECT_CONSTRUCT('section', 'Section 5(a)') as PROPERTIES
FROM EXTRACTED_ISDA_MASTER ex,
LATERAL FLATTEN(input => ex.EVENTS_OF_DEFAULT) eod;

-- Document -> Termination Events
INSERT INTO DOCUMENT_EDGES (EDGE_ID, SOURCE_NODE_ID, TARGET_NODE_ID, EDGE_TYPE, PROPERTIES)
SELECT 
    CONCAT('EDGE_DOC_TE_', ex.DOCUMENT_ID, '_', te.index) as EDGE_ID,
    CONCAT('DOC_', ex.DOCUMENT_ID) as SOURCE_NODE_ID,
    CONCAT('TE_', ex.DOCUMENT_ID, '_', te.index) as TARGET_NODE_ID,
    'CONTAINS' as EDGE_TYPE,
    OBJECT_CONSTRUCT('section', 'Section 5(b)') as PROPERTIES
FROM EXTRACTED_ISDA_MASTER ex,
LATERAL FLATTEN(input => ex.TERMINATION_EVENTS) te;

-- =============================================================================
-- STEP 7: Populate Counterparty Relationships
-- =============================================================================
INSERT INTO COUNTERPARTY_RELATIONSHIPS 
(RELATIONSHIP_ID, PARTY_A_NAME, PARTY_B_NAME, RELATIONSHIP_TYPE, BASE_DOCUMENT_ID, 
 CURRENT_EFFECTIVE_DATE, RELATIONSHIP_STATUS, PROPERTIES)
SELECT 
    CONCAT('REL_', DOCUMENT_ID) as RELATIONSHIP_ID,
    PARTY_A_NAME,
    PARTY_B_NAME,
    CASE 
        WHEN AGREEMENT_VERSION = '2002' THEN 'ISDA_2002_MASTER'
        ELSE 'ISDA_1992_MASTER'
    END as RELATIONSHIP_TYPE,
    DOCUMENT_ID as BASE_DOCUMENT_ID,
    EFFECTIVE_DATE as CURRENT_EFFECTIVE_DATE,
    'ACTIVE' as RELATIONSHIP_STATUS,
    OBJECT_CONSTRUCT(
        'governing_law', GOVERNING_LAW,
        'cross_default', CROSS_DEFAULT_APPLICABLE,
        'automatic_early_termination', OBJECT_CONSTRUCT(
            'party_a', AUTOMATIC_EARLY_TERMINATION_PARTY_A,
            'party_b', AUTOMATIC_EARLY_TERMINATION_PARTY_B
        )
    ) as PROPERTIES
FROM EXTRACTED_ISDA_MASTER;

-- =============================================================================
-- STEP 8: Verify Knowledge Graph
-- =============================================================================
-- Node counts by type
SELECT 
    NODE_TYPE,
    COUNT(*) as NODE_COUNT
FROM DOCUMENT_NODES
GROUP BY NODE_TYPE
ORDER BY NODE_COUNT DESC;

-- Edge counts by type
SELECT 
    EDGE_TYPE,
    COUNT(*) as EDGE_COUNT
FROM DOCUMENT_EDGES
GROUP BY EDGE_TYPE
ORDER BY EDGE_COUNT DESC;

-- Counterparty relationships
SELECT 
    PARTY_A_NAME,
    PARTY_B_NAME,
    RELATIONSHIP_TYPE,
    CURRENT_EFFECTIVE_DATE,
    RELATIONSHIP_STATUS
FROM COUNTERPARTY_RELATIONSHIPS
ORDER BY CURRENT_EFFECTIVE_DATE;

SELECT 'Knowledge graph populated successfully!' as STATUS;
