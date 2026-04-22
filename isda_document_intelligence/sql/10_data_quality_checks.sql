/*
================================================================================
ISDA Document Intelligence POC - Data Quality Checks
================================================================================
Run these queries to verify data integrity across all tables.

Prerequisites: Run scripts 01-08 first.
================================================================================
*/

USE DATABASE ISDA_DOCUMENT_POC;
USE SCHEMA DOCUMENT_INTELLIGENCE;

-- ============================================================================
-- SUMMARY STATISTICS
-- ============================================================================

SELECT '=== DATA SUMMARY ===' as SECTION;

SELECT 'RAW_DOCUMENT_METADATA' as TABLE_NAME, COUNT(*) as ROW_COUNT FROM RAW_DOCUMENT_METADATA
UNION ALL SELECT 'DOCUMENT_FULL_TEXT', COUNT(*) FROM DOCUMENT_FULL_TEXT
UNION ALL SELECT 'EXTRACTED_ISDA_MASTER', COUNT(*) FROM EXTRACTED_ISDA_MASTER
UNION ALL SELECT 'EXTRACTED_CSA', COUNT(*) FROM EXTRACTED_CSA
UNION ALL SELECT 'EXTRACTED_AMENDMENTS', COUNT(*) FROM EXTRACTED_AMENDMENTS
UNION ALL SELECT 'DOCUMENT_NODES', COUNT(*) FROM DOCUMENT_NODES
UNION ALL SELECT 'DOCUMENT_EDGES', COUNT(*) FROM DOCUMENT_EDGES
UNION ALL SELECT 'COUNTERPARTY_RELATIONSHIPS', COUNT(*) FROM COUNTERPARTY_RELATIONSHIPS
UNION ALL SELECT 'CLAUSE_VERSIONS', COUNT(*) FROM CLAUSE_VERSIONS;


-- ============================================================================
-- DATA QUALITY CHECK 1: Orphaned Documents
-- ============================================================================
-- Documents that were uploaded but not fully processed

SELECT '=== ORPHANED DOCUMENTS ===' as SECTION;

-- Documents with metadata but no parsed text
SELECT 'Missing DOCUMENT_FULL_TEXT' as ISSUE, m.DOCUMENT_ID, m.FILE_NAME
FROM RAW_DOCUMENT_METADATA m
LEFT JOIN DOCUMENT_FULL_TEXT ft ON m.DOCUMENT_ID = ft.DOCUMENT_ID
WHERE ft.DOCUMENT_ID IS NULL;

-- ISDA documents with parsed text but no extraction
SELECT 'Missing EXTRACTED_ISDA_MASTER' as ISSUE, m.DOCUMENT_ID, m.FILE_NAME
FROM RAW_DOCUMENT_METADATA m
JOIN DOCUMENT_FULL_TEXT ft ON m.DOCUMENT_ID = ft.DOCUMENT_ID
LEFT JOIN EXTRACTED_ISDA_MASTER im ON m.DOCUMENT_ID = im.DOCUMENT_ID
WHERE im.DOCUMENT_ID IS NULL 
  AND m.DOCUMENT_TYPE IN ('ISDA_MASTER_AGREEMENT', 'ISDA_2002_MASTER_AGREEMENT');


-- ============================================================================
-- DATA QUALITY CHECK 2: Missing or Invalid Data
-- ============================================================================

SELECT '=== MISSING/INVALID DATA ===' as SECTION;

-- Documents with NULL page count that should have it
SELECT 'Missing PAGE_COUNT' as ISSUE, DOCUMENT_ID
FROM DOCUMENT_FULL_TEXT 
WHERE PAGE_COUNT IS NULL 
  AND PARSED_CONTENT:metadata:pageCount IS NOT NULL;

-- Master agreements missing party names
SELECT 'Missing Party Names' as ISSUE, DOCUMENT_ID, PARTY_A_NAME, PARTY_B_NAME
FROM EXTRACTED_ISDA_MASTER
WHERE PARTY_A_NAME IS NULL OR PARTY_B_NAME IS NULL;

-- Amendments without parent agreement reference
SELECT 'Orphaned Amendments' as ISSUE, a.DOCUMENT_ID, a.PARENT_MASTER_AGREEMENT_ID
FROM EXTRACTED_AMENDMENTS a
LEFT JOIN EXTRACTED_ISDA_MASTER m ON a.PARENT_MASTER_AGREEMENT_ID = m.DOCUMENT_ID
WHERE m.DOCUMENT_ID IS NULL;

-- CSAs without parent agreement reference
SELECT 'Orphaned CSAs' as ISSUE, c.DOCUMENT_ID, c.PARENT_MASTER_AGREEMENT_ID
FROM EXTRACTED_CSA c
LEFT JOIN EXTRACTED_ISDA_MASTER m ON c.PARENT_MASTER_AGREEMENT_ID = m.DOCUMENT_ID
WHERE m.DOCUMENT_ID IS NULL;


-- ============================================================================
-- DATA QUALITY CHECK 3: Knowledge Graph Integrity
-- ============================================================================

SELECT '=== KNOWLEDGE GRAPH INTEGRITY ===' as SECTION;

-- Edges with missing source nodes
SELECT 'Missing Source Node' as ISSUE, e.EDGE_ID, e.SOURCE_NODE_ID
FROM DOCUMENT_EDGES e
LEFT JOIN DOCUMENT_NODES n ON e.SOURCE_NODE_ID = n.NODE_ID
WHERE n.NODE_ID IS NULL;

-- Edges with missing target nodes
SELECT 'Missing Target Node' as ISSUE, e.EDGE_ID, e.TARGET_NODE_ID
FROM DOCUMENT_EDGES e
LEFT JOIN DOCUMENT_NODES n ON e.TARGET_NODE_ID = n.NODE_ID
WHERE n.NODE_ID IS NULL;

-- Documents without any nodes
SELECT 'Document Missing Node' as ISSUE, m.DOCUMENT_ID, m.FILE_NAME
FROM RAW_DOCUMENT_METADATA m
LEFT JOIN DOCUMENT_NODES n ON n.NODE_ID = 'DOC_' || m.DOCUMENT_ID
WHERE n.NODE_ID IS NULL
  AND m.DOCUMENT_TYPE NOT LIKE 'syn-%';  -- Exclude synthetic docs that may not have nodes

-- Node type distribution
SELECT NODE_TYPE, COUNT(*) as COUNT
FROM DOCUMENT_NODES
GROUP BY NODE_TYPE
ORDER BY COUNT DESC;

-- Edge type distribution
SELECT EDGE_TYPE, COUNT(*) as COUNT
FROM DOCUMENT_EDGES
GROUP BY EDGE_TYPE
ORDER BY COUNT DESC;


-- ============================================================================
-- DATA QUALITY CHECK 4: Amendment Resolution Logic
-- ============================================================================

SELECT '=== AMENDMENT RESOLUTION ===' as SECTION;

-- Verify amendment hierarchy is correctly ranked
SELECT 
    m.PARTY_A_NAME,
    m.PARTY_B_NAME,
    a.AMENDMENT_NUMBER,
    a.EFFECTIVE_DATE,
    ROW_NUMBER() OVER (PARTITION BY a.PARENT_MASTER_AGREEMENT_ID ORDER BY a.EFFECTIVE_DATE DESC) as EXPECTED_RANK,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY a.PARENT_MASTER_AGREEMENT_ID ORDER BY a.EFFECTIVE_DATE DESC) = 1 
         THEN 'CURRENT' ELSE 'SUPERSEDED' END as STATUS
FROM EXTRACTED_AMENDMENTS a
JOIN EXTRACTED_ISDA_MASTER m ON a.PARENT_MASTER_AGREEMENT_ID = m.DOCUMENT_ID
ORDER BY a.PARENT_MASTER_AGREEMENT_ID, a.EFFECTIVE_DATE DESC;

-- Show resolution result for cross-default thresholds
SELECT 
    v.PARTY_A,
    v.PARTY_B,
    v.ORIGINAL_CROSS_DEFAULT_THRESHOLD as ORIGINAL,
    v.CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A as CURRENT,
    v.CROSS_DEFAULT_SOURCE as SOURCE,
    v.HAS_AMENDMENTS
FROM SEMANTIC_VIEWS.V_AGREEMENT_TERMS v
ORDER BY v.PARTY_A;


-- ============================================================================
-- DATA QUALITY CHECK 5: Counterparty Relationships
-- ============================================================================

SELECT '=== COUNTERPARTY RELATIONSHIPS ===' as SECTION;

-- All relationships should have valid base agreement reference
SELECT 'Invalid Base Agreement' as ISSUE, cr.RELATIONSHIP_ID, cr.BASE_ISDA_DOCUMENT_ID
FROM COUNTERPARTY_RELATIONSHIPS cr
LEFT JOIN EXTRACTED_ISDA_MASTER m ON cr.BASE_ISDA_DOCUMENT_ID = m.DOCUMENT_ID
WHERE m.DOCUMENT_ID IS NULL;

-- CSA references should be valid
SELECT 'Invalid CSA Reference' as ISSUE, cr.RELATIONSHIP_ID, cr.ACTIVE_CSA_DOCUMENT_ID
FROM COUNTERPARTY_RELATIONSHIPS cr
LEFT JOIN EXTRACTED_CSA c ON cr.ACTIVE_CSA_DOCUMENT_ID = c.DOCUMENT_ID
WHERE cr.ACTIVE_CSA_DOCUMENT_ID IS NOT NULL AND c.DOCUMENT_ID IS NULL;

-- Amendment references should be valid
SELECT 'Invalid Amendment Reference' as ISSUE, cr.RELATIONSHIP_ID, cr.LATEST_AMENDMENT_DOCUMENT_ID
FROM COUNTERPARTY_RELATIONSHIPS cr
LEFT JOIN EXTRACTED_AMENDMENTS a ON cr.LATEST_AMENDMENT_DOCUMENT_ID = a.DOCUMENT_ID
WHERE cr.LATEST_AMENDMENT_DOCUMENT_ID IS NOT NULL AND a.DOCUMENT_ID IS NULL;

-- Summary view of all relationships
SELECT 
    PARTY_A_NAME,
    PARTY_B_NAME,
    BASE_ISDA_DOCUMENT_ID IS NOT NULL as HAS_BASE,
    ACTIVE_CSA_DOCUMENT_ID IS NOT NULL as HAS_CSA,
    LATEST_AMENDMENT_DOCUMENT_ID IS NOT NULL as HAS_AMENDMENT,
    RELATIONSHIP_STATUS
FROM COUNTERPARTY_RELATIONSHIPS;


-- ============================================================================
-- DATA QUALITY CHECK 6: Semantic Views Schema
-- ============================================================================

SELECT '=== SEMANTIC VIEWS ===' as SECTION;

-- Verify V_AGREEMENT_TERMS has data
SELECT 'V_AGREEMENT_TERMS' as VIEW_NAME, COUNT(*) as ROW_COUNT 
FROM SEMANTIC_VIEWS.V_AGREEMENT_TERMS;

-- Verify V_KNOWLEDGE_GRAPH has data
SELECT 'V_KNOWLEDGE_GRAPH' as VIEW_NAME, COUNT(*) as ROW_COUNT 
FROM SEMANTIC_VIEWS.V_KNOWLEDGE_GRAPH;

-- Check semantic views exist
SHOW SEMANTIC VIEWS IN ISDA_DOCUMENT_POC.SEMANTIC_VIEWS;

-- Check search service status
SHOW CORTEX SEARCH SERVICES IN ISDA_DOCUMENT_POC.SEMANTIC_VIEWS;

-- Check agent exists
SHOW AGENTS IN ISDA_DOCUMENT_POC.SEMANTIC_VIEWS;


-- ============================================================================
-- FINAL SUMMARY
-- ============================================================================

SELECT '=== FINAL QUALITY SCORE ===' as SECTION;

WITH quality_checks AS (
    SELECT 'Orphaned documents' as CHECK_NAME,
           (SELECT COUNT(*) FROM RAW_DOCUMENT_METADATA m 
            LEFT JOIN DOCUMENT_FULL_TEXT ft ON m.DOCUMENT_ID = ft.DOCUMENT_ID 
            WHERE ft.DOCUMENT_ID IS NULL) as ISSUES
    UNION ALL
    SELECT 'Missing page counts',
           (SELECT COUNT(*) FROM DOCUMENT_FULL_TEXT 
            WHERE PAGE_COUNT IS NULL AND PARSED_CONTENT:metadata:pageCount IS NOT NULL)
    UNION ALL
    SELECT 'Missing party names',
           (SELECT COUNT(*) FROM EXTRACTED_ISDA_MASTER 
            WHERE PARTY_A_NAME IS NULL OR PARTY_B_NAME IS NULL)
    UNION ALL
    SELECT 'Orphaned amendments',
           (SELECT COUNT(*) FROM EXTRACTED_AMENDMENTS a 
            LEFT JOIN EXTRACTED_ISDA_MASTER m ON a.PARENT_MASTER_AGREEMENT_ID = m.DOCUMENT_ID 
            WHERE m.DOCUMENT_ID IS NULL)
    UNION ALL
    SELECT 'Missing graph edges',
           (SELECT COUNT(*) FROM DOCUMENT_EDGES e 
            LEFT JOIN DOCUMENT_NODES n ON e.SOURCE_NODE_ID = n.NODE_ID 
            WHERE n.NODE_ID IS NULL)
    UNION ALL
    SELECT 'Empty counterparty relationships',
           CASE WHEN (SELECT COUNT(*) FROM COUNTERPARTY_RELATIONSHIPS) = 0 THEN 1 ELSE 0 END
)
SELECT 
    CHECK_NAME,
    ISSUES,
    CASE WHEN ISSUES = 0 THEN '✓ PASS' ELSE '✗ FAIL' END as STATUS
FROM quality_checks;

SELECT 'Data quality check complete!' as STATUS;
