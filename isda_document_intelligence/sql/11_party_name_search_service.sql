-- ============================================================================
-- 11_party_name_search_service.sql
-- Creates Cortex Search Service for party name lookup to improve Cortex Analyst accuracy
-- ============================================================================

USE DATABASE ISDA_DOCUMENT_POC;
USE SCHEMA SEMANTIC_VIEWS;

-- ----------------------------------------------------------------------------
-- Create Cortex Search Service for Party Names
-- ----------------------------------------------------------------------------
-- This search service enables fuzzy/semantic matching on party names, so when
-- a user asks about "Barclays", it can match "BARCLAYS BANK PLC" accurately.
--
-- The service indexes unique party names from both PARTY_A and PARTY_B columns
-- in the V_AGREEMENT_TERMS view.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE CORTEX SEARCH SERVICE PARTY_NAME_SEARCH
ON PARTY_NAME
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT DISTINCT PARTY_NAME FROM (
        SELECT PARTY_A AS PARTY_NAME FROM ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.V_AGREEMENT_TERMS
        UNION
        SELECT PARTY_B AS PARTY_NAME FROM ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.V_AGREEMENT_TERMS
    ) WHERE PARTY_NAME IS NOT NULL
);

-- ----------------------------------------------------------------------------
-- Verify the search service was created
-- ----------------------------------------------------------------------------
SHOW CORTEX SEARCH SERVICES LIKE 'PARTY_NAME_SEARCH';

-- ----------------------------------------------------------------------------
-- Test the search service with a sample query
-- ----------------------------------------------------------------------------
-- This demonstrates how the service can match "Barclays" to "BARCLAYS BANK PLC"

SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.PARTY_NAME_SEARCH',
    '{
        "query": "Barclays",
        "columns": ["PARTY_NAME"],
        "limit": 5
    }'
);

-- ----------------------------------------------------------------------------
-- Usage Notes:
-- ----------------------------------------------------------------------------
-- The semantic model (isda_agreement_terms.yaml) references this search service
-- in the cortex_search_service section of the party_a and party_b dimensions:
--
--   cortex_search_service:
--     service: PARTY_NAME_SEARCH
--     literal_column: PARTY_NAME
--     database: ISDA_DOCUMENT_POC
--     schema: SEMANTIC_VIEWS
--
-- When Cortex Analyst processes a query like "What is the cross-default threshold
-- for Barclays?", it will use this search service to find the exact party name
-- ("BARCLAYS BANK PLC") before generating the SQL WHERE clause.
-- ----------------------------------------------------------------------------
