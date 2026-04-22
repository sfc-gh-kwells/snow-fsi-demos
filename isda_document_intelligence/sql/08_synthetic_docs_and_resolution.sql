/*
================================================================================
ISDA Document Intelligence POC - Synthetic Documents & Resolution Logic
================================================================================
This script creates synthetic documents (CSA, Amendments) and demonstrates
the amendment resolution logic where "later supersedes earlier".

Prerequisites:
- Run scripts 01-05 first
- This script creates synthetic documents that reference actual extracted parties
================================================================================
*/

USE SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

-- =============================================================================
-- STEP 1: Create Tables for CSA and Amendment Extractions
-- =============================================================================

CREATE TABLE IF NOT EXISTS EXTRACTED_CSA (
    DOCUMENT_ID VARCHAR(50) PRIMARY KEY,
    PARENT_MASTER_AGREEMENT_ID VARCHAR(50),
    EFFECTIVE_DATE DATE,
    PARTY_A_NAME VARCHAR(500),
    PARTY_B_NAME VARCHAR(500),
    PARTY_A_THRESHOLD_AMOUNT NUMBER(20,2),
    PARTY_A_THRESHOLD_CURRENCY VARCHAR(3),
    PARTY_A_THRESHOLD_CONDITIONS VARCHAR(2000),
    PARTY_B_THRESHOLD_AMOUNT NUMBER(20,2),
    PARTY_B_THRESHOLD_CURRENCY VARCHAR(3),
    PARTY_B_THRESHOLD_CONDITIONS VARCHAR(2000),
    ELIGIBLE_COLLATERAL ARRAY,
    MINIMUM_TRANSFER_AMOUNT NUMBER(20,2),
    VALUATION_AGENT VARCHAR(100),
    INTEREST_RATE VARCHAR(200),
    EXTRACTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RAW_EXTRACTION_JSON VARIANT
);

CREATE TABLE IF NOT EXISTS EXTRACTED_AMENDMENTS (
    DOCUMENT_ID VARCHAR(50) PRIMARY KEY,
    PARENT_MASTER_AGREEMENT_ID VARCHAR(50),
    AMENDMENT_NUMBER INTEGER,
    EFFECTIVE_DATE DATE,
    PARTY_A_NAME VARCHAR(500),
    PARTY_B_NAME VARCHAR(500),
    SUPERSEDES_DOCUMENT_ID VARCHAR(50),
    AMENDED_SECTIONS ARRAY,
    NEW_CROSS_DEFAULT_THRESHOLD_PARTY_A NUMBER(20,2),
    NEW_CROSS_DEFAULT_THRESHOLD_PARTY_B NUMBER(20,2),
    NEW_SPECIFIED_ENTITIES VARIANT,
    NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_A BOOLEAN,
    NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_B BOOLEAN,
    ADDITIONAL_TERMINATION_EVENTS ARRAY,
    CREDIT_SUPPORT_CHANGES VARIANT,
    EXTRACTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RAW_EXTRACTION_JSON VARIANT
);

-- Add METADATA column if not exists
ALTER TABLE RAW_DOCUMENT_METADATA ADD COLUMN IF NOT EXISTS METADATA VARIANT;

-- =============================================================================
-- STEP 2: Insert Synthetic Document Metadata
-- =============================================================================

-- CSA for Bank of America / LKQ Corporation
INSERT INTO RAW_DOCUMENT_METADATA (DOCUMENT_ID, FILE_NAME, DOCUMENT_TYPE, PROCESSING_STATUS, METADATA)
SELECT 'syn-csa-bofa-lkq-2024', 'CSA_BofA_LKQ_2024.txt', 'CREDIT_SUPPORT_ANNEX', 'PENDING',
       PARSE_JSON('{"synthetic": true, "parent_agreement": "6ff38f12-8ea5-4e81-8e29-6b0b018de1af"}')
WHERE NOT EXISTS (SELECT 1 FROM RAW_DOCUMENT_METADATA WHERE DOCUMENT_ID = 'syn-csa-bofa-lkq-2024');

-- Amendment 1 for Barclays / World Omni
INSERT INTO RAW_DOCUMENT_METADATA (DOCUMENT_ID, FILE_NAME, DOCUMENT_TYPE, PROCESSING_STATUS, METADATA)
SELECT 'syn-amend1-barclays-worldomni-2024', 'Amendment_1_Barclays_WorldOmni_2024.txt', 'AMENDMENT', 'PENDING',
       PARSE_JSON('{"synthetic": true, "parent_agreement": "6f01ffed-576d-4a48-8d4d-8f8c68c5bc5a", "amendment_number": 1}')
WHERE NOT EXISTS (SELECT 1 FROM RAW_DOCUMENT_METADATA WHERE DOCUMENT_ID = 'syn-amend1-barclays-worldomni-2024');

-- Amendment 2 for Barclays / World Omni (supersedes Amendment 1)
INSERT INTO RAW_DOCUMENT_METADATA (DOCUMENT_ID, FILE_NAME, DOCUMENT_TYPE, PROCESSING_STATUS, METADATA)
SELECT 'syn-amend2-barclays-worldomni-2024', 'Amendment_2_Barclays_WorldOmni_2024.txt', 'AMENDMENT', 'PENDING',
       PARSE_JSON('{"synthetic": true, "parent_agreement": "6f01ffed-576d-4a48-8d4d-8f8c68c5bc5a", "amendment_number": 2, "supersedes": "syn-amend1-barclays-worldomni-2024"}')
WHERE NOT EXISTS (SELECT 1 FROM RAW_DOCUMENT_METADATA WHERE DOCUMENT_ID = 'syn-amend2-barclays-worldomni-2024');

-- =============================================================================
-- STEP 3: Insert Synthetic Document Full Text
-- =============================================================================
-- (In production, these would be parsed from actual uploaded documents)

-- CSA Full Text
INSERT INTO DOCUMENT_FULL_TEXT (DOCUMENT_ID, FULL_TEXT)
SELECT 'syn-csa-bofa-lkq-2024',
$$CREDIT SUPPORT ANNEX to the Schedule to the ISDA MASTER AGREEMENT
dated as of January 15, 2024
between BANK OF AMERICA, N.A. ("Party A") and LKQ CORPORATION ("Party B")

This Annex supplements the ISDA Master Agreement dated September 15, 2023.

Thresholds:
- Party A Threshold: USD 25,000,000 (reduces to zero if rating below BBB-/Baa3)
- Party B Threshold: USD 10,000,000 (reduces to zero if rating below BB+/Ba1)
- Minimum Transfer Amount: USD 500,000 for each party

Eligible Collateral: Cash (100%), US Treasuries (93-98% depending on maturity)
Valuation Agent: Party A
Interest Rate: Federal Funds Overnight Rate
$$
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_FULL_TEXT WHERE DOCUMENT_ID = 'syn-csa-bofa-lkq-2024');

-- Amendment 1 Full Text
INSERT INTO DOCUMENT_FULL_TEXT (DOCUMENT_ID, FULL_TEXT)
SELECT 'syn-amend1-barclays-worldomni-2024',
$$FIRST AMENDMENT to the ISDA MASTER AGREEMENT
dated as of March 1, 2024
between BARCLAYS BANK PLC ("Party A") and WORLD OMNI AUTO RECEIVABLES TRUST 2007-B ("Party B")

Amends the ISDA Master Agreement dated September 26, 2007.

AMENDMENTS:
1. Cross Default Threshold Amount:
   - Party A: USD 10,000,000
   - Party B: USD 5,000,000

2. Automatic Early Termination: Applies to both parties

3. Credit Event Upon Merger: Applies to both parties

4. Set-off Rights: Added
$$
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_FULL_TEXT WHERE DOCUMENT_ID = 'syn-amend1-barclays-worldomni-2024');

-- Amendment 2 Full Text
INSERT INTO DOCUMENT_FULL_TEXT (DOCUMENT_ID, FULL_TEXT)
SELECT 'syn-amend2-barclays-worldomni-2024',
$$SECOND AMENDMENT to the ISDA MASTER AGREEMENT
dated as of September 15, 2024
between BARCLAYS BANK PLC ("Party A") and WORLD OMNI AUTO RECEIVABLES TRUST 2007-B ("Party B")

Amends the ISDA Master Agreement dated September 26, 2007, as amended by First Amendment dated March 1, 2024.

THIS AMENDMENT SUPERSEDES SECTION 1.1 OF THE FIRST AMENDMENT.

AMENDMENTS:
1. Cross Default Threshold Amount (SUPERSEDES Amendment 1):
   - Party A: USD 25,000,000 (or 3% of shareholders equity if higher)
   - Party B: USD 15,000,000
   - Specified Entities added for both parties

2. Additional Termination Events:
   - Rating Downgrade Event
   - Net Asset Value Decline (Party B)

3. Credit Support Provider for Party B: World Omni Financial Corp.

AMENDMENT HIERARCHY: This Second Amendment prevails over First Amendment on conflicting terms.
$$
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_FULL_TEXT WHERE DOCUMENT_ID = 'syn-amend2-barclays-worldomni-2024');

-- =============================================================================
-- STEP 4: Insert Extractions
-- =============================================================================

-- CSA Extraction
INSERT INTO EXTRACTED_CSA (DOCUMENT_ID, PARENT_MASTER_AGREEMENT_ID, EFFECTIVE_DATE, 
    PARTY_A_NAME, PARTY_B_NAME, PARTY_A_THRESHOLD_AMOUNT, PARTY_A_THRESHOLD_CURRENCY,
    PARTY_A_THRESHOLD_CONDITIONS, PARTY_B_THRESHOLD_AMOUNT, PARTY_B_THRESHOLD_CURRENCY,
    PARTY_B_THRESHOLD_CONDITIONS, MINIMUM_TRANSFER_AMOUNT, VALUATION_AGENT, INTEREST_RATE)
SELECT 'syn-csa-bofa-lkq-2024', '6ff38f12-8ea5-4e81-8e29-6b0b018de1af', '2024-01-15',
    'BANK OF AMERICA, N.A.', 'LKQ CORPORATION', 25000000, 'USD',
    'Reduces to zero if rating below BBB-/Baa3 or Event of Default',
    10000000, 'USD', 'Reduces to zero if rating below BB+/Ba1 or Event of Default',
    500000, 'Party A', 'Federal Funds Overnight Rate'
WHERE NOT EXISTS (SELECT 1 FROM EXTRACTED_CSA WHERE DOCUMENT_ID = 'syn-csa-bofa-lkq-2024');

-- Amendment 1 Extraction
INSERT INTO EXTRACTED_AMENDMENTS (DOCUMENT_ID, PARENT_MASTER_AGREEMENT_ID, AMENDMENT_NUMBER,
    EFFECTIVE_DATE, PARTY_A_NAME, PARTY_B_NAME, SUPERSEDES_DOCUMENT_ID,
    NEW_CROSS_DEFAULT_THRESHOLD_PARTY_A, NEW_CROSS_DEFAULT_THRESHOLD_PARTY_B,
    NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_A, NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_B,
    AMENDED_SECTIONS)
SELECT 'syn-amend1-barclays-worldomni-2024', '6f01ffed-576d-4a48-8d4d-8f8c68c5bc5a', 1,
    '2024-03-01', 'BARCLAYS BANK PLC', 'WORLD OMNI AUTO RECEIVABLES TRUST 2007-B', NULL,
    10000000, 5000000, TRUE, TRUE,
    ARRAY_CONSTRUCT('Cross Default Threshold', 'Automatic Early Termination', 'Credit Event Upon Merger')
WHERE NOT EXISTS (SELECT 1 FROM EXTRACTED_AMENDMENTS WHERE DOCUMENT_ID = 'syn-amend1-barclays-worldomni-2024');

-- Amendment 2 Extraction (supersedes Amendment 1 on Cross Default)
INSERT INTO EXTRACTED_AMENDMENTS (DOCUMENT_ID, PARENT_MASTER_AGREEMENT_ID, AMENDMENT_NUMBER,
    EFFECTIVE_DATE, PARTY_A_NAME, PARTY_B_NAME, SUPERSEDES_DOCUMENT_ID,
    NEW_CROSS_DEFAULT_THRESHOLD_PARTY_A, NEW_CROSS_DEFAULT_THRESHOLD_PARTY_B,
    NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_A, NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_B,
    AMENDED_SECTIONS, ADDITIONAL_TERMINATION_EVENTS)
SELECT 'syn-amend2-barclays-worldomni-2024', '6f01ffed-576d-4a48-8d4d-8f8c68c5bc5a', 2,
    '2024-09-15', 'BARCLAYS BANK PLC', 'WORLD OMNI AUTO RECEIVABLES TRUST 2007-B', 
    'syn-amend1-barclays-worldomni-2024',  -- SUPERSEDES Amendment 1
    25000000, 15000000, TRUE, TRUE,
    ARRAY_CONSTRUCT('Cross Default Threshold - SUPERSEDES Amendment 1', 'Additional Termination Events'),
    ARRAY_CONSTRUCT('Rating Downgrade Event', 'Net Asset Value Decline')
WHERE NOT EXISTS (SELECT 1 FROM EXTRACTED_AMENDMENTS WHERE DOCUMENT_ID = 'syn-amend2-barclays-worldomni-2024');

-- Update processing status
UPDATE RAW_DOCUMENT_METADATA SET PROCESSING_STATUS = 'EXTRACTED' WHERE DOCUMENT_ID LIKE 'syn-%';

-- =============================================================================
-- STEP 5: Add to Knowledge Graph
-- =============================================================================

-- Add nodes for synthetic documents
INSERT INTO DOCUMENT_NODES (NODE_ID, NODE_TYPE, NODE_LABEL, DOCUMENT_ID, PROPERTIES)
SELECT 'DOC_syn-csa-bofa-lkq-2024', 'DOCUMENT', 'CSA - Bank of America / LKQ', 'syn-csa-bofa-lkq-2024',
       PARSE_JSON('{"type": "CSA", "effective_date": "2024-01-15"}')
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_NODES WHERE NODE_ID = 'DOC_syn-csa-bofa-lkq-2024');

INSERT INTO DOCUMENT_NODES (NODE_ID, NODE_TYPE, NODE_LABEL, DOCUMENT_ID, PROPERTIES)
SELECT 'DOC_syn-amend1-barclays-worldomni-2024', 'DOCUMENT', 'Amendment 1 - Barclays/WorldOmni', 'syn-amend1-barclays-worldomni-2024',
       PARSE_JSON('{"type": "AMENDMENT", "number": 1}')
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_NODES WHERE NODE_ID = 'DOC_syn-amend1-barclays-worldomni-2024');

INSERT INTO DOCUMENT_NODES (NODE_ID, NODE_TYPE, NODE_LABEL, DOCUMENT_ID, PROPERTIES)
SELECT 'DOC_syn-amend2-barclays-worldomni-2024', 'DOCUMENT', 'Amendment 2 - Barclays/WorldOmni', 'syn-amend2-barclays-worldomni-2024',
       PARSE_JSON('{"type": "AMENDMENT", "number": 2}')
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_NODES WHERE NODE_ID = 'DOC_syn-amend2-barclays-worldomni-2024');

-- Add edges for relationships
INSERT INTO DOCUMENT_EDGES (EDGE_ID, SOURCE_NODE_ID, TARGET_NODE_ID, EDGE_TYPE, EFFECTIVE_DATE, PROPERTIES)
SELECT 'EDGE_CSA_SUPPLEMENTS', 'DOC_syn-csa-bofa-lkq-2024', 'DOC_6ff38f12-8ea5-4e81-8e29-6b0b018de1af', 
       'SUPPLEMENTS', '2024-01-15'::DATE, PARSE_JSON('{"relationship": "CSA supplements Master Agreement"}')
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_EDGES WHERE EDGE_ID = 'EDGE_CSA_SUPPLEMENTS');

INSERT INTO DOCUMENT_EDGES (EDGE_ID, SOURCE_NODE_ID, TARGET_NODE_ID, EDGE_TYPE, EFFECTIVE_DATE, PROPERTIES)
SELECT 'EDGE_AMEND1_AMENDS', 'DOC_syn-amend1-barclays-worldomni-2024', 'DOC_6f01ffed-576d-4a48-8d4d-8f8c68c5bc5a', 
       'AMENDS', '2024-03-01'::DATE, PARSE_JSON('{"changes": "Cross Default, AET"}')
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_EDGES WHERE EDGE_ID = 'EDGE_AMEND1_AMENDS');

INSERT INTO DOCUMENT_EDGES (EDGE_ID, SOURCE_NODE_ID, TARGET_NODE_ID, EDGE_TYPE, EFFECTIVE_DATE, PROPERTIES)
SELECT 'EDGE_AMEND2_AMENDS', 'DOC_syn-amend2-barclays-worldomni-2024', 'DOC_6f01ffed-576d-4a48-8d4d-8f8c68c5bc5a', 
       'AMENDS', '2024-09-15'::DATE, PARSE_JSON('{"changes": "Cross Default INCREASED"}')
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_EDGES WHERE EDGE_ID = 'EDGE_AMEND2_AMENDS');

INSERT INTO DOCUMENT_EDGES (EDGE_ID, SOURCE_NODE_ID, TARGET_NODE_ID, EDGE_TYPE, EFFECTIVE_DATE, PROPERTIES)
SELECT 'EDGE_AMEND2_SUPERSEDES_AMEND1', 'DOC_syn-amend2-barclays-worldomni-2024', 'DOC_syn-amend1-barclays-worldomni-2024', 
       'SUPERSEDES', '2024-09-15'::DATE, PARSE_JSON('{"superseded_section": "Cross Default Threshold"}')
WHERE NOT EXISTS (SELECT 1 FROM DOCUMENT_EDGES WHERE EDGE_ID = 'EDGE_AMEND2_SUPERSEDES_AMEND1');

-- =============================================================================
-- STEP 6: Create Resolution View
-- =============================================================================

CREATE OR REPLACE VIEW V_CURRENT_AGREEMENT_TERMS AS
WITH amendment_hierarchy AS (
    SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.PARENT_MASTER_AGREEMENT_ID ORDER BY a.EFFECTIVE_DATE DESC) as AMENDMENT_RANK
    FROM EXTRACTED_AMENDMENTS a
),
latest_amendments AS (
    SELECT * FROM amendment_hierarchy WHERE AMENDMENT_RANK = 1
)
SELECT 
    m.DOCUMENT_ID as MASTER_AGREEMENT_ID,
    m.FILE_NAME as MASTER_AGREEMENT_FILE,
    ex.AGREEMENT_VERSION,
    ex.EFFECTIVE_DATE as ORIGINAL_EFFECTIVE_DATE,
    ex.PARTY_A_NAME,
    ex.PARTY_B_NAME,
    ex.GOVERNING_LAW,
    
    -- RESOLUTION: Use latest amendment value if exists, otherwise original
    COALESCE(la.NEW_CROSS_DEFAULT_THRESHOLD_PARTY_A, ex.CROSS_DEFAULT_THRESHOLD_AMOUNT) as CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A,
    COALESCE(la.NEW_CROSS_DEFAULT_THRESHOLD_PARTY_B, ex.CROSS_DEFAULT_THRESHOLD_AMOUNT) as CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_B,
    
    CASE 
        WHEN la.NEW_CROSS_DEFAULT_THRESHOLD_PARTY_A IS NOT NULL 
        THEN CONCAT('Amendment ', la.AMENDMENT_NUMBER, ' (', la.EFFECTIVE_DATE, ')')
        ELSE 'Original Agreement'
    END as CROSS_DEFAULT_SOURCE,
    
    COALESCE(la.NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_A, ex.AUTOMATIC_EARLY_TERMINATION_PARTY_A) as CURRENT_AET_PARTY_A,
    COALESCE(la.NEW_AUTOMATIC_EARLY_TERMINATION_PARTY_B, ex.AUTOMATIC_EARLY_TERMINATION_PARTY_B) as CURRENT_AET_PARTY_B,
    
    ex.PAYMENT_METHOD,
    ex.CLOSE_OUT_CALCULATION,
    
    la.AMENDMENT_NUMBER as LATEST_AMENDMENT_NUMBER,
    la.EFFECTIVE_DATE as LATEST_AMENDMENT_DATE,
    la.SUPERSEDES_DOCUMENT_ID,
    la.ADDITIONAL_TERMINATION_EVENTS,
    
    csa.DOCUMENT_ID as CSA_DOCUMENT_ID,
    csa.PARTY_A_THRESHOLD_AMOUNT as CSA_THRESHOLD_PARTY_A,
    csa.PARTY_B_THRESHOLD_AMOUNT as CSA_THRESHOLD_PARTY_B
    
FROM RAW_DOCUMENT_METADATA m
JOIN EXTRACTED_ISDA_MASTER ex ON m.DOCUMENT_ID = ex.DOCUMENT_ID
LEFT JOIN latest_amendments la ON m.DOCUMENT_ID = la.PARENT_MASTER_AGREEMENT_ID
LEFT JOIN EXTRACTED_CSA csa ON m.DOCUMENT_ID = csa.PARENT_MASTER_AGREEMENT_ID
WHERE m.DOCUMENT_TYPE IN ('ISDA_MASTER_AGREEMENT', 'ISDA_2002_MASTER_AGREEMENT');

-- =============================================================================
-- STEP 7: Verify Resolution Logic
-- =============================================================================

-- Show how values resolve for Barclays/World Omni
SELECT 
    MASTER_AGREEMENT_FILE,
    PARTY_A_NAME,
    PARTY_B_NAME,
    CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_A,
    CURRENT_CROSS_DEFAULT_THRESHOLD_PARTY_B,
    CROSS_DEFAULT_SOURCE,
    LATEST_AMENDMENT_NUMBER,
    SUPERSEDES_DOCUMENT_ID
FROM V_CURRENT_AGREEMENT_TERMS
WHERE PARTY_A_NAME LIKE '%BARCLAYS%';

SELECT 'Synthetic documents and resolution logic complete!' as STATUS;
