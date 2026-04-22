/*
================================================================================
ISDA Document Intelligence POC - Setup Infrastructure
================================================================================
This script creates all required database objects for the ISDA Document 
Intelligence POC using Snowflake Cortex AI.

Prerequisites:
- Snowflake account with Cortex AI enabled
- Role with CREATE DATABASE privileges
- SNOWFLAKE.CORTEX functions available

Run this script first before uploading documents.
================================================================================
*/

-- =============================================================================
-- STEP 1: Create Database and Schema
-- =============================================================================
CREATE DATABASE IF NOT EXISTS ISDA_DOCUMENT_POC;

CREATE SCHEMA IF NOT EXISTS ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

USE SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

-- =============================================================================
-- STEP 2: Create Stage for Document Storage
-- =============================================================================
-- Stage with DIRECTORY enabled for document metadata queries
CREATE OR REPLACE STAGE ISDA_DOCUMENTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for ISDA Master Agreements, CSAs, and related documents';

-- =============================================================================
-- STEP 3: Create Core Tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- RAW_DOCUMENT_METADATA: Tracks all uploaded documents
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW_DOCUMENT_METADATA (
    DOCUMENT_ID VARCHAR(50) PRIMARY KEY,
    FILE_NAME VARCHAR(500) NOT NULL,
    FILE_PATH VARCHAR(1000),
    DOCUMENT_TYPE VARCHAR(100),  -- ISDA_MASTER_AGREEMENT, ISDA_2002_MASTER_AGREEMENT, CSA, AMENDMENT, MSA
    UPLOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_SIZE_BYTES NUMBER,
    PROCESSING_STATUS VARCHAR(50) DEFAULT 'PENDING',  -- PENDING, PARSED, EXTRACTED, ERROR
    ERROR_MESSAGE VARCHAR(5000),
    METADATA VARIANT
);

-- -----------------------------------------------------------------------------
-- DOCUMENT_FULL_TEXT: Stores parsed document content from AI_PARSE_DOCUMENT
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DOCUMENT_FULL_TEXT (
    DOCUMENT_ID VARCHAR(50) PRIMARY KEY,
    FULL_TEXT VARCHAR(16777216),  -- Full extracted text
    PAGE_COUNT NUMBER,
    PARSED_CONTENT VARIANT,       -- Raw JSON from AI_PARSE_DOCUMENT
    PARSE_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (DOCUMENT_ID) REFERENCES RAW_DOCUMENT_METADATA(DOCUMENT_ID)
);

-- -----------------------------------------------------------------------------
-- EXTRACTED_ISDA_MASTER: Structured extraction from ISDA Master Agreements
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE EXTRACTED_ISDA_MASTER (
    DOCUMENT_ID VARCHAR(50) PRIMARY KEY,
    
    -- Core Agreement Info
    AGREEMENT_VERSION VARCHAR(10),           -- 1992 or 2002
    EFFECTIVE_DATE DATE,
    PARTY_A_NAME VARCHAR(500),
    PARTY_A_TYPE VARCHAR(100),               -- bank, corporation, trust, etc.
    PARTY_B_NAME VARCHAR(500),
    PARTY_B_TYPE VARCHAR(100),
    GOVERNING_LAW VARCHAR(100),
    
    -- Events of Default (Section 5(a)) - Array of objects
    EVENTS_OF_DEFAULT ARRAY,
    
    -- Termination Events (Section 5(b)) - Array of objects
    TERMINATION_EVENTS ARRAY,
    
    -- Cross Default Provisions (Section 5(a)(vi))
    CROSS_DEFAULT_APPLICABLE BOOLEAN,
    CROSS_DEFAULT_THRESHOLD_AMOUNT NUMBER(20,2),
    CROSS_DEFAULT_THRESHOLD_CURRENCY VARCHAR(3),
    SPECIFIED_ENTITIES_PARTY_A ARRAY,
    SPECIFIED_ENTITIES_PARTY_B ARRAY,
    
    -- Netting Provisions
    NETTING_APPLICABLE BOOLEAN,
    CLOSE_OUT_NETTING BOOLEAN,
    
    -- Early Termination Provisions
    AUTOMATIC_EARLY_TERMINATION_PARTY_A BOOLEAN,
    AUTOMATIC_EARLY_TERMINATION_PARTY_B BOOLEAN,
    PAYMENT_METHOD VARCHAR(50),              -- First Method or Second Method
    CLOSE_OUT_CALCULATION VARCHAR(100),      -- Market Quotation/Loss (1992) or Close-out Amount (2002)
    SET_OFF_RIGHTS BOOLEAN,
    EARLY_TERMINATION_DETAILS VARIANT,
    
    -- Metadata
    EXTRACTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXTRACTION_CONFIDENCE FLOAT,
    RAW_EXTRACTION_JSON VARIANT,
    
    FOREIGN KEY (DOCUMENT_ID) REFERENCES RAW_DOCUMENT_METADATA(DOCUMENT_ID)
);

-- =============================================================================
-- STEP 4: Knowledge Graph Tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DOCUMENT_NODES: Nodes in the document knowledge graph
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DOCUMENT_NODES (
    NODE_ID VARCHAR(50) PRIMARY KEY,
    NODE_TYPE VARCHAR(50) NOT NULL,          -- DOCUMENT, PARTY, CLAUSE, PROVISION, AMOUNT
    NODE_LABEL VARCHAR(500),
    DOCUMENT_ID VARCHAR(50),                 -- Source document (if applicable)
    PROPERTIES VARIANT,                      -- Flexible properties
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -----------------------------------------------------------------------------
-- DOCUMENT_EDGES: Relationships between nodes
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE DOCUMENT_EDGES (
    EDGE_ID VARCHAR(50) PRIMARY KEY,
    SOURCE_NODE_ID VARCHAR(50) NOT NULL,
    TARGET_NODE_ID VARCHAR(50) NOT NULL,
    EDGE_TYPE VARCHAR(50) NOT NULL,          -- PARTY_TO, AMENDS, SUPERSEDES, CONTAINS, REFERENCES
    PROPERTIES VARIANT,
    EFFECTIVE_DATE DATE,
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (SOURCE_NODE_ID) REFERENCES DOCUMENT_NODES(NODE_ID),
    FOREIGN KEY (TARGET_NODE_ID) REFERENCES DOCUMENT_NODES(NODE_ID)
);

-- -----------------------------------------------------------------------------
-- CLAUSE_VERSIONS: Tracks clause changes across amendments
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE CLAUSE_VERSIONS (
    VERSION_ID VARCHAR(50) PRIMARY KEY,
    CLAUSE_TYPE VARCHAR(100) NOT NULL,       -- CROSS_DEFAULT, THRESHOLD, NETTING, etc.
    DOCUMENT_ID VARCHAR(50) NOT NULL,
    EFFECTIVE_DATE DATE,
    CLAUSE_TEXT VARCHAR(16777216),
    EXTRACTED_VALUES VARIANT,
    SUPERSEDES_VERSION_ID VARCHAR(50),       -- Previous version this supersedes
    IS_CURRENT BOOLEAN DEFAULT TRUE,
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (DOCUMENT_ID) REFERENCES RAW_DOCUMENT_METADATA(DOCUMENT_ID)
);

-- -----------------------------------------------------------------------------
-- COUNTERPARTY_RELATIONSHIPS: Tracks relationships between counterparties
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE COUNTERPARTY_RELATIONSHIPS (
    RELATIONSHIP_ID VARCHAR(50) PRIMARY KEY,
    PARTY_A_NAME VARCHAR(500),
    PARTY_B_NAME VARCHAR(500),
    RELATIONSHIP_TYPE VARCHAR(50),           -- ISDA_MASTER, CSA, AMENDMENT
    BASE_DOCUMENT_ID VARCHAR(50),            -- The master agreement
    CURRENT_EFFECTIVE_DATE DATE,
    AMENDMENT_COUNT NUMBER DEFAULT 0,
    LATEST_AMENDMENT_ID VARCHAR(50),
    RELATIONSHIP_STATUS VARCHAR(50),         -- ACTIVE, TERMINATED, NOVATED
    PROPERTIES VARIANT,
    CREATED_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (BASE_DOCUMENT_ID) REFERENCES RAW_DOCUMENT_METADATA(DOCUMENT_ID)
);

-- =============================================================================
-- STEP 5: Verify Setup
-- =============================================================================
-- Show all created objects
SHOW TABLES IN SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;
SHOW STAGES IN SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

SELECT 'Infrastructure setup complete!' as STATUS;
