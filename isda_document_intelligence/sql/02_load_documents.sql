/*
================================================================================
ISDA Document Intelligence POC - Load Documents
================================================================================
This script loads document metadata after PDFs have been uploaded to the stage.

Prerequisites:
- Run 01_setup_infrastructure.sql first
- Upload PDF documents to the stage using snow CLI:
  
  snow stage copy "ISDA Master Agreement - example 1.pdf" \
      @ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE.ISDA_DOCUMENTS_STAGE \
      --connection YOUR_CONNECTION --overwrite

================================================================================
*/

USE SCHEMA ISDA_DOCUMENT_POC.DOCUMENT_INTELLIGENCE;

-- =============================================================================
-- STEP 1: Refresh Stage Directory
-- =============================================================================
-- This updates the directory metadata to reflect uploaded files
ALTER STAGE ISDA_DOCUMENTS_STAGE REFRESH;

-- =============================================================================
-- STEP 2: View Uploaded Documents
-- =============================================================================
SELECT 
    RELATIVE_PATH as FILE_NAME,
    SIZE as FILE_SIZE_BYTES,
    LAST_MODIFIED,
    FILE_URL
FROM DIRECTORY(@ISDA_DOCUMENTS_STAGE)
WHERE RELATIVE_PATH LIKE '%.pdf'
ORDER BY RELATIVE_PATH;

-- =============================================================================
-- STEP 3: Insert Document Metadata
-- =============================================================================
-- This inserts metadata for all PDFs not already in the table
INSERT INTO RAW_DOCUMENT_METADATA 
(DOCUMENT_ID, FILE_NAME, FILE_PATH, DOCUMENT_TYPE, UPLOAD_TIMESTAMP, FILE_SIZE_BYTES, PROCESSING_STATUS)
SELECT 
    UUID_STRING() as DOCUMENT_ID,
    d.RELATIVE_PATH as FILE_NAME,
    d.FILE_URL as FILE_PATH,
    CASE 
        WHEN d.RELATIVE_PATH ILIKE '%2002%' THEN 'ISDA_2002_MASTER_AGREEMENT'
        WHEN d.RELATIVE_PATH ILIKE '%CSA%' OR d.RELATIVE_PATH ILIKE '%Credit Support%' THEN 'CREDIT_SUPPORT_ANNEX'
        WHEN d.RELATIVE_PATH ILIKE '%Amendment%' THEN 'AMENDMENT'
        ELSE 'ISDA_MASTER_AGREEMENT'
    END as DOCUMENT_TYPE,
    CURRENT_TIMESTAMP() as UPLOAD_TIMESTAMP,
    d.SIZE as FILE_SIZE_BYTES,
    'PENDING' as PROCESSING_STATUS
FROM DIRECTORY(@ISDA_DOCUMENTS_STAGE) d
LEFT JOIN RAW_DOCUMENT_METADATA m
    ON d.RELATIVE_PATH = m.FILE_NAME
WHERE d.RELATIVE_PATH LIKE '%.pdf'
  AND m.FILE_NAME IS NULL;  -- Only insert new documents

-- =============================================================================
-- STEP 4: Verify Loaded Documents
-- =============================================================================
SELECT 
    DOCUMENT_ID,
    FILE_NAME,
    DOCUMENT_TYPE,
    FILE_SIZE_BYTES,
    PROCESSING_STATUS,
    UPLOAD_TIMESTAMP
FROM RAW_DOCUMENT_METADATA
ORDER BY UPLOAD_TIMESTAMP;

SELECT 
    DOCUMENT_TYPE,
    COUNT(*) as DOCUMENT_COUNT
FROM RAW_DOCUMENT_METADATA
GROUP BY DOCUMENT_TYPE;

SELECT 'Document metadata loaded successfully!' as STATUS;
