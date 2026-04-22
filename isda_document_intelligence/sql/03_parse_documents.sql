/*
================================================================================
ISDA Document Intelligence POC - Document Parsing Procedures
================================================================================
This script creates stored procedures for parsing ISDA documents using 
SNOWFLAKE.CORTEX.PARSE_DOCUMENT.

Procedures:
- PARSE_SINGLE_DOCUMENT(document_id) - Parse one document
- PARSE_ALL_PENDING_DOCUMENTS() - Batch parse all pending documents

Prerequisites:
- Run 01_setup_infrastructure.sql
- Run 02_load_documents.sql
================================================================================
*/

USE DATABASE ISDA_DOCUMENT_POC;
USE SCHEMA DOCUMENT_INTELLIGENCE;

-- =============================================================================
-- PROCEDURE: Parse a Single Document
-- =============================================================================
-- Called by Streamlit app when uploading new documents

CREATE OR REPLACE PROCEDURE PARSE_SINGLE_DOCUMENT(DOCUMENT_ID_PARAM VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    file_path VARCHAR;
    file_name VARCHAR;
    parsed_result VARIANT;
BEGIN
    -- Get file path
    SELECT FILE_PATH, FILE_NAME INTO :file_path, :file_name
    FROM RAW_DOCUMENT_METADATA
    WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
    
    IF (file_path IS NULL) THEN
        RETURN 'Document not found: ' || :DOCUMENT_ID_PARAM;
    END IF;
    
    -- Check if already parsed
    IF (EXISTS (SELECT 1 FROM DOCUMENT_FULL_TEXT WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM)) THEN
        RETURN 'Document already parsed: ' || :file_name;
    END IF;
    
    -- Parse document using PARSE_DOCUMENT
    SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
        :file_path,
        {'mode': 'LAYOUT'}
    ) INTO :parsed_result;
    
    -- Insert into DOCUMENT_FULL_TEXT
    INSERT INTO DOCUMENT_FULL_TEXT (DOCUMENT_ID, FULL_TEXT, PAGE_COUNT, PARSED_CONTENT)
    SELECT 
        :DOCUMENT_ID_PARAM,
        :parsed_result:content::STRING,
        :parsed_result:metadata:pageCount::INTEGER,
        :parsed_result;
    
    -- Update processing status
    UPDATE RAW_DOCUMENT_METADATA 
    SET PROCESSING_STATUS = 'PARSED'
    WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
    
    RETURN 'Successfully parsed: ' || :file_name || ' (' || :parsed_result:metadata:pageCount::INTEGER || ' pages)';
    
EXCEPTION
    WHEN OTHER THEN
        UPDATE RAW_DOCUMENT_METADATA 
        SET PROCESSING_STATUS = 'PARSE_FAILED',
            ERROR_MESSAGE = SQLERRM
        WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
        RETURN 'Parse failed: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- PROCEDURE: Parse All Pending Documents
-- =============================================================================
-- Batch process all documents with UPLOADED or PENDING status

CREATE OR REPLACE PROCEDURE PARSE_ALL_PENDING_DOCUMENTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    doc_cursor CURSOR FOR 
        SELECT DOCUMENT_ID, FILE_NAME, FILE_PATH
        FROM RAW_DOCUMENT_METADATA
        WHERE PROCESSING_STATUS IN ('UPLOADED', 'PENDING')
          AND DOCUMENT_ID NOT IN (SELECT DOCUMENT_ID FROM DOCUMENT_FULL_TEXT);
    
    doc_id VARCHAR;
    file_name VARCHAR;
    file_path VARCHAR;
    parsed_result VARIANT;
    processed_count INTEGER DEFAULT 0;
    error_count INTEGER DEFAULT 0;
BEGIN
    FOR record IN doc_cursor DO
        doc_id := record.DOCUMENT_ID;
        file_name := record.FILE_NAME;
        file_path := record.FILE_PATH;
        
        BEGIN
            -- Parse document using PARSE_DOCUMENT
            SELECT SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
                :file_path,
                {'mode': 'LAYOUT'}
            ) INTO :parsed_result;
            
            -- Insert into DOCUMENT_FULL_TEXT
            INSERT INTO DOCUMENT_FULL_TEXT (DOCUMENT_ID, FULL_TEXT, PAGE_COUNT, PARSED_CONTENT)
            SELECT 
                :doc_id,
                :parsed_result:content::STRING,
                :parsed_result:metadata:pageCount::INTEGER,
                :parsed_result;
            
            -- Update processing status
            UPDATE RAW_DOCUMENT_METADATA 
            SET PROCESSING_STATUS = 'PARSED'
            WHERE DOCUMENT_ID = :doc_id;
            
            processed_count := processed_count + 1;
            
        EXCEPTION
            WHEN OTHER THEN
                error_count := error_count + 1;
                UPDATE RAW_DOCUMENT_METADATA 
                SET PROCESSING_STATUS = 'PARSE_FAILED',
                    ERROR_MESSAGE = SQLERRM
                WHERE DOCUMENT_ID = :doc_id;
        END;
    END FOR;
    
    RETURN 'Parsing complete. Processed: ' || processed_count || ', Errors: ' || error_count;
END;
$$;

-- =============================================================================
-- Test Procedures
-- =============================================================================
-- Uncomment to test:
-- CALL PARSE_ALL_PENDING_DOCUMENTS();

SELECT 'Document parsing procedures created successfully!' as STATUS;
