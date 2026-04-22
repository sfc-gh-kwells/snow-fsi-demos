/*
================================================================================
ISDA Document Intelligence POC - Extract ISDA Fields using AI_EXTRACT
================================================================================
This script uses SNOWFLAKE.CORTEX.AI_EXTRACT to extract structured fields from 
parsed ISDA Master Agreements, including:
- Core agreement information
- Events of Default with termination triggers and unwinding mechanisms
- Termination Events with affected parties
- Early termination provisions

Prerequisites:
- Run 01_setup_infrastructure.sql
- Run 02_load_documents.sql  
- Run 03_parse_documents.sql
================================================================================
*/

USE DATABASE ISDA_DOCUMENT_POC;
USE SCHEMA DOCUMENT_INTELLIGENCE;

-- =============================================================================
-- STEP 1: Create Procedure for Extracting ISDA Fields using AI_EXTRACT
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXTRACT_ISDA_FIELDS_FOR_DOCUMENT(DOCUMENT_ID_PARAM VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    extraction_result VARIANT;
    doc_text VARCHAR;
BEGIN
    -- Get document text
    SELECT FULL_TEXT INTO :doc_text
    FROM DOCUMENT_FULL_TEXT 
    WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
    
    IF (doc_text IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('error', 'Document not found or not parsed');
    END IF;
    
    -- Extract core fields using AI_EXTRACT
    SELECT SNOWFLAKE.CORTEX.AI_EXTRACT(
        :doc_text,
        [
            'agreement_version: Is this a 1992 or 2002 ISDA Master Agreement? Return only "1992" or "2002"',
            'effective_date: What is the effective date? Return in YYYY-MM-DD format',
            'party_a_name: What is the full legal name of Party A?',
            'party_a_type: What type of entity is Party A (bank, corporation, trust, fund)?',
            'party_b_name: What is the full legal name of Party B?',
            'party_b_type: What type of entity is Party B (bank, corporation, trust, fund)?',
            'governing_law: What law governs this agreement?',
            'cross_default_applicable: Is cross-default applicable? Return true or false',
            'cross_default_threshold_amount: What is the cross-default threshold amount? Return number only or null',
            'cross_default_threshold_currency: What currency is the threshold in (USD, EUR, GBP)?',
            'automatic_early_termination_party_a: Is Automatic Early Termination enabled for Party A? Return true or false',
            'automatic_early_termination_party_b: Is Automatic Early Termination enabled for Party B? Return true or false',
            'close_out_calculation: What is the close-out calculation method? Return "Market Quotation", "Loss", or "Close-out Amount"',
            'payment_method: Is it First Method or Second Method for payments?',
            'set_off_rights: Are set-off rights applicable? Return true or false',
            'netting_applicable: Is netting applicable? Return true or false',
            'credit_support_documents: List any referenced CSA or credit support documents'
        ]
    ) INTO :extraction_result;
    
    RETURN extraction_result;
END;
$$;

-- =============================================================================
-- STEP 2: Create Procedure to Extract Events of Default
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXTRACT_EVENTS_OF_DEFAULT(DOCUMENT_ID_PARAM VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    events_result VARIANT;
    doc_text VARCHAR;
BEGIN
    SELECT FULL_TEXT INTO :doc_text
    FROM DOCUMENT_FULL_TEXT 
    WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
    
    IF (doc_text IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('error', 'Document not found');
    END IF;
    
    SELECT SNOWFLAKE.CORTEX.AI_EXTRACT(
        :doc_text,
        [
            'events_of_default: List all Events of Default from Section 5(a). For each event include: event_type, description, grace_period_days, applicable_to (Party A/Party B/both), triggers_early_termination (automatic/optional), unwinding_mechanism. Return as JSON array.'
        ]
    ) INTO :events_result;
    
    RETURN events_result;
END;
$$;

-- =============================================================================
-- STEP 3: Create Procedure to Extract Termination Events
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXTRACT_TERMINATION_EVENTS(DOCUMENT_ID_PARAM VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    events_result VARIANT;
    doc_text VARCHAR;
BEGIN
    SELECT FULL_TEXT INTO :doc_text
    FROM DOCUMENT_FULL_TEXT 
    WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
    
    IF (doc_text IS NULL) THEN
        RETURN OBJECT_CONSTRUCT('error', 'Document not found');
    END IF;
    
    SELECT SNOWFLAKE.CORTEX.AI_EXTRACT(
        :doc_text,
        [
            'termination_events: List all Termination Events from Section 5(b). For each event include: event_type, description, affected_party, waiting_period_days, transfer_option (true/false), triggers_early_termination (automatic/optional), unwinding_mechanism. Return as JSON array.'
        ]
    ) INTO :events_result;
    
    RETURN events_result;
END;
$$;

-- =============================================================================
-- STEP 4: Create Main Extraction Procedure
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXTRACT_ALL_PENDING_DOCUMENTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    doc_cursor CURSOR FOR 
        SELECT m.DOCUMENT_ID, m.FILE_NAME
        FROM RAW_DOCUMENT_METADATA m
        JOIN DOCUMENT_FULL_TEXT ft ON m.DOCUMENT_ID = ft.DOCUMENT_ID
        WHERE m.PROCESSING_STATUS = 'PARSED'
          AND m.DOCUMENT_ID NOT IN (SELECT DOCUMENT_ID FROM EXTRACTED_ISDA_MASTER);
    
    doc_id VARCHAR;
    file_name VARCHAR;
    extracted VARIANT;
    events_of_default VARIANT;
    termination_events VARIANT;
    processed_count INTEGER DEFAULT 0;
    error_count INTEGER DEFAULT 0;
BEGIN
    FOR record IN doc_cursor DO
        doc_id := record.DOCUMENT_ID;
        file_name := record.FILE_NAME;
        
        BEGIN
            -- Extract core fields
            CALL EXTRACT_ISDA_FIELDS_FOR_DOCUMENT(:doc_id) INTO :extracted;
            
            -- Extract events of default
            CALL EXTRACT_EVENTS_OF_DEFAULT(:doc_id) INTO :events_of_default;
            
            -- Extract termination events
            CALL EXTRACT_TERMINATION_EVENTS(:doc_id) INTO :termination_events;
            
            -- Insert into EXTRACTED_ISDA_MASTER
            INSERT INTO EXTRACTED_ISDA_MASTER (
                DOCUMENT_ID,
                AGREEMENT_VERSION,
                EFFECTIVE_DATE,
                PARTY_A_NAME,
                PARTY_A_TYPE,
                PARTY_B_NAME,
                PARTY_B_TYPE,
                GOVERNING_LAW,
                CROSS_DEFAULT_APPLICABLE,
                CROSS_DEFAULT_THRESHOLD_AMOUNT,
                CROSS_DEFAULT_THRESHOLD_CURRENCY,
                NETTING_APPLICABLE,
                AUTOMATIC_EARLY_TERMINATION_PARTY_A,
                AUTOMATIC_EARLY_TERMINATION_PARTY_B,
                PAYMENT_METHOD,
                CLOSE_OUT_CALCULATION,
                SET_OFF_RIGHTS,
                EVENTS_OF_DEFAULT,
                TERMINATION_EVENTS,
                RAW_EXTRACTION_JSON,
                EXTRACTION_TIMESTAMP
            )
            SELECT
                :doc_id,
                :extracted:response:agreement_version::VARCHAR,
                TRY_TO_DATE(:extracted:response:effective_date::VARCHAR),
                :extracted:response:party_a_name::VARCHAR,
                :extracted:response:party_a_type::VARCHAR,
                :extracted:response:party_b_name::VARCHAR,
                :extracted:response:party_b_type::VARCHAR,
                :extracted:response:governing_law::VARCHAR,
                TRY_TO_BOOLEAN(:extracted:response:cross_default_applicable::VARCHAR),
                TRY_TO_NUMBER(REGEXP_REPLACE(:extracted:response:cross_default_threshold_amount::VARCHAR, '[^0-9.]', '')),
                :extracted:response:cross_default_threshold_currency::VARCHAR,
                TRY_TO_BOOLEAN(:extracted:response:netting_applicable::VARCHAR),
                TRY_TO_BOOLEAN(:extracted:response:automatic_early_termination_party_a::VARCHAR),
                TRY_TO_BOOLEAN(:extracted:response:automatic_early_termination_party_b::VARCHAR),
                :extracted:response:payment_method::VARCHAR,
                :extracted:response:close_out_calculation::VARCHAR,
                TRY_TO_BOOLEAN(:extracted:response:set_off_rights::VARCHAR),
                TRY_PARSE_JSON(:events_of_default:response:events_of_default::VARCHAR),
                TRY_PARSE_JSON(:termination_events:response:termination_events::VARCHAR),
                OBJECT_CONSTRUCT(
                    'core_extraction', :extracted,
                    'events_of_default', :events_of_default,
                    'termination_events', :termination_events
                ),
                CURRENT_TIMESTAMP();
            
            -- Update processing status
            UPDATE RAW_DOCUMENT_METADATA 
            SET PROCESSING_STATUS = 'EXTRACTED'
            WHERE DOCUMENT_ID = :doc_id;
            
            processed_count := processed_count + 1;
            
        EXCEPTION
            WHEN OTHER THEN
                error_count := error_count + 1;
                -- Log error but continue processing
                UPDATE RAW_DOCUMENT_METADATA 
                SET PROCESSING_STATUS = 'EXTRACTION_FAILED',
                    ERROR_MESSAGE = SQLERRM
                WHERE DOCUMENT_ID = :doc_id;
        END;
    END FOR;
    
    RETURN 'Extraction complete. Processed: ' || processed_count || ', Errors: ' || error_count;
END;
$$;

-- =============================================================================
-- STEP 5: Create Single Document Extraction Procedure (for Streamlit)
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXTRACT_SINGLE_DOCUMENT(DOCUMENT_ID_PARAM VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    extracted VARIANT;
    events_of_default VARIANT;
    termination_events VARIANT;
    file_name VARCHAR;
BEGIN
    -- Get file name
    SELECT FILE_NAME INTO :file_name
    FROM RAW_DOCUMENT_METADATA
    WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
    
    -- Check if already extracted
    IF (EXISTS (SELECT 1 FROM EXTRACTED_ISDA_MASTER WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM)) THEN
        RETURN 'Document already extracted: ' || :DOCUMENT_ID_PARAM;
    END IF;
    
    -- Extract core fields
    CALL EXTRACT_ISDA_FIELDS_FOR_DOCUMENT(:DOCUMENT_ID_PARAM) INTO :extracted;
    
    -- Extract events of default
    CALL EXTRACT_EVENTS_OF_DEFAULT(:DOCUMENT_ID_PARAM) INTO :events_of_default;
    
    -- Extract termination events
    CALL EXTRACT_TERMINATION_EVENTS(:DOCUMENT_ID_PARAM) INTO :termination_events;
    
    -- Insert into EXTRACTED_ISDA_MASTER
    INSERT INTO EXTRACTED_ISDA_MASTER (
        DOCUMENT_ID,
        AGREEMENT_VERSION,
        EFFECTIVE_DATE,
        PARTY_A_NAME,
        PARTY_A_TYPE,
        PARTY_B_NAME,
        PARTY_B_TYPE,
        GOVERNING_LAW,
        CROSS_DEFAULT_APPLICABLE,
        CROSS_DEFAULT_THRESHOLD_AMOUNT,
        CROSS_DEFAULT_THRESHOLD_CURRENCY,
        NETTING_APPLICABLE,
        AUTOMATIC_EARLY_TERMINATION_PARTY_A,
        AUTOMATIC_EARLY_TERMINATION_PARTY_B,
        PAYMENT_METHOD,
        CLOSE_OUT_CALCULATION,
        SET_OFF_RIGHTS,
        EVENTS_OF_DEFAULT,
        TERMINATION_EVENTS,
        RAW_EXTRACTION_JSON,
        EXTRACTION_TIMESTAMP
    )
    SELECT
        :DOCUMENT_ID_PARAM,
        :extracted:response:agreement_version::VARCHAR,
        TRY_TO_DATE(:extracted:response:effective_date::VARCHAR),
        :extracted:response:party_a_name::VARCHAR,
        :extracted:response:party_a_type::VARCHAR,
        :extracted:response:party_b_name::VARCHAR,
        :extracted:response:party_b_type::VARCHAR,
        :extracted:response:governing_law::VARCHAR,
        TRY_TO_BOOLEAN(:extracted:response:cross_default_applicable::VARCHAR),
        TRY_TO_NUMBER(REGEXP_REPLACE(:extracted:response:cross_default_threshold_amount::VARCHAR, '[^0-9.]', '')),
        :extracted:response:cross_default_threshold_currency::VARCHAR,
        TRY_TO_BOOLEAN(:extracted:response:netting_applicable::VARCHAR),
        TRY_TO_BOOLEAN(:extracted:response:automatic_early_termination_party_a::VARCHAR),
        TRY_TO_BOOLEAN(:extracted:response:automatic_early_termination_party_b::VARCHAR),
        :extracted:response:payment_method::VARCHAR,
        :extracted:response:close_out_calculation::VARCHAR,
        TRY_TO_BOOLEAN(:extracted:response:set_off_rights::VARCHAR),
        TRY_PARSE_JSON(:events_of_default:response:events_of_default::VARCHAR),
        TRY_PARSE_JSON(:termination_events:response:termination_events::VARCHAR),
        OBJECT_CONSTRUCT(
            'core_extraction', :extracted,
            'events_of_default', :events_of_default,
            'termination_events', :termination_events
        ),
        CURRENT_TIMESTAMP();
    
    -- Update processing status
    UPDATE RAW_DOCUMENT_METADATA 
    SET PROCESSING_STATUS = 'EXTRACTED'
    WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
    
    RETURN 'Successfully extracted: ' || :file_name;
    
EXCEPTION
    WHEN OTHER THEN
        UPDATE RAW_DOCUMENT_METADATA 
        SET PROCESSING_STATUS = 'EXTRACTION_FAILED',
            ERROR_MESSAGE = SQLERRM
        WHERE DOCUMENT_ID = :DOCUMENT_ID_PARAM;
        RETURN 'Extraction failed: ' || SQLERRM;
END;
$$;

-- =============================================================================
-- STEP 6: Run Extraction on All Pending Documents
-- =============================================================================

-- CALL EXTRACT_ALL_PENDING_DOCUMENTS();

-- =============================================================================
-- STEP 7: View Extraction Results
-- =============================================================================
SELECT 
    m.FILE_NAME,
    ex.AGREEMENT_VERSION,
    ex.EFFECTIVE_DATE,
    ex.PARTY_A_NAME,
    ex.PARTY_B_NAME,
    ex.GOVERNING_LAW,
    ex.CROSS_DEFAULT_APPLICABLE,
    ex.CROSS_DEFAULT_THRESHOLD_AMOUNT,
    ex.CLOSE_OUT_CALCULATION,
    ex.EXTRACTION_TIMESTAMP
FROM EXTRACTED_ISDA_MASTER ex
JOIN RAW_DOCUMENT_METADATA m ON ex.DOCUMENT_ID = m.DOCUMENT_ID
ORDER BY ex.EXTRACTION_TIMESTAMP DESC;

SELECT 'AI_EXTRACT procedures created successfully!' as STATUS;
