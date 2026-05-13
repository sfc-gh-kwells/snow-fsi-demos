/*
==============================================================
  SETUP: Equity Research - AI_COMPLETE Document Intelligence
==============================================================

Prerequisites:
  1. You need a role with SNOWFLAKE.CORTEX_USER database role granted
  2. A warehouse (we use AICOLLEGE below — change to yours)

Instructions:
  1. Run this script in a Snowflake worksheet
  2. Upload your equity research PDFs to the @REPORTS stage
     (drag & drop in Snowsight, use PUT from SnowSQL, or use Snowflake CLI)
  3. Open the notebook and run all cells
==============================================================
*/

-- 1. Create database and schema
CREATE DATABASE IF NOT EXISTS FSI_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS FSI_DEMO_DB.EQUITY_RESEARCH;

USE DATABASE FSI_DEMO_DB;
USE SCHEMA EQUITY_RESEARCH;

-- 2. Create stage with server-side encryption (REQUIRED for AI_COMPLETE document processing)
CREATE STAGE IF NOT EXISTS REPORTS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Equity research analyst reports (PDFs) - server-side encryption for AI_COMPLETE';

-- 3. Upload files
-- Option A: Use Snowsight UI
--   Navigate to Data > Databases > FSI_DEMO_DB > EQUITY_RESEARCH > Stages > REPORTS
--   Click "+ Files" and upload all PDFs
--
-- Option B: Use SnowSQL / PUT command (run from your local terminal)
--   PUT file:///path/to/your/pdfs/*.pdf @FSI_DEMO_DB.EQUITY_RESEARCH.REPORTS AUTO_COMPRESS=FALSE;
--
-- Option C: Use Snowflake CLI
--   snow stage copy "report.pdf" @FSI_DEMO_DB.EQUITY_RESEARCH.REPORTS --overwrite

-- 4. Refresh the directory metadata after upload
ALTER STAGE REPORTS REFRESH;

-- 5. Verify files are accessible
SELECT RELATIVE_PATH, ROUND(SIZE / 1024, 1) AS size_kb
FROM DIRECTORY(@REPORTS)
ORDER BY RELATIVE_PATH;

-- 6. Quick test: confirm AI_COMPLETE can read the documents
-- (Replace the filename below with any PDF you uploaded)
SELECT AI_COMPLETE(
    MODEL => 'claude-opus-4-6',
    PROMPT => PROMPT(
        'What is the main topic of this document? Answer in one sentence. {0}',
        TO_FILE('@REPORTS', 'your_report.pdf')  -- <-- replace with your filename
    )
) AS test_result;

-- If the above returns a result, you're ready to run the notebook!
