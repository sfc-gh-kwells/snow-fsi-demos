-- Refresh: Column-Level Change Detection
-- Detects changes to a specific column: current definition, DDL history, and usage stats.
-- Adapted from lineage/templates/column-change-detection.sql.
-- Replace :database, :schema, :table, :column, :lookback_days.

WITH current_column AS (
    SELECT
        c.table_catalog  AS database_name,
        c.table_schema   AS schema_name,
        c.table_name,
        c.column_name,
        c.ordinal_position,
        c.data_type,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        c.is_nullable,
        c.column_default,
        c.comment
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
    WHERE c.table_catalog = ':database'
      AND c.table_schema  = ':schema'
      AND c.table_name    = ':table'
      AND c.column_name   = ':column'
      AND c.deleted IS NULL
),
table_history AS (
    SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.created,
        t.last_altered,
        t.last_ddl,
        t.table_owner
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES t
    WHERE t.table_catalog = ':database'
      AND t.table_schema  = ':schema'
      AND t.table_name    = ':table'
      AND t.deleted IS NULL
),
recent_ddl_queries AS (
    SELECT
        qh.query_id,
        qh.query_text,
        qh.user_name,
        qh.start_time,
        qh.query_type
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
    WHERE qh.query_type IN ('ALTER_TABLE', 'ALTER', 'CREATE_TABLE_AS_SELECT')
      AND UPPER(qh.query_text) LIKE '%:table%'
      AND (
          UPPER(qh.query_text) LIKE '%:column%'
          OR UPPER(qh.query_text) LIKE '%ALTER%COLUMN%'
      )
      AND qh.start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
    ORDER BY qh.start_time DESC
    LIMIT 10
),
column_stats AS (
    SELECT
        COUNT(DISTINCT ah.query_id) AS read_count,
        COUNT(DISTINCT ah.user_name) AS users,
        MAX(ah.query_start_time)    AS last_read
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => base_objects_accessed)          AS base,
    LATERAL FLATTEN(input => base.value:columns, outer => true) AS col
    WHERE base.value:objectName::STRING = ':database.:schema.:table'
      AND col.value:columnName::STRING  = ':column'
      AND ah.query_start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
)
SELECT
    'COLUMN_DEFINITION'                   AS section,
    cc.column_name,
    cc.data_type,
    cc.is_nullable,
    NULL::STRING                          AS change_detail,
    cc.comment
FROM current_column cc

UNION ALL

SELECT
    'TABLE_HISTORY'                       AS section,
    th.table_name,
    'last_altered: ' || th.last_altered::STRING,
    NULL,
    NULL,
    'owner: ' || th.table_owner
FROM table_history th

UNION ALL

SELECT
    'USAGE_STATS'                         AS section,
    'read_count_' || :lookback_days || 'd',
    cs.read_count::STRING,
    NULL,
    'users: ' || cs.users::STRING,
    'last_read: ' || cs.last_read::STRING
FROM column_stats cs

UNION ALL

SELECT
    'RECENT_DDL'                          AS section,
    ddl.query_type,
    ddl.user_name,
    NULL,
    ddl.start_time::STRING,
    LEFT(ddl.query_text, 300)
FROM recent_ddl_queries ddl;
