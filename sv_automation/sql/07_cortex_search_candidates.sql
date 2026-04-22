-- Cortex Search Service Candidates
-- Identifies VARCHAR/TEXT columns with high cardinality that appear frequently in
-- WHERE-clause style access patterns. Use APPROX_COUNT_DISTINCT for performance.
-- Replace :database, :schema, :table_list, :min_distinct.

WITH varchar_columns AS (
    -- Get all VARCHAR/TEXT columns for the target tables
    SELECT
        c.table_catalog || '.' || c.table_schema || '.' || c.table_name AS table_fqn,
        c.table_name,
        c.table_schema,
        c.table_catalog                                                   AS table_database,
        c.column_name,
        c.data_type,
        c.character_maximum_length
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
    WHERE c.table_catalog = :database
      AND c.table_name   IN (:table_list)
      AND c.data_type    IN ('TEXT', 'VARCHAR', 'CHAR', 'STRING')
      AND c.deleted IS NULL
),
column_access AS (
    -- Find how often each column is accessed (proxy for WHERE clause usage)
    SELECT
        base.value:objectName::STRING           AS table_fqn,
        col.value:columnName::STRING            AS column_name,
        COUNT(DISTINCT ah.query_id)             AS access_count,
        COUNT(DISTINCT ah.user_name)            AS unique_users,
        MAX(ah.query_start_time)                AS last_accessed
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.base_objects_accessed)          AS base,
    LATERAL FLATTEN(input => base.value:columns, outer => true) AS col
    WHERE base.value:objectName::STRING IN (
              SELECT table_fqn FROM varchar_columns
          )
      AND ah.query_start_time >= DATEADD(day, -90, CURRENT_TIMESTAMP())
    GROUP BY 1, 2
)
SELECT
    vc.table_fqn,
    vc.table_database,
    vc.table_schema,
    vc.table_name,
    vc.column_name,
    vc.data_type,
    COALESCE(ca.access_count,  0) AS access_count_90d,
    COALESCE(ca.unique_users,  0) AS unique_users_90d,
    ca.last_accessed
FROM varchar_columns  vc
LEFT JOIN column_access ca
    ON  ca.table_fqn   = vc.table_fqn
    AND ca.column_name = vc.column_name
WHERE COALESCE(ca.access_count, 0) > 0   -- only columns that have been accessed
ORDER BY access_count_90d DESC, vc.table_fqn, vc.column_name;
-- NOTE: After this query, run APPROX_COUNT_DISTINCT per candidate column
-- to confirm distinct value count >= :min_distinct before creating CSS.
