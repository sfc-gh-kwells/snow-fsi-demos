-- Sample Evaluation Queries from Query History
-- Returns up to :per_table_limit single-table queries and multi-table join queries
-- for a given set of tables, for use as Cortex Analyst eval questions.
-- Replace :table_list, :lookback_days, :per_table_limit, :min_query_length.
--
-- Uses direct_objects_accessed (correct for views) with a pre-filter CTE to
-- avoid full-scan on ACCESS_HISTORY before joining QUERY_HISTORY.

WITH filtered_ah AS (
    -- Pre-filter ACCESS_HISTORY to just the tables we care about and the time window.
    -- This avoids a full scan of the 683M-row table before the QH join.
    SELECT ah.query_id, obj.value:objectName::STRING AS table_fqn
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.direct_objects_accessed) AS obj
    WHERE ah.query_start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
      AND obj.value:objectDomain::STRING IN ('Table', 'View')
      AND obj.value:objectName::STRING IN (:table_list)
),
query_table_counts AS (
    -- Count how many of our target tables appear in each query.
    -- Used to classify single-table vs join queries without a self-join.
    SELECT query_id, COUNT(DISTINCT table_fqn) AS tables_in_query
    FROM filtered_ah
    GROUP BY query_id
),
table_queries AS (
    SELECT
        fah.table_fqn,
        qh.query_id,
        qh.query_text,
        qh.user_name,
        qtc.tables_in_query,
        ROW_NUMBER()
            OVER (PARTITION BY fah.table_fqn ORDER BY qh.start_time DESC) AS recency_rn
    FROM filtered_ah fah
    JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        ON fah.query_id = qh.query_id
       AND qh.start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
    JOIN query_table_counts qtc
        ON fah.query_id = qtc.query_id
    WHERE qh.query_type = 'SELECT'
      AND qh.execution_status = 'SUCCESS'
      AND LENGTH(qh.query_text) >= :min_query_length
)
SELECT
    table_fqn,
    query_id,
    LEFT(query_text, 4000)                                          AS query_text,
    CASE WHEN tables_in_query > 1 THEN 'JOIN_QUERY'
         ELSE 'SINGLE_TABLE'
    END                                                             AS query_category,
    tables_in_query,
    user_name
FROM table_queries
WHERE recency_rn <= :per_table_limit
ORDER BY table_fqn, tables_in_query DESC, recency_rn;
