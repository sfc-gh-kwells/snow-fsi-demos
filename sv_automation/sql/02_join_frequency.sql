-- Join Frequency Matrix
-- Counts how often pairs of tables/views appear in the same query (co-occurrence).
-- Uses direct_objects_accessed so view names are captured directly.
-- No QUERY_HISTORY join needed — query_id from ACCESS_HISTORY is sufficient for co-occurrence.

WITH query_tables AS (
    SELECT
        ah.query_id,
        base.value:objectName::STRING AS table_fqn
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.direct_objects_accessed) AS base
    WHERE ah.query_start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
      AND base.value:objectDomain::STRING IN ('Table', 'View')
      AND base.value:objectName::STRING IN (:table_list)
),
pairs AS (
    SELECT
        t1.table_fqn                  AS table_a,
        t2.table_fqn                  AS table_b,
        COUNT(DISTINCT t1.query_id)   AS co_occurrence_count
    FROM query_tables t1
    JOIN query_tables t2
        ON  t1.query_id  = t2.query_id
        AND t1.table_fqn < t2.table_fqn   -- avoid duplicates and self-pairs
    GROUP BY 1, 2
)
SELECT
    table_a,
    table_b,
    co_occurrence_count
FROM pairs
ORDER BY co_occurrence_count DESC;
