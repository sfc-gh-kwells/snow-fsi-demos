-- Table to Query Tag / Warehouse Map
-- Maps each table/view to the query tags and warehouse names most associated with it.
-- Uses direct_objects_accessed so view names are captured directly (not resolved to base tables).
-- Pre-filters ACCESS_HISTORY before joining QUERY_HISTORY for performance on large accounts.

WITH filtered_ah AS (
    SELECT
        ah.query_id,
        base.value:objectName::STRING AS table_fqn
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.direct_objects_accessed) AS base
    WHERE ah.query_start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
      AND base.value:objectDomain::STRING IN ('Table', 'View')
      AND base.value:objectName::STRING IN (:table_list)
),
tagged AS (
    SELECT
        f.table_fqn,
        qh.query_tag,
        qh.warehouse_name,
        COUNT(DISTINCT f.query_id) AS query_count
    FROM filtered_ah f
    JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        ON f.query_id = qh.query_id
       AND qh.start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3
)
SELECT
    table_fqn,
    ARRAY_AGG(DISTINCT query_tag)      WITHIN GROUP (ORDER BY query_tag)      AS query_tags,
    ARRAY_AGG(DISTINCT warehouse_name) WITHIN GROUP (ORDER BY warehouse_name) AS warehouse_names,
    SUM(query_count)                                                            AS total_queries
FROM tagged
WHERE query_tag IS NOT NULL
   OR warehouse_name IS NOT NULL
GROUP BY table_fqn
ORDER BY total_queries DESC;
