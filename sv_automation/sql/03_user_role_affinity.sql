-- User and Role Affinity per Table
-- Returns the top users and roles that access each table/view.
-- Uses direct_objects_accessed so view names are captured directly.
-- Pre-filters ACCESS_HISTORY before joining QUERY_HISTORY for performance on large accounts.
-- Replace :top_n with how many top users/roles to return per table (default 5).

WITH filtered_ah AS (
    SELECT
        ah.query_id,
        base.value:objectName::STRING AS table_fqn,
        ah.user_name
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.direct_objects_accessed) AS base
    WHERE ah.query_start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
      AND base.value:objectDomain::STRING IN ('Table', 'View')
      AND base.value:objectName::STRING IN (:table_list)
),
joined AS (
    SELECT
        f.table_fqn,
        f.user_name,
        qh.role_name,
        COUNT(DISTINCT f.query_id) AS query_count
    FROM filtered_ah f
    JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        ON f.query_id = qh.query_id
       AND qh.start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
    GROUP BY 1, 2, 3
),
ranked AS (
    SELECT
        table_fqn,
        user_name,
        role_name,
        query_count,
        ROW_NUMBER() OVER (PARTITION BY table_fqn ORDER BY query_count DESC) AS rn
    FROM joined
)
SELECT
    table_fqn,
    ARRAY_AGG(OBJECT_CONSTRUCT('user', user_name, 'queries', query_count))
        WITHIN GROUP (ORDER BY query_count DESC) AS top_users,
    ARRAY_AGG(OBJECT_CONSTRUCT('role', role_name, 'queries', query_count))
        WITHIN GROUP (ORDER BY query_count DESC) AS top_roles,
    SUM(query_count)                              AS total_queries
FROM ranked
WHERE rn <= :top_n
GROUP BY table_fqn
ORDER BY total_queries DESC;
