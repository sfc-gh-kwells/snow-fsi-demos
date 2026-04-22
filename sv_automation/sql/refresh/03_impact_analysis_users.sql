-- Refresh: Impact Analysis — Affected Users
-- Finds downstream dependents of a table and the users who query them.
-- Adapted from lineage/templates/impact-analysis-users.sql.
-- Replace :database, :schema, :table, :lookback_days.

WITH lineage_raw AS (
    SELECT
        gl.TARGET_OBJECT_DATABASE AS dep_database,
        gl.TARGET_OBJECT_SCHEMA   AS dep_schema,
        gl.TARGET_OBJECT_NAME     AS dep_object,
        gl.TARGET_OBJECT_DOMAIN   AS object_type,
        gl.DISTANCE
    FROM TABLE(
        SNOWFLAKE.CORE.GET_LINEAGE(
            ':database.:schema.:table', 'TABLE', 'DOWNSTREAM', 5
        )
    ) gl
    WHERE gl.TARGET_OBJECT_DOMAIN IN (
        'TABLE', 'VIEW', 'DYNAMIC TABLE', 'MATERIALIZED VIEW', 'SEMANTIC_VIEW'
    )
),
downstream_deps AS (
    SELECT
        dep_database || '.' || dep_schema || '.' || dep_object AS dependent_object,
        object_type,
        MIN(DISTANCE)                                           AS distance
    FROM lineage_raw
    GROUP BY dep_database, dep_schema, dep_object, object_type
),
affected_users AS (
    SELECT
        ah.user_name,
        base.value:objectName::STRING   AS accessed_object,
        COUNT(DISTINCT ah.query_id)     AS query_count,
        MAX(ah.query_start_time)        AS last_access
    FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
    LATERAL FLATTEN(input => ah.base_objects_accessed) AS base
    WHERE ah.query_start_time >= DATEADD(day, -:lookback_days, CURRENT_TIMESTAMP())
      AND base.value:objectName::STRING IN (
              SELECT dependent_object FROM downstream_deps
          )
    GROUP BY 1, 2
)
SELECT
    au.user_name,
    au.accessed_object,
    au.query_count                      AS queries_last_n_days,
    au.last_access,
    d.object_type,
    d.distance                          AS lineage_hops,
    CASE
        WHEN au.query_count > 50 THEN 'CRITICAL'
        WHEN au.query_count BETWEEN 10 AND 50 THEN 'MODERATE'
        ELSE 'LOW'
    END                                 AS impact_level
FROM affected_users au
JOIN downstream_deps d
    ON au.accessed_object = d.dependent_object
ORDER BY au.query_count DESC, au.last_access DESC;
