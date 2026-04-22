-- Table Upstream Lineage
-- Finds upstream source objects for a single table using SNOWFLAKE.CORE.GET_LINEAGE.
-- Replace :database, :schema, :table with the target table parts.
-- Falls back gracefully if GET_LINEAGE is unavailable.

SELECT
    gl.SOURCE_OBJECT_DATABASE || '.' ||
    gl.SOURCE_OBJECT_SCHEMA   || '.' ||
    gl.SOURCE_OBJECT_NAME       AS source_object,
    gl.SOURCE_OBJECT_DOMAIN     AS object_type,
    gl.DISTANCE                 AS lineage_depth
FROM TABLE(
    SNOWFLAKE.CORE.GET_LINEAGE(
        ':database.:schema.:table',
        'TABLE',
        'UPSTREAM',
        3
    )
) gl
WHERE gl.SOURCE_OBJECT_DOMAIN IN (
    'TABLE', 'VIEW', 'DYNAMIC TABLE', 'MATERIALIZED VIEW', 'STAGE', 'STREAM'
)
ORDER BY gl.DISTANCE, source_object;
