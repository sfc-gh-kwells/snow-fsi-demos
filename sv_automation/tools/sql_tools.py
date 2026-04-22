"""
sql_tools.py — MCP @tool definitions for the SV Automation project.

Each tool wraps a deterministic SQL script or Snowflake operation.
Tools are registered with create_sdk_mcp_server() in mcp_server.py and
injected into every sub-agent's option set via CortexCodeAgentOptions(mcp_servers=...).

Snowflake connection is established once per tool call using the connection
name from config, reading credentials from ~/.snowflake/connections.toml.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import snowflake.connector

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[no-redef]

from cortex_code_agent_sdk import tool

# ---------------------------------------------------------------------------
# Snowflake connection helper
# ---------------------------------------------------------------------------

_SQL_DIR = Path(__file__).parent.parent / "sql"
_connection_cache: dict[str, snowflake.connector.SnowflakeConnection] = {}
_role_override: str = ""


def configure_role(role: str) -> None:
    """Set a role to inject into all new Snowflake connections for this process."""
    global _role_override
    _role_override = role


def _get_connection(connection_name: str) -> snowflake.connector.SnowflakeConnection:
    """Return a cached Snowflake connection, creating it if needed."""
    if connection_name not in _connection_cache:
        config_path = Path.home() / ".snowflake" / "config.toml"
        # Fall back to legacy connections.toml if config.toml doesn't exist
        if not config_path.exists():
            config_path = Path.home() / ".snowflake" / "connections.toml"
        with open(config_path, "rb") as f:
            all_config = tomllib.load(f)
        # config.toml stores connections under [connections.<name>]
        connections = all_config.get("connections", all_config)
        params = dict(connections.get(connection_name, {}))
        if not params:
            raise ValueError(f"Connection '{connection_name}' not found in {config_path}")
        # Remove keys that snowflake.connector doesn't accept
        params.pop("authenticator_type", None)
        # Inject role override from configure_role() if not already set in connection config
        if _role_override and "role" not in params:
            params["role"] = _role_override
        # Map 'token' to 'password' + set authenticator for PAT connections
        if "token" in params and "password" not in params:
            params["password"] = params.pop("token")
            params.setdefault("authenticator", "PROGRAMMATIC_ACCESS_TOKEN")
        _connection_cache[connection_name] = snowflake.connector.connect(**params)
    return _connection_cache[connection_name]


def _run_sql(
    connection_name: str,
    sql: str,
    bindings: dict[str, Any] | None = None,
) -> list[dict]:
    """Execute SQL and return rows as a list of dicts."""
    conn = _get_connection(connection_name)
    cur = conn.cursor(snowflake.connector.DictCursor)
    cur.execute(sql, bindings or {})
    return cur.fetchall()


def _load_sql(filename: str, replacements: dict[str, str]) -> str:
    """Load a SQL template and apply plain-text replacements for :param style."""
    path = _SQL_DIR / filename
    text = path.read_text()
    for key, val in replacements.items():
        text = text.replace(f":{key}", val)
    return text


# ---------------------------------------------------------------------------
# Phase 1 tools — Context Mining
# ---------------------------------------------------------------------------

@tool(
    "get_table_tag_warehouse_map",
    "Get query tag and warehouse associations for a list of tables from query history.",
    {"connection": str, "table_list_json": str, "lookback_days": int},
)
async def get_table_tag_warehouse_map(args: dict) -> dict:
    tables: list[str] = json.loads(args["table_list_json"])
    quoted = ", ".join(f"'{t}'" for t in tables)
    sql = _load_sql(
        "01_table_tag_warehouse_map.sql",
        {"table_list": quoted, "lookback_days": str(args["lookback_days"])},
    )
    rows = _run_sql(args["connection"], sql)
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


@tool(
    "get_join_frequency",
    "Get co-occurrence (join frequency) counts for pairs of tables from query history.",
    {"connection": str, "table_list_json": str, "lookback_days": int},
)
async def get_join_frequency(args: dict) -> dict:
    tables: list[str] = json.loads(args["table_list_json"])
    quoted = ", ".join(f"'{t}'" for t in tables)
    sql = _load_sql(
        "02_join_frequency.sql",
        {"table_list": quoted, "lookback_days": str(args["lookback_days"])},
    )
    rows = _run_sql(args["connection"], sql)
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


@tool(
    "get_user_role_affinity",
    "Get the top users and roles that access each table in the provided list.",
    {"connection": str, "table_list_json": str, "lookback_days": int, "top_n": int},
)
async def get_user_role_affinity(args: dict) -> dict:
    tables: list[str] = json.loads(args["table_list_json"])
    quoted = ", ".join(f"'{t}'" for t in tables)
    sql = _load_sql(
        "03_user_role_affinity.sql",
        {
            "table_list": quoted,
            "lookback_days": str(args["lookback_days"]),
            "top_n": str(args.get("top_n", 5)),
        },
    )
    rows = _run_sql(args["connection"], sql)
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


# ---------------------------------------------------------------------------
# Phase 3 tools — Lineage + DDL
# ---------------------------------------------------------------------------

@tool(
    "get_table_ddl",
    "Get the DDL definition (column names and types) for a fully-qualified table.",
    {"connection": str, "table_fqn": str},
)
async def get_table_ddl(args: dict) -> dict:
    parts = args["table_fqn"].split(".")
    if len(parts) != 3:
        return {"content": [{"type": "text", "text": "ERROR: table_fqn must be DB.SCHEMA.TABLE"}]}
    database, schema, table = parts

    # Use INFORMATION_SCHEMA for column metadata (no privileges required beyond SELECT)
    sql = f"""
        SELECT column_name, data_type, is_nullable, comment
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema}'
          AND table_name   = '{table}'
        ORDER BY ordinal_position
    """
    rows = _run_sql(args["connection"], sql)
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


@tool(
    "get_table_lineage",
    "Get upstream lineage (source objects) for a fully-qualified table.",
    {"connection": str, "table_fqn": str},
)
async def get_table_lineage(args: dict) -> dict:
    parts = args["table_fqn"].split(".")
    if len(parts) != 3:
        return {"content": [{"type": "text", "text": "ERROR: table_fqn must be DB.SCHEMA.TABLE"}]}
    database, schema, table = parts
    sql = _load_sql(
        "05_table_lineage.sql",
        {"database": database, "schema": schema, "table": table},
    )
    try:
        rows = _run_sql(args["connection"], sql)
    except Exception as exc:
        rows = [{"error": str(exc), "note": "GET_LINEAGE may require additional privileges"}]
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


# ---------------------------------------------------------------------------
# Phase 4 tool — Fast SV Generation
# ---------------------------------------------------------------------------

@tool(
    "fast_generate_semantic_view",
    (
        "Generate a semantic view YAML using SYSTEM$CORTEX_ANALYST_FAST_GENERATION. "
        "tables_json: list of {database, schema, table, columnNames}. "
        "sql_examples_json: list of {sqlText, correspondingQuestion}."
    ),
    {
        "connection": str,
        "sv_name": str,
        "target_database": str,
        "target_schema": str,
        "tables_json": str,
        "sql_examples_json": str,
        "description": str,
        "warehouse": str,
    },
)
async def fast_generate_semantic_view(args: dict) -> dict:
    tables = json.loads(args["tables_json"])
    sql_examples = json.loads(args["sql_examples_json"])

    payload = {
        "json_proto": {
            "name": args["sv_name"],
            "database": args["target_database"],
            "schema": args["target_schema"],
            "tables": tables,
            "sqlSource": {
                "queries": sql_examples[:10]  # cap at 10 to keep prompt lean
            },
            "semanticDescription": args["description"],
            "metadata": {
                "warehouse": args["warehouse"]
            },
        }
    }

    # Use $$ dollar-quoting to avoid single-quote escaping issues in the JSON
    payload_str = json.dumps(payload)
    sql = f"SELECT SYSTEM$CORTEX_ANALYST_FAST_GENERATION($${ payload_str }$$) AS FASTGEN_RESULT"

    try:
        rows = _run_sql(args["connection"], sql)
        result = rows[0].get("FASTGEN_RESULT", "") if rows else ""
        return {"content": [{"type": "text", "text": result}]}
    except Exception as exc:
        return {"content": [{"type": "text", "text": f"ERROR: {exc}"}]}


# ---------------------------------------------------------------------------
# Phase 4 tool — cortex reflect validation
# ---------------------------------------------------------------------------

@tool(
    "run_cortex_reflect",
    "Validate a semantic view YAML file using 'cortex reflect'. Returns any errors found. "
    "Pass target_schema as 'DATABASE.SCHEMA' to validate against a specific target schema.",
    {"yaml_path": str, "target_schema": str},
)
async def run_cortex_reflect(args: dict) -> dict:
    cmd = ["cortex", "reflect", args["yaml_path"]]
    target_schema = (args.get("target_schema") or "").strip()
    if target_schema:
        cmd += ["--target-schema", target_schema]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    output = (result.stdout + result.stderr).strip()
    status = "VALID" if result.returncode == 0 else "ERRORS_FOUND"
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps({"status": status, "output": output, "returncode": result.returncode}),
            }
        ]
    }


# ---------------------------------------------------------------------------
# Phase 4b tool — Deploy semantic view to Snowflake
# ---------------------------------------------------------------------------

@tool(
    "deploy_semantic_view",
    "Deploy a validated semantic view YAML file to Snowflake using "
    "SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML. Call this after run_cortex_reflect succeeds.",
    {"connection": str, "yaml_path": str, "target_schema": str},
)
async def deploy_semantic_view(args: dict) -> dict:
    yaml_file = Path(args["yaml_path"])
    if not yaml_file.exists():
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {"status": "error", "message": f"YAML file not found: {args['yaml_path']}"}
                    ),
                }
            ]
        }
    yaml_content = yaml_file.read_text(encoding="utf-8")
    # Dollar-quote the YAML to avoid single-quote escaping issues
    sql = (
        f"CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML("
        f"'{args['target_schema']}', $${yaml_content}$$, FALSE)"
    )
    try:
        rows = _run_sql(args["connection"], sql)
        # DictCursor returns the CALL result as a single-row dict; key is the proc name
        message = next(iter(rows[0].values()), "") if rows else "No response"
        return {
            "content": [
                {"type": "text", "text": json.dumps({"status": "deployed", "message": message})}
            ]
        }
    except Exception as exc:
        return {
            "content": [
                {"type": "text", "text": json.dumps({"status": "error", "message": str(exc)})}
            ]
        }


# ---------------------------------------------------------------------------
# Phase 4c tool — Column sample values (for SV enrichment)
# ---------------------------------------------------------------------------

@tool(
    "get_column_samples",
    (
        "Fetch up to max_samples distinct non-null values from a single column. "
        "Use for categorical VARCHAR/TEXT columns to populate sample_values in SV YAML."
    ),
    {
        "connection": str,
        "database": str,
        "schema": str,
        "table": str,
        "column_name": str,
        "max_samples": int,
    },
)
async def get_column_samples(args: dict) -> dict:
    db = args["database"]
    schema = args["schema"]
    table = args["table"]
    col = args["column_name"]
    limit = int(args.get("max_samples", 10))
    sql = (
        f'SELECT DISTINCT "{col}" AS val '
        f'FROM "{db}"."{schema}"."{table}" '
        f'WHERE "{col}" IS NOT NULL '
        f"LIMIT {limit}"
    )
    try:
        rows = _run_sql(args["connection"], sql)
        values = [r["VAL"] for r in rows if r.get("VAL") is not None]
        return {"content": [{"type": "text", "text": json.dumps(values)}]}
    except Exception as exc:
        return {"content": [{"type": "text", "text": json.dumps([])}]}


# ---------------------------------------------------------------------------
# Phase 5 tools — Evaluation
# ---------------------------------------------------------------------------

@tool(
    "get_eval_questions",
    "Get sample SQL queries from query history to use as Cortex Analyst evaluation questions.",
    {
        "connection": str,
        "table_list_json": str,
        "lookback_days": int,
        "per_table_limit": int,
        "min_query_length": int,
    },
)
async def get_eval_questions(args: dict) -> dict:
    tables: list[str] = json.loads(args["table_list_json"])
    quoted = ", ".join(f"'{t}'" for t in tables)
    sql = _load_sql(
        "06_eval_questions.sql",
        {
            "table_list": quoted,
            "lookback_days": str(args["lookback_days"]),
            "per_table_limit": str(args["per_table_limit"]),
            "min_query_length": str(args["min_query_length"]),
        },
    )
    rows = _run_sql(args["connection"], sql)
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


@tool(
    "call_cortex_analyst",
    "Run a natural language question through Cortex Analyst against a local YAML semantic model file.",
    {"yaml_path": str, "question": str, "connection": str},
)
async def call_cortex_analyst(args: dict) -> dict:
    """Calls `cortex analyst query` CLI against a local YAML file."""
    result = subprocess.run(
        ["cortex", "analyst", "query", args["question"], f"--model={args['yaml_path']}"],
        capture_output=True,
        text=True,
    )
    output = result.stdout or result.stderr
    return {"content": [{"type": "text", "text": output.strip()}]}


@tool(
    "run_sql_query",
    "Execute a SQL query and return the results as JSON. Use for comparing Cortex Analyst output to original.",
    {"connection": str, "sql": str},
)
async def run_sql_query(args: dict) -> dict:
    try:
        rows = _run_sql(args["connection"], args["sql"])
        return {"content": [{"type": "text", "text": json.dumps({"rows": rows, "count": len(rows)})}]}
    except Exception as exc:
        return {"content": [{"type": "text", "text": json.dumps({"error": str(exc)})}]}


# ---------------------------------------------------------------------------
# Phase 6 tools — Cortex Search
# ---------------------------------------------------------------------------

@tool(
    "get_cortex_search_candidates",
    "Find high-cardinality VARCHAR columns that are frequently accessed — candidates for Cortex Search Services.",
    {
        "connection": str,
        "database": str,
        "table_list_json": str,
        "min_distinct": int,
    },
)
async def get_cortex_search_candidates(args: dict) -> dict:
    tables: list[str] = json.loads(args["table_list_json"])
    # Extract just the table names (last part of FQN) for INFORMATION_SCHEMA queries
    table_names = [t.split(".")[-1] for t in tables]
    quoted_names = ", ".join(f"'{t}'" for t in table_names)
    sql = _load_sql(
        "07_cortex_search_candidates.sql",
        {
            "database": args["database"],
            "table_list": quoted_names,
            "min_distinct": str(args["min_distinct"]),
        },
    )
    rows = _run_sql(args["connection"], sql)
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


@tool(
    "check_distinct_count",
    "Run APPROX_COUNT_DISTINCT on a column to confirm cardinality before creating a Cortex Search Service.",
    {"connection": str, "table_fqn": str, "column_name": str},
)
async def check_distinct_count(args: dict) -> dict:
    sql = f"SELECT APPROX_COUNT_DISTINCT({args['column_name']}) AS distinct_count FROM {args['table_fqn']}"
    try:
        rows = _run_sql(args["connection"], sql)
        count = rows[0].get("DISTINCT_COUNT", 0) if rows else 0
        return {"content": [{"type": "text", "text": json.dumps({"distinct_count": count})}]}
    except Exception as exc:
        return {"content": [{"type": "text", "text": json.dumps({"error": str(exc)})}]}


@tool(
    "create_cortex_search_service",
    "Create a Cortex Search Service on a VARCHAR column. Fails gracefully if change tracking is not enabled.",
    {"connection": str, "service_name": str, "table_fqn": str, "column_name": str, "warehouse": str},
)
async def create_cortex_search_service(args: dict) -> dict:
    service_fqn = args["service_name"]
    sql = f"""
        CREATE OR REPLACE CORTEX SEARCH SERVICE {service_fqn}
        ON {args['column_name']}
        WAREHOUSE = {args['warehouse']}
        AS SELECT DISTINCT {args['column_name']} FROM {args['table_fqn']}
    """
    try:
        _run_sql(args["connection"], sql)
        return {"content": [{"type": "text", "text": json.dumps({"status": "created", "service": service_fqn})}]}
    except Exception as exc:
        error_str = str(exc)
        reason = "change_tracking_not_enabled" if "change tracking" in error_str.lower() else "other_error"
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({"status": "failed", "reason": reason, "error": error_str}),
                }
            ]
        }


# ---------------------------------------------------------------------------
# Refresh tools
# ---------------------------------------------------------------------------

@tool(
    "get_refresh_change_detection",
    "Detect schema, column, and data changes on a table's upstream lineage since last N days.",
    {"connection": str, "database": str, "schema": str, "table": str, "lookback_days": int},
)
async def get_refresh_change_detection(args: dict) -> dict:
    sql = _load_sql(
        "refresh/01_change_detection.sql",
        {
            "database": args["database"],
            "schema": args["schema"],
            "table": args["table"],
            "lookback_days": str(args["lookback_days"]),
        },
    )
    try:
        rows = _run_sql(args["connection"], sql)
    except Exception as exc:
        rows = [{"error": str(exc)}]
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


@tool(
    "get_column_change_detection",
    "Get change history and usage stats for a specific column (for refresh impact analysis).",
    {"connection": str, "database": str, "schema": str, "table": str, "column": str, "lookback_days": int},
)
async def get_column_change_detection(args: dict) -> dict:
    sql = _load_sql(
        "refresh/02_column_change_detection.sql",
        {
            "database": args["database"],
            "schema": args["schema"],
            "table": args["table"],
            "column": args["column"],
            "lookback_days": str(args["lookback_days"]),
        },
    )
    rows = _run_sql(args["connection"], sql)
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


@tool(
    "get_impact_analysis_users",
    "Get downstream dependents of a table and the users who query them (for refresh impact assessment).",
    {"connection": str, "database": str, "schema": str, "table": str, "lookback_days": int},
)
async def get_impact_analysis_users(args: dict) -> dict:
    sql = _load_sql(
        "refresh/03_impact_analysis_users.sql",
        {
            "database": args["database"],
            "schema": args["schema"],
            "table": args["table"],
            "lookback_days": str(args["lookback_days"]),
        },
    )
    try:
        rows = _run_sql(args["connection"], sql)
    except Exception as exc:
        rows = [{"error": str(exc)}]
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


# ---------------------------------------------------------------------------
# Utility tools
# ---------------------------------------------------------------------------

@tool(
    "enumerate_tables",
    "List all tables in a given Snowflake database or schema.",
    {"connection": str, "database": str, "schema": str},
)
async def enumerate_tables(args: dict) -> dict:
    schema_filter = f"AND table_schema = '{args['schema']}'" if args.get("schema") else ""
    sql = f"""
        SELECT
            table_catalog || '.' || table_schema || '.' || table_name AS table_fqn,
            table_schema,
            table_name,
            table_type,
            row_count,
            bytes
        FROM {args['database']}.INFORMATION_SCHEMA.TABLES
        WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
          AND table_type IN ('BASE TABLE', 'VIEW')
          {schema_filter}
        ORDER BY table_schema, table_name
    """
    rows = _run_sql(args["connection"], sql)
    return {"content": [{"type": "text", "text": json.dumps(rows)}]}


# Expose all tools as a flat list for mcp_server registration
ALL_TOOLS = [
    get_table_tag_warehouse_map,
    get_join_frequency,
    get_user_role_affinity,
    get_table_ddl,
    get_table_lineage,
    fast_generate_semantic_view,
    run_cortex_reflect,
    deploy_semantic_view,
    get_column_samples,
    get_eval_questions,
    call_cortex_analyst,
    run_sql_query,
    get_cortex_search_candidates,
    check_distinct_count,
    create_cortex_search_service,
    get_refresh_change_detection,
    get_column_change_detection,
    get_impact_analysis_users,
    enumerate_tables,
]
