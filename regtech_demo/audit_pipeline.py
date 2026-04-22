"""
audit_pipeline.py — Cortex Code Agent-powered regulatory audit pipeline

Reads SQL pipeline files from pipelines/ and analyzes them against
extracted regulatory requirements using Snowflake Cortex AI.

The Cortex Code Agent SDK is used ONLY for AI reasoning (analyze_pipeline).
Plain SQL operations (fetch, insert, truncate) use snowflake.connector directly.

Usage:
    python audit_pipeline.py [--connection MY_DEMO] [--force] [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
import tomllib
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from snowflake.connector import DictCursor

try:
    from cortex_code_agent_sdk import (
        AssistantMessage,
        CortexCodeAgentOptions,
        ResultMessage,
        query,
    )

    # Patch: SDK 0.1.0 message parser requires 'signature' on ThinkingBlock,
    # but some CLI versions omit it. Make it optional.
    # The client does `from ._internal.message_parser import parse_message`
    # inside a method, so we must patch the module attribute in-place.
    import cortex_code_agent_sdk._internal.message_parser as _mp
    _orig_parse_fn = _mp.parse_message

    def _patched_parse(data):
        if isinstance(data, dict):
            for block in data.get("message", {}).get("content", []):
                if isinstance(block, dict) and block.get("type") == "thinking" and "signature" not in block:
                    block["signature"] = ""
        return _orig_parse_fn(data)

    _mp.parse_message = _patched_parse
    # Also patch the module's __dict__ so `from X import parse_message` gets it
    import types
    if hasattr(_mp, '__spec__'):
        _mp.__dict__['parse_message'] = _patched_parse

except ImportError as exc:
    print(f"ERROR: Missing dependency — {exc}")
    print("Run:  pip install cortex_code_agent_sdk")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("audit_pipeline")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent
PIPELINES_DIR = BASE_DIR / "pipelines"
DATABASE = "REGTECH_DEMO_DB"
SCHEMA = "REGULATORY_REPORTING"
REQUIREMENTS_TABLE = f"{DATABASE}.{SCHEMA}.EXTRACTED_REQUIREMENTS"
FINDINGS_TABLE = f"{DATABASE}.{SCHEMA}.AUDIT_FINDINGS"
RUN_LOG_TABLE = f"{DATABASE}.{SCHEMA}.AUDIT_RUN_LOG"
AGENT_TIMEOUT_SECONDS = 600


# ---------------------------------------------------------------------------
# Agent helpers (follows orchestrator.py SDK pattern)
# ---------------------------------------------------------------------------

async def _collect_response(msg_iter) -> str:
    """Drain an async iterator of SDK messages, return concatenated text."""
    text_parts: list[str] = []
    async for msg in msg_iter:
        if isinstance(msg, AssistantMessage):
            for block in msg.content:
                if hasattr(block, "text"):
                    text_parts.append(block.text)
        elif isinstance(msg, ResultMessage):
            await msg_iter.aclose()
            break
    return "".join(text_parts)


def _make_options(connection: str) -> CortexCodeAgentOptions:
    return CortexCodeAgentOptions(
        cwd=str(BASE_DIR),
        connection=connection,
        model="auto",
        dangerously_allow_all_tool_calls=True,
        setting_sources=[],
    )


# ---------------------------------------------------------------------------
# JSON extraction
# ---------------------------------------------------------------------------

def _extract_json(text: str) -> str:
    """Extract a JSON array or object from text that may have surrounding prose."""
    text = text.strip()
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        start = text.find(start_char)
        end = text.rfind(end_char)
        if start != -1 and end != -1 and end > start:
            candidate = text[start : end + 1]
            try:
                json.loads(candidate)
                return candidate
            except json.JSONDecodeError:
                continue
    return text


# ---------------------------------------------------------------------------
# Snowflake connector helper
# ---------------------------------------------------------------------------

def _get_sf_conn(connection_name: str) -> snowflake.connector.SnowflakeConnection:
    """Return a snowflake.connector connection for a named Snowflake CLI connection.

    Reads connection parameters from ~/.snowflake/config.toml (Snowflake CLI format).
    Defaults for warehouse, database, schema, and role are applied so callers do not
    need to qualify every object name.
    """
    config_path = Path.home() / ".snowflake" / "config.toml"
    if not config_path.exists():
        raise FileNotFoundError(f"Snowflake config not found: {config_path}")

    with open(config_path, "rb") as fh:
        config = tomllib.load(fh)

    connections = config.get("connections", {})
    params = connections.get(connection_name) or connections.get(connection_name.lower())
    if params is None:
        available = list(connections.keys())
        raise KeyError(
            f"Connection '{connection_name}' not found in {config_path}. "
            f"Available connections: {available}"
        )

    return snowflake.connector.connect(
        **params,
        warehouse="COMPUTE_WH",
        database=DATABASE,
        schema=SCHEMA,
        role="SYSADMIN",
    )


# ---------------------------------------------------------------------------
# Pipeline header metadata parser
# ---------------------------------------------------------------------------

# Pipeline SQL files have a structured header comment block like:
#   -- Pipeline:      credit_risk_rwa
#   -- Owner:         Risk Analytics
#   -- Schedule:      Daily 06:00 UTC
#   -- Description:   Computes risk-weighted assets ...
#   -- Source Tables:  RAW_EXPOSURES, COUNTERPARTY_RATINGS
#   -- Target:        RWA_SUMMARY

_HEADER_RE = re.compile(
    r"^--\s*(Pipeline|Owner|Schedule|Description|Source Tables|Target)\s*:\s*(.+)$",
    re.IGNORECASE | re.MULTILINE,
)


def _parse_pipeline_header(sql: str) -> dict[str, str]:
    """Extract structured metadata from the SQL file's header comment block."""
    metadata: dict[str, str] = {}
    for match in _HEADER_RE.finditer(sql):
        key = match.group(1).strip().lower().replace(" ", "_")
        metadata[key] = match.group(2).strip()
    return metadata


# ---------------------------------------------------------------------------
# Phase 1: Load pipeline SQL files
# ---------------------------------------------------------------------------

def load_pipelines() -> list[dict]:
    """Read all .sql files from pipelines/ directory, including header metadata."""
    if not PIPELINES_DIR.exists():
        log.error("Pipelines directory not found: %s", PIPELINES_DIR)
        sys.exit(1)

    sql_files = sorted(PIPELINES_DIR.glob("*.sql"))
    if not sql_files:
        log.error("No .sql files found in %s", PIPELINES_DIR)
        sys.exit(1)

    pipelines = []
    for f in sql_files:
        sql = f.read_text(encoding="utf-8")
        metadata = _parse_pipeline_header(sql)
        pipelines.append({
            "name": metadata.get("pipeline", f.stem),
            "filename": f.name,
            "owner": metadata.get("owner", "Unknown"),
            "schedule": metadata.get("schedule", "Unknown"),
            "description": metadata.get("description", ""),
            "source_tables": metadata.get("source_tables", ""),
            "target": metadata.get("target", ""),
            "sql": sql,
        })
        log.info(
            "  Loaded: %-40s owner=%-20s target=%s",
            pipelines[-1]["name"], pipelines[-1]["owner"], pipelines[-1]["target"],
        )
    log.info("Loaded %d pipeline SQL files from %s", len(pipelines), PIPELINES_DIR)
    return pipelines


# ---------------------------------------------------------------------------
# Phase 2: Fetch regulatory requirements via direct connector query
# ---------------------------------------------------------------------------

def fetch_requirements(sf_conn: snowflake.connector.SnowflakeConnection) -> str:
    """Read extracted requirements from Snowflake and return as formatted text.

    Uses a direct connector query — no agent SDK needed for a plain SELECT.
    The returned text is passed verbatim into the audit prompt for each pipeline.
    """
    log.info("Fetching regulatory requirements from %s ...", REQUIREMENTS_TABLE)
    cur = sf_conn.cursor(DictCursor)
    cur.execute(f"SELECT * FROM {REQUIREMENTS_TABLE} ORDER BY REQ_ID")
    rows = cur.fetchall()

    if not rows:
        log.warning("No requirements found in %s", REQUIREMENTS_TABLE)
        return "(No requirements found)"

    lines: list[str] = []
    for row in rows:
        lines.append(
            f"[{row['REQ_ID']}] {row['CATEGORY']} (Severity: {row['SEVERITY']})\n"
            f"  Requirement: {row['REQUIREMENT']}\n"
            f"  Threshold:   {row.get('THRESHOLD') or 'N/A'}\n"
            f"  Impact Area: {row.get('IMPACT_AREA') or 'N/A'}"
        )
    result = "\n\n".join(lines)
    log.info("Requirements fetched: %d rows (%d chars)", len(rows), len(result))
    return result


# ---------------------------------------------------------------------------
# Phase 3: Analyze each pipeline
# ---------------------------------------------------------------------------

def _build_audit_prompt(pipeline: dict, requirements_text: str) -> str:
    name = pipeline["name"]
    return f"""You are a regulatory compliance auditor. Analyze this SQL data pipeline for Basel III/IV compliance issues.

PIPELINE: {name}
OWNER: {pipeline.get('owner', 'Unknown')}
SCHEDULE: {pipeline.get('schedule', 'Unknown')}
DESCRIPTION: {pipeline.get('description', 'N/A')}
SOURCE TABLES: {pipeline.get('source_tables', 'N/A')}
TARGET TABLE: {pipeline.get('target', 'N/A')}

SQL:
{pipeline['sql']}

REGULATORY REQUIREMENTS:
{requirements_text}

Analyze the pipeline SQL and identify compliance gaps. For each finding, return a JSON object with these fields:
- finding_id: unique ID like "AUD-001"
- pipeline_name: name of the pipeline file
- severity: "Critical", "High", or "Medium"
- category: type of issue (e.g. "Model Methodology", "Exposure Measurement", "Output Floor")
- description: clear explanation of the compliance gap
- affected_table: which downstream table is affected
- old_logic: the problematic SQL pattern from the pipeline
- suggested_fix: corrected SQL that would be compliant
- regulation_ref: specific BCBS/Basel reference (e.g. "BCBS FRTB MAR33.1")

Return ONLY valid JSON array. No markdown, no explanation outside the JSON."""


async def analyze_pipeline(
    pipeline: dict,
    requirements_text: str,
    connection: str,
    finding_counter: int,
) -> tuple[list[dict], int]:
    """Analyze one pipeline SQL file. Returns (findings, updated_counter).

    Retries once if JSON parsing fails on the first attempt.
    """
    name = pipeline["name"]
    options = _make_options(connection)
    prompt = _build_audit_prompt(pipeline, requirements_text)
    max_attempts = 2

    for attempt in range(1, max_attempts + 1):
        retry_note = ""
        if attempt > 1:
            retry_note = (
                "\n\nYour previous response was not valid JSON. "
                "Return ONLY a JSON array — no markdown fences, no explanation."
            )
        try:
            result = await asyncio.wait_for(
                _collect_response(query(prompt=prompt + retry_note, options=options)),
                timeout=AGENT_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            log.error("  [%s] Agent timed out (attempt %d/%d)", name, attempt, max_attempts)
            if attempt == max_attempts:
                return [], finding_counter
            continue
        except Exception as exc:
            log.error("  [%s] Agent error (attempt %d/%d): %s", name, attempt, max_attempts, exc)
            if attempt == max_attempts:
                return [], finding_counter
            continue

        # Parse JSON from response
        raw_json = _extract_json(result)
        try:
            findings = json.loads(raw_json)
            if isinstance(findings, dict):
                findings = [findings]
            if not isinstance(findings, list):
                raise ValueError(f"Expected list, got {type(findings).__name__}")
        except (json.JSONDecodeError, ValueError) as exc:
            log.warning(
                "  [%s] JSON parse failed (attempt %d/%d): %s",
                name, attempt, max_attempts, exc,
            )
            if attempt < max_attempts:
                log.info("  [%s] Retrying ...", name)
                continue
            log.error("  [%s] Giving up after %d attempts", name, max_attempts)
            return [], finding_counter

        # Normalize finding IDs to be globally unique
        for f in findings:
            finding_counter += 1
            f["finding_id"] = f"AUD-{finding_counter:03d}"
            f["pipeline_name"] = name

        log.info("  [%s] Found %d compliance findings", name, len(findings))
        return findings, finding_counter

    return [], finding_counter


# ---------------------------------------------------------------------------
# Phase 4: Write findings to Snowflake
# ---------------------------------------------------------------------------

def write_findings_to_snowflake(
    all_findings: list[dict],
    sf_conn: snowflake.connector.SnowflakeConnection,
    force: bool,
) -> None:
    """Insert findings into AUDIT_FINDINGS using a direct connector connection.

    Uses parameterized executemany() — no string escaping required.
    Saves a local JSON cache first as a backup regardless of DB outcome.
    """
    cache_path = BASE_DIR / "audit_findings_cache.json"
    with open(cache_path, "w") as fp:
        json.dump(all_findings, fp, indent=2)
    log.info("Cached %d findings to %s", len(all_findings), cache_path)

    cur = sf_conn.cursor()

    if force:
        log.info("--force: truncating existing findings ...")
        cur.execute(f"TRUNCATE TABLE {FINDINGS_TABLE}")
        log.info("Truncated %s", FINDINGS_TABLE)

    if not all_findings:
        log.info("No findings to write.")
        return

    rows = [
        (
            f.get("finding_id", ""),
            f.get("pipeline_name", ""),
            f.get("severity", ""),
            f.get("category", ""),
            f.get("description", ""),
            f.get("affected_table", ""),
            f.get("old_logic", ""),
            f.get("suggested_fix", ""),
            f.get("regulation_ref", ""),
        )
        for f in all_findings
    ]
    cur.executemany(
        f"INSERT INTO {FINDINGS_TABLE} "
        f"(FINDING_ID, PIPELINE_NAME, SEVERITY, CATEGORY, DESCRIPTION, "
        f"AFFECTED_TABLE, OLD_LOGIC, SUGGESTED_FIX, REGULATION_REF) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        rows,
    )
    log.info("Inserted %d findings into %s", len(all_findings), FINDINGS_TABLE)


# ---------------------------------------------------------------------------
# Phase 5: Write run log entry
# ---------------------------------------------------------------------------

def write_run_log(
    run_id: str,
    pipelines_scanned: int,
    total_findings: int,
    critical_count: int,
    high_count: int,
    medium_count: int,
    duration_seconds: float,
    sf_conn: snowflake.connector.SnowflakeConnection,
) -> None:
    """Insert a summary row into AUDIT_RUN_LOG via a direct connector call."""
    log.info("Writing run log entry for %s ...", run_id)
    try:
        cur = sf_conn.cursor()
        cur.execute(
            f"INSERT INTO {RUN_LOG_TABLE} "
            f"(RUN_ID, RUN_TIMESTAMP, STATUS, PIPELINES_SCANNED, FINDINGS_COUNT, "
            f"CRITICAL_COUNT, HIGH_COUNT, MEDIUM_COUNT, DURATION_SECONDS) "
            f"VALUES (%s, CURRENT_TIMESTAMP(), 'COMPLETED', %s, %s, %s, %s, %s, %s)",
            (
                run_id,
                pipelines_scanned,
                total_findings,
                critical_count,
                high_count,
                medium_count,
                round(duration_seconds, 1),
            ),
        )
        log.info("Run log entry written")
    except Exception as exc:
        log.warning("Failed to write run log: %s", exc)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(args: argparse.Namespace) -> None:
    run_id = f"audit_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    connection = args.connection
    t_start = time.monotonic()

    log.info("=" * 60)
    log.info("REGULATORY AUDIT PIPELINE — %s", run_id)
    log.info("Connection: %s  Force: %s  Dry-run: %s", connection, args.force, args.dry_run)
    log.info("=" * 60)

    # Phase 1: Load pipelines
    pipelines = load_pipelines()

    # Open one shared connector connection for all direct SQL operations.
    # analyze_pipeline uses the agent SDK separately (it needs AI reasoning).
    sf_conn = _get_sf_conn(connection)
    try:
        # Phase 2: Fetch requirements
        requirements_text = fetch_requirements(sf_conn)

        # Phase 3: Analyze each pipeline sequentially
        all_findings: list[dict] = []
        finding_counter = 0

        for i, pipeline in enumerate(pipelines, 1):
            log.info("[%d/%d] Analyzing pipeline: %s", i, len(pipelines), pipeline["name"])
            findings, finding_counter = await analyze_pipeline(
                pipeline, requirements_text, connection, finding_counter,
            )
            all_findings.extend(findings)

        # Summarize
        critical = sum(1 for f in all_findings if f.get("severity") == "Critical")
        high = sum(1 for f in all_findings if f.get("severity") == "High")
        medium = sum(1 for f in all_findings if f.get("severity") == "Medium")
        duration = time.monotonic() - t_start

        log.info("-" * 60)
        log.info("AUDIT COMPLETE")
        log.info("  Pipelines scanned:  %d", len(pipelines))
        log.info("  Total findings:     %d", len(all_findings))
        log.info("  Critical:           %d", critical)
        log.info("  High:               %d", high)
        log.info("  Medium:             %d", medium)
        log.info("  Duration:           %.1fs", duration)
        log.info("-" * 60)

        if args.dry_run:
            log.info("DRY RUN — printing findings to stdout (not writing to Snowflake)")
            print(json.dumps(all_findings, indent=2))
            return

        # Phase 4: Write findings
        write_findings_to_snowflake(all_findings, sf_conn, args.force)

        # Phase 5: Write run log
        write_run_log(
            run_id=run_id,
            pipelines_scanned=len(pipelines),
            total_findings=len(all_findings),
            critical_count=critical,
            high_count=high,
            medium_count=medium,
            duration_seconds=duration,
            sf_conn=sf_conn,
        )

    finally:
        sf_conn.close()

    log.info("=" * 60)
    log.info("DONE — %d findings written to %s", len(all_findings), FINDINGS_TABLE)
    log.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cortex Code Agent-powered regulatory audit pipeline",
    )
    parser.add_argument(
        "--connection",
        default="MY_DEMO",
        help="Snowflake connection name (default: MY_DEMO)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Truncate existing findings before inserting new ones",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print findings to stdout without writing to Snowflake",
    )
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
