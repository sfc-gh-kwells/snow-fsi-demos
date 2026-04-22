"""
audit_pipeline.py — Cortex Code Agent-powered regulatory audit pipeline

Reads SQL pipeline files from pipelines/ and analyzes them against
extracted regulatory requirements using Snowflake Cortex AI.

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
from datetime import datetime, timezone
from pathlib import Path

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
# Phase 2: Fetch regulatory requirements via agent
# ---------------------------------------------------------------------------

async def fetch_requirements(connection: str) -> str:
    """Use an agent call to read extracted requirements from Snowflake."""
    log.info("Fetching regulatory requirements from %s ...", REQUIREMENTS_TABLE)
    options = _make_options(connection)

    prompt = f"""You have access to the sql_execute tool. Run this query and return the full result as text:

SELECT * FROM {REQUIREMENTS_TABLE};

Return the query results as-is. Do not summarize or omit any rows."""

    try:
        result = await asyncio.wait_for(
            _collect_response(query(prompt=prompt, options=options)),
            timeout=AGENT_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        log.error("Timed out fetching requirements after %ds", AGENT_TIMEOUT_SECONDS)
        sys.exit(1)
    except Exception as exc:
        log.error("Failed to fetch requirements: %s", exc)
        sys.exit(1)

    log.info("Requirements fetched (%d chars)", len(result))
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

def _escape_sql_string(value: str) -> str:
    """Escape single quotes for safe SQL string interpolation."""
    if value is None:
        return ""
    return str(value).replace("\\", "\\\\").replace("'", "''")


async def write_findings_to_snowflake(
    all_findings: list[dict],
    connection: str,
    force: bool,
) -> None:
    """Insert findings into AUDIT_FINDINGS table via agent sql_execute calls.

    Saves findings to a local JSON cache first, then inserts in small batches
    to avoid SDK timeout issues.
    """
    options = _make_options(connection)

    # Always save findings locally as a cache/backup
    cache_path = BASE_DIR / "audit_findings_cache.json"
    with open(cache_path, "w") as fp:
        json.dump(all_findings, fp, indent=2)
    log.info("Cached %d findings to %s", len(all_findings), cache_path)

    if force:
        log.info("--force: truncating existing findings ...")
        truncate_prompt = f"Use the sql_execute tool to run: TRUNCATE TABLE {FINDINGS_TABLE};"
        try:
            await asyncio.wait_for(
                _collect_response(query(prompt=truncate_prompt, options=options)),
                timeout=60,
            )
            log.info("Truncated %s", FINDINGS_TABLE)
        except Exception as exc:
            log.warning("Failed to truncate findings table: %s", exc)

    if not all_findings:
        log.info("No findings to write.")
        return

    # Insert in batches of 5 to avoid timeout on large payloads
    BATCH_SIZE = 5
    inserted = 0
    for batch_start in range(0, len(all_findings), BATCH_SIZE):
        batch = all_findings[batch_start : batch_start + BATCH_SIZE]
        value_rows = []
        for f in batch:
            vals = (
                f"('{_escape_sql_string(f.get('finding_id', ''))}', "
                f"'{_escape_sql_string(f.get('pipeline_name', ''))}', "
                f"'{_escape_sql_string(f.get('severity', ''))}', "
                f"'{_escape_sql_string(f.get('category', ''))}', "
                f"'{_escape_sql_string(f.get('description', ''))}', "
                f"'{_escape_sql_string(f.get('affected_table', ''))}', "
                f"'{_escape_sql_string(f.get('old_logic', ''))}', "
                f"'{_escape_sql_string(f.get('suggested_fix', ''))}', "
                f"'{_escape_sql_string(f.get('regulation_ref', ''))}')"
            )
            value_rows.append(vals)

        insert_sql = (
            f"INSERT INTO {FINDINGS_TABLE} "
            f"(FINDING_ID, PIPELINE_NAME, SEVERITY, CATEGORY, DESCRIPTION, "
            f"AFFECTED_TABLE, OLD_LOGIC, SUGGESTED_FIX, REGULATION_REF) VALUES\n"
            + ",\n".join(value_rows)
            + ";"
        )

        insert_prompt = f"Use the sql_execute tool to run this SQL:\n\n{insert_sql}"
        batch_num = batch_start // BATCH_SIZE + 1
        total_batches = (len(all_findings) + BATCH_SIZE - 1) // BATCH_SIZE
        log.info("Inserting batch %d/%d (%d rows) ...", batch_num, total_batches, len(batch))
        try:
            await asyncio.wait_for(
                _collect_response(query(prompt=insert_prompt, options=options)),
                timeout=120,
            )
            inserted += len(batch)
        except asyncio.TimeoutError:
            log.error("Timed out inserting batch %d", batch_num)
        except Exception as exc:
            log.error("Failed to insert batch %d: %s", batch_num, exc)

    log.info("Inserted %d/%d findings into %s", inserted, len(all_findings), FINDINGS_TABLE)


# ---------------------------------------------------------------------------
# Phase 5: Write run log entry
# ---------------------------------------------------------------------------

async def write_run_log(
    run_id: str,
    pipelines_scanned: int,
    total_findings: int,
    critical_count: int,
    high_count: int,
    medium_count: int,
    duration_seconds: float,
    connection: str,
) -> None:
    """Insert a summary row into AUDIT_RUN_LOG."""
    options = _make_options(connection)

    insert_sql = (
        f"INSERT INTO {RUN_LOG_TABLE} "
        f"(RUN_ID, RUN_TIMESTAMP, STATUS, PIPELINES_SCANNED, FINDINGS_COUNT, "
        f"CRITICAL_COUNT, HIGH_COUNT, MEDIUM_COUNT, DURATION_SECONDS) VALUES "
        f"('{_escape_sql_string(run_id)}', CURRENT_TIMESTAMP(), 'COMPLETED', "
        f"{pipelines_scanned}, {total_findings}, "
        f"{critical_count}, {high_count}, {medium_count}, "
        f"{round(duration_seconds, 1)});"
    )

    prompt = f"Use the sql_execute tool to run this SQL:\n\n{insert_sql}"

    log.info("Writing run log entry for %s ...", run_id)
    try:
        await asyncio.wait_for(
            _collect_response(query(prompt=prompt, options=options)),
            timeout=60,
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

    # Phase 2: Fetch requirements
    requirements_text = await fetch_requirements(connection)

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
    await write_findings_to_snowflake(all_findings, connection, args.force)

    # Phase 5: Write run log
    await write_run_log(
        run_id=run_id,
        pipelines_scanned=len(pipelines),
        total_findings=len(all_findings),
        critical_count=critical,
        high_count=high,
        medium_count=medium,
        duration_seconds=duration,
        connection=connection,
    )

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
