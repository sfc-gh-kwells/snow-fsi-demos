"""
orchestrator.py — Main asyncio coordinator for the SV Automation creation workflow.

Phases:
    1  Context Mining       (parallel shards via query())
    2  Clustering           (single query() with retry)
    3  Lineage + Plan       (parallel per cluster via query())
       HITL Checkpoint 1
    4  SV Creation          (parallel per cluster via query(), self-healing validate loop)
    5  Evaluation           (parallel per SV via query())
       HITL Checkpoint 2
    6  Cortex Search        (parallel per SV / column via query())
    7  Point Lookup Testing (parallel per SV via query())
    8  Summary Report       (deterministic Python)

Usage:
    python orchestrator.py --config config.yaml [--scope-database MYDB] [--mode hitl]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

try:
    from cortex_code_agent_sdk import (
        AssistantMessage,
        CortexCodeAgentOptions,
        ResultMessage,
        query,
    )
    from tools.mcp_server import build_mcp_server
except ImportError as exc:
    print(f"ERROR: Missing dependency — {exc}")
    print("Run: pip install -r requirements.txt")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sv_orchestrator")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).parent


@dataclass
class Config:
    scope_level: str = "database"
    database: str = ""
    schema: str = ""
    mode: str = "automated"
    hitl_checkpoints: list[str] = field(
        default_factory=lambda: ["post_plan", "post_evaluation"])
    max_tables_per_sv: int = 15
    min_accuracy_score: int = 70
    cortex_search_min_distinct: int = 50
    query_history_lookback_days: int = 90
    min_eval_questions_per_table: int = 5
    min_join_eval_questions: int = 10
    min_query_length: int = 50
    max_self_heal_retries: int = 3
    max_orchestrator_retries: int = 1
    table_batch_size: int = 100
    max_concurrent_agents: int = 10
    agent_timeout_seconds: int = 900
    # exclude tables with fewer queries (0 = no filter)
    min_table_queries: int = 0
    connection: str = "MY_DEMO"
    role: str = ""
    model: str = "auto"
    target_database: str = ""
    target_schema: str = "PUBLIC"

    @classmethod
    def from_yaml(cls, path: str) -> "Config":
        with open(path) as f:
            raw = yaml.safe_load(f)
        c = cls()
        scope = raw.get("scope", {})
        c.scope_level = scope.get("level", "database")
        c.database = scope.get("database", "")
        c.schema = scope.get("schema", "")
        m = raw.get("mode", {})
        c.mode = m.get("type", "automated")
        c.hitl_checkpoints = m.get("hitl_checkpoints", c.hitl_checkpoints)
        t = raw.get("thresholds", {})
        c.max_tables_per_sv = t.get("max_tables_per_sv", 15)
        c.min_accuracy_score = t.get("min_accuracy_score", 70)
        c.cortex_search_min_distinct = t.get("cortex_search_min_distinct", 50)
        c.query_history_lookback_days = t.get(
            "query_history_lookback_days", 90)
        c.min_eval_questions_per_table = t.get(
            "min_eval_questions_per_table", 5)
        c.min_join_eval_questions = t.get("min_join_eval_questions", 10)
        c.min_query_length = t.get("min_query_length", 50)
        sh = raw.get("self_healing", {})
        c.max_self_heal_retries = sh.get("max_self_heal_retries", 3)
        c.max_orchestrator_retries = sh.get("max_orchestrator_retries", 1)
        b = raw.get("batching", {})
        c.table_batch_size = b.get("table_batch_size", 100)
        c.max_concurrent_agents = b.get("max_concurrent_agents", 10)
        c.agent_timeout_seconds = b.get("agent_timeout_seconds", 900)
        c.min_table_queries = t.get("min_table_queries", 0)
        sf = raw.get("snowflake", {})
        c.connection = sf.get("connection", "MY_DEMO")
        c.role = sf.get("role", "")
        c.model = sf.get("model", "auto")
        c.target_database = sf.get("target_database", "") or c.database
        c.target_schema = sf.get("target_schema", "PUBLIC")
        return c

    def apply_overrides(self, args: argparse.Namespace) -> None:
        if getattr(args, "scope_database", None):
            self.scope_level = "database"
            self.database = args.scope_database
        if getattr(args, "scope_schema", None):
            self.scope_level = "schema"
            db, _, schema = args.scope_schema.partition(".")
            self.database = db
            self.schema = schema
        if getattr(args, "mode", None):
            self.mode = args.mode


# ---------------------------------------------------------------------------
# Run log
# ---------------------------------------------------------------------------

class RunLog:
    def __init__(self, run_dir: Path, run_id: str = "") -> None:
        self.run_dir = run_dir
        self.run_id = run_id
        self.errors: list[dict] = []
        self.self_heal_stats: dict[str, int] = {}
        self.phase_timings: dict[str, str] = {}
        self.quarantined: list[dict] = []
        self.agent_calls: list[dict] = []
        self._jsonl_path = run_dir / "pipeline.log.jsonl"
        self._phase_start_mono: dict[str, float] = {}
        self._agent_start_mono: dict[str, float] = {}

    # ------------------------------------------------------------------
    # JSONL event stream
    # ------------------------------------------------------------------

    def emit(self, event: str, **fields) -> None:
        """Append one structured JSON line to pipeline.log.jsonl."""
        record = {"ts": datetime.utcnow().isoformat(
        ), "run_id": self.run_id, "event": event, **fields}
        with open(self._jsonl_path, "a") as fh:
            fh.write(json.dumps(record) + "\n")

    def agent_call_start(self, phase: str, item: str) -> None:
        key = f"{phase}:{item}"
        self._agent_start_mono[key] = time.monotonic()
        self.emit("agent_call_start", phase=phase, item=item)

    def agent_call_end(self, phase: str, item: str, status: str, duration_ms: float) -> None:
        record = {"phase": phase, "item": item,
                  "status": status, "duration_ms": round(duration_ms)}
        self.agent_calls.append(record)
        self.emit("agent_call_end", **record)

    def log_enrichment(self, sv_name: str, summary: dict, reflect_attempts: int = 0) -> None:
        self.emit("enrichment", sv_name=sv_name,
                  reflect_attempts=reflect_attempts, **summary)

    def log_sv_score(self, sv_name: str, score: dict) -> None:
        self.emit(
            "sv_score",
            sv_name=sv_name,
            accuracy_score=score.get("accuracy_score", 0),
            passed=score.get("pass", 0),
            partial=score.get("partial", 0),
            failed=score.get("fail", 0),
            needs_human_validation=score.get("needs_human_validation", False),
        )

    # ------------------------------------------------------------------
    # Existing methods (now also emit JSONL events)
    # ------------------------------------------------------------------

    def error(self, phase: str, item: str, error: str, action: str) -> None:
        entry = {
            "phase": phase, "item": item,
            "error": error, "action": action,
            "ts": datetime.utcnow().isoformat(),
        }
        self.errors.append(entry)
        log.warning("[%s] %s — %s (%s)", phase, item, error, action)
        self.emit("error", phase=phase, item=item, error=error, action=action)
        self._save()

    def quarantine(self, phase: str, item: str, reason: str) -> None:
        entry = {"phase": phase, "item": item, "reason": reason,
                 "ts": datetime.utcnow().isoformat()}
        self.quarantined.append(entry)
        log.error("[%s] QUARANTINED %s: %s", phase, item, reason)
        self.emit("quarantine", phase=phase, item=item, reason=reason)
        self._save()

    def self_heal(self, sv_name: str, attempts: int) -> None:
        self.self_heal_stats[sv_name] = attempts
        self._save()

    def phase_start(self, phase: str) -> None:
        self._phase_start_mono[phase] = time.monotonic()
        self.phase_timings[f"{phase}_start"] = datetime.utcnow().isoformat()
        log.info("=== Phase %s started ===", phase)
        self.emit("phase_start", phase=phase)

    def phase_end(self, phase: str) -> None:
        duration_ms = round(
            (time.monotonic() - self._phase_start_mono.get(phase, time.monotonic())) * 1000)
        self.phase_timings[f"{phase}_end"] = datetime.utcnow().isoformat()
        log.info("=== Phase %s complete ===", phase)
        self.emit("phase_end", phase=phase, duration_ms=duration_ms)

    def _save(self) -> None:
        path = self.run_dir / "run_log.json"
        with open(path, "w") as f:
            json.dump(
                {
                    "run_id": self.run_id,
                    "errors": self.errors,
                    "quarantined": self.quarantined,
                    "self_heal_stats": self.self_heal_stats,
                    "phase_timings": self.phase_timings,
                },
                f,
                indent=2,
            )


# ---------------------------------------------------------------------------
# HITL gate
# ---------------------------------------------------------------------------

class HITLGate:
    def __init__(self, config: Config, run_dir: Path) -> None:
        self.config = config
        self.run_dir = run_dir
        self._gate_dir = run_dir / "checkpoints"
        self._gate_dir.mkdir(exist_ok=True)

    async def checkpoint(self, checkpoint_id: str, report_path: Path, summary: str) -> None:
        if self.config.mode != "hitl" or checkpoint_id not in self.config.hitl_checkpoints:
            return
        approved = self._gate_dir / f"{checkpoint_id}.approved"
        rejected = self._gate_dir / f"{checkpoint_id}.rejected"
        bar = "=" * 60
        print(f"\n{bar}")
        print(f"[CHECKPOINT] {checkpoint_id}")
        print(f"[CHECKPOINT] Report:  {report_path}")
        print(f"[CHECKPOINT] Summary: {summary}")
        print(f"[CHECKPOINT] Approve: touch {approved}")
        print(f"[CHECKPOINT] Reject:  touch {rejected}")
        print(f"{bar}\n")
        while True:
            if approved.exists():
                log.info("Checkpoint %s approved.", checkpoint_id)
                return
            if rejected.exists():
                raise RuntimeError(
                    f"Checkpoint '{checkpoint_id}' rejected by user.")
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Agent runner helpers
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
            # Explicitly close the generator in the current task to avoid
            # anyio cancel-scope errors during GC-driven cleanup.
            await msg_iter.aclose()
            break
    return "".join(text_parts)


def _make_options(config: Config, mcp_server, cwd: Path) -> CortexCodeAgentOptions:
    return CortexCodeAgentOptions(
        cwd=str(cwd),
        connection=config.connection,
        model=config.model,
        dangerously_allow_all_tool_calls=True,
        mcp_servers={"sv-tools": mcp_server},
        # don't inherit user/project settings — prevents Glean MCP auth in headless runs
        setting_sources=[],
    )


def _load_agent_context(agent_dir: Path) -> str:
    """Read agent.md from the agent's directory and return as a system context prefix."""
    agent_md = agent_dir / "agent.md"
    if agent_md.exists():
        return agent_md.read_text().strip() + "\n\n---\n\n"
    return ""


async def _run_agent_with_retry(
    prompt: str,
    options: CortexCodeAgentOptions,
    max_retries: int,
    phase: str,
    item: str,
    run_log: RunLog,
    check_fn=None,
    timeout_seconds: int = 900,
) -> str | None:
    """
    Run a single-shot query() agent with up to max_retries retries.
    check_fn(result: str) -> str | None returns an error string if invalid, else None.
    Each attempt is bounded by timeout_seconds; a timeout quarantines the item immediately.
    """
    last_result: str | None = None
    agent_context = _load_agent_context(Path(options.cwd))
    _t0 = time.monotonic()
    run_log.agent_call_start(phase, item)
    for attempt in range(max_retries + 1):
        retry_note = f"\n\nPrevious attempt error:\n{last_result}\n\nFix the issue." if attempt > 0 else ""
        full_prompt = agent_context + prompt + retry_note
        try:
            result = await asyncio.wait_for(
                _collect_response(query(prompt=full_prompt, options=options)),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            msg = f"Agent timed out after {timeout_seconds}s"
            log.error("[%s] %s — %s (attempt %d)",
                      phase, item, msg, attempt + 1)
            run_log.agent_call_end(
                phase, item, "timeout", (time.monotonic() - _t0) * 1000)
            run_log.quarantine(phase, item, msg)
            return None
        except Exception as exc:
            run_log.error(phase, item, str(exc),
                          f"retry {attempt + 1}/{max_retries + 1}")
            if attempt == max_retries:
                run_log.agent_call_end(
                    phase, item, "failed", (time.monotonic() - _t0) * 1000)
                return None
            continue

        if check_fn is None:
            run_log.agent_call_end(
                phase, item, "success", (time.monotonic() - _t0) * 1000)
            return result

        error = check_fn(result)
        if error is None:
            run_log.agent_call_end(
                phase, item, "success", (time.monotonic() - _t0) * 1000)
            return result

        last_result = error
        if attempt < max_retries:
            log.warning("[%s] %s — validation error on attempt %d: %s",
                        phase, item, attempt + 1, error)

    run_log.agent_call_end(phase, item, "failed",
                           (time.monotonic() - _t0) * 1000)
    run_log.quarantine(
        phase, item, f"Failed after {max_retries + 1} attempts: {last_result}")
    return None


# ---------------------------------------------------------------------------
# Query-history table filter
# ---------------------------------------------------------------------------

def _get_table_query_counts(
    connection: str,
    database: str,
    schema: str,
    lookback_days: int,
) -> dict[str, int]:
    """Return {TABLE_FQN_UPPER: query_count} from ACCESS_HISTORY over lookback_days.

    Falls back to an empty dict if ACCESS_HISTORY is unavailable (e.g. missing
    SNOWFLAKE.ACCOUNT_USAGE import privilege), in which case no tables are filtered.
    """
    from tools.sql_tools import _run_sql

    fqn_prefix = f"{database.upper()}.{schema.upper()}." if schema else f"{database.upper()}."
    sql = f"""
        SELECT
            UPPER(obj.value:objectName::STRING) AS table_fqn,
            COUNT(*) AS query_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY,
             LATERAL FLATTEN(INPUT => direct_objects_accessed) obj
        WHERE obj.value:objectDomain::STRING IN ('Table', 'View')
          AND query_start_time >= DATEADD('day', -{lookback_days}, CURRENT_TIMESTAMP())
          AND UPPER(obj.value:objectName::STRING) LIKE '{fqn_prefix}%'
        GROUP BY 1
    """
    try:
        rows = _run_sql(connection, sql)
        return {
            (r.get("TABLE_FQN") or r.get("table_fqn", "")).upper(): int(
                r.get("QUERY_COUNT") or r.get("query_count", 0)
            )
            for r in rows
        }
    except Exception as exc:
        log.warning(
            "Could not query ACCESS_HISTORY for query counts (skipping filter): %s", exc
        )
        return {}


# ---------------------------------------------------------------------------
# Phase 1 — Context Mining
# ---------------------------------------------------------------------------

async def phase1_context_mining(
    tables: list[str],
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> dict:
    """Phase 1: run context mining SQL directly in Python — no agent, no timeout risk.

    Q1 (tag/warehouse) and Q3 (user/role affinity) each JOIN ACCESS_HISTORY to
    QUERY_HISTORY. On large accounts this is expensive, so tables are split into
    batches and each batch runs as a concurrent asyncio.to_thread call.

    Q2 (join frequency) requires all tables in a single query to detect cross-table
    co-occurrence pairs; it uses direct_objects_accessed only so no QH join is needed
    and is fast even at full scale.
    """
    run_log.phase_start("1_context_mining")
    log.info(
        "Phase 1: running context SQL for %d tables (direct, no agent) ...", len(tables))

    from tools.sql_tools import _run_sql, _load_sql

    batch_size = config.table_batch_size if hasattr(
        config, "table_batch_size") else 25
    batches = [tables[i:i + batch_size]
               for i in range(0, len(tables), batch_size)]
    lookback = str(config.query_history_lookback_days)
    log.info("Phase 1: %d batches of up to %d tables each",
             len(batches), batch_size)

    def _batch_params(batch: list[str]) -> dict:
        return {
            "table_list": ", ".join(f"'{t}'" for t in batch),
            "lookback_days": lookback,
        }

    # All-tables params (Q2 only — needs cross-table pairs)
    all_params = _batch_params(tables)

    tag_map: dict[str, dict] = {}
    join_freq_list: list[dict] = []
    affinity: dict[str, dict] = {}

    # --- 01: tag / warehouse map — batched, parallel ---
    async def _q1_batch(batch: list[str]) -> list[dict]:
        sql = _load_sql("01_table_tag_warehouse_map.sql", _batch_params(batch))
        return await asyncio.to_thread(_run_sql, config.connection, sql)

    try:
        q1_results = await asyncio.gather(*[_q1_batch(b) for b in batches], return_exceptions=True)
        for i, result in enumerate(q1_results):
            if isinstance(result, Exception):
                log.warning(
                    "Phase 1: Q1 batch %d failed (non-fatal): %s", i, result)
                continue
            for row in result:
                fqn = (row.get("TABLE_FQN") or row.get(
                    "table_fqn", "")).upper()
                tag_map[fqn] = {
                    "tags":          row.get("QUERY_TAGS") or row.get("query_tags", []),
                    "warehouses":    row.get("WAREHOUSE_NAMES") or row.get("warehouse_names", []),
                    "total_queries": int(row.get("TOTAL_QUERIES") or row.get("total_queries", 0)),
                }
        log.info("Phase 1: tag/warehouse map — %d tables", len(tag_map))
    except Exception as exc:
        log.warning("Phase 1: tag/warehouse map failed (non-fatal): %s", exc)
        run_log.error("1_context_mining", "tag_warehouse_map",
                      str(exc), "skipped")

    # --- 02: join frequency — single query, all tables (cross-table pairs) ---
    try:
        sql = _load_sql("02_join_frequency.sql", all_params)
        rows = await asyncio.to_thread(_run_sql, config.connection, sql)
        for row in rows:
            join_freq_list.append({
                "table_a":             row.get("TABLE_A") or row.get("table_a", ""),
                "table_b":             row.get("TABLE_B") or row.get("table_b", ""),
                "co_occurrence_count": int(row.get("CO_OCCURRENCE_COUNT") or row.get("co_occurrence_count", 0)),
            })
        log.info("Phase 1: join frequency — %d pairs", len(join_freq_list))
    except Exception as exc:
        log.warning("Phase 1: join frequency failed (non-fatal): %s", exc)
        run_log.error("1_context_mining", "join_frequency",
                      str(exc), "skipped")

    # --- 03: user / role affinity — batched, parallel ---
    async def _q3_batch(batch: list[str]) -> list[dict]:
        sql = _load_sql("03_user_role_affinity.sql", {
                        **_batch_params(batch), "top_n": "5"})
        return await asyncio.to_thread(_run_sql, config.connection, sql)

    try:
        q3_results = await asyncio.gather(*[_q3_batch(b) for b in batches], return_exceptions=True)
        for i, result in enumerate(q3_results):
            if isinstance(result, Exception):
                log.warning(
                    "Phase 1: Q3 batch %d failed (non-fatal): %s", i, result)
                continue
            for row in result:
                fqn = (row.get("TABLE_FQN") or row.get(
                    "table_fqn", "")).upper()
                affinity[fqn] = {
                    "top_users": row.get("TOP_USERS") or row.get("top_users", []),
                    "top_roles": row.get("TOP_ROLES") or row.get("top_roles", []),
                }
        log.info("Phase 1: user/role affinity — %d tables", len(affinity))
    except Exception as exc:
        log.warning("Phase 1: user/role affinity failed (non-fatal): %s", exc)
        run_log.error("1_context_mining", "user_role_affinity",
                      str(exc), "skipped")

    merged = {
        "tag_warehouse_map": tag_map,
        "join_frequency": join_freq_list,
        "user_role_affinity": affinity,
    }
    out_path = run_dir / "phase1_context.json"
    out_path.write_text(json.dumps(merged, indent=2))
    log.info("Phase 1: context written to %s", out_path)
    run_log.phase_end("1_context_mining")
    return merged


def _merge_phase1_results(shards: list[dict]) -> dict:
    tag_map: dict[str, dict] = {}
    join_freq: dict[tuple, int] = {}
    affinity: dict[str, dict] = {}

    for shard in shards:
        for row in shard.get("tag_warehouse_map", []):
            fqn = row.get("TABLE_FQN") or row.get("table_fqn", "")
            tag_map[fqn] = {"tags": row.get("QUERY_TAGS") or row.get("query_tags", []),
                            "warehouses": row.get("WAREHOUSE_NAMES") or row.get("warehouse_names", []),
                            "total_queries": row.get("TOTAL_QUERIES") or row.get("total_queries", 0)}
        for row in shard.get("join_frequency", []):
            a = row.get("TABLE_A") or row.get("table_a", "")
            b = row.get("TABLE_B") or row.get("table_b", "")
            count = int(row.get("CO_OCCURRENCE_COUNT")
                        or row.get("co_occurrence_count", 0))
            key = (min(a, b), max(a, b))
            join_freq[key] = join_freq.get(key, 0) + count
        for row in shard.get("user_role_affinity", []):
            fqn = row.get("TABLE_FQN") or row.get("table_fqn", "")
            affinity[fqn] = {"top_users": row.get("TOP_USERS") or row.get("top_users", []),
                             "top_roles": row.get("TOP_ROLES") or row.get("top_roles", [])}

    return {
        "tag_warehouse_map": tag_map,
        "join_frequency": [{"table_a": k[0], "table_b": k[1], "co_occurrence_count": v}
                           for k, v in sorted(join_freq.items(), key=lambda x: -x[1])],
        "user_role_affinity": affinity,
    }


# ---------------------------------------------------------------------------
# Phase 2 — Clustering
# ---------------------------------------------------------------------------

def _summarize_context_for_clustering(context: dict, max_join_pairs: int = 200) -> dict:
    """Compress raw phase1_context to a prompt-safe size.

    Raw context can be 9+ MB: query_tags arrays contain thousands of cortex-agent
    UUIDs and full Sigma URLs stored as JSON strings from Snowflake VARIANT columns.
    Returns only what the clustering agent needs: top app sources, top warehouses,
    query volume, top join pairs, and top roles per table.
    """
    def _parse_variant(val):
        if isinstance(val, str):
            try:
                return json.loads(val)
            except Exception:
                return []
        return val if isinstance(val, list) else []

    def _extract_app_sources(tags: list) -> list:
        counts: dict = {}
        for tag in tags:
            if not tag:
                continue
            try:
                t = json.loads(tag) if isinstance(tag, str) else tag
                if isinstance(t, dict):
                    if "app" in t:
                        src = t["app"]
                    elif "StreamlitEngine" in t:
                        src = "streamlit"
                    elif "cortex-agent" in str(t):
                        src = "cortex_agent"
                    else:
                        src = str(list(t.keys())[0])[:40]
                else:
                    src = str(tag)[:40]
            except Exception:
                src = str(tag)[:40]
            counts[src] = counts.get(src, 0) + 1
        return [s for s, _ in sorted(counts.items(), key=lambda x: -x[1])[:5]]

    compact_tag_map = {}
    for fqn, entry in context.get("tag_warehouse_map", {}).items():
        raw_tags = _parse_variant(entry.get("tags", []))
        raw_wh = _parse_variant(entry.get("warehouses", []))
        compact_tag_map[fqn] = {
            "total_queries":   entry.get("total_queries", 0),
            "top_app_sources": _extract_app_sources(raw_tags),
            "top_warehouses":  raw_wh[:3],
        }

    compact_affinity = {}
    for fqn, entry in context.get("user_role_affinity", {}).items():
        raw_roles = _parse_variant(entry.get("top_roles", []))
        compact_affinity[fqn] = [
            r.get("role") for r in raw_roles[:2]
            if isinstance(r, dict) and r.get("role")
        ]

    join_freq = sorted(
        context.get("join_frequency", []),
        key=lambda r: r.get("co_occurrence_count", 0),
        reverse=True,
    )[:max_join_pairs]

    return {
        "table_signals":  compact_tag_map,
        "top_roles":      compact_affinity,
        "join_frequency": join_freq,
    }


def _precompute_table_clusters(
    all_tables: list[str],
    join_frequency: list[dict],
    max_per_cluster: int,
) -> tuple[list[list[str]], list[str]]:
    """Python-based pre-clustering using Union-Find over join co-occurrence pairs.

    Processes join pairs in descending co_occurrence_count order. Two tables are
    merged if the resulting cluster would not exceed max_per_cluster. Tables with
    no join history land in unclustered for the model to handle.

    Returns (candidate_clusters, unclustered_tables).
    """
    parent: dict[str, str] = {t: t for t in all_tables}
    size:   dict[str, int] = {t: 1 for t in all_tables}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> bool:
        ra, rb = find(a), find(b)
        if ra == rb:
            return True
        if size[ra] + size[rb] > max_per_cluster:
            return False
        if size[ra] < size[rb]:
            ra, rb = rb, ra
        parent[rb] = ra
        size[ra] += size[rb]
        return True

    # Only merge tables that are actually in scope
    table_set = {t.upper(): t for t in all_tables}
    pairs_sorted = sorted(join_frequency, key=lambda r: r.get(
        "co_occurrence_count", 0), reverse=True)
    for pair in pairs_sorted:
        a = (pair.get("table_a") or "").upper()
        b = (pair.get("table_b") or "").upper()
        if a in table_set and b in table_set:
            union(table_set[a], table_set[b])

    # Collect groups
    groups: dict[str, list[str]] = {}
    for t in all_tables:
        root = find(t)
        groups.setdefault(root, []).append(t)

    # Tables that were never merged with anyone else are unclustered
    joined_tables = set()
    for pair in pairs_sorted:
        a = (pair.get("table_a") or "").upper()
        b = (pair.get("table_b") or "").upper()
        if a in table_set and b in table_set:
            joined_tables.add(table_set[a])
            joined_tables.add(table_set[b])

    candidate_clusters = []
    unclustered = []
    for root, members in groups.items():
        if len(members) == 1 and members[0] not in joined_tables:
            unclustered.append(members[0])
        else:
            candidate_clusters.append(sorted(members))

    return candidate_clusters, unclustered


async def phase2_clustering(
    context: dict,
    all_tables: list[str],
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> dict | None:
    run_log.phase_start("2_clustering")
    options = _make_options(
        config, mcp_server, BASE_DIR / "agents" / "clusterer")

    def check_clusters(result: str) -> str | None:
        try:
            data = json.loads(_extract_json(result))
            if not data.get("clusters") and not data.get("unclustered_tables"):
                return "Response has no clusters and no unclustered_tables — all tables must be accounted for."
            for cluster in data.get("clusters", []):
                if len(cluster.get("tables", [])) > config.max_tables_per_sv:
                    return (
                        f"Cluster '{cluster.get('cluster_id')}' has "
                        f"{len(cluster['tables'])} tables — max is {config.max_tables_per_sv}. "
                        f"Split it at the weakest join edge."
                    )
            return None
        except Exception as exc:
            return f"Invalid JSON: {exc}"

    compact_context = _summarize_context_for_clustering(context)
    log.info(
        "Phase 2: context compressed — %d table signals, %d join pairs",
        len(compact_context["table_signals"]),
        len(compact_context["join_frequency"]),
    )

    # Python pre-clustering: group tables by join co-occurrence (Union-Find).
    # The model only needs to name/annotate the resulting groups — not assign 232 tables.
    candidate_clusters, unclustered = _precompute_table_clusters(
        all_tables, compact_context["join_frequency"], config.max_tables_per_sv
    )
    log.info(
        "Phase 2: pre-clustered %d tables → %d candidate clusters, %d unclustered",
        len(all_tables), len(candidate_clusters), len(unclustered),
    )

    # Build a compact summary of signals for each candidate cluster (top 3 app sources + roles)
    signals = compact_context["table_signals"]
    roles = compact_context["top_roles"]
    cluster_summaries = []
    for i, members in enumerate(candidate_clusters):
        apps:  dict[str, int] = {}
        whs:   dict[str, int] = {}
        rl:    dict[str, int] = {}
        total = 0
        for t in members:
            sig = signals.get(t, {})
            total += sig.get("total_queries", 0)
            for a in sig.get("top_app_sources", []):
                apps[a] = apps.get(a, 0) + 1
            for w in sig.get("top_warehouses", []):
                whs[w] = whs.get(w, 0) + 1
            for r in roles.get(t, []):
                if r:
                    rl[r] = rl.get(r, 0) + 1
        top_apps = [a for a, _ in sorted(
            apps.items(), key=lambda x: -x[1])[:3]]
        top_whs = [w for w, _ in sorted(whs.items(),  key=lambda x: -x[1])[:2]]
        top_rl = [r for r, _ in sorted(rl.items(),   key=lambda x: -x[1])[:2]]
        cluster_summaries.append({
            "candidate_id": i,
            "tables": members,
            "total_queries": total,
            "top_apps": top_apps,
            "top_warehouses": top_whs,
            "top_roles": top_rl,
        })

    has_history = bool(compact_context["join_frequency"])
    fallback_note = "" if has_history else (
        "\nNOTE: No join history available — candidate clusters are based on naming conventions only. "
        "Use low confidence for all clusters.\n"
    )

    prompt = f"""You are a data architecture expert naming and annotating pre-formed Snowflake table clusters.

Python has already grouped {len(all_tables)} tables into {len(candidate_clusters)} candidate clusters
using join co-occurrence data. Your job is NOT to re-assign tables — only to:
1. Give each cluster a descriptive snake_case name (cluster_id)
2. Set confidence (high/medium/low) and a one-line rationale
3. Add personas (from top_roles), primary_tag (from top_apps), primary_warehouse
4. Flag weak_cohesion_tables (members with zero join history) and ambiguous_tables
5. Assign each unclustered table to the most appropriate cluster OR leave in unclustered_tables
{fallback_note}
Pre-formed candidate clusters:
{json.dumps(cluster_summaries, indent=2)}

Unclustered tables (no join history — assign or leave unclustered):
{json.dumps(unclustered, indent=2)}

Return ONLY a JSON object:
{{
  "clusters": [
    {{
      "cluster_id": "<descriptive_snake_case_name>",
      "tables": ["DB.SCHEMA.TABLE1"],
      "personas": ["role_label"],
      "confidence": "high|medium|low",
      "confidence_rationale": "<one-line: e.g. '6113 co-occurring queries, 4 join pairs; SNOWADHOC warehouse'>",
      "primary_tag": "<app name or null>",
      "primary_warehouse": "<warehouse or null>",
      "split_reason": null,
      "weak_cohesion_tables": [],
      "ambiguous_tables": []
    }}
  ],
  "unclustered_tables": [],
  "total_clusters": 0
}}

Rules:
- KEEP the tables list exactly as given in candidate_id — do not add or remove tables except when assigning unclustered tables.
- STRICT MAXIMUM: {config.max_tables_per_sv} tables per cluster.
- Set total_clusters to the actual count.
- JSON only. No preamble."""

    result = await _run_agent_with_retry(
        prompt=prompt,
        options=options,
        max_retries=config.max_orchestrator_retries,
        phase="2_clustering",
        item="cluster_plan",
        run_log=run_log,
        check_fn=check_clusters,
        timeout_seconds=config.agent_timeout_seconds,
    )
    if result is None:
        return None

    cluster_plan = json.loads(_extract_json(result))
    out_path = run_dir / "cluster_plan.json"
    out_path.write_text(json.dumps(cluster_plan, indent=2))
    log.info("Phase 2: %d clusters → %s",
             cluster_plan.get("total_clusters", 0), out_path)
    run_log.phase_end("2_clustering")
    return cluster_plan


# ---------------------------------------------------------------------------
# Phase 3 — Lineage + Plan
# ---------------------------------------------------------------------------

async def phase3_lineage_and_plan(
    cluster_plan: dict,
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> list[dict]:
    run_log.phase_start("3_lineage_plan")
    sem = asyncio.Semaphore(config.max_concurrent_agents)
    options = _make_options(
        config, mcp_server, BASE_DIR / "agents" / "lineage_mapper")

    async def map_cluster(cluster: dict) -> dict | None:
        async with sem:
            cid = cluster["cluster_id"]
            prompt = f"""You are mapping data lineage for a cluster of Snowflake tables.

Cluster definition:
{json.dumps(cluster, indent=2)}

Instructions:
For EACH table in the cluster:
1. Call `get_table_ddl` with connection="{config.connection}", table_fqn="<table>"
2. Call `get_table_lineage` with connection="{config.connection}", table_fqn="<table>"

Then identify:
- Which tables are base tables (no upstream) vs derived (views, dynamic tables)
- Likely join keys: column names that appear in multiple tables with matching types
  (e.g. ORDER_ID, CUSTOMER_ID, etc.)

Return ONLY a JSON object:
{{
  "cluster_id": "{cid}",
  "table_details": {{
    "DB.SCHEMA.TABLE": {{
      "columns": [{{"name": "COL", "type": "VARCHAR", "nullable": true}}],
      "table_type": "base_table|view|dynamic_table",
      "upstream": [],
      "lineage_depth": 0
    }}
  }},
  "join_keys": [
    {{"table_a": "DB.SCHEMA.T1", "table_b": "DB.SCHEMA.T2", "key_columns": ["ORDER_ID"]}}
  ],
  "recommended_sv_name": "<snake_case_name>",
  "recommended_description": "<one sentence>"
}}

JSON only."""

            result = await _run_agent_with_retry(
                prompt=prompt,
                options=options,
                max_retries=config.max_orchestrator_retries,
                phase="3_lineage_plan",
                item=cid,
                run_log=run_log,
                check_fn=lambda r: None if _is_valid_json(
                    _extract_json(r)) else "Not valid JSON",
                timeout_seconds=config.agent_timeout_seconds,
            )
            if result is None:
                return None
            lineage = json.loads(_extract_json(result))
            out_path = run_dir / f"lineage_{cid}.json"
            out_path.write_text(json.dumps(lineage, indent=2))
            return lineage

    tasks = [map_cluster(c) for c in cluster_plan.get("clusters", [])]
    results = await asyncio.gather(*tasks)
    valid = [r for r in results if r is not None]
    run_log.phase_end("3_lineage_plan")
    return valid


# ---------------------------------------------------------------------------
# Report generation (Jinja)
# ---------------------------------------------------------------------------

def generate_assessment_report(
    cluster_plan: dict,
    lineage_results: list[dict],
    run_dir: Path,
) -> Path:
    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(str(BASE_DIR / "reports")))
    template = env.get_template("assessment_report.html.jinja")
    html = template.render(cluster_plan=cluster_plan, lineage_results=lineage_results,
                           generated_at=datetime.utcnow().isoformat())
    out_path = run_dir / "assessment_report.html"
    out_path.write_text(html)
    log.info("Assessment report: %s", out_path)
    return out_path


def generate_evaluation_report(sv_scores: list[dict], run_dir: Path) -> Path:
    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(str(BASE_DIR / "reports")))
    template = env.get_template("evaluation_report.html.jinja")
    html = template.render(sv_scores=sv_scores,
                           generated_at=datetime.utcnow().isoformat())
    out_path = run_dir / "evaluation_report.html"
    out_path.write_text(html)
    log.info("Evaluation report: %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# Phase 4 — SV Creation + Self-Healing Validation
# ---------------------------------------------------------------------------

def _deploy_semantic_view(connection: str, yaml_path: str, target_schema: str) -> tuple[bool, str]:
    """Call SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML to deploy a validated SV YAML to Snowflake."""
    from tools.sql_tools import _run_sql
    yaml_content = Path(yaml_path).read_text(encoding="utf-8")
    sql = (
        f"CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML("
        f"'{target_schema}', $${yaml_content}$$, FALSE)"
    )
    try:
        rows = _run_sql(connection, sql)
        message = next(iter(rows[0].values()), "") if rows else "No response"
        return True, str(message)
    except Exception as exc:
        return False, str(exc)


async def phase4_sv_creation(
    cluster_plan: dict,
    lineage_results: list[dict],
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> list[dict]:
    run_log.phase_start("4_sv_creation")
    lineage_by_id = {lr["cluster_id"]: lr for lr in lineage_results}
    sem = asyncio.Semaphore(config.max_concurrent_agents)
    options = _make_options(
        config, mcp_server, BASE_DIR / "agents" / "sv_creator")
    sv_dir = run_dir / "semantic_views"
    sv_dir.mkdir(exist_ok=True)

    async def create_sv(cluster: dict) -> dict | None:
        async with sem:
            cid = cluster["cluster_id"]
            lineage = lineage_by_id.get(cid)
            if lineage is None:
                run_log.quarantine("4_sv_creation", cid,
                                   "No lineage data available")
                return None

            sv_name = lineage.get("recommended_sv_name", cid)
            yaml_path = str(sv_dir / f"{sv_name}.yaml")

            prompt = f"""You are creating a high-quality, fully enriched Snowflake Semantic View YAML file.

## Context

**Cluster plan:**
{json.dumps(cluster, indent=2)}

**Lineage and schema analysis:**
{json.dumps(lineage, indent=2)}

**Configuration:**
- Connection: {config.connection}
- Source database: {config.database}
- Source schema: {config.schema}
- Target: {config.target_database or config.database}.{config.target_schema}
- Output YAML path: {yaml_path}
- Max self-heal retries: {config.max_self_heal_retries}

## Steps

### Step 1 — Build tables list
From `lineage.table_details`, build the tables array for FastGen:
```
[{{"database": "...", "schema": "...", "table": "...", "columnNames": [...]}}]
```
Use the exact column names from DDL. All identifiers must be UPPERCASE.

### Step 2 — Collect SQL examples
Use up to 5 SQL queries from `lineage.eval_questions` (if present) as `sql_examples_json`.
These help FastGen generate verified queries. If none available, use `"[]"`.

### Step 3 — Generate base YAML
Call `fast_generate_semantic_view` with:
- connection="{config.connection}"
- sv_name="{sv_name}"
- target_database="{config.target_database or config.database}"
- target_schema="{config.target_schema}"
- tables_json=<JSON array from Step 1>
- sql_examples_json=<from Step 2>
- description="{lineage.get('recommended_description', 'Semantic view for ' + cid)}"
- warehouse="COMPUTE_WH"

### Step 4 — Enrich the YAML (MANDATORY before writing to disk)

Apply all enrichment rules from your agent.md to the generated YAML:

**4a. Descriptions**
- Add a `description:` to every table entry (domain-meaningful, 1–2 sentences)
- Add a `description:` to every dimension, fact, and time_dimension
- Use the lineage data and column names for business context — not just pattern substitution

**4b. Synonyms**
- Add `synonyms:` to every dimension and fact (2–4 business-friendly alternatives)
- Draw on the domain: this is a **{lineage.get('domain_hint', 'enterprise')}** dataset

**4c. Sample values for categorical columns**
For each VARCHAR dimension whose name suggests categorical content (contains _STATUS,
_TYPE, _CODE, _CD, _CATEGORY, _FLAG, _CHANNEL, _CURRENCY, _REGION, _TIER, _SEGMENT):
- Call `get_column_samples` with:
  - connection="{config.connection}"
  - database=<source database from lineage>
  - schema=<source schema from lineage>
  - table=<table name>
  - column_name=<column name>
  - max_samples=10
- Add the returned values as `sample_values:` on that dimension
- Skip if get_column_samples returns an empty list

**4d. Primary key and unique flag**
- For each column listed in `primary_key.columns`, add `unique: true` on that dimension

**4e. access_modifier on facts**
- Every entry under `facts:` must have `access_modifier: public_access`

**4f. Fix misclassifications**
- Integer/numeric columns whose name ends in _ID, _KEY, _NUM, _NBR → move to `dimensions`
  (they are foreign keys, not measures)
- VARCHAR columns that are timestamps/dates → move to `time_dimensions`

**4g. Computed metrics**
Add `metrics:` blocks for meaningful KPIs. For each table:
- If it has monetary fact columns: add `TOTAL_<AMOUNT>` (SUM) and transaction COUNT
- If it has a primary key: add `UNIQUE_<ENTITY>_COUNT` (COUNT DISTINCT)
- All metrics need `access_modifier: public_access`

**4h. Named filters**
Add `filters:` blocks for common business logic based on column patterns:
- STATUS/STATE columns → add an ACTIVE filter
- Date columns → add a LAST_90_DAYS filter
- Flag/alert columns → add a FLAGGED_ONLY filter

**4i. Relationships**
Use `lineage.join_keys` (if present) to define relationships. Follow the structure:
```yaml
relationships:
  - name: <name>
    left_table: <fact_table>
    right_table: <dim_table>
    relationship_columns:
      - left_column: <fk>
        right_column: <pk>
    relationship_type: many_to_one
    join_type: left_outer
```
Never use deprecated types: one_to_many, many_to_many, full_outer, right_outer, cross.

**4j. module_custom_instructions**
Add top-level `module_custom_instructions:` with `sql_generation` and
`question_categorization` guidance based on the domain and table structure.

### Step 5 — Write and validate
1. Write the fully enriched YAML to: {yaml_path}
2. Call `run_cortex_reflect` with yaml_path="{yaml_path}" and target_schema="{config.target_database or config.database}.{config.target_schema}"
   - VALID → done, report success
   - ERRORS_FOUND → fix in-place (do NOT regenerate), overwrite, re-validate
   - If a relationship causes a reflect error: remove it, add a sql_generation hint instead
   - Repeat up to {config.max_self_heal_retries} times
   - If still failing: report quarantined with remaining errors

## Final response

Return a single JSON object (no preamble):
{{
  "status": "success|quarantined",
  "sv_name": "{sv_name}",
  "yaml_path": "{yaml_path}",
  "self_heal_attempts": N,
  "enrichment_summary": {{
    "tables_with_descriptions": N,
    "columns_with_descriptions": N,
    "columns_with_synonyms": N,
    "columns_with_sample_values": N,
    "metrics_added": N,
    "filters_added": N,
    "relationships_added": N
  }}
}}"""

            result = await _run_agent_with_retry(
                prompt=prompt,
                options=options,
                max_retries=0,  # self-healing is handled inside the agent
                phase="4_sv_creation",
                item=cid,
                run_log=run_log,
                timeout_seconds=config.agent_timeout_seconds,
            )
            if result is None:
                return None

            # Parse agent's reported status
            try:
                status_data = json.loads(_extract_json(result))
            except Exception:
                status_data = {"status": "unknown",
                               "sv_name": sv_name, "yaml_path": yaml_path}

            if status_data.get("status") == "success" and Path(yaml_path).exists():
                run_log.self_heal(sv_name, status_data.get(
                    "self_heal_attempts", 0))
                enrichment = status_data.get("enrichment_summary", {})
                run_log.log_enrichment(
                    sv_name, enrichment, status_data.get("self_heal_attempts", 0))
                log.info(
                    "Phase 4: SV %s created at %s | enrichment: %d tables, "
                    "%d cols described, %d synonyms, %d sample_values cols, "
                    "%d metrics, %d filters, %d relationships",
                    sv_name, yaml_path,
                    enrichment.get("tables_with_descriptions", 0),
                    enrichment.get("columns_with_descriptions", 0),
                    enrichment.get("columns_with_synonyms", 0),
                    enrichment.get("columns_with_sample_values", 0),
                    enrichment.get("metrics_added", 0),
                    enrichment.get("filters_added", 0),
                    enrichment.get("relationships_added", 0),
                )
                # Deploy the validated YAML to Snowflake
                target_schema = f"{config.target_database or config.database}.{config.target_schema}"
                deployed, deploy_msg = await asyncio.to_thread(
                    _deploy_semantic_view, config.connection, yaml_path, target_schema
                )
                if deployed:
                    log.info("Phase 4: SV %s deployed to %s",
                             sv_name, target_schema)
                else:
                    log.warning(
                        "Phase 4: SV %s deployment failed: %s", sv_name, deploy_msg)
                return {"cluster_id": cid, "sv_name": sv_name, "yaml_path": yaml_path,
                        "enrichment_summary": enrichment,
                        "deployed": deployed, "deploy_message": deploy_msg}
            else:
                run_log.quarantine("4_sv_creation", cid, str(
                    status_data.get("status", "unknown")))
                return None

    tasks = [create_sv(c) for c in cluster_plan.get("clusters", [])]
    results = await asyncio.gather(*tasks)
    valid = [r for r in results if r is not None]
    log.info("Phase 4: %d SVs created, %d quarantined", len(valid),
             len(cluster_plan.get("clusters", [])) - len(valid))
    run_log.phase_end("4_sv_creation")
    return valid


# ---------------------------------------------------------------------------
# Phase 5 — Evaluation
# ---------------------------------------------------------------------------

def _prefetch_eval_questions(
    connection: str,
    tables: list[str],
    lookback_days: int,
    per_table_limit: int,
    min_query_length: int,
) -> list[dict]:
    """Run 06_eval_questions.sql directly so the agent doesn't burn its timeout on the SQL."""
    from tools.sql_tools import _run_sql, _load_sql
    if not tables:
        return []
    quoted = ", ".join(f"'{t}'" for t in tables)
    sql = _load_sql(
        "06_eval_questions.sql",
        {
            "table_list": quoted,
            "lookback_days": str(lookback_days),
            "per_table_limit": str(per_table_limit),
            "min_query_length": str(min_query_length),
        },
    )
    rows = _run_sql(connection, sql)
    return _normalize_eval_questions(rows)


def _normalize_eval_questions(rows: list[dict]) -> list[dict]:
    """
    Post-process raw eval questions from query history:
    1. Deduplicate near-identical queries by normalizing away string literals and numbers.
    2. Filter out point-lookup queries (WHERE clause is entirely specific IDs/literals).
    3. Sort so JOIN queries and analytical (GROUP BY / aggregate) queries come first.
    """
    import re
    import hashlib

    _LITERAL_RE = re.compile(r"'[^']*'")
    _NUMBER_RE = re.compile(r"\b\d+\b")
    _SFID_RE = re.compile(r"'00[A-Za-z0-9]{13,16}'")  # Salesforce-style IDs
    _ANALYTICAL_KW = re.compile(
        r"\b(GROUP BY|HAVING|COUNT|SUM|AVG|MAX|MIN|PERCENTILE)\b", re.IGNORECASE)

    def _fingerprint(sql: str) -> str:
        norm = _LITERAL_RE.sub("?", sql)
        norm = _NUMBER_RE.sub("N", norm)
        norm = " ".join(norm.lower().split())
        return hashlib.md5(norm.encode()).hexdigest()

    def _is_point_lookup(sql: str) -> bool:
        """True if the query's WHERE clause contains only specific literal IDs with no aggregation."""
        if _ANALYTICAL_KW.search(sql):
            return False
        # Count Salesforce-style ID literals; if they dominate the WHERE clause it's a lookup
        sf_ids = _SFID_RE.findall(sql)
        if sf_ids and "GROUP BY" not in sql.upper():
            # More than 2 specific IDs and no aggregation → point lookup
            return len(sf_ids) >= 2
        return False

    def _interest_score(row: dict) -> int:
        sql = row.get("QUERY_TEXT", row.get("query_text", ""))
        if row.get("QUERY_CATEGORY", row.get("query_category", "")) == "JOIN_QUERY":
            base = 2
        else:
            base = 0
        if _ANALYTICAL_KW.search(sql):
            base += 1
        return base

    seen: set[str] = set()
    deduped: list[dict] = []
    for row in rows:
        sql = row.get("QUERY_TEXT", row.get("query_text", ""))
        if _is_point_lookup(sql):
            continue
        fp = _fingerprint(sql)
        if fp in seen:
            continue
        seen.add(fp)
        deduped.append(row)

    deduped.sort(key=_interest_score, reverse=True)
    return deduped


async def phase5_evaluation(
    sv_results: list[dict],
    cluster_plan: dict,
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> list[dict]:
    run_log.phase_start("5_evaluation")
    clusters_by_id = {c["cluster_id"]                      : c for c in cluster_plan.get("clusters", [])}
    sem = asyncio.Semaphore(config.max_concurrent_agents)
    options = _make_options(
        config, mcp_server, BASE_DIR / "agents" / "evaluator")

    async def evaluate_sv(sv: dict) -> dict:
        async with sem:
            cid = sv["cluster_id"]
            cluster = clusters_by_id.get(cid, {})
            tables = cluster.get("tables", [])

            # Pre-fetch eval questions in the orchestrator so the agent doesn't
            # burn its timeout budget running the expensive ACCESS_HISTORY query.
            try:
                eval_questions = await asyncio.to_thread(
                    _prefetch_eval_questions,
                    config.connection,
                    tables,
                    config.query_history_lookback_days,
                    config.min_eval_questions_per_table * 2,
                    config.min_query_length,
                )
            except Exception as exc:
                log.warning(
                    "Phase 5: eval questions prefetch failed for %s: %s", sv["sv_name"], exc)
                eval_questions = []

            log.info("Phase 5: %s — %d eval questions pre-fetched",
                     sv["sv_name"], len(eval_questions))

            prompt = f"""You are evaluating the accuracy of a Snowflake Semantic View using Cortex Analyst.

Semantic view: {sv['sv_name']}
YAML path: {sv['yaml_path']}
Member tables: {json.dumps(tables)}
Connection: {config.connection}
Min eval questions per table: {config.min_eval_questions_per_table}
Min join eval questions: {config.min_join_eval_questions}

## Pre-fetched eval questions ({len(eval_questions)} total)

{json.dumps(eval_questions, indent=2)}

Step 1 — Quality check:
Count single-table queries (query_category=SINGLE_TABLE) per table and join queries (query_category=JOIN_QUERY) total from the pre-fetched list above.
If fewer than {config.min_eval_questions_per_table} single-table queries per table OR fewer than {config.min_join_eval_questions} join queries overall:
flag as needs_human_validation=true and note which tables are under-represented.

Step 2 — Evaluate (for each query in the pre-fetched list):
Call `call_cortex_analyst` with yaml_path="{sv['yaml_path']}", question=<natural language interpretation of query_text>, connection="{config.connection}"
Then call `run_sql_query` with connection="{config.connection}" and the ORIGINAL query_text to get its result.
Compare results using the scoring rules in your agent.md.

Step 3 — Return JSON:
{{
  "sv_name": "{sv['sv_name']}",
  "cluster_id": "{cid}",
  "yaml_path": "{sv['yaml_path']}",
  "needs_human_validation": false,
  "human_validation_reason": null,
  "total_questions": 0,
  "pass": 0,
  "partial": 0,
  "fail": 0,
  "accuracy_score": 0,
  "question_results": []
}}"""

            result = await _run_agent_with_retry(
                prompt=prompt,
                options=options,
                max_retries=config.max_orchestrator_retries,
                phase="5_evaluation",
                item=sv["sv_name"],
                run_log=run_log,
                check_fn=lambda r: None if _is_valid_json(
                    _extract_json(r)) else "Not valid JSON",
                timeout_seconds=config.agent_timeout_seconds,
            )
            if result is None:
                return {
                    "sv_name": sv["sv_name"], "cluster_id": cid,
                    "yaml_path": sv["yaml_path"],
                    "accuracy_score": 0, "needs_human_validation": True,
                    "human_validation_reason": "Evaluation agent failed",
                    "total_questions": 0, "pass": 0, "partial": 0, "fail": 0,
                    "question_results": [],
                }
            try:
                score_data = json.loads(_extract_json(result))
                run_log.log_sv_score(sv["sv_name"], score_data)
                return score_data
            except Exception:
                return {"sv_name": sv["sv_name"], "cluster_id": cid,
                        "yaml_path": sv["yaml_path"], "accuracy_score": 0,
                        "needs_human_validation": True, "human_validation_reason": "Parse error",
                        "total_questions": 0, "pass": 0, "partial": 0, "fail": 0,
                        "question_results": []}

    tasks = [evaluate_sv(sv) for sv in sv_results]
    scores = await asyncio.gather(*tasks)
    score_list = list(scores)

    # Save scores
    (run_dir / "sv_scores.json").write_text(json.dumps(score_list, indent=2))
    run_log.phase_end("5_evaluation")
    return score_list


# ---------------------------------------------------------------------------
# Phase 6 — Cortex Search Integration
# ---------------------------------------------------------------------------

async def phase6_cortex_search(
    sv_results: list[dict],
    cluster_plan: dict,
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> list[dict]:
    run_log.phase_start("6_cortex_search")
    clusters_by_id = {c["cluster_id"]                      : c for c in cluster_plan.get("clusters", [])}
    sem = asyncio.Semaphore(config.max_concurrent_agents)
    options = _make_options(
        config, mcp_server, BASE_DIR / "agents" / "cortex_search")
    manual_action: list[dict] = []

    async def integrate_sv(sv: dict) -> dict:
        async with sem:
            cid = sv["cluster_id"]
            cluster = clusters_by_id.get(cid, {})
            tables = cluster.get("tables", [])
            # Extract database from first table FQN
            database = tables[0].split(".")[0] if tables else config.database
            table_names_json = json.dumps([t.split(".")[-1] for t in tables])

            prompt = f"""You are identifying and creating Cortex Search Services for a semantic view.

Semantic view: {sv['sv_name']}
YAML path: {sv['yaml_path']}
Tables: {json.dumps(tables)}
Connection: {config.connection}
Min distinct values for CSS candidate: {config.cortex_search_min_distinct}
Target database: {config.target_database or config.database}
Target schema: {config.target_schema}

Step 1 — Find candidates:
Call `get_cortex_search_candidates` with:
- connection="{config.connection}"
- database="{database}"
- table_list_json={json.dumps(json.dumps([t.split('.')[-1] for t in tables]))}
- min_distinct={config.cortex_search_min_distinct}

Step 2 — Confirm cardinality:
For each candidate column returned, call `check_distinct_count` to verify
APPROX_COUNT_DISTINCT >= {config.cortex_search_min_distinct}.
Discard candidates below the threshold.

Step 3 — Create CSS for confirmed candidates:
Call `create_cortex_search_service` for each confirmed column:
- connection="{config.connection}"
- service_name="{config.target_database or config.database}.{config.target_schema}.css_{{sv_name}}_{{column_name}}"
- table_fqn="<full table FQN>"
- column_name="<column>"
- warehouse="COMPUTE_WH"

If status is "failed" with reason "change_tracking_not_enabled":
  Add to manual_action list — do NOT retry.
If status is "created": note success.

Step 4 — Update YAML:
For each successfully created CSS, add a cortex_search_service reference to {sv['yaml_path']}.
After all updates, call `run_cortex_reflect` on {sv['yaml_path']} to validate.
If errors, self-heal up to {config.max_self_heal_retries} times.

Step 5 — Return JSON:
{{
  "sv_name": "{sv['sv_name']}",
  "yaml_path": "{sv['yaml_path']}",
  "css_created": [],
  "css_manual_action": [],
  "css_failed": []
}}"""

            result = await _run_agent_with_retry(
                prompt=prompt,
                options=options,
                max_retries=0,
                phase="6_cortex_search",
                item=sv["sv_name"],
                run_log=run_log,
                timeout_seconds=config.agent_timeout_seconds,
            )
            if result is None:
                return {"sv_name": sv["sv_name"], "yaml_path": sv["yaml_path"],
                        "css_created": [], "css_manual_action": [], "css_failed": []}
            try:
                data = json.loads(_extract_json(result))
                # Accumulate manual action items
                for item in data.get("css_manual_action", []):
                    manual_action.append({"sv": sv["sv_name"], **item})
                return data
            except Exception:
                return {"sv_name": sv["sv_name"], "yaml_path": sv["yaml_path"],
                        "css_created": [], "css_manual_action": [], "css_failed": []}

    tasks = [integrate_sv(sv) for sv in sv_results]
    results = await asyncio.gather(*tasks)

    # Save manual action list
    if manual_action:
        (run_dir / "cortex_search_manual_action.json").write_text(json.dumps(manual_action, indent=2))
        log.warning(
            "Phase 6: %d columns need manual change-tracking setup — see cortex_search_manual_action.json", len(manual_action))

    run_log.phase_end("6_cortex_search")
    return list(results)


# ---------------------------------------------------------------------------
# Phase 7 — Point Lookup Testing
# ---------------------------------------------------------------------------

async def phase7_search_testing(
    css_results: list[dict],
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> None:
    run_log.phase_start("7_search_testing")
    failures: list[dict] = []
    sem = asyncio.Semaphore(config.max_concurrent_agents)
    options = _make_options(
        config, mcp_server, BASE_DIR / "agents" / "evaluator")

    async def test_sv(sv: dict) -> None:
        async with sem:
            css_list = sv.get("css_created", [])
            if not css_list:
                return
            prompt = f"""You are testing point-lookup accuracy for a semantic view with Cortex Search Services.

Semantic view: {sv['sv_name']}
YAML path: {sv['yaml_path']}
Cortex Search Services created: {json.dumps(css_list)}
Connection: {config.connection}

For each CSS column, generate 2 point-lookup questions:
Example: if column is PRODUCT_NAME, question = "Show me orders for product X"
(use an actual value from the column if possible via run_sql_query SELECT DISTINCT)

Then call `call_cortex_analyst` for each question.
Score: pass if Cortex Analyst returns a result; fail if it errors or returns no rows.

Return JSON: {{"sv_name": "{sv['sv_name']}", "tests": [{{"column": "...", "question": "...", "result": "pass|fail", "note": "..."}}]}}"""

            result = await _run_agent_with_retry(
                prompt=prompt,
                options=options,
                max_retries=0,
                phase="7_search_testing",
                item=sv["sv_name"],
                run_log=run_log,
                timeout_seconds=config.agent_timeout_seconds,
            )
            if result:
                try:
                    data = json.loads(_extract_json(result))
                    for test in data.get("tests", []):
                        if test.get("result") == "fail":
                            failures.append({"sv": sv["sv_name"], **test})
                except Exception:
                    pass

    tasks = [test_sv(sv) for sv in css_results]
    await asyncio.gather(*tasks)

    if failures:
        (run_dir / "search_test_failures.json").write_text(json.dumps(failures, indent=2))
        log.warning(
            "Phase 7: %d point-lookup test failures — see search_test_failures.json", len(failures))
    run_log.phase_end("7_search_testing")


# ---------------------------------------------------------------------------
# Phase 8 — Summary Report
# ---------------------------------------------------------------------------

def phase8_summary_report(
    sv_results: list[dict],
    sv_scores: list[dict],
    css_results: list[dict],
    run_log: RunLog,
    config: Config,
    run_dir: Path,
) -> Path:
    scores_by_name = {s["sv_name"]: s for s in sv_scores}
    css_by_name = {c["sv_name"]: c for c in css_results}

    rows = []
    for sv in sv_results:
        score = scores_by_name.get(sv["sv_name"], {})
        css = css_by_name.get(sv["sv_name"], {})
        rows.append({
            "sv_name": sv["sv_name"],
            "cluster_id": sv["cluster_id"],
            "yaml_path": sv["yaml_path"],
            "accuracy_score": score.get("accuracy_score", 0),
            "needs_human_validation": score.get("needs_human_validation", False),
            "css_created": len(css.get("css_created", [])),
            "css_manual_action": len(css.get("css_manual_action", [])),
        })

    below_threshold = [r for r in rows if r["accuracy_score"]
                       < config.min_accuracy_score]

    report = {
        "run_dir": str(run_dir),
        "generated_at": datetime.utcnow().isoformat(),
        "scope": {"level": config.scope_level, "database": config.database, "schema": config.schema},
        "summary": {
            "total_svs_created": len(sv_results),
            "total_quarantined": len(run_log.quarantined),
            "svs_below_accuracy_threshold": len(below_threshold),
            "total_errors": len(run_log.errors),
        },
        "sv_details": rows,
        "quarantined": run_log.quarantined,
        "errors": run_log.errors,
        "self_heal_stats": run_log.self_heal_stats,
        "cortex_search_manual_action_file": str(run_dir / "cortex_search_manual_action.json")
        if (run_dir / "cortex_search_manual_action.json").exists() else None,
    }

    out_path = run_dir / "creation_summary_report.json"
    out_path.write_text(json.dumps(report, indent=2))

    print("\n" + "=" * 60)
    print("SV AUTOMATION COMPLETE")
    print("=" * 60)
    print(f"  SVs created:      {report['summary']['total_svs_created']}")
    print(f"  Quarantined:      {report['summary']['total_quarantined']}")
    print(
        f"  Below threshold:  {report['summary']['svs_below_accuracy_threshold']}")
    print(f"  Total errors:     {report['summary']['total_errors']}")
    print(f"  Report:           {out_path}")
    print("=" * 60 + "\n")
    return out_path


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _is_valid_json(text: str) -> bool:
    try:
        json.loads(text)
        return True
    except Exception:
        return False


def _extract_json(text: str) -> str:
    """Extract JSON from a string that may have surrounding prose."""
    text = text.strip()
    # Try to find a JSON object or array
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        start = text.find(start_char)
        end = text.rfind(end_char)
        if start != -1 and end != -1 and end > start:
            candidate = text[start:end + 1]
            if _is_valid_json(candidate):
                return candidate
    return text  # return as-is and let caller handle parse errors


# ---------------------------------------------------------------------------
# Run manifest
# ---------------------------------------------------------------------------

def _generate_run_manifest_report(manifest: dict, run_dir: Path) -> Path:
    """Render run_manifest_report.html from run manifest data."""
    from jinja2 import Environment, FileSystemLoader
    env = Environment(loader=FileSystemLoader(str(BASE_DIR / "reports")))
    template = env.get_template("run_manifest_report.html.jinja")
    html = template.render(**manifest)
    out_path = run_dir / "run_manifest_report.html"
    out_path.write_text(html)
    log.info("Run manifest report: %s", out_path)
    return out_path


def _write_run_manifest(
    run_log: RunLog,
    sv_results: list[dict],
    sv_scores: list[dict],
    config: Config,
    run_dir: Path,
    total_duration_ms: float,
) -> None:
    """Write run_manifest.json — comprehensive, machine-readable end-of-run artifact."""
    # Compute per-phase durations from JSONL
    phase_summary: dict[str, dict] = {}
    try:
        with open(run_log._jsonl_path) as fh:
            for line in fh:
                evt = json.loads(line)
                phase = evt.get("phase")
                if not phase:
                    continue
                if evt["event"] == "phase_start":
                    phase_summary.setdefault(phase, {})["start"] = evt["ts"]
                elif evt["event"] == "phase_end":
                    phase_summary.setdefault(phase, {})["end"] = evt["ts"]
                    phase_summary[phase]["duration_ms"] = evt.get(
                        "duration_ms", 0)
    except Exception:
        pass

    # Aggregate tool call stats from JSONL
    tool_stats: dict[str, dict] = {}
    try:
        with open(run_log._jsonl_path) as fh:
            for line in fh:
                evt = json.loads(line)
                if evt["event"] != "tool_call":
                    continue
                tool = evt.get("tool", "unknown")
                if tool not in tool_stats:
                    tool_stats[tool] = {"count": 0, "total_ms": 0}
                tool_stats[tool]["count"] += 1
                tool_stats[tool]["total_ms"] += evt.get("duration_ms", 0)
    except Exception:
        pass

    # Join sv_results (enrichment) with sv_scores (accuracy + question detail)
    scores_by_name = {s["sv_name"]: s for s in sv_scores}
    sv_manifest = []
    for sv in sv_results:
        score = scores_by_name.get(sv["sv_name"], {})
        sv_manifest.append({
            "sv_name":                 sv["sv_name"],
            "cluster_id":              sv["cluster_id"],
            "yaml_path":               sv["yaml_path"],
            "enrichment":              sv.get("enrichment_summary", {}),
            "accuracy_score":          score.get("accuracy_score", 0),
            "needs_human_validation":  score.get("needs_human_validation", False),
            "human_validation_reason": score.get("human_validation_reason", ""),
            "total_questions":         score.get("total_questions", 0),
            "pass":                    score.get("pass", 0),
            "partial":                 score.get("partial", 0),
            "fail":                    score.get("fail", 0),
            "question_results":        score.get("question_results", []),
        })

    manifest = {
        "run_id": run_log.run_id,
        "generated_at": datetime.utcnow().isoformat(),
        "config": {
            "database": config.database,
            "schema": config.schema,
            "target_database": config.target_database,
            "target_schema": config.target_schema,
            "connection": config.connection,
            "model": config.model,
            "agent_timeout_seconds": config.agent_timeout_seconds,
        },
        "phases": phase_summary,
        "agent_calls": run_log.agent_calls,
        "tool_calls": [
            {"tool": t, "count": v["count"], "total_ms": v["total_ms"]}
            for t, v in sorted(tool_stats.items())
        ],
        "svs": sv_manifest,
        "quarantined": run_log.quarantined,
        "errors": run_log.errors,
        "summary": {
            "total_tables": 0,  # filled in by caller
            "filtered_tables": 0,  # filled in by caller
            "svs_created": len(sv_results),
            "quarantined": len(run_log.quarantined),
            "total_errors": len(run_log.errors),
            "total_duration_ms": round(total_duration_ms),
        },
    }
    out = run_dir / "run_manifest.json"
    out.write_text(json.dumps(manifest, indent=2))
    log.info("Run manifest written to %s", out)
    try:
        _generate_run_manifest_report(manifest, run_dir)
    except Exception as exc:
        log.warning("Failed to render run manifest HTML report: %s", exc)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def main(config: Config, resume_dir: Path | None = None) -> None:
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_id = f"run_{run_ts}"

    if resume_dir is not None:
        run_dir = resume_dir.resolve()
        if not run_dir.exists():
            log.error("--resume path does not exist: %s", run_dir)
            return
        log.info("Resuming run from: %s", run_dir)
        run_id = run_dir.name
    else:
        run_dir = BASE_DIR / "runs" / run_id
        run_dir.mkdir(parents=True, exist_ok=True)

    # Add a persistent file log handler for this run
    _fh = logging.FileHandler(run_dir / "run.log")
    _fh.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%H:%M:%S"))
    logging.getLogger().addHandler(_fh)

    run_log = RunLog(run_dir, run_id=run_id)
    _run_start = time.monotonic()
    hitl = HITLGate(config, run_dir)
    mcp_server = build_mcp_server(run_log=run_log)

    log.info("Run directory: %s", run_dir)
    log.info("Scope: %s=%s/%s  Mode: %s", config.scope_level,
             config.database, config.schema, config.mode)

    # Enumerate tables in scope — skipped if resuming and run_tables.json exists
    from tools.sql_tools import _run_sql
    excluded_tables: list[str] = []
    run_tables_path = run_dir / "run_tables.json"

    if run_tables_path.exists():
        all_tables = json.loads(run_tables_path.read_text())
        log.info("Resuming: loaded %d tables from %s",
                 len(all_tables), run_tables_path)
    else:
        schema_filter = f"AND table_schema = '{config.schema}'" if config.schema else ""
        tables_sql = f"""
            SELECT table_catalog || '.' || table_schema || '.' || table_name AS table_fqn
            FROM {config.database}.INFORMATION_SCHEMA.TABLES
            WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
              AND table_type IN ('BASE TABLE', 'VIEW')
              {schema_filter}
            ORDER BY table_schema, table_name
        """
        table_rows = _run_sql(config.connection, tables_sql)
        all_tables = [r.get("TABLE_FQN") or r.get("table_fqn", "")
                      for r in table_rows]
        log.info("Found %d tables in scope", len(all_tables))

        # Query-history-based table filtering
        if config.min_table_queries > 0:
            log.info(
                "Querying ACCESS_HISTORY: excluding tables with < %d queries in last %d days ...",
                config.min_table_queries,
                config.query_history_lookback_days,
            )
            query_counts = _get_table_query_counts(
                config.connection,
                config.database,
                config.schema,
                config.query_history_lookback_days,
            )
            if query_counts:
                excluded_tables = [
                    t for t in all_tables
                    if query_counts.get(t.upper(), 0) < config.min_table_queries
                ]
                if excluded_tables:
                    preview = json.dumps(excluded_tables[:10])
                    suffix = " ..." if len(excluded_tables) > 10 else ""
                    log.info(
                        "Excluding %d tables with < %d queries: %s%s",
                        len(excluded_tables), config.min_table_queries, preview, suffix,
                    )
                    run_log.emit(
                        "table_filter",
                        min_table_queries=config.min_table_queries,
                        excluded_count=len(excluded_tables),
                        excluded_tables=excluded_tables,
                    )
                all_tables = [
                    t for t in all_tables
                    if query_counts.get(t.upper(), 0) >= config.min_table_queries
                ]
                log.info("Tables after query-history filter: %d",
                         len(all_tables))
            else:
                log.warning(
                    "ACCESS_HISTORY returned no rows or was inaccessible — min_table_queries filter skipped."
                )

        # Persist filtered table list so resume can skip this step
        run_tables_path.write_text(json.dumps(all_tables))

    if not all_tables:
        log.error(
            "No tables found in scope. Check database/schema config and connection.")
        return

    run_log.emit(
        "run_start",
        scope={"database": config.database, "schema": config.schema},
        total_tables=len(all_tables),
        config={"connection": config.connection, "model": config.model,
                "agent_timeout_seconds": config.agent_timeout_seconds},
    )

    # Phase 1
    p1_path = run_dir / "phase1_context.json"
    if p1_path.exists():
        log.info("Resuming: loading Phase 1 context from %s", p1_path)
        context = json.loads(p1_path.read_text())
    else:
        context = await phase1_context_mining(all_tables, config, run_dir, mcp_server, run_log)

    # Phase 2
    p2_path = run_dir / "cluster_plan.json"
    if p2_path.exists():
        log.info("Resuming: loading Phase 2 cluster plan from %s", p2_path)
        cluster_plan = json.loads(p2_path.read_text())
    else:
        cluster_plan = await phase2_clustering(context, all_tables, config, run_dir, mcp_server, run_log)
    if cluster_plan is None:
        log.error("Clustering failed — cannot continue.")
        return

    # Phase 3 — each cluster writes lineage_{cid}.json; reconstruct list on resume
    lineage_files = sorted(run_dir.glob("lineage_*.json"))
    if lineage_files:
        log.info("Resuming: loading Phase 3 lineage from %d files",
                 len(lineage_files))
        lineage_results = [json.loads(f.read_text()) for f in lineage_files]
    else:
        lineage_results = await phase3_lineage_and_plan(cluster_plan, config, run_dir, mcp_server, run_log)

    # Assessment report + HITL Checkpoint 1
    report_path = generate_assessment_report(
        cluster_plan, lineage_results, run_dir)
    n_clusters = cluster_plan.get(
        "total_clusters", len(cluster_plan.get("clusters", [])))
    await hitl.checkpoint("post_plan", report_path, f"{n_clusters} clusters identified across {len(all_tables)} tables.")

    # Phase 4
    p4_path = run_dir / "sv_results.json"
    if p4_path.exists():
        log.info("Resuming: loading Phase 4 sv_results from %s", p4_path)
        sv_results = json.loads(p4_path.read_text())
    else:
        sv_results = await phase4_sv_creation(cluster_plan, lineage_results, config, run_dir, mcp_server, run_log)
        if sv_results:
            p4_path.write_text(json.dumps(sv_results, indent=2))
    if not sv_results:
        log.error("No SVs were created — stopping.")
        return

    # Phase 5
    p5_path = run_dir / "sv_scores.json"
    if p5_path.exists():
        log.info("Resuming: loading Phase 5 sv_scores from %s", p5_path)
        sv_scores = json.loads(p5_path.read_text())
    else:
        sv_scores = await phase5_evaluation(sv_results, cluster_plan, config, run_dir, mcp_server, run_log)

    # Evaluation report + HITL Checkpoint 2
    eval_report_path = generate_evaluation_report(sv_scores, run_dir)
    below = [s for s in sv_scores if s.get(
        "accuracy_score", 0) < config.min_accuracy_score]
    await hitl.checkpoint(
        "post_evaluation",
        eval_report_path,
        f"{len(sv_scores)} SVs evaluated. {len(below)} below {config.min_accuracy_score}% accuracy threshold.",
    )

    # Phase 6
    css_results = await phase6_cortex_search(sv_results, cluster_plan, config, run_dir, mcp_server, run_log)

    # Phase 7
    await phase7_search_testing(css_results, config, run_dir, mcp_server, run_log)

    # Phase 8
    phase8_summary_report(sv_results, sv_scores,
                          css_results, run_log, config, run_dir)

    # Emit run_end and write the comprehensive run manifest
    _total_ms = (time.monotonic() - _run_start) * 1000
    run_log.emit(
        "run_end",
        svs_created=len(sv_results),
        quarantined=len(run_log.quarantined),
        total_errors=len(run_log.errors),
        total_duration_ms=round(_total_ms),
    )
    manifest = _write_run_manifest(
        run_log, sv_results, sv_scores, config, run_dir, _total_ms)
    # Patch total_tables / filtered_tables into manifest summary now that we have them
    try:
        _m = json.loads((run_dir / "run_manifest.json").read_text())
        _m["summary"]["total_tables"] = len(all_tables)
        _m["summary"]["filtered_tables"] = len(excluded_tables)
        if excluded_tables:
            _m["excluded_tables"] = excluded_tables
        (run_dir / "run_manifest.json").write_text(json.dumps(_m, indent=2))
    except Exception:
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SV Automation Orchestrator")
    parser.add_argument("--config", default="config.yaml",
                        help="Path to config.yaml")
    parser.add_argument("--scope-database",
                        help="Override: run for this database")
    parser.add_argument("--scope-schema", help="Override: run for DB.SCHEMA")
    parser.add_argument(
        "--mode", choices=["automated", "hitl"], help="Override execution mode")
    parser.add_argument(
        "--resume", metavar="RUN_DIR",
        help="Resume from an existing run directory, skipping phases whose output already exists",
    )
    args = parser.parse_args()

    cfg = Config.from_yaml(args.config)
    cfg.apply_overrides(args)

    if cfg.role:
        from tools.sql_tools import configure_role
        configure_role(cfg.role)

    resume_dir = Path(args.resume) if args.resume else None
    asyncio.run(main(cfg, resume_dir=resume_dir))
