"""
refresh/orchestrator.py — Refresh/update workflow for existing Semantic Views.

Detects schema changes on SV member tables and applies incremental updates.

Phases:
    R1  Change Detection   (parallel per SV)
    R2  Impact Analysis    (sequential — builds global change manifest)
        Optional HITL checkpoint for breaking changes
    R3  Incremental Update (parallel per affected SV)
    R4  Re-evaluation      (parallel per updated SV)
    R5  Refresh Report

Usage:
    python refresh/orchestrator.py --config config.yaml --sv-registry runs/run_XXXX/creation_summary_report.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import yaml

# Add parent to path so tools/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from cortex_code_agent_sdk import AssistantMessage, CortexCodeAgentOptions, ResultMessage, query
    from tools.mcp_server import build_mcp_server
except ImportError as exc:
    print(f"ERROR: Missing dependency — {exc}")
    sys.exit(1)

# Reuse helpers from parent orchestrator
from orchestrator import (  # type: ignore[import]
    Config,
    HITLGate,
    RunLog,
    _collect_response,
    _extract_json,
    _is_valid_json,
    _make_options,
    _run_agent_with_retry,
    generate_evaluation_report,
    phase5_evaluation,
    phase6_cortex_search,
    phase8_summary_report,
)

BASE_DIR = Path(__file__).parent.parent
log = logging.getLogger("sv_refresh")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

# ---------------------------------------------------------------------------
# Phase R1 — Change Detection
# ---------------------------------------------------------------------------

SEVERITY = {
    "COLUMN_REMOVED": "breaking",
    "SCHEMA_CHANGE": "additive",
    "DATA_MODIFICATION": "cosmetic",
}


async def phaseR1_change_detection(
    sv_registry: list[dict],
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> dict:
    """Detect changes on member tables for each registered SV."""
    run_log.phase_start("R1_change_detection")
    sem = asyncio.Semaphore(config.max_concurrent_agents)
    options = _make_options(config, mcp_server, BASE_DIR / "agents" / "lineage_mapper")

    async def detect_sv(sv: dict) -> dict:
        async with sem:
            sv_name = sv["sv_name"]
            cluster_id = sv.get("cluster_id", sv_name)
            tables = sv.get("tables", [])

            changes_per_table: dict[str, list[dict]] = {}
            for table_fqn in tables:
                parts = table_fqn.split(".")
                if len(parts) != 3:
                    continue
                db, schema, table = parts

                prompt = f"""Detect recent changes on table {table_fqn}.

Call `get_refresh_change_detection` with:
  connection="{config.connection}", database="{db}", schema="{schema}",
  table="{table}", lookback_days={config.query_history_lookback_days}

Return JSON: {{"table": "{table_fqn}", "changes": [<rows from tool>]}}"""

                result = await _run_agent_with_retry(
                    prompt=prompt,
                    options=options,
                    max_retries=0,
                    phase="R1_change_detection",
                    item=table_fqn,
                    run_log=run_log,
                )
                if result:
                    try:
                        data = json.loads(_extract_json(result))
                        changes_per_table[table_fqn] = data.get("changes", [])
                    except Exception:
                        changes_per_table[table_fqn] = []

            # Classify severity
            has_breaking = any(
                SEVERITY.get(c.get("CHANGE_TYPE", c.get("change_type", "")), "cosmetic") == "breaking"
                for changes in changes_per_table.values()
                for c in changes
            )

            return {
                "sv_name": sv_name,
                "cluster_id": cluster_id,
                "yaml_path": sv.get("yaml_path", ""),
                "tables": tables,
                "changes_per_table": changes_per_table,
                "has_breaking_changes": has_breaking,
                "total_changes": sum(len(v) for v in changes_per_table.values()),
            }

    tasks = [detect_sv(sv) for sv in sv_registry]
    results = await asyncio.gather(*tasks)
    manifest = list(results)

    out_path = run_dir / "change_manifest.json"
    out_path.write_text(json.dumps(manifest, indent=2))
    breaking_count = sum(1 for m in manifest if m["has_breaking_changes"])
    log.info(
        "Phase R1: %d SVs checked, %d with breaking changes → %s",
        len(manifest), breaking_count, out_path,
    )
    run_log.phase_end("R1_change_detection")
    return {"manifest": manifest, "breaking_count": breaking_count}


# ---------------------------------------------------------------------------
# Phase R2 — Impact Analysis
# ---------------------------------------------------------------------------

async def phaseR2_impact_analysis(
    change_manifest: dict,
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
    hitl: HITLGate,
) -> list[dict]:
    """Assess downstream impact and classify changes. HITL gate for breaking changes."""
    run_log.phase_start("R2_impact_analysis")
    options = _make_options(config, mcp_server, BASE_DIR / "agents" / "lineage_mapper")
    affected: list[dict] = []

    for sv_entry in change_manifest["manifest"]:
        if sv_entry["total_changes"] == 0:
            continue

        sv_name = sv_entry["sv_name"]
        changes_summary: list[dict] = []

        for table_fqn, changes in sv_entry["changes_per_table"].items():
            parts = table_fqn.split(".")
            if len(parts) != 3:
                continue
            db, schema, table = parts

            prompt = f"""Analyze the downstream impact of changes on {table_fqn}.

Changes detected:
{json.dumps(changes, indent=2)}

Call `get_impact_analysis_users` with:
  connection="{config.connection}", database="{db}", schema="{schema}",
  table="{table}", lookback_days={config.query_history_lookback_days}

Classify each change:
- "breaking": join key or required column removed, or data type changed incompatibly
- "additive": new column or table added
- "cosmetic": description, comment, or metadata change

Return JSON:
{{
  "table": "{table_fqn}",
  "sv_name": "{sv_name}",
  "classified_changes": [{{"change_type": "...", "severity": "breaking|additive|cosmetic",
    "detail": "...", "affected_users": []}}]
}}"""

            result = await _run_agent_with_retry(
                prompt=prompt,
                options=options,
                max_retries=0,
                phase="R2_impact_analysis",
                item=table_fqn,
                run_log=run_log,
            )
            if result:
                try:
                    data = json.loads(_extract_json(result))
                    changes_summary.extend(data.get("classified_changes", []))
                except Exception:
                    pass

        breaking = [c for c in changes_summary if c.get("severity") == "breaking"]
        additive = [c for c in changes_summary if c.get("severity") == "additive"]

        affected.append({
            "sv_name": sv_name,
            "yaml_path": sv_entry["yaml_path"],
            "cluster_id": sv_entry["cluster_id"],
            "tables": sv_entry["tables"],
            "breaking_changes": breaking,
            "additive_changes": additive,
            "has_breaking": len(breaking) > 0,
        })

    out_path = run_dir / "impact_report.json"
    out_path.write_text(json.dumps(affected, indent=2))

    # HITL gate for breaking changes
    if any(sv["has_breaking"] for sv in affected):
        await hitl.checkpoint(
            "on_breaking_refresh_change",
            out_path,
            f"{sum(1 for s in affected if s['has_breaking'])} SVs have breaking changes. Review before proceeding.",
        )

    run_log.phase_end("R2_impact_analysis")
    return affected


# ---------------------------------------------------------------------------
# Phase R3 — Incremental Update
# ---------------------------------------------------------------------------

async def phaseR3_incremental_update(
    affected_svs: list[dict],
    config: Config,
    run_dir: Path,
    mcp_server,
    run_log: RunLog,
) -> list[dict]:
    """Apply additive changes; quarantine breaking changes for human review."""
    run_log.phase_start("R3_incremental_update")
    sem = asyncio.Semaphore(config.max_concurrent_agents)
    options = _make_options(config, mcp_server, BASE_DIR / "agents" / "sv_creator")
    updated: list[dict] = []

    async def update_sv(sv: dict) -> dict | None:
        async with sem:
            sv_name = sv["sv_name"]

            # Breaking changes: never auto-apply
            if sv["has_breaking"]:
                run_log.quarantine(
                    "R3_incremental_update", sv_name,
                    f"Breaking changes detected: {[c['detail'] for c in sv['breaking_changes']]}",
                )
                return None

            if not sv["additive_changes"]:
                return None  # Nothing to update

            prompt = f"""Update the semantic view YAML for additive schema changes.

Semantic view: {sv_name}
YAML path: {sv['yaml_path']}
Connection: {config.connection}
Max self-heal retries: {config.max_self_heal_retries}

Additive changes to apply:
{json.dumps(sv['additive_changes'], indent=2)}

Steps:
1. Read the current YAML at {sv['yaml_path']}
2. For each additive change (new column / new table):
   - If new column: add it as a dimension or fact in the appropriate table section
   - If new table: call get_table_ddl and get_table_lineage, then add a new table section
3. Write the updated YAML back to {sv['yaml_path']}
4. Call run_cortex_reflect — self-heal up to {config.max_self_heal_retries} times if errors

Return JSON: {{"status": "success|quarantined", "sv_name": "{sv_name}", "changes_applied": N}}"""

            result = await _run_agent_with_retry(
                prompt=prompt,
                options=options,
                max_retries=0,
                phase="R3_incremental_update",
                item=sv_name,
                run_log=run_log,
            )
            if result:
                try:
                    data = json.loads(_extract_json(result))
                    if data.get("status") == "success":
                        return {"sv_name": sv_name, "yaml_path": sv["yaml_path"],
                                "cluster_id": sv["cluster_id"], "tables": sv["tables"]}
                except Exception:
                    pass
            run_log.quarantine("R3_incremental_update", sv_name, "Update agent failed")
            return None

    tasks = [update_sv(sv) for sv in affected_svs]
    results = await asyncio.gather(*tasks)
    updated = [r for r in results if r is not None]
    log.info("Phase R3: %d SVs updated, %d quarantined", len(updated),
             sum(1 for sv in affected_svs if sv["has_breaking"]))
    run_log.phase_end("R3_incremental_update")
    return updated


# ---------------------------------------------------------------------------
# Phase R5 — Refresh Report
# ---------------------------------------------------------------------------

def phaseR5_refresh_report(
    change_manifest: dict,
    affected_svs: list[dict],
    updated_svs: list[dict],
    sv_scores: list[dict],
    run_log: RunLog,
    run_dir: Path,
) -> None:
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "summary": {
            "total_svs_checked": len(change_manifest["manifest"]),
            "svs_with_changes": len(affected_svs),
            "svs_updated": len(updated_svs),
            "svs_quarantined_breaking": sum(1 for sv in affected_svs if sv["has_breaking"]),
            "total_errors": len(run_log.errors),
        },
        "quarantined_breaking": [
            sv for sv in affected_svs if sv["has_breaking"]
        ],
        "updated_svs": updated_svs,
        "evaluation_scores": sv_scores,
        "errors": run_log.errors,
    }
    out_path = run_dir / "refresh_report.json"
    out_path.write_text(json.dumps(report, indent=2))

    print("\n" + "=" * 60)
    print("REFRESH WORKFLOW COMPLETE")
    print("=" * 60)
    s = report["summary"]
    print(f"  SVs checked:       {s['total_svs_checked']}")
    print(f"  With changes:      {s['svs_with_changes']}")
    print(f"  Updated:           {s['svs_updated']}")
    print(f"  Quarantined:       {s['svs_quarantined_breaking']} (breaking — human action required)")
    print(f"  Report:            {out_path}")
    print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(config: Config, sv_registry_path: str) -> None:
    with open(sv_registry_path) as f:
        registry_data = json.load(f)

    # sv_registry is the sv_details list from creation_summary_report.json
    sv_registry = registry_data.get("sv_details", [])
    if not sv_registry:
        log.error("No SV registry found in %s", sv_registry_path)
        return

    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = BASE_DIR / "runs" / f"refresh_{run_ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    run_log = RunLog(run_dir)
    hitl = HITLGate(config, run_dir)
    mcp_server = build_mcp_server()

    log.info("Refresh run: %s | %d SVs in registry", run_dir, len(sv_registry))

    # Phase R1
    change_manifest = await phaseR1_change_detection(sv_registry, config, run_dir, mcp_server, run_log)

    if change_manifest["breaking_count"] == 0 and all(
        m["total_changes"] == 0 for m in change_manifest["manifest"]
    ):
        log.info("No changes detected. All SVs are up to date.")
        phaseR5_refresh_report(change_manifest, [], [], [], run_log, run_dir)
        return

    # Phase R2
    affected_svs = await phaseR2_impact_analysis(
        change_manifest, config, run_dir, mcp_server, run_log, hitl
    )

    # Phase R3
    updated_svs = await phaseR3_incremental_update(affected_svs, config, run_dir, mcp_server, run_log)

    # Phase R4 — re-evaluate updated SVs
    cluster_plan_stub = {
        "clusters": [
            {"cluster_id": sv["cluster_id"], "tables": sv.get("tables", [])}
            for sv in updated_svs
        ]
    }
    sv_scores: list[dict] = []
    if updated_svs:
        sv_scores = await phase5_evaluation(
            updated_svs, cluster_plan_stub, config, run_dir, mcp_server, run_log
        )

    # Phase R5
    phaseR5_refresh_report(change_manifest, affected_svs, updated_svs, sv_scores, run_log, run_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SV Automation Refresh Orchestrator")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--sv-registry", required=True,
                        help="Path to creation_summary_report.json from a previous creation run")
    parser.add_argument("--mode", choices=["automated", "hitl"])
    args = parser.parse_args()

    cfg = Config.from_yaml(args.config)
    if args.mode:
        cfg.mode = args.mode

    asyncio.run(main(cfg, args.sv_registry))
