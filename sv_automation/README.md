# end-to-end-sv
# SV Automation Skill

Automates creation and refresh of Snowflake Semantic Views (SVs) by mining query history,
clustering related tables, generating SV YAML via `SYSTEM$CORTEX_ANALYST_FAST_GENERATION`,
evaluating accuracy with Cortex Analyst, and integrating Cortex Search Services.

## How to invoke

```bash
cd sv_automation
python orchestrator.py --config config.yaml
```

### CLI overrides

| Flag | Description |
|------|-------------|
| `--scope-database DB` | Run against a specific database |
| `--scope-schema DB.SCHEMA` | Run against a single schema |
| `--mode automated\|hitl` | Override execution mode from config |

## Execution modes

**`automated`** — Runs all phases without stopping. Best for scheduled/batch use.

**`hitl`** (human-in-the-loop) — Pauses at two checkpoints:
1. **post_plan** — After clustering + lineage. Review `assessment_report.html`, then:
   - Approve: `touch runs/<run>/post_plan.approved`
   - Reject:  `touch runs/<run>/post_plan.rejected`
2. **post_evaluation** — After SV accuracy scoring. Review `evaluation_report.html`, then:
   - Approve: `touch runs/<run>/post_evaluation.approved`
   - Reject:  `touch runs/<run>/post_evaluation.rejected`

## Phases (creation workflow)

| Phase | Description |
|-------|-------------|
| 1 — Context Mining | Mines `ACCOUNT_USAGE.ACCESS_HISTORY` and `QUERY_HISTORY` for tag, warehouse, join, and user/role affinity per table. Tables are batched (default 100/shard) and processed in parallel. |
| 2 — Clustering | Groups tables into semantic clusters using join co-occurrence and tag/warehouse affinity. Enforces `max_tables_per_sv` hard cap by splitting at weakest join edges. |
| 3 — Lineage + Plan | Fetches DDL and upstream lineage for each cluster. Identifies join keys and recommends SV names. Generates `assessment_report.html`. |
| 4 — SV Creation | Calls `SYSTEM$CORTEX_ANALYST_FAST_GENERATION` to produce SV YAML, validates with `cortex reflect`, and self-heals up to `max_self_heal_retries` times per SV. |
| 5 — Evaluation | Samples historical SQL queries and tests Cortex Analyst accuracy against the generated SVs. Produces `evaluation_report.html`. |
| 6 — Cortex Search | Identifies high-cardinality VARCHAR columns and creates Cortex Search Services for point-lookup enrichment. Updates SV YAML with CSS references. |
| 7 — Search Testing | Validates each CSS with synthetic point-lookup questions via Cortex Analyst. |
| 8 — Summary Report | Writes `creation_summary_report.json` with totals, quarantined SVs, self-heal stats, and accuracy scores. |

## Refresh workflow

```bash
python refresh/orchestrator.py --config config.yaml --sv-registry runs/<run>/creation_summary_report.json
```

| Phase | Description |
|-------|-------------|
| R1 — Change Detection | Scans for schema/column/data changes affecting tables in each SV. |
| R2 — Impact Analysis | Classifies changes as breaking / additive / cosmetic. Breaking changes are always quarantined for manual review. |
| R3 — Incremental Update | Re-runs SV creator agent (with self-healing) for additive/cosmetic changes. |
| R4 — Re-evaluation | Re-runs accuracy evaluation on updated SVs. |
| R5 — Refresh Report | Summary of what changed, what was updated, and what needs manual attention. |

## Output structure

```
runs/run_YYYYMMDD_HHMMSS/
  phase1_context.json          # Merged query history context
  cluster_plan.json            # Cluster assignments + confidence
  lineage_<cluster>.json       # Per-cluster DDL + lineage
  assessment_report.html       # HITL Checkpoint 1 review doc
  semantic_views/
    <sv_name>.yaml             # Generated + validated SV YAML
  sv_scores.json               # Per-SV accuracy scores
  evaluation_report.html       # HITL Checkpoint 2 review doc
  cortex_search_manual_action.json  # Columns needing manual change-tracking
  search_test_failures.json    # Failed point-lookup tests (if any)
  creation_summary_report.json # Final summary
  run_log.json                 # Errors, quarantined items, self-heal stats
```

## Configuration

Key settings in `config.yaml`:

```yaml
scope:
  level: database          # account | database | schema
  database: MY_DB
  schema: null             # set for schema-level scope

mode: hitl                 # automated | hitl

thresholds:
  max_tables_per_sv: 15
  min_accuracy_score: 70
  cortex_search_min_distinct: 100

self_healing:
  max_retries: 3

batching:
  table_batch_size: 100
  max_concurrent_agents: 10

snowflake:
  connection: MY_DEMO
  target_database: null    # defaults to source database
  target_schema: PUBLIC
```
