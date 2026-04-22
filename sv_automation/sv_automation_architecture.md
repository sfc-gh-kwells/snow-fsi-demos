# Semantic View Automation Architecture

> **Version**: 0.2 — Removed standalone validation layer; error fixing co-located with generation via self-healing loops  
> **Deployment target**: Cortex Agent SDK (Python) + Cortex Code CLI Skill  
> **Scope**: Enterprise-scale creation and refresh of Snowflake Semantic Views

---

## Table of Contents

1. [Overview](#overview)
2. [Deployment Architecture](#deployment-architecture)
3. [Orchestration Hierarchy](#orchestration-hierarchy)
4. [Batching Strategy for 700+ Tables](#batching-strategy-for-700-tables)
5. [HITL Implementation](#hitl-implementation)
6. [Self-Healing Agent Pattern](#self-healing-agent-pattern)
7. [Creation Workflow — Phase by Phase](#creation-workflow--phase-by-phase)
8. [Refresh / Update Workflow](#refresh--update-workflow)
9. [Project Structure](#project-structure)
10. [Configuration Schema](#configuration-schema)
11. [Execution Modes](#execution-modes)
12. [Error Handling Strategy](#error-handling-strategy)
13. [Implementation Roadmap](#implementation-roadmap)

---

## Overview

Two top-level workflows, each composed of specialized sub-agents with defined parallelism boundaries and optional human-in-the-loop (HITL) checkpoints.

```
Workflow Entry
    │
    ├─ Scope Config: { account | database | schema }
    ├─ Mode Config: { automated | hitl }
    │
    ├─── CREATION WORKFLOW ──────────────────────────────────────────────
    │     Phase 1: Context Mining         (parallel — batched by table count)
    │     Phase 2: Clustering             (sequential after P1)
    │     Phase 3: Lineage + Plan         (parallel per cluster)
    │     [HITL CHECKPOINT 1: Approve assessment report]
    │     Phase 4: SV Creation + Self-Healing Validation  (parallel per cluster)
    │     Phase 5: Evaluation             (parallel per SV)
    │     [HITL CHECKPOINT 2: Review accuracy report]
    │     Phase 6: Cortex Search          (parallel per SV)
    │     Phase 7: Search Testing         (parallel per SV)
    │     Phase 8: Summary Report
    │
    └─── REFRESH WORKFLOW ───────────────────────────────────────────────
          Phase R1: Change Detection      (parallel per SV)
          Phase R2: Impact Analysis       (sequential after R1)
          Phase R3: Incremental Update    (parallel per affected SV)
          Phase R4: Re-evaluation         (parallel per updated SV)
          Phase R5: Refresh Report
```

---

## Deployment Architecture

### Chosen Approach: Cortex Agent SDK (Python) + Cortex Code CLI Skill

Neither Snowflake Tasks nor a Streamlit app. The architecture is:

- A **Python asyncio orchestrator** (`orchestrator.py`) using the Cortex Agent SDK as the execution engine
- **`asyncio.gather()`** for true parallel phase execution across multiple independent `query()` agent instances
- **Deterministic SQL scripts** exposed as MCP tools via `@tool` + `create_sdk_mcp_server()`
- Each sub-agent receives a **`CLAUDE.md` skill file** as its instruction set (version-controlled, reusable)
- The entire project is packaged as a **Cortex Code Skill** for invocation from the CLI

#### SDK Primitives Used

| Primitive | How It's Used |
|---|---|
| `query()` | Single-shot sub-agent per batch/cluster/SV — runs in parallel via `asyncio.gather()` |
| `CortexCodeSDKClient` | Multi-turn self-healing sessions for SV creation — generate, validate, fix in one session |
| `@tool` + `create_sdk_mcp_server()` | Exposes deterministic SQL scripts as MCP tools available to all sub-agents |
| `canUseTool` callback | HITL mechanism — intercepts tool calls at checkpoint boundaries |
| `SubagentStart/Stop` hooks | Lifecycle tracking for logging and progress reporting |
| Structured output (JSON Schema) | Forces agents to return machine-readable artifacts for inter-agent handoff |
| `outputFormat: json_schema` | Ensures clean JSON at phase boundaries — no parsing fragility |

#### Invocation

As a skill from the CLI:
```bash
# Interactive (HITL) mode
cortex --print "Run SV automation for database PROD_DB in hitl mode"

# Automated mode
cortex --print "Run SV automation for schema PROD_DB.ANALYTICS in automated mode"
```

Programmatically via SDK:
```python
from cortex_code_agent_sdk import query, CortexCodeAgentOptions

async for message in query(
    prompt="Run SV automation for database PROD_DB scope=database mode=hitl",
    options=CortexCodeAgentOptions(
        cwd="./sv_automation",
        connection="MY_DEMO",
        dangerously_allow_all_tool_calls=True,
    ),
):
    if isinstance(message, AssistantMessage):
        for block in message.content:
            if hasattr(block, "text"):
                print(block.text, end="")
```

---

## Orchestration Hierarchy

### The Main Orchestrator is NOT an LLM Agent

`orchestrator.py` is a Python `asyncio` coordinator script. It:
- Reads config
- Runs deterministic SQL to enumerate tables in scope
- Shards work into batches
- Launches LLM sub-agents via `asyncio.gather()`
- Collects and merges structured JSON outputs
- Detects errors via deterministic checks and routes them back to the originating agent
- Manages HITL gates

Only the **per-batch analysis tasks** (join pattern extraction, clustering logic, SV generation, evaluation) are LLM agents. The coordination, error detection, and retry routing are all deterministic Python.

```
orchestrator.py  (Python asyncio — NOT an LLM agent)
    │
    ├─ Phase 1: Context Mining Sub-Orchestrator
    │   └─ asyncio.gather() → N parallel query() agents (one per shard)
    │       Each shard: 100–150 tables → returns structured JSON
    │       Merge: deterministic Python (no LLM)
    │
    ├─ Phase 2: Clustering Agent (single query() call)
    │   Input: merged context JSON from Phase 1
    │   Output: cluster_plan.json (structured output w/ JSON Schema)
    │   On violation: orchestrator retries with constraint error appended to prompt
    │
    ├─ Phase 3: Lineage + Plan (parallel per cluster)
    │   └─ asyncio.gather() → one query() per cluster
    │
    ├─ [HITL CHECKPOINT 1]
    │
    ├─ Phase 4: SV Creation + Self-Healing Validation (parallel per cluster)
    │   └─ asyncio.gather() → one CortexCodeSDKClient session per cluster
    │       Turn 1: generate YAML
    │       Turn N: orchestrator feeds back cortex reflect errors → agent fixes
    │       Loop until clean or max_self_heal_retries reached
    │
    ├─ Phase 5: Evaluation (parallel per SV)
    │   ├─ EvalQuestionGenerator: asyncio.gather() per SV
    │   └─ CortexAnalystEvaluator: asyncio.gather() per SV
    │
    ├─ [HITL CHECKPOINT 2]
    │
    ├─ Phase 6: Cortex Search (parallel per SV)
    │   ├─ CandidateFinder: asyncio.gather() per SV
    │   ├─ Provisioner: asyncio.gather() per candidate column
    │   └─ SVSearchIntegrator + self-healing validation: asyncio.gather() per SV
    │
    ├─ Phase 7: Search Testing (parallel per SV)
    │
    └─ Phase 8: Summary Report (deterministic Python aggregation)
```

### Do We Need Sub-Orchestrators?

**Yes, for Phases 1, 4, 5, and 6.** Each of these has unbounded fan-out that grows with table/cluster/SV count. The Python `asyncio` coordinator handles this without needing an LLM orchestrator agent at each level.

For Phase 1 specifically at 700+ tables: a dedicated `ContextMiningSubOrchestrator` class in `orchestrator.py` manages the shard lifecycle, concurrency limits (default: 10 parallel shards), and merge logic.

---

## Batching Strategy for 700+ Tables

### Sharding Logic

```
Total tables in scope
    → Enumerate via INFORMATION_SCHEMA SQL (deterministic)
    → Sort by schema + table name (deterministic order)
    → Shard into batches of BATCH_SIZE (default: 100)
    → Each batch → one query() agent instance

Example: 700 tables, batch_size=100
    → 7 shards
    → asyncio.gather(shard_1, shard_2, ..., shard_7)
    → With max_concurrent=10, all 7 run in parallel
```

### Why This Matters

- **Context window**: Each shard agent handles only 100 tables — keeps prompts focused, costs predictable
- **Failure isolation**: One failed shard doesn't block others — failed shards are logged and retried
- **Cost control**: `BATCH_SIZE` is a config knob — reduce to 50 for cost control, increase to 150 for speed
- **Determinism**: The merge step is pure Python — no LLM hallucination risk on the aggregation

### Shard Agent Pattern

Each shard agent:
1. Receives a pre-built SQL query parameterized with its table list
2. Executes via the built-in `SnowflakeSQL` tool
3. Returns structured JSON via `outputFormat: json_schema`
4. The orchestrator collects and merges all shard outputs

---

## HITL Implementation

### SDK Constraint

`AskUserQuestion` is not yet supported in the Cortex Agent SDK v0.1.0. The agent proceeds with best judgment in headless mode.

### HITL via `canUseTool` + Signal File Pattern

A custom `@tool` called `request_human_approval` is registered in the MCP server. When an agent calls it at a checkpoint:

1. The `canUseTool` callback intercepts the call
2. The checkpoint payload (report path, summary, decision needed) is written to a well-known path
3. In **HITL mode**: execution blocks, waiting for a `{checkpoint}.approved` signal file
4. The human reviews the HTML report and creates the signal file to proceed (or `{checkpoint}.rejected` to abort)
5. In **automated mode**: the callback immediately returns `allow` — no blocking

```python
# HITL gate in orchestrator.py
async def can_use_tool(tool_name, tool_input, context):
    if tool_name == "request_human_approval" and config.mode == "hitl":
        checkpoint_id = tool_input["checkpoint_id"]
        report_path = tool_input["report_path"]

        print(f"\n[CHECKPOINT] Review required: {report_path}")
        print(f"[CHECKPOINT] Create '{checkpoint_id}.approved' to continue")

        # Poll for signal file
        while not Path(f"{checkpoint_id}.approved").exists():
            if Path(f"{checkpoint_id}.rejected").exists():
                raise CheckpointRejectedError(checkpoint_id)
            await asyncio.sleep(5)

        return PermissionResultAllow()

    return PermissionResultAllow()
```

---

## Self-Healing Agent Pattern

### Core Principle

There is no separate reasoning validation layer between phases. Validation is **deterministic** (JSON Schema, `cortex reflect`, Python assertions). When a deterministic check catches an error, the fix responsibility stays with the **generating agent** — not a new agent.

This avoids:
- A meta-reliability problem (LLM validating LLM output, with no ground truth)
- Extra latency and cost at every phase boundary
- A new class of failure mode at each validation step

### Two Fix Mechanisms

**Mechanism 1 — Multi-turn self-healing** (`CortexCodeSDKClient`)

Used for Phase 4 (SV creation + YAML validation), where errors require iterative reasoning to fix correctly. The orchestrator feeds deterministic error output back into the same agent session:

```python
async with CortexCodeSDKClient(options) as client:
    # Turn 1: generate
    await client.query(f"Generate YAML for cluster: {cluster_spec}")
    yaml_output = await collect_response(client)

    for attempt in range(config.max_self_heal_retries):
        errors = run_cortex_reflect(yaml_output)   # deterministic
        if not errors:
            break
        # Turn N: fix
        await client.query(
            f"cortex reflect returned these errors:\n{errors}\n\nFix them."
        )
        yaml_output = await collect_response(client)
    else:
        quarantine(cluster_id, yaml_output, errors)
```

**Mechanism 2 — Orchestrator retry with error context** (`query()`)

Used for phases that run as single-shot agents (Phases 1, 2, 3, 5). The Python orchestrator catches a business rule violation, then re-launches `query()` with the original prompt plus the specific error appended. One retry maximum — if still failing, quarantine and log.

```python
result = await run_agent(prompt=base_prompt)
violations = check_business_rules(result)   # deterministic

if violations:
    result = await run_agent(
        prompt=base_prompt + f"\n\nPrevious attempt had violations:\n{violations}\nFix them."
    )
    if check_business_rules(result):
        quarantine(result, violations)
```

### Where Each Mechanism Applies

| Phase | Error type | Detected by | Fixed by |
|---|---|---|---|
| Phase 1: Context Mining | JSON Schema violation | `outputFormat` enforcement | Orchestrator retry (Mechanism 2) |
| Phase 2: Clustering | >15 tables in cluster | Python assertion | Orchestrator retry with constraint stated (Mechanism 2) |
| Phase 2: Clustering | JSON Schema violation | `outputFormat` enforcement | Orchestrator retry (Mechanism 2) |
| Phase 4: SV Creation | YAML syntax / semantic error | `cortex reflect` (deterministic) | Multi-turn self-healing in same session (Mechanism 1) |
| Phase 4: SV Creation | Unresolvable join keys | Python check on YAML content | Multi-turn: feed back specific missing keys (Mechanism 1) |
| Phase 5: Evaluation | Poor quality eval questions | Heuristic score in Python | Orchestrator retry with examples of good questions (Mechanism 2) |
| Phase 5: Evaluation | JSON Schema violation | `outputFormat` enforcement | Orchestrator retry (Mechanism 2) |
| Phase 6: Cortex Search | No change tracking | SQL exception (deterministic) | No LLM — log, skip column, continue |
| Phase 6: Cortex Search | Insufficient privileges | SQL exception (deterministic) | No LLM — log, flag, continue |
| All phases | Shard/agent failure | SDK `ResultMessage.subtype == "error"` | Orchestrator retry once, then quarantine |
| Refresh R3 | Breaking schema change | Deterministic diff | Never auto-fix — quarantine, require human |

### What Stays Deterministic

These checks run in Python before and after every agent call — no LLM involved:

- JSON Schema enforcement via `outputFormat: json_schema` (automatic, SDK-level)
- Business rule assertions: ≤15 tables per cluster, required fields present, no null join keys
- Completeness checks: expected keys exist, no empty arrays where content is required
- `cortex reflect` for YAML syntax and semantic validity
- SQL exception handling for Cortex Search provisioning

---

## Creation Workflow — Phase by Phase

### Phase 1: Context Mining

**Agent type**: `query()` per shard (parallel)  
**Inputs**: table list shard, lookback window (default: 90 days)

Tasks per shard (via SQL tools):

| SQL Tool | What it returns |
|---|---|
| `table_tag_warehouse_map` | `{ table_fqn: [tags, warehouses] }` |
| `join_frequency_matrix` | `{ (t1, t2): co_occurrence_count }` |
| `user_role_affinity` | `{ table_fqn: { top_roles: [], top_users: [] } }` |

**Error handling:**
- Empty query history (<30 days): flag tables, reduce cluster confidence score
- SQL parsing failures (dynamic SQL, stored procs): log unparseable queries, continue with partial data
- Missing `OBJECTS_ACCESSED` data: fall back to DDL-only clustering
- JSON Schema violation: orchestrator retry (Mechanism 2)

**Output artifacts**: `phase1_shard_{n}.json` → merged to `phase1_context.json`

---

### Phase 2: Clustering

**Agent type**: Single `query()` call  
**Input**: `phase1_context.json`  
**Output**: `cluster_plan.json` (JSON Schema enforced)

Algorithm:
1. Seed clusters using tag/warehouse affinity (co-membership in same tag/warehouse = candidate co-members)
2. Re-rank using join frequency as edge weights (graph community detection)
3. Enforce **≤15 table constraint**: split oversized clusters at weakest join edge
4. Label each cluster with persona (from role/user affinity)
5. Flag ambiguous tables (appear as strong candidate in >1 cluster)

**Error handling:**
- Cluster exceeds 15 tables: Python assertion → orchestrator retry with explicit split instruction (Mechanism 2)
- JSON Schema violation: orchestrator retry (Mechanism 2)

**Output schema per cluster:**
```json
{
  "cluster_id": "analytics_revenue",
  "tables": ["DB.SCHEMA.ORDERS", "DB.SCHEMA.LINE_ITEMS"],
  "personas": ["analyst", "finance_team"],
  "confidence": "high|medium|low",
  "primary_tag": "finance_reporting",
  "split_reason": null
}
```

---

### Phase 3: Lineage + Plan Generation

**Agent type**: `query()` per cluster (parallel)  
**Input**: cluster definition from Phase 2

Per cluster:
- `GET_DDL` for each member table
- Query `OBJECT_DEPENDENCIES` for upstream/downstream relationships
- Identify base tables vs derived views
- Build relationship map: `{ table: { upstream, downstream, join_keys } }`

**`assessment_report.html`** (Jinja template) contains:
- Per-cluster card: member tables, personas, join key summary, lineage depth
- Tradeoffs section:
  - Cross-team dependency risk (clusters split across tag/warehouse boundaries)
  - Weak cohesion flags (tables with no join history in cluster)
  - Disambiguation needed (tables in multiple candidate clusters)
  - Low-confidence clusters (<5 queries in history)
- Recommended SV names and descriptions
- Confidence score per cluster

> **HITL CHECKPOINT 1**: Report surfaced for human review. In HITL mode, blocks until approved. In automated mode, saves report and continues.

---

### Phase 4: SV Creation + Self-Healing Validation

**Agent type**: `CortexCodeSDKClient` (multi-turn) per cluster (parallel)  
**Integrates**: Semantic View Creation Skill (deterministic scripts)

This is the primary site of self-healing (Mechanism 1). Creation and validation run inside a single multi-turn session — there is no separate validation agent.

**Session flow per cluster:**
```
Turn 1: Generate YAML using cluster membership, lineage map, join keys, persona descriptions
         → calls semantic_view_get.py / semantic_view_set.py via @tool wrappers
         → outputs: semantic_view_{cluster_name}.yaml

[Python: run cortex reflect — deterministic]

Turn 2 (if errors): "cortex reflect returned: <errors>. Fix them."
Turn 3 (if errors): "cortex reflect returned: <remaining errors>. Fix them."
...
Turn N: cortex reflect clean → session closes, YAML artifact saved
```

**Max retries**: configurable via `max_self_heal_retries` (default: 3).  
**On exhaustion**: quarantine YAML to `failed_svs.json` with full error log, continue with other clusters.

**Other error handling:**
- DDL unavailable (insufficient privileges): log, skip table, flag in summary — do not retry
- Unresolvable join keys: feed back specifically which keys are missing (Mechanism 1), mark `unresolved` if still failing after retries

**Output**: `semantic_view_{cluster_name}.yaml` (validated clean by `cortex reflect`)

---

### Phase 5: Evaluation

**Agent type**: `query()` per SV (parallel, two sub-phases)

#### 5a: EvalQuestionGenerator

For each SV's member tables:
- Pull ≥5 single-table queries per table from `QUERY_HISTORY`
- Pull ≥10 multi-table queries joining ≥2 member tables
- Dedup + normalize to natural language question stubs
- **Flag**: if <5 single-table or <10 join queries → mark `needs_human_validation`

**Error handling:**
- Questions score below quality heuristic threshold: orchestrator retry with examples of high-quality questions (Mechanism 2)
- JSON Schema violation: orchestrator retry (Mechanism 2)

#### 5b: CortexAnalystEvaluator

- Batch-run Cortex Analyst against each eval question
- Execute both the Analyst-generated SQL and the original SQL
- Compare: result set row count + column structure
- Score per question: `pass` / `partial` / `fail`
- Roll up to per-SV accuracy score (0–100%)

#### Evaluation Report

**`evaluation_report.html`** contains:
- Per-SV accuracy score + question-level pass/fail breakdown
- Priority ranking: lowest scores surfaced first
- Human-validation flags: SVs or tables with insufficient query history
- Suggested next actions per SV

> **HITL CHECKPOINT 2**: Evaluation report surfaced. In HITL mode, blocks until approved. SVs below threshold (default: 70%) are flagged regardless of mode.

---

### Phase 6: Cortex Search Integration

**Agent type**: `query()` per SV, then per candidate column (parallel)

#### 6a: CortexSearchCandidateFinder

- Identify VARCHAR/TEXT columns with >50 distinct values (`APPROX_COUNT_DISTINCT`)
- Cross-reference with WHERE clause usage frequency from query history
- Output: ranked candidate list `{ column_fqn, distinct_count, where_clause_frequency }`

#### 6b: CortexSearchProvisioner

```sql
CREATE CORTEX SEARCH SERVICE <name>
  ON <column>
  AS SELECT DISTINCT <column> FROM <table>;
```

**Error handling (all deterministic — no LLM retry):**
- **Change tracking not enabled**: catch SQL exception, add to `cortex_search_manual_action.json`, continue
- **Insufficient privileges**: log, flag in summary, continue
- **Table too large / timeout**: log, flag, continue

#### 6c: SVSearchIntegrator

- Update SV YAML to reference each successfully created Cortex Search Service
- Run `cortex reflect` on updated YAMLs — if errors, self-healing loop (Mechanism 1) in a `CortexCodeSDKClient` session

---

### Phase 7: Point Lookup Testing

For each SV with Cortex Search integrated:
- Generate point-lookup test questions against search-enabled columns
- Run via Cortex Analyst
- Score: pass if exact match in top result
- Log failures to `search_test_failures.json`

---

### Phase 8: Summary Report

Single HTML report (`creation_summary_report.html`):
- SVs created successfully (YAML artifact paths)
- SVs quarantined / failed (with reason codes and full error logs)
- Accuracy scores per SV
- Cortex Search: services created vs manual action required (with specific column + reason)
- Self-healing stats: how many retries were needed per phase, which SVs required max retries
- Cluster confidence scores
- Query coverage before vs after

---

## Refresh / Update Workflow

### Phase R1: Change Detection (parallel per existing SV)

- Compare current `GET_DDL` output against stored snapshot at SV creation time
- Detect: column additions, removals, type changes
- Detect: new tables added to the schema not yet in any SV
- Output: `change_manifest.json` per SV

**Severity levels:**
- `breaking`: join key removed, required field type changed — do NOT auto-apply
- `additive`: new column added — auto-apply as new dimension/fact
- `cosmetic`: description drift — auto-apply

### Phase R2: Impact Analysis (sequential)

- For each changed column: determine if referenced in a dimension, fact, metric, or join key
- New tables: mini-clustering to assign to existing SV or flag for new SV
- Output: `impact_report.json` + HTML summary
- Optional HITL checkpoint for breaking changes

### Phase R3: Incremental Update (parallel per affected SV)

- **Breaking changes**: quarantine, flag for human — never auto-apply, never auto-fix
- **Additive changes**: `CortexCodeSDKClient` session adds new columns, then self-healing validation loop
- **New tables assigned to existing SV**: add with lineage context, self-healing validation
- **New tables warranting new SV**: trigger mini-creation workflow (Phases 4–7)

### Phase R4: Re-evaluation

- Re-run `EvalQuestionGenerator` for updated SVs (recent query history since last update)
- Re-run `CortexAnalystEvaluator`
- Produce delta accuracy report: before vs after change

### Phase R5: Refresh Report

- What changed, what was auto-applied, what requires human action

---

## Project Structure

```
sv_automation/
├── CLAUDE.md                        # Skill entry point — routes to orchestrator.py
├── orchestrator.py                  # Main asyncio coordinator (entry point)
├── refresh/
│   └── orchestrator.py             # Refresh/update workflow entry point
├── config.yaml                     # Scope + mode + threshold config
│
├── tools/
│   ├── sql_tools.py                # @tool definitions wrapping SQL scripts
│   └── mcp_server.py               # create_sdk_mcp_server() setup
│
├── agents/
│   ├── query_history_miner/
│   │   └── CLAUDE.md               # Shard-level mining instructions
│   ├── clusterer/
│   │   └── CLAUDE.md
│   ├── lineage_mapper/
│   │   └── CLAUDE.md
│   ├── sv_creator/
│   │   └── CLAUDE.md               # Imports semantic view creation skill; includes self-healing instructions
│   ├── evaluator/
│   │   └── CLAUDE.md
│   └── cortex_search/
│       └── CLAUDE.md
│
├── sql/
│   ├── 01_table_tag_warehouse_map.sql
│   ├── 02_join_frequency.sql
│   ├── 03_user_role_affinity.sql
│   ├── 04_lineage_query.sql
│   ├── 05_eval_questions.sql
│   └── 06_cortex_search_candidates.sql
│
└── reports/
    ├── assessment_report.html.jinja
    └── evaluation_report.html.jinja
```

---

## Configuration Schema

```yaml
scope:
  level: account | database | schema
  database: PROD_DB              # required if level = database or schema
  schema: ANALYTICS              # required if level = schema

mode:
  type: automated | hitl
  hitl_checkpoints:
    - post_plan                  # after Phase 3
    - post_evaluation            # after Phase 5
    - on_breaking_refresh_change # during refresh R2

thresholds:
  max_tables_per_sv: 15
  min_accuracy_score: 70         # SVs below this flagged in automated mode
  cortex_search_min_distinct: 50
  query_history_lookback_days: 90
  min_eval_questions_per_table: 5
  min_join_eval_questions: 10

self_healing:
  max_self_heal_retries: 3       # max multi-turn fix attempts per SV (Phase 4)
  max_orchestrator_retries: 1    # max single-shot agent retries (all other phases)

batching:
  table_batch_size: 100          # tables per shard agent
  max_concurrent_agents: 10      # asyncio.gather() concurrency cap

snowflake:
  connection: MY_DEMO
  model: auto                    # or claude-sonnet-4-6, claude-opus-4-6
```

---

## Execution Modes

| Mode | HITL checkpoints | On accuracy < threshold | On breaking refresh change |
|---|---|---|---|
| `automated` | None — execution continues | Flag in report, continue | Quarantine, log, continue |
| `hitl` | Blocks at Checkpoint 1 + 2 | Block + require approval | Block + require approval |

---

## Error Handling Strategy

All errors accumulate in a shared `run_log.json` for the session. The Phase 8 summary report includes an error section with counts and actionable items per category.

| Error type | Detected by | Fixed by | On fix failure |
|---|---|---|---|
| JSON Schema violation (any phase) | `outputFormat` enforcement | Orchestrator retry with error context (Mechanism 2) | Quarantine, log |
| Cluster >15 tables | Python assertion | Orchestrator retry with split instruction (Mechanism 2) | Quarantine cluster |
| YAML syntax / semantic error | `cortex reflect` (deterministic) | Multi-turn self-healing in same session (Mechanism 1) | Quarantine SV after `max_self_heal_retries` |
| Unresolvable join keys | Python check on YAML | Multi-turn: feed back missing keys (Mechanism 1) | Mark `unresolved`, flag for human |
| Poor quality eval questions | Heuristic score in Python | Orchestrator retry with examples (Mechanism 2) | Mark `needs_human_validation` |
| Cortex Search: no change tracking | SQL exception | No LLM — log, skip column, continue | Listed in `cortex_search_manual_action.json` |
| Cortex Search: insufficient privileges | SQL exception | No LLM — log, flag, continue | Listed in summary report |
| Shard / agent failure | SDK `ResultMessage.subtype == "error"` | Orchestrator retry once | Quarantine shard, continue with rest |
| Breaking schema change (refresh) | Deterministic DDL diff | Never auto-fix — human required | Quarantined in refresh report |

---

## Implementation Roadmap

Steps are ordered by dependency. Work within each numbered group can proceed in parallel.

**Group 1: Foundation**
- [ ] `config.yaml` schema + Python config loader
- [ ] `sql/` scripts — write and test each parameterized SQL independently
- [ ] `tools/sql_tools.py` — `@tool` wrappers for each SQL script
- [ ] `tools/mcp_server.py` — `create_sdk_mcp_server()` registration

**Group 2: Orchestrator Skeleton**
- [ ] `orchestrator.py` — phase stubs, shard/merge logic, HITL gate, error accumulator, retry router
- [ ] Top-level `CLAUDE.md` skill entry point

**Group 3: Phase 1**
- [ ] `agents/query_history_miner/CLAUDE.md`
- [ ] Shard agent + structured JSON output schema
- [ ] Merge logic in orchestrator
- [ ] Test against small schema (10–20 tables)

**Group 4: Phases 2–3**
- [ ] `agents/clusterer/CLAUDE.md` + cluster_plan JSON schema + ≤15 table assertion
- [ ] `agents/lineage_mapper/CLAUDE.md`
- [ ] Jinja template for `assessment_report.html`
- [ ] HITL Checkpoint 1 gate

**Group 5: Phase 4**
- [ ] `agents/sv_creator/CLAUDE.md` — integrate semantic view creation skill + self-healing instructions
- [ ] `CortexCodeSDKClient` multi-turn loop with `cortex reflect` feedback
- [ ] Quarantine logic on `max_self_heal_retries` exhaustion

**Group 6: Phase 5**
- [ ] `agents/evaluator/CLAUDE.md`
- [ ] Eval question generator + quality heuristic + orchestrator retry
- [ ] Cortex Analyst batch runner
- [ ] Jinja template for `evaluation_report.html`
- [ ] HITL Checkpoint 2 gate

**Group 7: Phases 6–7**
- [ ] `agents/cortex_search/CLAUDE.md`
- [ ] Candidate finder + provisioner + deterministic error handling for change tracking
- [ ] SV search integrator with self-healing validation loop
- [ ] Point lookup tester

**Group 8: Summary + Refresh**
- [ ] Phase 8 summary report aggregation (include self-healing stats)
- [ ] `refresh/orchestrator.py` — change detection + impact analysis + incremental update
- [ ] End-to-end test: full 700-table run against dev environment

---

## Critical Files

| File | Purpose |
|---|---|
| `orchestrator.py` | Main asyncio coordinator — shard/merge logic, phase sequencing, retry routing, HITL gates |
| `tools/sql_tools.py` | `@tool` definitions — the interface between LLM agents and deterministic SQL |
| `tools/mcp_server.py` | `create_sdk_mcp_server()` — wires all tools into every sub-agent |
| `agents/sv_creator/CLAUDE.md` | Most complex agent — integrates semantic view skill + self-healing instructions |
| `config.yaml` | Top-level variable surface — scope, mode, batch size, thresholds, retry limits |
