# Cortex Agent Feedback Analysis using AI_COMPLETE

## Overview

This script analyzes user feedback collected from the **Cortex Agent REST API** using `AI_COMPLETE`. It turns raw thumbs-up/thumbs-down feedback into structured problem categories and actionable improvement recommendations — entirely in SQL.

Use it to understand what your agent is getting wrong, prioritize fixes, and track satisfaction trends over time.

## What It Does

| Step | Description |
|------|-------------|
| 1. Raw feedback | View all feedback events with user, message, sentiment, and thread ID |
| 2. Per-row classification | Use `AI_COMPLETE` (claude-haiku-4-5) to assign each message to a problem category |
| 3. Categorized results | Browse feedback alongside AI-assigned categories and explanations |
| 4. Category summary | Count negative feedback by problem type to find the biggest pain points |
| 5. User-selected categories | Explode and aggregate the categories users picked themselves |
| 6. Actionable insights | Use `AI_COMPLETE` (claude-sonnet-4-6) to synthesize all negative feedback into ranked recommendations |
| 7. Trend analysis | Weekly sentiment score to track whether agent quality is improving |

## Problem Categories

The classifier assigns each feedback message to one of these categories:

| Category | Meaning |
|----------|---------|
| `INCORRECT_ANSWER` | Agent gave wrong or inaccurate information |
| `INCOMPLETE_ANSWER` | Answer was partially correct but missing key details |
| `TOOL_FAILURE` | A tool (search, analyst, SQL) failed or returned bad results |
| `HALLUCINATION` | Agent made up information not grounded in sources |
| `SLOW_RESPONSE` | Performance or latency complaints |
| `STALE_DATA` | Data sources are outdated or not refreshed |
| `OFF_TOPIC` | Agent did not understand the question or went off-topic |
| `PERMISSION_ERROR` | User could not access data or got authorization errors |
| `MISSING_CAPABILITY` | Agent lacks a feature or data source the user needs |
| `OTHER` | Does not fit other categories |

## Prerequisites

1. **Snowflake Account** with Cortex AI enabled
2. **Role** with `SNOWFLAKE.CORTEX_USER` database role granted
3. **MONITOR privilege** on the Cortex Agent object
4. **READ UNREDACTED AI OBSERVABILITY EVENTS TABLE privilege** to see full feedback message text

## Quick Start

### Step 1: Set your agent variables

At the top of `agent_feedback_analysis.sql`, update the three session variables to point to your agent:

```sql
SET database_name = 'YOUR_DB';
SET schema_name   = 'YOUR_SCHEMA';
SET agent_name    = 'YOUR_AGENT_NAME';
```

### Step 2: Run the script

Execute the script in a Snowflake worksheet or Snowsight. Run sections sequentially — step 2 creates the `feedback_categorized` temporary table that steps 3–6 depend on.

### Step 3: Review recommendations

Step 6 returns a free-text analysis from `claude-sonnet-4-6` with ranked, specific recommendations for improving your agent (e.g., refresh data sources, update the system prompt, add tools).

## Architecture

```
┌──────────────────────────────┐
│  Cortex Agent REST API       │
│  (user thumbs up/down)       │
└──────────────┬───────────────┘
               │  feedback events
               ▼
┌──────────────────────────────┐
│  GET_AI_OBSERVABILITY_EVENTS │  ← SNOWFLAKE.LOCAL table function
│  CORTEX_AGENT_FEEDBACK rows  │
└──────────────┬───────────────┘
               │
       ┌───────┴────────┐
       ▼                ▼
┌─────────────┐  ┌──────────────────────┐
│ Per-row     │  │ Aggregated negative  │
│ AI_COMPLETE │  │ feedback             │
│ (haiku)     │  │                      │
│ classify    │  │ AI_COMPLETE (sonnet) │
│ category    │  │ recommendations      │
└─────────────┘  └──────────────────────┘
```

## Notes

- Steps 3–6 depend on the `feedback_categorized` temp table created in step 2. Re-run step 2 if you start a new session.
- Classification uses `temperature: 0` for consistent, deterministic category assignment.
- Recommendation synthesis uses `temperature: 0.2` to allow slightly more creative analysis.
- The `REGEXP_REPLACE` in step 2 strips any markdown code fences that models occasionally emit despite instructions, ensuring `PARSE_JSON` succeeds.
