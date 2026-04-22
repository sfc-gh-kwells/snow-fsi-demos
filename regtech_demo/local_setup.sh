#!/bin/bash
# ============================================================
# RegTech Demo — Local Setup
# ============================================================
# Commands that must run locally (not in Snowsight).
# Run this AFTER executing Steps 1-2 in the Snowsight notebook
# (database, schema, tables, stage created).
#
# Prerequisites:
#   - Snowflake CLI (snow) installed
#   - python3 available
#   - Connection "MY_DEMO" configured in ~/.snowflake/connections.toml
# ============================================================

set -euo pipefail
cd "$(dirname "$0")"

SNOW_CONN="${SNOW_CONN:-MY_DEMO}"
STAGE="REGTECH_DEMO_DB.REGULATORY_REPORTING.REGULATION_DOCS_STAGE"

echo "=== Step 1: Fetch regulations from eCFR API ==="
python3 fetch_regulations.py
echo ""

echo "=== Step 2: Upload files to @${STAGE} ==="
for f in regulations/*.txt; do
  echo "  uploading $(basename "$f") ..."
  snow stage copy "$f" "@${STAGE}/regulations/" --connection "$SNOW_CONN" --overwrite
done
echo ""

echo "=== Step 3: Refresh stage directory ==="
snow sql -q "ALTER STAGE ${STAGE} REFRESH;" --connection "$SNOW_CONN"
snow sql -q "SELECT RELATIVE_PATH, SIZE FROM DIRECTORY(@${STAGE});" --connection "$SNOW_CONN"
echo ""

echo "=== Done — return to Snowsight notebook for Step 3b (PARSE_DOCUMENT) ==="
echo ""
echo "Later, after Steps 3b-7, run the audit pipeline:"
echo "  python3 audit_pipeline.py --connection MY_DEMO --force"
