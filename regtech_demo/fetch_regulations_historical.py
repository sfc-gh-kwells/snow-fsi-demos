#!/usr/bin/env python3
"""
Fetch HISTORICAL eCFR regulation text (default: 2020-01-01) and load into Snowflake.

Steps:
  1. Fetch the same 7 regulation sections from eCFR at a historical date
  2. Save plain-text files to regulations/v{YEAR}/
  3. INSERT historical docs into REGULATORY_DOCUMENTS (STATUS='Superseded')
  4. Use SNOWFLAKE.CORTEX.COMPLETE() to extract what changed vs current version
  5. INSERT diff results into EXTRACTED_REQUIREMENTS with OLD_REQUIREMENT / CHANGE_TYPE

Usage:
    python fetch_regulations_historical.py
    python fetch_regulations_historical.py --date 2019-01-01
    python fetch_regulations_historical.py --fetch-only   # skip Snowflake steps
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

# ── same sections as fetch_regulations.py ────────────────────────────────────
SECTIONS = [
    {"part": 3,  "subpart": "A", "doc_id_suffix": "1",
     "filename": "title12_part3_subpartA_general_provisions.txt",
     "display_name": "12 CFR Part 3 – Subpart A: General Provisions",
     "framework": "Basel III"},
    {"part": 3,  "subpart": "B", "doc_id_suffix": "2",
     "filename": "title12_part3_subpartB_capital_ratios.txt",
     "display_name": "12 CFR Part 3 – Subpart B: Capital Ratio Requirements",
     "framework": "Basel III"},
    {"part": 3,  "subpart": "C", "doc_id_suffix": "3",
     "filename": "title12_part3_subpartC_definition_of_capital.txt",
     "display_name": "12 CFR Part 3 – Subpart C: Definition of Capital",
     "framework": "Basel III"},
    {"part": 3,  "subpart": "D", "doc_id_suffix": "4",
     "filename": "title12_part3_subpartD_rwa_standardized.txt",
     "display_name": "12 CFR Part 3 – Subpart D: RWA (Standardized)",
     "framework": "Basel III"},
    {"part": 3,  "subpart": "E", "doc_id_suffix": "5",
     "filename": "title12_part3_subpartE_rwa_irb.txt",
     "display_name": "12 CFR Part 3 – Subpart E: RWA (IRB/Advanced)",
     "framework": "Basel III"},
    {"part": 3,  "subpart": "F", "doc_id_suffix": "6",
     "filename": "title12_part3_subpartF_rwa_market_risk.txt",
     "display_name": "12 CFR Part 3 – Subpart F: RWA (Market Risk)",
     "framework": "Basel III"},
    {"part": 50, "subpart": "B", "doc_id_suffix": "7",
     "filename": "title12_part50_subpartB_lcr_hqla.txt",
     "display_name": "12 CFR Part 50 – Subpart B: LCR & HQLA",
     "framework": "Basel III"},
]

# ── HTML stripper (stdlib) ────────────────────────────────────────────────────
class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._pieces: list[str] = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True
        if tag in ("p","div","br","h1","h2","h3","h4","h5","h6",
                   "li","tr","blockquote","section","article"):
            self._pieces.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        if tag in ("p","div","h1","h2","h3","h4","h5","h6",
                   "li","tr","blockquote","section","article"):
            self._pieces.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._pieces.append(data)

    def get_text(self):
        raw = "".join(self._pieces)
        lines = [" ".join(line.split()) for line in raw.splitlines()]
        text = "\n".join(lines)
        return re.sub(r"\n{3,}", "\n\n", text).strip()


def strip_html(html: str) -> str:
    e = _HTMLTextExtractor()
    e.feed(html)
    return e.get_text()


# ── eCFR fetch ────────────────────────────────────────────────────────────────
def fetch_section(part: int, subpart: str, date: str) -> str:
    url = (f"https://www.ecfr.gov/api/renderer/v1/content/enhanced"
           f"/{date}/title-12?part={part}&subpart={subpart}")
    req = urllib.request.Request(url, headers={"Accept": "text/html"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP {e.code} for part={part} subpart={subpart} date={date}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error: {e.reason}") from e


# ── Snowflake REST helper ─────────────────────────────────────────────────────
def sf_execute(sql: str, account: str, pat: str) -> dict:
    """Run a SQL statement via Snowflake REST API, return response JSON."""
    url = f"https://{account}.snowflakecomputing.com/api/v2/statements"
    body = json.dumps({
        "statement": sql,
        "timeout": 120,
        "database": "REGTECH_DEMO_DB",
        "schema": "REGULATORY_REPORTING",
        "warehouse": "COMPUTE_WH",
        "role": "SYSADMIN",
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {pat}",
            "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
        },
    )
    with urllib.request.urlopen(req, timeout=130) as r:
        return json.loads(r.read().decode("utf-8"))


def sf_query_one(sql: str, account: str, pat: str) -> str | None:
    """Execute a SELECT and return the first cell of the first row."""
    resp = sf_execute(sql, account, pat)
    rows = resp.get("data", [])
    if rows and rows[0]:
        return rows[0][0]
    return None


def esc(s: str) -> str:
    """Escape single quotes for SQL string literals."""
    return s.replace("'", "''")


def _extract_relevant_section(text: str, keywords: list[str], max_chars: int = 3500) -> str:
    """Extract the most keyword-relevant section of a regulation text.

    Splits the document on paragraph/section boundaries, scores each chunk by
    keyword frequency, and returns the highest-scoring chunks up to max_chars.
    This replaces the old LEFT(4000) SQL approach which only grabbed the
    boilerplate preamble at the start of every CFR document.
    """
    # Split on paragraph/section boundaries
    chunks = re.split(
        r"\n(?=§\s*\d|\(\w\)|\d+\.\d+|\bSubpart\b|\bSection\b)",
        text,
    )
    # Also split on double newlines if chunks are still very large
    fine_chunks: list[str] = []
    for chunk in chunks:
        if len(chunk) > 1200:
            fine_chunks.extend(re.split(r"\n{2,}", chunk))
        else:
            fine_chunks.append(chunk)

    # Score each chunk by keyword hits (case-insensitive)
    def score(chunk: str) -> int:
        cl = chunk.lower()
        return sum(1 for kw in keywords if kw.lower() in cl)

    scored = [(score(c), c) for c in fine_chunks if len(c.strip()) > 30]
    scored.sort(key=lambda x: x[0], reverse=True)

    # Concatenate top-scoring chunks until we hit max_chars
    result_parts: list[str] = []
    total = 0
    for _, chunk in scored:
        if total + len(chunk) > max_chars:
            # Add as much of this chunk as fits
            remaining = max_chars - total
            if remaining > 200:
                result_parts.append(chunk[:remaining])
            break
        result_parts.append(chunk)
        total += len(chunk)

    if not result_parts:
        return text[:max_chars]

    return "\n\n".join(result_parts)


# ── Main ──────────────────────────────────────────────────────────────────────
def run(date: str, fetch_only: bool, output_dir: Path) -> None:
    year = date[:4]
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load .env for PAT / account
    env_path = Path(__file__).resolve().parent / ".env"
    account = pat = None
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("SNOWFLAKE_PAT="):
                pat = line.split("=", 1)[1].strip()
            if line.startswith("SNOWFLAKE_ACCOUNT="):
                account = line.split("=", 1)[1].strip()
    pat   = pat   or os.environ.get("SNOWFLAKE_PAT")
    account = account or os.environ.get("SNOWFLAKE_ACCOUNT", "SFSENORTHAMERICA-KAITLYNWELLS")

    if not pat and not fetch_only:
        print("ERROR: SNOWFLAKE_PAT not found in .env — use --fetch-only or set the env var.")
        sys.exit(1)

    # ── Step 1: fetch historical text ────────────────────────────────────────
    texts: dict[str, str] = {}
    total = len(SECTIONS)
    for idx, sec in enumerate(SECTIONS, 1):
        dest = output_dir / sec["filename"]
        label = f"[{idx}/{total}] Part {sec['part']} Subpart {sec['subpart']}"
        if dest.exists():
            print(f"{label} — already fetched, loading from disk")
            texts[sec["doc_id_suffix"]] = dest.read_text(encoding="utf-8")
            continue
        print(f"{label} — fetching from eCFR at {date} ...")
        html = fetch_section(sec["part"], sec["subpart"], date)
        text = strip_html(html)
        dest.write_text(text, encoding="utf-8")
        texts[sec["doc_id_suffix"]] = text
        print(f"         saved {sec['filename']} ({len(text):,} chars)")
        if idx < total:
            time.sleep(1)

    print(f"\n✓ Fetched {len(texts)} historical sections to {output_dir}\n")

    if fetch_only:
        print("--fetch-only set, stopping before Snowflake steps.")
        return

    # ── Step 2: ensure schema columns exist ──────────────────────────────────
    print("Ensuring schema columns exist ...")
    for col_sql in [
        "ALTER TABLE REGTECH_DEMO_DB.REGULATORY_REPORTING.REGULATORY_DOCUMENTS ADD COLUMN IF NOT EXISTS SUMMARY VARCHAR(2000)",
        "ALTER TABLE REGTECH_DEMO_DB.REGULATORY_REPORTING.EXTRACTED_REQUIREMENTS ADD COLUMN IF NOT EXISTS RULE_NAME VARCHAR(200)",
        "ALTER TABLE REGTECH_DEMO_DB.REGULATORY_REPORTING.EXTRACTED_REQUIREMENTS ADD COLUMN IF NOT EXISTS RULE_SECTION VARCHAR(100)",
        "ALTER TABLE REGTECH_DEMO_DB.REGULATORY_REPORTING.EXTRACTED_REQUIREMENTS ADD COLUMN IF NOT EXISTS OLD_REQUIREMENT VARCHAR(16777216)",
        "ALTER TABLE REGTECH_DEMO_DB.REGULATORY_REPORTING.EXTRACTED_REQUIREMENTS ADD COLUMN IF NOT EXISTS CHANGE_TYPE VARCHAR(50)",
        "ALTER TABLE REGTECH_DEMO_DB.REGULATORY_REPORTING.EXTRACTED_REQUIREMENTS ADD COLUMN IF NOT EXISTS IMPACTED_REPORT VARCHAR(200)",
    ]:
        try:
            sf_execute(col_sql, account, pat)
        except Exception as e:
            print(f"  (schema note: {e})")
    print("  done.\n")

    # ── Step 3: upsert summaries for current docs ─────────────────────────────
    SUMMARIES = {
        "DOC-ECFR-1": "Establishes scope and applicability of capital adequacy rules for national banks and federal savings associations. Defines qualifying capital instruments, consolidation requirements, and transition provisions under 12 CFR Part 3.",
        "DOC-ECFR-2": "Sets minimum capital ratio requirements: CET1 ≥ 4.5%, Tier 1 ≥ 6%, Total Capital ≥ 8%, Leverage ≥ 4%. Includes capital conservation buffer (2.5%) and countercyclical buffer framework.",
        "DOC-ECFR-3": "Defines eligible CET1, AT1, and Tier 2 capital instruments. Specifies deduction rules for DTAs, goodwill, intangibles, and significant investments. The DTA dual-threshold test (10% individual, 17.65% combined) is a key compliance area.",
        "DOC-ECFR-4": "Prescribes standardized risk weights for credit exposures: sovereign (0–150%), corporate (20–150%), residential mortgage (35–100%), retail (75%), and off-balance-sheet CCF factors.",
        "DOC-ECFR-5": "Advanced IRB approach for credit risk RWA using internal PD, LGD, and EAD models. Subject to supervisory approval. Basel IV output floor requires capital ≥ 72.5% of standardized RWA, phasing in from 50% (2025) to 72.5% (2030).",
        "DOC-ECFR-6": "Market risk capital framework for banks with trading assets ≥ 10% of total assets. Currently uses VaR at 99th percentile. Basel IV FRTB replaces VaR with Expected Shortfall at 97.5% with desk-level PLA tests.",
        "DOC-ECFR-7": "Liquidity Coverage Ratio rules requiring HQLA ≥ total net cash outflows over 30-day stress scenario. Level 2B RMBS assets subject to 25–50% haircuts, capped at 15% of total HQLA buffer.",
    }
    print("Updating summaries for current documents ...")
    for doc_id, summary in SUMMARIES.items():
        sf_execute(
            f"UPDATE REGTECH_DEMO_DB.REGULATORY_REPORTING.REGULATORY_DOCUMENTS "
            f"SET SUMMARY = '{esc(summary)}' WHERE DOC_ID = '{doc_id}'",
            account, pat,
        )
    print("  done.\n")

    # ── Step 4: INSERT historical documents ───────────────────────────────────
    print(f"Inserting historical ({year}) documents into Snowflake ...")
    for sec in SECTIONS:
        suffix = sec["doc_id_suffix"]
        hist_id = f"DOC-ECFR-{suffix}-V{year}"
        text = texts.get(suffix, "")
        # Truncate to avoid SQL size limits — first 50k chars is plenty for search
        text_excerpt = text[:50000]
        title_raw = sec["filename"].replace(".txt", "").replace("_", " ")
        sf_execute(
            f"""
            MERGE INTO REGTECH_DEMO_DB.REGULATORY_REPORTING.REGULATORY_DOCUMENTS t
            USING (SELECT '{hist_id}' AS DOC_ID) s ON t.DOC_ID = s.DOC_ID
            WHEN NOT MATCHED THEN INSERT (
                DOC_ID, FRAMEWORK, TITLE, CHAPTER, VERSION, EFFECTIVE_DATE,
                STATUS, RAW_TEXT, PAGE_COUNT, SUMMARY
            ) VALUES (
                '{hist_id}',
                '{esc(sec["framework"])}',
                '{esc(title_raw)}',
                'Historical version {year}',
                '{year}',
                '{date}',
                'Superseded',
                '{esc(text_excerpt)}',
                {len(text)},
                'Historical version ({year}) of {esc(sec["display_name"])}. Fetched from eCFR versioner API for regulatory change analysis.'
            )
            """,
            account, pat,
        )
        print(f"  inserted {hist_id}")
    print()

    # ── Step 5: AI-powered diff — for key sections ────────────────────────────
    # For each topic, extract the RELEVANT section from both historical and current
    # text using keyword search (replaces the old LEFT(4000) SQL approach which
    # only grabbed the boilerplate preamble and produced useless diffs).
    DIFF_SECTIONS = [
        # (sec_suffix, cur_doc_id, hist_doc_id, focus_keywords, rule_name, rule_section, change_type, impacted_report)
        ("2", "DOC-ECFR-2", f"DOC-ECFR-2-V{year}",
         ["conservation buffer", "4.5", "6.0", "8.0", "CET1", "countercyclical", "stress capital"],
         "Capital Conservation Buffer", "12 CFR 3.11", "Updated", "Capital Ratio Reporting"),
        ("3", "DOC-ECFR-3", f"DOC-ECFR-3-V{year}",
         ["deferred tax", "DTA", "10 percent", "17.65", "significant investment", "deduction threshold"],
         "DTA Dual Threshold", "12 CFR 3.22(d)", "Updated", "CET1 Capital Calculation"),
        ("4", "DOC-ECFR-4", f"DOC-ECFR-4-V{year}",
         ["residential mortgage", "risk weight", "loan-to-value", "LTV", "35 percent", "PPP"],
         "Mortgage & Derivatives RWA", "12 CFR 3.32", "Updated", "Credit Risk RWA"),
        ("6", "DOC-ECFR-6", f"DOC-ECFR-6-V{year}",
         ["value-at-risk", "VaR", "expected shortfall", "market risk", "99th percentile", "97.5", "FRTB"],
         "Market Risk — VaR vs FRTB", "12 CFR 3.204", "Replacement", "Market Risk RWA"),
        ("7", "DOC-ECFR-7", f"DOC-ECFR-7-V{year}",
         ["high-quality liquid asset", "HQLA", "Level 2B", "RMBS", "haircut", "25 percent", "50 percent"],
         "LCR Level 2B HQLA", "12 CFR 50.22", "Updated", "LCR HQLA Buffer"),
    ]

    # Current regulation text directory (sibling to the historical directory)
    current_regs_dir = Path(__file__).resolve().parent / "regulations"

    print("Running clause-level COMPLETE() diffs for key requirement changes ...")
    for i, (suffix, cur_id, hist_id, keywords, rule_name, rule_section, change_type, impacted_report) in enumerate(DIFF_SECTIONS, 1):
        req_id = f"REQ-HIST-{i:03d}"
        print(f"  [{i}/{len(DIFF_SECTIONS)}] {rule_name}: {cur_id} vs {hist_id} ...")

        # ── Load historical text (already downloaded in Step 1)
        hist_text = texts.get(suffix, "")
        if not hist_text:
            print(f"    no historical text for suffix {suffix}, skipping")
            continue

        # ── Load current text from local files (avoids Snowflake LEFT() truncation)
        sec_info = next((s for s in SECTIONS if s["doc_id_suffix"] == suffix), None)
        curr_path = current_regs_dir / sec_info["filename"] if sec_info else None
        if curr_path and curr_path.exists():
            curr_text = curr_path.read_text(encoding="utf-8")
        else:
            # Fall back to Snowflake if local file missing
            print(f"    current text not found locally, fetching from Snowflake ...")
            curr_text = sf_query_one(
                f"SELECT LEFT(RAW_TEXT, 12000) FROM REGTECH_DEMO_DB.REGULATORY_REPORTING.REGULATORY_DOCUMENTS WHERE DOC_ID = '{cur_id}'",
                account, pat
            ) or ""

        # ── Extract the most relevant section from each text using keyword scoring
        hist_section = _extract_relevant_section(hist_text, keywords, max_chars=3500)
        curr_section = _extract_relevant_section(curr_text, keywords, max_chars=3500)

        # ── Build the structured diff prompt
        prompt = (
            "You are a US banking regulation expert. Compare these two versions of a "
            "12 CFR regulatory provision and identify what changed. "
            f"Focus specifically on: {', '.join(keywords[:4])}. "
            "Return ONLY a JSON object with these exact fields: "
            '"old_requirement" (1-2 sentences describing the old rule — be specific with '
            'numbers, thresholds, and section references), '
            '"new_requirement" (1-2 sentences describing the current rule — be specific), '
            '"change_summary" (one sentence: what changed and why it matters). '
            "If the texts are identical or the relevant section is not present, say so explicitly.\\n\\n"
            f"=== HISTORICAL VERSION ({year}) — Relevant Section ===\\n"
            "{HIST}\\n\\n"
            "=== CURRENT VERSION (2025) — Relevant Section ===\\n"
            "{CUR}"
        )

        # Escape for SQL and substitute
        hist_escaped = esc(hist_section)
        curr_escaped = esc(curr_section)
        full_prompt = prompt.replace("{HIST}", hist_escaped).replace("{CUR}", curr_escaped)

        diff_sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-6', '{esc(full_prompt)}')"

        try:
            result_json = sf_query_one(diff_sql, account, pat)
            if not result_json:
                print(f"    no result, skipping")
                continue

            # Parse — model may wrap in markdown code block
            clean = re.sub(r"```(?:json)?\s*|\s*```", "", result_json.strip())
            try:
                diff = json.loads(clean)
            except json.JSONDecodeError:
                # Regex fallback
                def _re_field(name: str) -> str:
                    m = re.search(rf'"{name}"\s*:\s*"((?:[^"\\]|\\.)*)"', clean, re.S)
                    return m.group(1).replace('\\"', '"') if m else ""
                diff = {
                    "old_requirement": _re_field("old_requirement"),
                    "new_requirement": _re_field("new_requirement"),
                }

            old_req = diff.get("old_requirement", "")[:2000]
            new_req = (diff.get("new_requirement", "") or result_json[:500])[:2000]

            sf_execute(f"""
                MERGE INTO REGTECH_DEMO_DB.REGULATORY_REPORTING.EXTRACTED_REQUIREMENTS t
                USING (SELECT '{req_id}' AS REQ_ID) s ON t.REQ_ID = s.REQ_ID
                WHEN NOT MATCHED THEN INSERT (
                    REQ_ID, DOC_ID, CATEGORY, REQUIREMENT,
                    SEVERITY, IMPACT_AREA,
                    RULE_NAME, RULE_SECTION,
                    OLD_REQUIREMENT, CHANGE_TYPE, IMPACTED_REPORT
                ) VALUES (
                    '{req_id}', '{cur_id}', 'Regulatory Change',
                    '{esc(new_req)}', 'High', '{esc(impacted_report)}',
                    '{esc(rule_name)}', '{esc(rule_section)}',
                    '{esc(old_req)}', '{esc(change_type)}', '{esc(impacted_report)}'
                )
                WHEN MATCHED THEN UPDATE SET
                    OLD_REQUIREMENT = '{esc(old_req)}',
                    REQUIREMENT     = '{esc(new_req)}',
                    RULE_NAME       = '{esc(rule_name)}',
                    RULE_SECTION    = '{esc(rule_section)}',
                    CHANGE_TYPE     = '{esc(change_type)}',
                    IMPACTED_REPORT = '{esc(impacted_report)}'
            """, account, pat)
            print(f"    ✓ {req_id}: {old_req[:60]}... → {new_req[:60]}...")

        except Exception as e:
            print(f"    ERROR on {req_id}: {e}")

        time.sleep(2)  # be polite to Cortex

    print("\n✓ Done. Historical docs and clause-level AI diffs loaded into Snowflake.")
    print("Restart the Node server to pick up changes: node --env-file=.env server.js")


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch historical eCFR regulation and generate AI-powered diffs."
    )
    parser.add_argument(
        "--date", default="2020-01-01",
        help="Historical date to fetch (YYYY-MM-DD, default: 2020-01-01)",
    )
    parser.add_argument(
        "--fetch-only", action="store_true",
        help="Only download text files, skip Snowflake steps.",
    )
    parser.add_argument(
        "--output-dir", type=Path,
        help="Directory to save files (default: ./regulations/v{YEAR}/)",
    )
    args = parser.parse_args()

    year = args.date[:4]
    out = args.output_dir or Path(__file__).resolve().parent / "regulations" / f"v{year}"

    try:
        run(date=args.date, fetch_only=args.fetch_only, output_dir=out)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
