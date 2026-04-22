#!/usr/bin/env python3
"""
Fetch US banking regulations from the eCFR (Electronic Code of Federal Regulations) API.

Fetches Title 12 (Banks and Banking) regulation sections and saves them as plain text files
with a JSON metadata sidecar. Uses only Python stdlib — no pip dependencies required.

Usage:
    python fetch_regulations.py            # fetch missing sections only
    python fetch_regulations.py --force    # re-fetch all sections
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

# ---------------------------------------------------------------------------
# Configuration: regulation sections to fetch
# ---------------------------------------------------------------------------

SECTIONS = [
    {
        "part": 3,
        "subpart": "A",
        "filename": "title12_part3_subpartA_general_provisions.txt",
        "description": "General Provisions (Capital Adequacy definitions)",
    },
    {
        "part": 3,
        "subpart": "B",
        "filename": "title12_part3_subpartB_capital_ratios.txt",
        "description": "Capital Ratio Requirements and Buffers (CET1 4.5%, Tier 1 6%, Total 8%)",
    },
    {
        "part": 3,
        "subpart": "C",
        "filename": "title12_part3_subpartC_definition_of_capital.txt",
        "description": "Definition of Capital (deductions, DTA threshold)",
    },
    {
        "part": 3,
        "subpart": "D",
        "filename": "title12_part3_subpartD_rwa_standardized.txt",
        "description": "Risk-Weighted Assets — Standardized Approach (risk weights, derivatives)",
    },
    {
        "part": 3,
        "subpart": "E",
        "filename": "title12_part3_subpartE_rwa_irb.txt",
        "description": "Risk-Weighted Assets — Internal Ratings-Based Approach",
    },
    {
        "part": 3,
        "subpart": "F",
        "filename": "title12_part3_subpartF_rwa_market_risk.txt",
        "description": "Risk-Weighted Assets — Market Risk",
    },
    {
        "part": 50,
        "subpart": "B",
        "filename": "title12_part50_subpartB_lcr_hqla.txt",
        "description": "Liquidity Risk Measurement Standards (LCR, HQLA definitions)",
    },
]

ECFR_CONTENT_URL = (
    "https://www.ecfr.gov/api/renderer/v1/content/enhanced/current/title-12"
)

# ---------------------------------------------------------------------------
# HTML-to-text stripper (stdlib only)
# ---------------------------------------------------------------------------


class _HTMLTextExtractor(HTMLParser):
    """Minimal HTML-to-text converter using the stdlib HTMLParser."""

    def __init__(self):
        super().__init__()
        self._pieces: list[str] = []
        self._skip = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # Skip <script> and <style> content
        if tag in ("script", "style"):
            self._skip = True
        # Insert newlines for block-level elements
        if tag in ("p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6",
                    "li", "tr", "blockquote", "section", "article"):
            self._pieces.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style"):
            self._skip = False
        if tag in ("p", "div", "h1", "h2", "h3", "h4", "h5", "h6",
                    "li", "tr", "blockquote", "section", "article"):
            self._pieces.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self._pieces.append(data)

    def get_text(self) -> str:
        raw = "".join(self._pieces)
        # Collapse runs of whitespace within lines, then collapse blank lines
        lines = [" ".join(line.split()) for line in raw.splitlines()]
        text = "\n".join(lines)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def strip_html(html: str) -> str:
    """Convert HTML to plain text."""
    extractor = _HTMLTextExtractor()
    extractor.feed(html)
    return extractor.get_text()


# ---------------------------------------------------------------------------
# eCFR fetcher
# ---------------------------------------------------------------------------


def fetch_section_html(part: int, subpart: str) -> str:
    """Fetch a regulation section from the eCFR content API. Returns raw HTML."""
    url = f"{ECFR_CONTENT_URL}?part={part}&subpart={subpart}"
    req = urllib.request.Request(url, headers={"Accept": "text/html"})

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"eCFR API returned HTTP {exc.code} for part={part}, subpart={subpart}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Network error fetching part={part}, subpart={subpart}: {exc.reason}"
        ) from exc


def fetch_structure_date(part: int) -> str | None:
    """Fetch the effective/amendment date from the eCFR structure API (best-effort)."""
    url = f"https://www.ecfr.gov/api/versioner/v1/structure/current/title-12.json?part={part}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        # The structure response nests date info at varying depths; grab top-level
        return data.get("meta", {}).get("date") or data.get("date")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def run(output_dir: Path, force: bool = False) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata_path = output_dir / "metadata.json"

    # Load existing metadata if present
    if metadata_path.exists():
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata: dict = json.load(f)
    else:
        metadata = {"title": "Title 12 — Banks and Banking", "sections": {}}

    # Cache structure dates per part so we don't re-fetch
    structure_dates: dict[int, str | None] = {}

    total = len(SECTIONS)
    for idx, section in enumerate(SECTIONS, start=1):
        part = section["part"]
        subpart = section["subpart"]
        filename = section["filename"]
        dest = output_dir / filename

        label = f"[{idx}/{total}] Part {part}, Subpart {subpart}"

        if dest.exists() and not force:
            print(f"{label} — already exists, skipping ({filename})")
            continue

        print(f"{label} — fetching {section['description']} ...")

        html = fetch_section_html(part, subpart)
        text = strip_html(html)

        with open(dest, "w", encoding="utf-8") as f:
            f.write(text)

        print(f"         saved {filename} ({len(text):,} chars)")

        # Fetch structure date if we haven't yet for this part
        if part not in structure_dates:
            structure_dates[part] = fetch_structure_date(part)

        # Update metadata
        metadata.setdefault("sections", {})[filename] = {
            "title": f"Title 12, Part {part}, Subpart {subpart}",
            "description": section["description"],
            "part": part,
            "subpart": subpart,
            "effective_date": structure_dates.get(part),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "char_count": len(text),
        }

        # Be polite to the API
        if idx < total:
            time.sleep(1)

    # Write metadata
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"\nMetadata written to {metadata_path}")
    print("Done.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch US banking regulations from the eCFR API."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-fetch all sections even if files already exist.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "regulations",
        help="Directory to save regulation text files (default: ./regulations/).",
    )
    args = parser.parse_args()

    try:
        run(output_dir=args.output_dir, force=args.force)
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)
