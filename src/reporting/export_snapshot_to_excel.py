from __future__ import annotations

"""
CLI tool to export a snapshot's results into an Excel-friendly CSV file.

For each document in a given snapshot, this script outputs:
- link (PubMed URL)
- article_title
- study_summary     (for now, the abstract; can later be replaced by an AI summary)
- competitors       (comma-separated canonical competitor names, if any)

Usage examples:

  # Export a specific snapshot by ID
  python -m src.reporting.export_snapshot_to_excel --snapshot-id 2026-02-16T12-48-37Z

  # Export the most recent snapshot
  python -m src.reporting.export_snapshot_to_excel --latest

By default, the CSV is written to:
  data/exports/<snapshot_id>.csv
You can override this with --output /path/to/file.csv
"""

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from src.database.db import connect


PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"


@dataclass
class ExportRow:
    link: str
    article_title: str
    study_summary: str
    slide_summary: str
    competitors: str
    source: str  # pubmed / ctgov / etc.


def get_latest_snapshot_id() -> Optional[str]:
    """Return the most recently created snapshot_id, or None if no snapshots exist."""
    with connect() as conn:
        row = conn.execute(
            """
            SELECT snapshot_id
            FROM snapshots
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
    return row[0] if row else None


def fetch_rows_for_snapshot(snapshot_id: str) -> list[ExportRow]:
    """
    Build export rows for a snapshot.

    IMPORTANT:
    - We only include documents that have at least one competitor mention
      (i.e. sponsor in the COMPETITORS list), matching what you see in the
      CLI \"filtered\" output.

    Currently uses the abstract as a proxy for study summary.
    Later we can join to a document_summaries table instead.
    """
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT
              d.title,
              d.abstract,
              d.url,
              COALESCE(ds.study_summary, d.abstract, '') AS study_summary,
              COALESCE(ds.slide_summary, '') AS slide_summary,
              COALESCE(GROUP_CONCAT(DISTINCT c.canonical_name), '') AS competitors,
              d.source
            FROM documents d
            JOIN competitor_mentions cm ON cm.doc_id = d.doc_id
            JOIN competitors c ON c.competitor_id = cm.competitor_id
            LEFT JOIN document_summaries ds ON ds.doc_id = d.doc_id
            WHERE d.snapshot_id = ?
            GROUP BY d.doc_id, d.title, d.abstract, d.url, ds.study_summary, ds.slide_summary, d.source
            ORDER BY d.published_date DESC, d.doc_id
            """,
            (snapshot_id,),
        ).fetchall()

    export_rows: list[ExportRow] = []
    for title, abstract, url, study_summary_db, slide_summary_db, competitors, source in rows:
        # Fallbacks for safety
        link = url or ""
        article_title = title or ""
        # Prefer LLM summary; fall back to abstract for study_summary
        study_summary = study_summary_db or abstract or ""
        # Prefer LLM slide_summary; if missing, show study_summary so column is never empty (same behaviour as study_summary)
        slide_summary = slide_summary_db or study_summary or ""
        competitors_str = competitors or ""

        # Human-friendly source label
        s = (source or "").strip().lower()
        source_label = "ClinicalTrials.gov" if s == "ctgov" else ("PubMed" if s == "pubmed" else (source or "PubMed"))

        export_rows.append(
            ExportRow(
                link=link,
                article_title=article_title,
                study_summary=study_summary,
                slide_summary=slide_summary,
                competitors=competitors_str,
                source=source_label,
            )
        )

    return export_rows


def write_csv(rows: list[ExportRow], output_path: Path) -> None:
    """Write export rows to a CSV file with a fixed header."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["link", "article_title", "study_summary", "slide_summary", "competitors", "source"]

    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "link": row.link,
                    "article_title": row.article_title,
                    "study_summary": row.study_summary,
                    "slide_summary": row.slide_summary,
                    "competitors": row.competitors,
                    "source": row.source,
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a snapshot's PubMed results to an Excel-friendly CSV file."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--snapshot-id",
        help="Snapshot ID to export, e.g. 2026-02-16T12-48-37Z",
    )
    group.add_argument(
        "--latest",
        action="store_true",
        help="Export the most recently created snapshot.",
    )
    parser.add_argument(
        "--output",
        help="Output CSV path. Defaults to data/exports/<snapshot_id>.csv",
    )

    args = parser.parse_args()

    if args.latest:
        snapshot_id = get_latest_snapshot_id()
        if not snapshot_id:
            raise SystemExit("No snapshots found in the database.")
        print(f"Using latest snapshot_id = {snapshot_id}")
    else:
        snapshot_id = args.snapshot_id

    rows = fetch_rows_for_snapshot(snapshot_id)
    print(f"Found {len(rows)} documents for snapshot {snapshot_id}")

    if args.output:
        output_path = Path(args.output)
    else:
        # Safe filename version of snapshot_id
        safe_snapshot = snapshot_id.replace(":", "-")
        output_path = EXPORT_DIR / f"{safe_snapshot}.csv"

    write_csv(rows, output_path)
    print(f"✅ Exported to {output_path}")


if __name__ == "__main__":
    main()

