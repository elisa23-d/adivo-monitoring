from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

from src.database.db import connect

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPORT_DIR = PROJECT_ROOT / "data" / "exports"


def get_latest_two_snapshots() -> tuple[str, str]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT snapshot_id FROM snapshots ORDER BY created_at DESC LIMIT 2"
        ).fetchall()
    if len(rows) < 2:
        raise ValueError("Need at least 2 snapshots to compute 'new since last snapshot'. Run ingest twice.")
    return rows[1][0], rows[0][0]  # (previous, current)


def export_new_pubmed(profile_id: int) -> Path:
    prev_sid, cur_sid = get_latest_two_snapshots()

    with connect() as conn:
        # New docs are those in current snapshot not present in previous snapshot (by doc_id)
        df = pd.read_sql_query(
            """
            WITH prev_docs AS (
                SELECT doc_id FROM documents WHERE snapshot_id = ? AND source = 'pubmed'
            ),
            cur_docs AS (
                SELECT d.doc_id, d.title, d.url, d.published_date, d.publication_type
                FROM documents d
                WHERE d.snapshot_id = ? AND d.source = 'pubmed'
            )
            SELECT
                c.doc_id,
                c.title,
                c.published_date,
                c.publication_type,
                c.url,
                CASE WHEN EXISTS (
                    SELECT 1 FROM competitor_mentions m WHERE m.doc_id = c.doc_id
                ) THEN 1 ELSE 0 END AS has_competitor_affiliation
            FROM cur_docs c
            WHERE c.doc_id NOT IN (SELECT doc_id FROM prev_docs)
            ORDER BY c.published_date DESC
            """,
            conn,
            params=[prev_sid, cur_sid],
        )

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out = EXPORT_DIR / f"new_pubmed_profile{profile_id}_{ts}.csv"
    df.to_csv(out, index=False)
    print(f"✅ exported {len(df)} new items to: {out}")
    print(f"   previous snapshot: {prev_sid}")
    print(f"   current snapshot:  {cur_sid}")
    return out


if __name__ == "__main__":
    export_new_pubmed(profile_id=1)
