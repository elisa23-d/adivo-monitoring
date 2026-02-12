from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "adivo_evidence.db"
SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"schema.sql not found at: {SCHEMA_PATH}")

    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")

    conn = connect(DB_PATH)
    try:
        conn.executescript(schema_sql)
        conn.commit()
        # Migration: add epub_date to existing documents table if missing
        cur = conn.execute("PRAGMA table_info(documents)")
        columns = [row[1] for row in cur.fetchall()]
        if "epub_date" not in columns:
            conn.execute("ALTER TABLE documents ADD COLUMN epub_date TEXT")
            conn.commit()
    finally:
        conn.close()

    print(f"✅ SQLite database ready: {DB_PATH}")


def ensure_snapshot(snapshot_id: str | None = None, notes: str | None = None) -> str:
    """
    Creates a snapshot row if it doesn't exist.
    Returns the snapshot_id.
    """
    if snapshot_id is None:
        snapshot_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")

    created_at = datetime.now(timezone.utc).isoformat()

    conn = connect(DB_PATH)
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO snapshots (snapshot_id, created_at, notes)
            VALUES (?, ?, ?)
            """,
            (snapshot_id, created_at, notes),
        )
        conn.commit()
    finally:
        conn.close()

    return snapshot_id


if __name__ == "__main__":
    init_db()
    sid = ensure_snapshot(notes="Initial DB creation snapshot")
    print(f"✅ Snapshot ensured: {sid}")
