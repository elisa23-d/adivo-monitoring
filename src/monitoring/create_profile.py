from __future__ import annotations

from datetime import datetime, timezone
from src.database.db import connect


def main() -> None:
    profile_name = "Guselkumab – Psoriasis (daily monitoring)"
    molecule_name = "guselkumab"

    # Simple v1 query_terms: you’ll refine later
    query_terms = 'guselkumab AND psoriasis'

    created_at = datetime.now(timezone.utc).isoformat()

    with connect() as conn:
        row = conn.execute(
            "SELECT molecule_id FROM molecules WHERE name = ?",
            (molecule_name,),
        ).fetchone()
        if not row:
            raise ValueError(f"molecule not found in DB: {molecule_name}")

        molecule_id = row[0]

        conn.execute(
            """
            INSERT INTO monitoring_profiles (name, molecule_id, query_terms, frequency, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (profile_name, molecule_id, query_terms, "daily", created_at),
        )
        profile_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    print(f"✅ profile created: {profile_name} (id={profile_id})")


if __name__ == "__main__":
    main()
