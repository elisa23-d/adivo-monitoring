from __future__ import annotations

from datetime import datetime, timezone
from src.database.db import connect


COMPETITORS = {
    "Johnson & Johnson": [
        "Johnson & Johnson",
        "J&J",
        "Johnson and Johnson",
        "Janssen",
        "Janssen Research & Development",
        "Janssen R&D",
        "Janssen Biotech",
        "Janssen Pharmaceutica",
        "Janssen-Cilag",
        "Janssen Pharmaceuticals",
        "J&J Innovative Medicine",
        "Johnson & Johnson Innovative Medicine",
    ],
    # Add more competitors as needed:
    # "AbbVie": ["Abbvie", "AbbVie Inc", ...],
    # "UCB": ["UCB Pharma", ...],
}


def upsert_competitor(canonical_name: str) -> int:
    with connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO competitors (canonical_name) VALUES (?)",
            (canonical_name,),
        )
        competitor_id = conn.execute(
            "SELECT competitor_id FROM competitors WHERE canonical_name = ?",
            (canonical_name,),
        ).fetchone()[0]
    return competitor_id


def add_aliases(competitor_id: int, aliases: list[str]) -> None:
    with connect() as conn:
        for alias in aliases:
            conn.execute(
                "INSERT OR IGNORE INTO competitor_aliases (competitor_id, alias) VALUES (?, ?)",
                (competitor_id, alias),
            )


def main() -> None:
    for canonical, aliases in COMPETITORS.items():
        cid = upsert_competitor(canonical)
        add_aliases(cid, aliases)
        print(f"✅ {canonical}: {len(aliases)} aliases loaded")


if __name__ == "__main__":
    main()
