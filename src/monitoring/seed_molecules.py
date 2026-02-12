from __future__ import annotations

from src.database.db import connect

MOLECULES = [
    # Example for your first test:
    ("guselkumab", "guselkumab|Gulsekumab"),
    # Add more later
]


def main() -> None:
    with connect() as conn:
        for name, synonyms in MOLECULES:
            conn.execute(
                "INSERT OR IGNORE INTO molecules (name, synonyms) VALUES (?, ?)",
                (name, synonyms),
            )
            print(f"✅ molecule loaded: {name} ({synonyms})")


if __name__ == "__main__":
    main()
