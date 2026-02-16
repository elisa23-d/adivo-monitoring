from __future__ import annotations

from datetime import datetime, timezone
from src.database.db import connect


# Matching is case-insensitive substring (alias.lower() in affiliation). So we only need
# one form per distinct name: no lowercase copies, no longer forms that contain a shorter
# alias (e.g. "AbbVie" already matches "AbbVie Inc", "abbvie inc pharmaceuticals").
# Include the canonical name in the alias list so it is matched in text.
COMPETITORS = {
    "Accro Bioscience": ["Accro Bioscience"],
    "AbbVie": ["AbbVie"],
    "AkesoBio": ["AkesoBio"],
    "Almirall": ["Almirall"],
    "Alumnis Inc": ["Alumnis Inc"],
    "Amgen": ["Amgen"],
    "AnaptysBio": ["AnaptysBio"],
    "Artax Biopharma": ["Artax Biopharma"],
    "AstraZeneca": ["AstraZeneca"],
    "Bausch Health": ["Bausch Health"],
    "Beijing InnoCare Pharma": ["Beijing InnoCare Pharma"],
    "Biocad": ["Biocad", "Biocad (Russia)"],
    "Bristol-Myers Squibb": ["Bristol-Myers Squibb", "BMS"],
    "Can-Fite Biopharma": ["Can-Fite Biopharma"],
    "Chia Tai Tianqing Pharmaceutical Group Co": [
        "Chia Tai Tianqing Pharmaceutical Group Co"
    ],
    "Chipscreen Biosciences": ["Chipscreen Biosciences"],
    "Chongqing Genrix Biopharmaceutical": ["Chongqing Genrix Biopharmaceutical"],
    "Clarent Biopharma": ["Clarent Biopharma"],
    "CSPC Ouyi Pharmaceutical": ["CSPC Ouyi Pharmaceutical"],
    "DICE Therapeutics": ["DICE Therapeutics"],
    "Eli Lilly": ["Eli Lilly", "Lilly"],
    "GlaxoSmithKline": ["GlaxoSmithKline", "GSK"],
    "Hangzhou H. Pharma": ["Hangzhou H. Pharma"],
    "Hansoh Pharmaceutical": [
        "Hansoh Pharmaceutical"],
    "Huabo Biopharm": ["Huabo Biopharm"],
    "Innovent Biologics": ["Innovent Biologics"],
    "InventisBio": ["InventisBio", "InventisBio Co"],
    "Io Therapeutics": ["Io Therapeutics"],
    "Jiangsu Hengrui Pharma": ["Jiangsu Hengrui Pharma"],
    "Johnson & Johnson": [
        "Johnson & Johnson",
        "J&J",
        "J and J",
        "Janssen",
        "Johnson",
    ],
    "LEO Pharma": ["LEO Pharma"],
    "Livzon Pharma": ["Livzon Pharma"],
    "Meiji Seika Pharma": ["Meiji Seika Pharma"],
    "Merck": ["Merck"],
    "MoonLake Immunotherapeutics": ["MoonLake Immunotherapeutics"],
    "Novartis": ["Novartis"],
    "Novo Nordisk": ["Novo Nordisk"],
    "Oruka Therapeutics": ["Oruka Therapeutics", "Oruka Tx"],
    "Pfizer": ["Pfizer"],
    "Protagonist Therapeutics": ["Protagonist Therapeutics", "Protagonist Tx"],
    "Qyuns Therapeutics": ["Qyuns Therapeutics"],
    "Roche": ["Roche"],
    "Sanofi": ["Sanofi"],
    "SFA Therapeutics": ["SFA Therapeutics"],
    "Shanghai Huaota Biopharmaceutical": [
        "Shanghai Huaota Biopharmaceutical",
        "Shanghai Huaota Biopharmaceutical Co",
    ],
    "Shanghai Junshi Bioscience": ["Shanghai Junshi Bioscience"],
    "Sun Pharmaceutical Industries": ["Sun Pharma"],
    "Sunshine Guojian Pharma": ["Sunshine Guojian Pharma"],
    "Takeda": ["Takeda"],
    "UCB": ["UCB"],
    "Usynova Pharmaceuticals": ["Usynova Pharmaceuticals"],
    "Vanda Pharmaceuticals": ["Vanda Pharmaceuticals"],
    "Zhejiang Wenda Medical Technology": ["Zhejiang Wenda Medical Technology"],
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
