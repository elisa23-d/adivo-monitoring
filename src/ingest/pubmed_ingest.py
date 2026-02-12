from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
import xml.etree.ElementTree as ET

import requests

from src.database.db import connect
from src.database.init_db import ensure_snapshot

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"


@dataclass
class PubMedRecord:
    pmid: str
    title: str
    abstract: str
    pub_date: Optional[str]         # best-effort ISO yyyy-mm-dd or yyyy-mm or yyyy (often print)
    epub_date: Optional[str]       # electronic first publication (PubMed History PubStatus=epub)
    publication_types: list[str]
    affiliations: list[str]
    url: str


def _safe_text(el: Optional[ET.Element]) -> str:
    if el is None:
        return ""
    return " ".join("".join(el.itertext()).split())


def _parse_pub_date(article: ET.Element) -> Optional[str]:
    """
    Best-effort: returns ISO-like string.
    PubMed date formats vary a lot; we keep a conservative parse.
    """
    # Try ArticleDate first (often most specific)
    ad = article.find(".//ArticleDate")
    if ad is not None:
        y = _safe_text(ad.find("Year"))
        m = _safe_text(ad.find("Month"))
        d = _safe_text(ad.find("Day"))
        if y and m and d:
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        if y and m:
            return f"{y}-{m.zfill(2)}"
        if y:
            return y

    # Then try Journal PubDate
    pd = article.find(".//Journal/JournalIssue/PubDate")
    if pd is not None:
        y = _safe_text(pd.find("Year"))
        m = _safe_text(pd.find("Month"))
        d = _safe_text(pd.find("Day"))
        medline = _safe_text(pd.find("MedlineDate"))

        # Month may be "Jan" etc; keep simple if numeric, else drop to year
        def month_to_num(mm: str) -> Optional[str]:
            mm = mm.strip()
            if mm.isdigit():
                return mm.zfill(2)
            mapping = {
                "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
                "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
                "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
            }
            return mapping.get(mm[:3])

        if y:
            mn = month_to_num(m) if m else None
            if mn and d and d.isdigit():
                return f"{y}-{mn}-{d.zfill(2)}"
            if mn:
                return f"{y}-{mn}"
            return y

        # fallback: medline date like "2024 Jan-Feb"
        if medline:
            # best effort: extract first 4 digits as year
            for token in medline.split():
                if token.isdigit() and len(token) == 4:
                    return token
    return None


def _parse_epub_date(pubmed_article: ET.Element) -> Optional[str]:
    """
    Parse electronic publication date from PubMedData/History/PubMedPubDate PubStatus="epub".
    Returns ISO yyyy-mm-dd or None if missing.
    """
    for pub_date_el in pubmed_article.findall(".//PubmedData/History/PubMedPubDate"):
        if pub_date_el.get("PubStatus") != "epub":
            continue
        y = _safe_text(pub_date_el.find("Year"))
        m = _safe_text(pub_date_el.find("Month"))
        d = _safe_text(pub_date_el.find("Day"))
        if y and m and d:
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        if y and m:
            return f"{y}-{m.zfill(2)}"
        if y:
            return y
        break
    return None


def _parse_date_to_ordinal(s: Optional[str]) -> Optional[int]:
    """Best-effort: convert our date string to a comparable ordinal (days since epoch). Returns None if unparseable."""
    if not s or not s.strip():
        return None
    parts = s.strip().split("-")
    if len(parts) >= 1 and parts[0].isdigit():
        year = int(parts[0])
        month = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else 1
        day = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 1
        try:
            return datetime(year, month, day, tzinfo=timezone.utc).date().toordinal()
        except (ValueError, TypeError):
            pass
    return None


def esearch(query: str, retmax: int = 200) -> list[str]:
    # Define explicit [start, end] window: last 30 days by publication date
    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=30)
    # PubMed eutils expects YYYY/MM/DD format for mindate/maxdate
    mindate = start_date.strftime("%Y/%m/%d")
    maxdate = today.strftime("%Y/%m/%d")

    params = {
        "db": "pubmed",
        "term": query,
        "retmax": str(retmax),
        "retmode": "json",
        "sort": "pub+date",  # sort by publication date
        # Explicit publication date window: last 30 days
        "datetype": "pdat",
        "mindate": mindate,
        "maxdate": maxdate,
    }
    r = requests.get(f"{EUTILS_BASE}/esearch.fcgi", params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data["esearchresult"].get("idlist", [])


def efetch(pmids: list[str]) -> str:
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
    }
    r = requests.get(f"{EUTILS_BASE}/efetch.fcgi", params=params, timeout=60)
    r.raise_for_status()
    return r.text


def parse_pubmed_xml(xml_text: str) -> list[PubMedRecord]:
    root = ET.fromstring(xml_text)
    records: list[PubMedRecord] = []

    for pubmed_article in root.findall(".//PubmedArticle"):
        pmid = _safe_text(pubmed_article.find(".//PMID"))
        article = pubmed_article.find(".//Article")
        if not pmid or article is None:
            continue

        title = _safe_text(article.find(".//ArticleTitle"))

        # Abstract may have multiple AbstractText sections
        abstract_parts = []
        for ab in article.findall(".//Abstract/AbstractText"):
            abstract_parts.append(_safe_text(ab))
        abstract = "\n".join([p for p in abstract_parts if p]).strip()

        pub_date = _parse_pub_date(article)

        publication_types = [
            _safe_text(pt) for pt in article.findall(".//PublicationTypeList/PublicationType")
        ]
        publication_types = [pt for pt in publication_types if pt]

        affiliations = []
        for aff in article.findall(".//AffiliationInfo/Affiliation"):
            txt = _safe_text(aff)
            if txt:
                affiliations.append(txt)

        epub_date = _parse_epub_date(pubmed_article)
        url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

        records.append(
            PubMedRecord(
                pmid=pmid,
                title=title,
                abstract=abstract,
                pub_date=pub_date,
                epub_date=epub_date,
                publication_types=publication_types,
                affiliations=affiliations,
                url=url,
            )
        )
    return records


def upsert_pubmed_records(records: list[PubMedRecord], snapshot_id: str) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    raw_base = RAW_DIR / snapshot_id / "pubmed"
    raw_base.mkdir(parents=True, exist_ok=True)

    with connect() as conn:
        for rec in records:
            doc_id = f"PMID:{rec.pmid}"
            raw_path = raw_base / f"{rec.pmid}.json"

            # Save raw JSON (provenance)
            raw_path.write_text(
                json.dumps(
                    {
                        "pmid": rec.pmid,
                        "title": rec.title,
                        "abstract": rec.abstract,
                        "pub_date": rec.pub_date,
                        "epub_date": rec.epub_date,
                        "publication_types": rec.publication_types,
                        "affiliations": rec.affiliations,
                        "url": rec.url,
                        "snapshot_id": snapshot_id,
                        "ingested_at": now_iso,
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            publication_type = rec.publication_types[0] if rec.publication_types else None

            conn.execute(
                """
                INSERT OR REPLACE INTO documents
                (doc_id, source, snapshot_id, title, abstract, url, published_date, epub_date, entry_date, publication_type, raw_json_path)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    doc_id,
                    "pubmed",
                    snapshot_id,
                    rec.title,
                    rec.abstract,
                    rec.url,
                    rec.pub_date,
                    rec.epub_date,
                    now_iso,
                    publication_type,
                    str(raw_path),
                ),
            )

            # Refresh affiliations: delete then insert (simple v1)
            conn.execute("DELETE FROM affiliations WHERE doc_id = ?", (doc_id,))
            for aff in rec.affiliations:
                conn.execute(
                    "INSERT INTO affiliations (doc_id, affiliation_text) VALUES (?, ?)",
                    (doc_id, aff),
                )

        conn.commit()


def tag_competitors(snapshot_id: str) -> int:
    """
    Naive inclusive tagging:
    - For each affiliation, if any alias appears (case-insensitive substring), record a mention.
    - We prefer recall over precision.
    """
    with connect() as conn:
        aliases = conn.execute(
            """
            SELECT ca.alias, c.competitor_id
            FROM competitor_aliases ca
            JOIN competitors c ON c.competitor_id = ca.competitor_id
            """
        ).fetchall()

        # Get docs ingested in this snapshot
        aff_rows = conn.execute(
            """
            SELECT a.doc_id, a.affiliation_text
            FROM affiliations a
            JOIN documents d ON d.doc_id = a.doc_id
            WHERE d.snapshot_id = ?
            """,
            (snapshot_id,),
        ).fetchall()

        count = 0
        for doc_id, aff_text in aff_rows:
            aff_lower = aff_text.lower()
            for alias, competitor_id in aliases:
                if alias.lower() in aff_lower:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO competitor_mentions
                        (doc_id, competitor_id, match_text, mention_type)
                        VALUES (?, ?, ?, 'affiliation')
                        """,
                        (doc_id, competitor_id, alias),
                    )
                    count += 1

        conn.commit()
        return count


def ingest_profile(profile_id: int, snapshot_id: Optional[str] = None, retmax: int = 200) -> str:
    with connect() as conn:
        row = conn.execute(
            "SELECT query_terms FROM monitoring_profiles WHERE profile_id = ? AND is_active = 1",
            (profile_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Active profile not found: {profile_id}")
        query = row[0]

    snapshot_id = ensure_snapshot(snapshot_id=snapshot_id, notes=f"PubMed ingest for profile {profile_id}")
    print(f"📌 snapshot_id = {snapshot_id}")
    print(f"🔎 PubMed query = {query}")

    pmids = esearch(query=query, retmax=retmax)
    print(f"📥 esearch returned {len(pmids)} PMIDs")

    # Same 30-day window as esearch: filter by first publication (epub when present, else print)
    today = datetime.now(timezone.utc).date()
    start_date = today - timedelta(days=30)
    start_ord = start_date.toordinal()
    end_ord = today.toordinal()

    # Fetch in batches (PubMed efetch limit practicalities)
    all_records: list[PubMedRecord] = []
    batch_size = 100
    for i in range(0, len(pmids), batch_size):
        batch = pmids[i : i + batch_size]
        xml_text = efetch(batch)
        records = parse_pubmed_xml(xml_text)
        all_records.extend(records)
        time.sleep(0.34)  # be polite to NCBI

    # Keep only records whose first publication (epub or print) falls in the last 30 days
    in_window: list[PubMedRecord] = []
    for rec in all_records:
        first_pub = rec.epub_date or rec.pub_date
        ord_val = _parse_date_to_ordinal(first_pub)
        if ord_val is None:
            in_window.append(rec)  # unparseable date: keep (don't drop)
        elif start_ord <= ord_val <= end_ord:
            in_window.append(rec)
        # else: first publication outside window (e.g. republished in 2026 but Epub 2025) -> drop

    dropped = len(all_records) - len(in_window)
    if dropped:
        print(f"📅 filtered out {dropped} papers with first publication outside last 30 days (epub/print)")
    print(f"🧾 ingesting {len(in_window)} PubMed records")
    upsert_pubmed_records(in_window, snapshot_id=snapshot_id)

    mentions = tag_competitors(snapshot_id=snapshot_id)
    print(f"🏷️ competitor mentions inserted (affiliation matches): {mentions}")

    # Update last_snapshot_id on profile
    with connect() as conn:
        conn.execute(
            "UPDATE monitoring_profiles SET last_snapshot_id = ? WHERE profile_id = ?",
            (snapshot_id, profile_id),
        )
        conn.commit()

    return snapshot_id


if __name__ == "__main__":
    # Default run: most recently created active profile
    with connect() as conn:
        row = conn.execute(
            """
            SELECT profile_id
            FROM monitoring_profiles
            WHERE is_active = 1
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()

    if not row:
        raise SystemExit("No active monitoring profiles found. Create one with src.monitoring.create_profile.")

    latest_profile_id = row[0]
    sid = ingest_profile(profile_id=latest_profile_id, retmax=200)
    print(f"✅ Done. snapshot_id={sid} (profile_id={latest_profile_id})")
