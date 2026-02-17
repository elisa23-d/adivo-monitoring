"""
Ingest clinical trials from ClinicalTrials.gov API v2 into the evidence DB.

Stores trials as documents with source='ctgov', doc_id='NCT:NCT01234567'.
Sponsor and collaborator names are stored in affiliations so the shared
tag_competitors() logic marks competitor-sponsored trials.

Usage:
  python -m src.ingest.clinicaltrials_ingest --condition psoriasis [--intervention ustekinumab] [--snapshot-id ...]
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import requests

from src.database.db import connect
from src.database.init_db import ensure_snapshot
from src.ingest.pubmed_ingest import tag_competitors

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

CT_API_BASE = "https://clinicaltrials.gov/api/v2/studies"


@dataclass
class CTRecord:
    nct_id: str
    title: str
    abstract: str  # brief_summary + optional detailed_description (trimmed)
    url: str
    start_date: Optional[str]
    completion_date: Optional[str]
    first_posted: Optional[str]
    last_updated: Optional[str]
    phases: list[str]
    conditions: list[str]
    lead_sponsor_name: str
    collaborator_names: list[str]


def _safe_get(obj: Any, *keys: str, default: Any = None) -> Any:
    for key in keys:
        if obj is None:
            return default
        obj = obj.get(key)
    return obj if obj is not None else default


def _date_from_struct(struct: Optional[dict]) -> Optional[str]:
    if not struct:
        return None
    d = struct.get("date")
    return d if isinstance(d, str) else None


def _parse_ct_date_to_ordinal(date_str: Optional[str]) -> Optional[int]:
    """
    Parse a ClinicalTrials.gov date string (YYYY, YYYY-MM, or YYYY-MM-DD) to a date ordinal.
    Returns None if parsing fails.
    """
    if not date_str:
        return None
    from datetime import datetime as _dt

    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return _dt.strptime(date_str, fmt).date().toordinal()
        except ValueError:
            continue
    return None


def _parse_study(study: dict) -> Optional[CTRecord]:
    """Parse one study from API v2 JSON into CTRecord."""
    proto = study.get("protocolSection") or {}
    ident = proto.get("identificationModule") or {}
    status = proto.get("statusModule") or {}
    desc = proto.get("descriptionModule") or {}
    design = proto.get("designModule") or {}
    conditions_mod = proto.get("conditionsModule") or {}
    sponsor_mod = proto.get("sponsorCollaboratorsModule") or {}

    nct_id = ident.get("nctId")
    if not nct_id:
        return None

    brief = (desc.get("briefSummary") or "").strip()
    detailed = (desc.get("detailedDescription") or "").strip()
    abstract = brief
    if detailed and len(abstract) + len(detailed) < 12000:
        abstract = f"{brief}\n\n{detailed}" if brief else detailed
    elif len(abstract) > 8000:
        abstract = abstract[:8000] + "…"

    title = ident.get("officialTitle") or ident.get("briefTitle") or ""

    lead = sponsor_mod.get("leadSponsor") or {}
    lead_name = lead.get("name") or ""
    collab = sponsor_mod.get("collaborators") or []
    collaborator_names = [c.get("name") for c in collab if c.get("name")]

    phases = design.get("phases") or []
    conditions = conditions_mod.get("conditions") or []

    return CTRecord(
        nct_id=nct_id,
        title=title,
        abstract=abstract,
        url=f"https://clinicaltrials.gov/study/{nct_id}",
        start_date=_date_from_struct(status.get("startDateStruct")),
        completion_date=_date_from_struct(status.get("completionDateStruct")),
        first_posted=_date_from_struct(status.get("studyFirstPostDateStruct")),
        last_updated=_date_from_struct(status.get("lastUpdatePostDateStruct")),
        phases=phases,
        conditions=conditions,
        lead_sponsor_name=lead_name,
        collaborator_names=collaborator_names,
    )


def search_studies(
    condition: str,
    intervention: Optional[str] = None,
    page_size: int = 100,
    max_studies: Optional[int] = 500,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> list[CTRecord]:
    """
    Query ClinicalTrials.gov API v2 by condition and optional intervention.
    Returns list of CTRecord (parsed). Paginates until no nextPageToken or max_studies.
    """
    params: dict[str, Any] = {
        "query.cond": condition,
        "pageSize": page_size,
    }
    if intervention:
        # API v2: query.int/query.incr are not accepted (400). Use query.term to narrow by drug/intervention.
        params["query.term"] = intervention

    records: list[CTRecord] = []
    page_token: Optional[str] = None

    while True:
        if page_token:
            params["pageToken"] = page_token
        resp = requests.get(CT_API_BASE, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for study in data.get("studies") or []:
            rec = _parse_study(study)
            if rec:
                records.append(rec)
            if max_studies and len(records) >= max_studies:
                # Apply date filter before returning early
                if start_date and end_date:
                    start_ord = _parse_ct_date_to_ordinal(start_date)
                    end_ord = _parse_ct_date_to_ordinal(end_date)
                    if start_ord is not None and end_ord is not None:
                        filtered: list[CTRecord] = []
                        for r in records:
                            key_date = r.first_posted or r.start_date
                            ord_val = _parse_ct_date_to_ordinal(key_date)
                            # Keep unparseable dates rather than silently dropping.
                            if ord_val is None or (start_ord <= ord_val <= end_ord):
                                filtered.append(r)
                        records = filtered
                return records

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)

    # Final page: apply optional date filter.
    if start_date and end_date:
        start_ord = _parse_ct_date_to_ordinal(start_date)
        end_ord = _parse_ct_date_to_ordinal(end_date)
        if start_ord is not None and end_ord is not None:
            filtered = []
            for r in records:
                key_date = r.first_posted or r.start_date
                ord_val = _parse_ct_date_to_ordinal(key_date)
                if ord_val is None or (start_ord <= ord_val <= end_ord):
                    filtered.append(r)
            records = filtered

    return records


def upsert_ctgov_records(records: list[CTRecord], snapshot_id: str) -> None:
    """Insert or replace documents and affiliations for CT.gov trials."""
    now_iso = datetime.now(timezone.utc).isoformat()
    raw_base = RAW_DIR / snapshot_id / "ctgov"
    raw_base.mkdir(parents=True, exist_ok=True)

    with connect() as conn:
        for rec in records:
            doc_id = f"NCT:{rec.nct_id}"
            raw_path = raw_base / f"{rec.nct_id}.json"

            raw_payload = {
                "nct_id": rec.nct_id,
                "title": rec.title,
                "lead_sponsor_name": rec.lead_sponsor_name,
                "collaborator_names": rec.collaborator_names,
                "conditions": rec.conditions,
                "phases": rec.phases,
                "snapshot_id": snapshot_id,
                "ingested_at": now_iso,
            }
            raw_path.write_text(
                json.dumps(raw_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

            # Use first_posted or start_date as published_date for ordering
            published_date = rec.first_posted or rec.start_date

            conn.execute(
                """
                INSERT OR REPLACE INTO documents
                (doc_id, source, snapshot_id, title, abstract, url, published_date, entry_date, last_updated, raw_json_path)
                VALUES (?, 'ctgov', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    doc_id,
                    snapshot_id,
                    rec.title,
                    rec.abstract or None,
                    rec.url,
                    published_date,
                    now_iso,
                    rec.last_updated,
                    str(raw_path),
                ),
            )

            # Affiliations: one row per sponsor/collaborator so tag_competitors can match
            conn.execute("DELETE FROM affiliations WHERE doc_id = ?", (doc_id,))
            seen: set[str] = set()
            for name in [rec.lead_sponsor_name] + rec.collaborator_names:
                name = (name or "").strip()
                if name and name not in seen:
                    seen.add(name)
                    conn.execute(
                        "INSERT INTO affiliations (doc_id, affiliation_text) VALUES (?, ?)",
                        (doc_id, name),
                    )

        conn.commit()


def one_off_ctgov_search(
    condition: str,
    intervention: Optional[str] = None,
    snapshot_id: Optional[str] = None,
    max_studies: int = 500,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:
    """
    Run a ClinicalTrials.gov search, ingest into DB, tag competitors.
    Returns snapshot_id (new or existing).
    """
    window_note = ""
    if start_date and end_date:
        window_note = f", window={start_date}..{end_date}"

    snapshot_id = ensure_snapshot(
        snapshot_id=snapshot_id,
        notes=(
            f"ClinicalTrials.gov ingest: condition={condition!r}"
            + (f", intervention={intervention!r}" if intervention else "")
            + window_note
        ),
    )
    print(f"📌 snapshot_id = {snapshot_id}")
    print(
        f"🔎 CT.gov condition = {condition!r}"
        + (f", intervention = {intervention!r}" if intervention else "")
        + (f", window = {start_date}..{end_date}" if start_date and end_date else "")
    )

    records = search_studies(
        condition=condition,
        intervention=intervention,
        max_studies=max_studies,
        start_date=start_date,
        end_date=end_date,
    )
    print(f"📥 API returned {len(records)} trials")

    if not records:
        return snapshot_id

    upsert_ctgov_records(records, snapshot_id=snapshot_id)
    mentions = tag_competitors(snapshot_id=snapshot_id)
    print(f"🏷️ competitor mentions (sponsor/collaborator matches): {mentions}")

    return snapshot_id


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description="Ingest ClinicalTrials.gov studies by condition (and optional intervention)")
    p.add_argument("--condition", required=True, help="Condition search term (e.g. psoriasis)")
    p.add_argument("--intervention", default=None, help="Optional intervention/drug name")
    p.add_argument("--snapshot-id", default=None, help="Snapshot to add to (default: create new)")
    p.add_argument("--max-studies", type=int, default=500, help="Max number of studies to fetch (default 500)")
    p.add_argument(
        "--start-date",
        default=None,
        help="Optional earliest first-posted date (YYYY-MM-DD, YYYY-MM, or YYYY).",
    )
    p.add_argument(
        "--end-date",
        default=None,
        help="Optional latest first-posted date (YYYY-MM-DD, YYYY-MM, or YYYY).",
    )
    args = p.parse_args()

    one_off_ctgov_search(
        condition=args.condition,
        intervention=args.intervention or None,
        snapshot_id=args.snapshot_id or None,
        max_studies=args.max_studies,
        start_date=args.start_date or None,
        end_date=args.end_date or None,
    )


if __name__ == "__main__":
    main()
