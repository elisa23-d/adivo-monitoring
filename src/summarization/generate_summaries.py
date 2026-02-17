from __future__ import annotations

"""
Generate LLM-powered summaries for PubMed documents and store them in document_summaries.

For each document, we create:
- study_summary: 2–3 bullet points focusing on design, population, and key efficacy/safety results.
- slide_summary: a short narrative (2–4 sentences) suitable for copying into slides.

We only summarise documents that:
- belong to the selected snapshot, and
- do not yet have a row in document_summaries, and
- have a non-empty abstract.

Usage examples:

  # Summarise all unsummarised docs for a specific snapshot (up to 50 docs)
  python -m src.summarization.generate_summaries --snapshot-id 2026-02-16T13-48-49Z --limit 50

  # Summarise for the most recent snapshot
  python -m src.summarization.generate_summaries --latest --limit 20

Environment variables:
- ANTHROPIC_API_KEY  (required)
- ANTHROPIC_MODEL    (optional, default: claude-3-haiku-20240307)
"""

import argparse
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from anthropic import Anthropic

from src.database.db import connect


ANTHROPIC_MODEL_DEFAULT = "claude-3-haiku-20240307"
# Max concurrent API calls (Anthropic free tier often allows 5 req/min)
MAX_CONCURRENT_SUMMARIES = 5


@dataclass
class DocToSummarise:
    doc_id: str
    snapshot_id: str
    title: str
    abstract: str


def get_latest_snapshot_id() -> Optional[str]:
    from src.reporting.export_snapshot_to_excel import get_latest_snapshot_id as _gls

    return _gls()


def fetch_docs_without_summaries(snapshot_id: str, limit: int) -> list[DocToSummarise]:
    """Return competitor-sponsored documents in the snapshot that do not yet have an LLM summary."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT d.doc_id, d.snapshot_id, d.title, d.abstract
            FROM documents d
            JOIN competitor_mentions cm ON cm.doc_id = d.doc_id
            LEFT JOIN document_summaries ds ON ds.doc_id = d.doc_id
            WHERE d.snapshot_id = ?
              AND ds.doc_id IS NULL
              AND d.abstract IS NOT NULL
              AND TRIM(d.abstract) != ''
            GROUP BY d.doc_id, d.snapshot_id, d.title, d.abstract
            ORDER BY d.published_date DESC, d.doc_id
            LIMIT ?
            """,
            (snapshot_id, limit),
        ).fetchall()

    return [
        DocToSummarise(
            doc_id=doc_id,
            snapshot_id=sid,
            title=title or "",
            abstract=abstract or "",
        )
        for doc_id, sid, title, abstract in rows
    ]


def fetch_docs_for_refresh(snapshot_id: str, limit: int) -> list[DocToSummarise]:
    """Return competitor-sponsored documents in the snapshot (for re-summarising with --refresh)."""
    with connect() as conn:
        rows = conn.execute(
            """
            SELECT d.doc_id, d.snapshot_id, d.title, d.abstract
            FROM documents d
            JOIN competitor_mentions cm ON cm.doc_id = d.doc_id
            WHERE d.snapshot_id = ?
              AND d.abstract IS NOT NULL
              AND TRIM(d.abstract) != ''
            GROUP BY d.doc_id, d.snapshot_id, d.title, d.abstract
            ORDER BY d.published_date DESC, d.doc_id
            LIMIT ?
            """,
            (snapshot_id, limit),
        ).fetchall()

    return [
        DocToSummarise(
            doc_id=doc_id,
            snapshot_id=sid,
            title=title or "",
            abstract=abstract or "",
        )
        for doc_id, sid, title, abstract in rows
    ]


def _make_client() -> Anthropic:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise SystemExit("ANTHROPIC_API_KEY environment variable is not set.")
    return Anthropic(api_key=api_key)


# Cap input size so the API responds faster (fewer input tokens).
MAX_ABSTRACT_CHARS = 2500
MAX_TITLE_CHARS = 400


def summarise_with_llm(title: str, abstract: str, model: str) -> tuple[str, str]:
    """
    Call Anthropic Claude to generate (study_summary, slide_summary) as text.
    """
    client = _make_client()

    title_trimmed = (title or "")[:MAX_TITLE_CHARS]
    abstract_trimmed = (abstract or "")[:MAX_ABSTRACT_CHARS]
    if (abstract or "") and len(abstract or "") > MAX_ABSTRACT_CHARS:
        abstract_trimmed = abstract_trimmed.rstrip() + "…"

    system_prompt = (
        "You are a medical writer helping consultants understand clinical papers quickly. "
        "You write concise, precise summaries without speculation."
    )

    user_prompt = f"""
Summarise the following psoriasis-related clinical paper.

Return a JSON object with exactly these two string fields:
- "study_summary": 2–3 bullet points focusing on study design, population, treatment, and main efficacy/safety results. Use short bullets (starting with "- ").
- "slide_summary": 2–3 bullet points that a consultant could paste directly into a slide (key numbers, main results, takeaway). Use short bullets (starting with "- ").

Title: {title_trimmed}

Abstract:
{abstract_trimmed}
"""

    # Use streaming so we get the first tokens sooner; collect full response for JSON parse.
    with client.messages.stream(
        model=model,
        max_tokens=450,
        temperature=0.2,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        content = "".join(stream.text_stream)

    # Strip markdown code fence if present so JSON parses
    raw = content.strip()
    if raw.startswith("```"):
        lines = raw.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines)

    try:
        data = json.loads(raw)
        study_summary = str(data.get("study_summary", data.get("study summary", ""))).strip()
        slide_summary = str(data.get("slide_summary", data.get("slide summary", ""))).strip()
    except json.JSONDecodeError:
        study_summary = content.strip()
        slide_summary = ""

    return study_summary, slide_summary


def upsert_summaries_for_snapshot(snapshot_id: str, limit: int, refresh: bool = False) -> int:
    """
    Generate and store summaries for up to `limit` documents in the snapshot.
    If refresh=True, re-summarise all docs (replace existing) to backfill e.g. slide_summary.
    Returns the number of docs summarised.
    """
    model = os.getenv("ANTHROPIC_MODEL", ANTHROPIC_MODEL_DEFAULT)

    docs = fetch_docs_for_refresh(snapshot_id, limit=limit) if refresh else fetch_docs_without_summaries(snapshot_id, limit=limit)
    if not docs:
        print(f"No documents to summarise for snapshot {snapshot_id}.")
        return 0

    print(f"{'Re-summarising' if refresh else 'Summarising'} {len(docs)} documents with Anthropic model {model} (up to {MAX_CONCURRENT_SUMMARIES} in parallel) …")
    now_iso = datetime.now(timezone.utc).isoformat()

    def _summarise_one(doc: DocToSummarise) -> tuple[DocToSummarise, str, str]:
        study_summary, slide_summary = summarise_with_llm(
            title=doc.title, abstract=doc.abstract, model=model
        )
        return (doc, study_summary, slide_summary)

    count = 0
    with connect() as conn:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_SUMMARIES) as executor:
            futures = {executor.submit(_summarise_one, doc): doc for doc in docs}
            for future in as_completed(futures):
                doc, study_summary, slide_summary = future.result()
                if not study_summary:
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO document_summaries
                    (doc_id, snapshot_id, study_summary, slide_summary, created_at, model)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        doc.doc_id,
                        doc.snapshot_id,
                        study_summary,
                        slide_summary,
                        now_iso,
                        model,
                    ),
                )
                conn.commit()
                count += 1
                print(f"✅ Summarised {doc.doc_id}")

    print(f"Done. Summaries generated for {count} documents.")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate LLM summaries for PubMed documents."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--snapshot-id",
        help="Snapshot ID to summarise, e.g. 2026-02-16T13-48-49Z",
    )
    group.add_argument(
        "--latest",
        action="store_true",
        help="Summarise the most recently created snapshot.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of documents to summarise (default: 20).",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-summarise documents that already have summaries (e.g. to backfill slide_summary).",
    )

    args = parser.parse_args()

    if args.latest:
        snapshot_id = get_latest_snapshot_id()
        if not snapshot_id:
            raise SystemExit("No snapshots found in the database.")
        print(f"Using latest snapshot_id = {snapshot_id}")
    else:
        snapshot_id = args.snapshot_id

    upsert_summaries_for_snapshot(snapshot_id=snapshot_id, limit=args.limit, refresh=args.refresh)


if __name__ == "__main__":
    main()

