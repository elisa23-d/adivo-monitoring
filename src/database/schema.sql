PRAGMA foreign_keys = ON;

-- Tracks dataset versions for reproducibility
CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id   TEXT PRIMARY KEY,     -- e.g. "2026-02-12" or "2026-02-12T09-00"
  created_at    TEXT NOT NULL,        -- ISO timestamp
  notes         TEXT
);

-- Canonical competitor names (e.g., "Johnson & Johnson")
CREATE TABLE IF NOT EXISTS competitors (
  competitor_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  canonical_name    TEXT NOT NULL UNIQUE
);

-- Alias mapping (e.g., "Janssen", "J&J Innovative Medicine" -> "Johnson & Johnson")
CREATE TABLE IF NOT EXISTS competitor_aliases (
  alias_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  competitor_id   INTEGER NOT NULL,
  alias           TEXT NOT NULL,
  FOREIGN KEY (competitor_id) REFERENCES competitors(competitor_id) ON DELETE CASCADE,
  UNIQUE(competitor_id, alias)
);

-- Molecules to monitor + synonyms (simple v1: store synonyms as JSON-ish text or pipe-separated)
CREATE TABLE IF NOT EXISTS molecules (
  molecule_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  name           TEXT NOT NULL UNIQUE,
  synonyms       TEXT                -- e.g. "guselkumab|GSK-375791|GSK 375791"
);

-- Monitoring profiles (saved alerts)
CREATE TABLE IF NOT EXISTS monitoring_profiles (
  profile_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  name             TEXT NOT NULL,            -- e.g. "TYK2 - Deucravacitinib - Psoriasis"
  molecule_id      INTEGER,
  query_terms      TEXT NOT NULL,            -- the actual PubMed query you run (expanded or not)
  competitor_scope TEXT,                     -- optional: pipe-separated canonical competitor names
  frequency        TEXT DEFAULT 'daily',     -- daily/weekly/monthly
  is_active        INTEGER DEFAULT 1,
  last_snapshot_id TEXT,
  created_at       TEXT NOT NULL,
  FOREIGN KEY (molecule_id) REFERENCES molecules(molecule_id),
  FOREIGN KEY (last_snapshot_id) REFERENCES snapshots(snapshot_id)
);

-- Main evidence table for publications/trials/etc.
CREATE TABLE IF NOT EXISTS documents (
  doc_id          TEXT PRIMARY KEY,   -- e.g. "PMID:12345678" or "NCT:NCT01234567"
  source          TEXT NOT NULL,       -- pubmed / ctgov / investor / news
  snapshot_id     TEXT NOT NULL,
  title           TEXT,
  abstract        TEXT,
  url             TEXT,
  published_date  TEXT,               -- ISO date if possible (print/publication date)
  epub_date       TEXT,               -- electronic first publication (PubMed History PubStatus=epub)
  entry_date      TEXT,               -- when ingested / when record appeared
  last_updated    TEXT,               -- for ctgov updates etc.
  publication_type TEXT,              -- e.g. "Review", "Journal Article" (PubMed)
  raw_json_path   TEXT,               -- provenance for reproducibility
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_documents_snapshot ON documents(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_documents_source_snapshot ON documents(source, snapshot_id);
CREATE INDEX IF NOT EXISTS idx_documents_published_date ON documents(published_date);

-- Store raw affiliations text (PubMed metadata); keep it simple and inclusive
CREATE TABLE IF NOT EXISTS affiliations (
  affiliation_id  INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id          TEXT NOT NULL,
  affiliation_text TEXT NOT NULL,
  FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_affiliations_doc ON affiliations(doc_id);

-- Competitor matches detected in affiliations/funding/etc.
CREATE TABLE IF NOT EXISTS competitor_mentions (
  mention_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id          TEXT NOT NULL,
  competitor_id   INTEGER NOT NULL,
  match_text      TEXT NOT NULL,      -- which alias matched
  mention_type    TEXT DEFAULT 'affiliation', -- affiliation / funding / other
  FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE,
  FOREIGN KEY (competitor_id) REFERENCES competitors(competitor_id) ON DELETE CASCADE,
  UNIQUE(doc_id, competitor_id, match_text, mention_type)
);

CREATE INDEX IF NOT EXISTS idx_mentions_doc ON competitor_mentions(doc_id);
CREATE INDEX IF NOT EXISTS idx_mentions_competitor ON competitor_mentions(competitor_id);

-- Consultant triage: flag/ignore/notes (supports daily workflow)
CREATE TABLE IF NOT EXISTS triage (
  triage_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id        TEXT NOT NULL,
  profile_id    INTEGER,
  status        TEXT NOT NULL DEFAULT 'unreviewed', -- unreviewed / flagged / ignored
  notes         TEXT,
  reviewed_at   TEXT,
  FOREIGN KEY (doc_id) REFERENCES documents(doc_id) ON DELETE CASCADE,
  FOREIGN KEY (profile_id) REFERENCES monitoring_profiles(profile_id) ON DELETE SET NULL,
  UNIQUE(doc_id, profile_id)
);

CREATE INDEX IF NOT EXISTS idx_triage_status ON triage(status);
