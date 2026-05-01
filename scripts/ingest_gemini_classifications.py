"""Ingest Gemini's genre-taxonomy + artist-classification JSON into the DB.

This script runs locally (Python 3.9) as well as on prod (3.11) — the
`from __future__ import annotations` at the bottom of this docstring's
module avoids PEP 604 `X | Y` runtime evaluation issues on 3.9.

Workflow:
  1. Drop Gemini's taxonomy response at scripts/gemini_responses/taxonomy.json
     (run once — establishes the canonical parent → sub-genre map).
  2. Drop each batch's response at scripts/gemini_responses/batch_NN.json.
  3. Run this script. It validates everything, writes to genre_taxonomy +
     artist_genre, and emits an audit summary you can spot-check.

Default behaviour: load taxonomy.json (if present), then ingest every
batch_*.json found in scripts/gemini_responses/. Idempotent — re-running
just refreshes existing rows.

Usage:
    python3 scripts/ingest_gemini_classifications.py
    python3 scripts/ingest_gemini_classifications.py --dry-run
    python3 scripts/ingest_gemini_classifications.py --batches batch_01.json batch_02.json
    python3 scripts/ingest_gemini_classifications.py --taxonomy-only

Expected JSON shape — taxonomy.json:
    {
      "taxonomy": {
        "Rock": ["Hard Rock", "Heavy Metal", ...],
        "Hip-Hop": ["Trap", "Boom Bap", ...]
      }
    }

Expected JSON shape — batch_NN.json:
    {
      "classifications": [
        {"artist": "AC/DC",
         "primary": "Hard Rock",
         "secondary_1": "Heavy Metal",
         "secondary_2": null,
         "confidence": "high"},
        {"artist": "Foo Bar",
         "primary": "UNKNOWN",
         "secondary_1": null,
         "secondary_2": null,
         "confidence": "low"}
      ]
    }

Field-name tolerance: the script accepts `primary` or `primary_genre`,
`artist` or `artist_name`, etc. — so minor wording drift in Gemini's
output doesn't break ingestion.

Validation rules (rejected rows go to _ingestion_audit.txt — the row is
SKIPPED, not partially written):
  - artist must be non-empty
  - primary must be in the taxonomy OR the literal "UNKNOWN"
  - secondaries must be in the taxonomy or null/empty (UNKNOWN not allowed
    as a secondary — secondaries are optional)
  - confidence must be one of {high, medium, low}
  - if primary == "UNKNOWN", confidence must be "low"
  - "Sports & Fitness" is not a permitted parent (we have separate sports
    infrastructure — drop the whole branch if Gemini included it)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select

from app.database import Base, engine, SessionLocal
from app.models import GenreTaxonomy, ArtistGenre

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
RESP_DIR = ROOT / "gemini_responses"
AUDIT_PATH = RESP_DIR / "_ingestion_audit.txt"

VALID_CONFIDENCE = {"high", "medium", "low"}
UNKNOWN = "UNKNOWN"

# Parents we explicitly reject if Gemini emitted them. We already have first-class
# sports infra (event_types Sports category, sport/home_team/away_team columns,
# dedicated scrapers under collectors/scrapers/sports/*), so a "Sports & Fitness"
# branch in the music/event taxonomy would just create double-counting and noise.
BLOCKED_PARENTS = {"sports & fitness", "sports"}


def normalize_name(s: str) -> str:
    """Mirror the Performer.normalized_name convention."""
    return " ".join(s.lower().split())


# ── Field-name tolerant getters ──────────────────────────────────────────────

def _get(d: dict, *keys, default=None):
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def _str_or_none(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


# ── Taxonomy ─────────────────────────────────────────────────────────────────

def load_taxonomy_file(path: Path) -> dict[str, str]:
    """Read taxonomy JSON → {sub_genre: parent_genre}.

    Enforces:
      - blocked parents skipped wholesale
      - sub-genre uniqueness across parents (first occurrence wins; duplicates audited)
    """
    raw = json.loads(path.read_text(encoding="utf-8"))
    tax = raw.get("taxonomy") or raw  # tolerate {"Rock":[...]} OR {"taxonomy":{...}}

    sub_to_parent: dict[str, str] = {}
    duplicates: list[tuple[str, str, str]] = []  # (sub, kept_parent, dropped_parent)
    blocked_skipped: list[str] = []

    for parent, subs in tax.items():
        if not isinstance(subs, list):
            log.warning(f"taxonomy: parent {parent!r} value is not a list — skipped")
            continue
        if parent.strip().lower() in BLOCKED_PARENTS:
            blocked_skipped.append(parent)
            continue
        for sub in subs:
            sub = (sub or "").strip()
            if not sub:
                continue
            if sub in sub_to_parent and sub_to_parent[sub] != parent:
                duplicates.append((sub, sub_to_parent[sub], parent))
                continue
            sub_to_parent[sub] = parent.strip()

    log.info(
        f"taxonomy parsed: {len(sub_to_parent)} sub-genres under "
        f"{len(set(sub_to_parent.values()))} parents "
        f"(blocked parents: {blocked_skipped or '—'}, "
        f"duplicate sub-genres dropped: {len(duplicates)})"
    )
    if duplicates:
        log.warning(f"  duplicate sub-genres (kept first): {duplicates[:5]}{'…' if len(duplicates) > 5 else ''}")
    return sub_to_parent


def write_taxonomy(db, sub_to_parent: dict[str, str], dry_run: bool) -> tuple[int, int]:
    """Upsert the taxonomy. Returns (inserted, updated)."""
    inserted = updated = 0
    existing = {row.sub_genre: row for row in db.query(GenreTaxonomy).all()}
    for sub, parent in sub_to_parent.items():
        row = existing.get(sub)
        if row is None:
            if not dry_run:
                db.add(GenreTaxonomy(sub_genre=sub, parent_genre=parent))
            inserted += 1
        elif row.parent_genre != parent:
            if not dry_run:
                row.parent_genre = parent
            updated += 1
    if not dry_run:
        db.commit()
    return inserted, updated


# ── Classification batches ───────────────────────────────────────────────────

def validate_classification(
    entry: dict,
    valid_subs: set[str],
    valid_parents: set[str] | None = None,
) -> tuple[dict | None, str | None, list[str]]:
    """Return (clean_record, reject_reason, fixup_notes).

    Lenient mode (when ``valid_parents`` is given): if Gemini violates rule 2
    by using a parent name as primary or secondary, we *fix up* rather than
    reject:
      - primary == parent name → set primary=UNKNOWN, confidence=low,
        drop both secondaries (they're meaningless without a real primary)
      - secondary_1/2 == parent name → set to None (drops a redundant tag)
    Both cases are reported in ``fixup_notes`` so the audit file logs the
    rewrite. Hallucinated labels (not parent, not sub) are still rejected.
    """
    valid_parents = valid_parents or set()
    fixups: list[str] = []

    artist = _str_or_none(_get(entry, "artist", "artist_name", "name"))
    if not artist:
        return None, "missing artist", fixups

    primary = _str_or_none(_get(entry, "primary", "primary_genre"))
    if primary is None:
        return None, "missing primary", fixups

    confidence = (_str_or_none(_get(entry, "confidence")) or "").lower()
    if confidence not in VALID_CONFIDENCE:
        return None, f"invalid confidence {confidence!r}", fixups

    sec1 = _str_or_none(_get(entry, "secondary_1", "secondary1"))
    sec2 = _str_or_none(_get(entry, "secondary_2", "secondary2"))

    # Strip a verbose " Music" suffix that Gemini sometimes appends to a parent
    # name (e.g. "Classical Music" → "Classical"). Treats the remainder as a
    # parent-name violation in the lenient path below.
    def _strip_music_suffix(v: str) -> str:
        if v and v.endswith(" Music") and v[:-len(" Music")] in valid_parents:
            return v[:-len(" Music")]
        return v

    primary_pre = primary
    primary = _strip_music_suffix(primary)
    if primary != primary_pre:
        fixups.append(f"primary {primary_pre!r} → {primary!r} (stripped ' Music' suffix)")

    if primary.upper() == UNKNOWN:
        primary = UNKNOWN
        if confidence != "low":
            return None, "primary=UNKNOWN requires confidence=low", fixups
    elif primary in valid_subs:
        pass  # ok
    elif primary in valid_parents:
        # Parent-name as primary → degrade to UNKNOWN, drop secondaries.
        fixups.append(f"primary {primary!r} is a parent name → UNKNOWN")
        primary = UNKNOWN
        confidence = "low"
        sec1 = sec2 = None
    else:
        return None, f"primary {primary!r} not in taxonomy", fixups

    # secondaries: drop parent-name violations to None (lenient); reject other
    # hallucinations.
    cleaned_secs: list[str | None] = []
    for label, val in (("secondary_1", sec1), ("secondary_2", sec2)):
        if val is None:
            cleaned_secs.append(None)
            continue
        val_pre = val
        val = _strip_music_suffix(val)
        if val != val_pre:
            fixups.append(f"{label} {val_pre!r} → {val!r} (stripped ' Music' suffix)")
        if val.upper() == UNKNOWN:
            return None, f"{label}=UNKNOWN not allowed (only primary may be UNKNOWN)", fixups
        if val in valid_subs:
            cleaned_secs.append(val)
            continue
        if val in valid_parents:
            fixups.append(f"{label} {val!r} is a parent name → null")
            cleaned_secs.append(None)
            continue
        return None, f"{label} {val!r} not in taxonomy", fixups
    sec1, sec2 = cleaned_secs

    return {
        "artist": artist,
        "primary": primary,
        "secondary_1": sec1,
        "secondary_2": sec2,
        "confidence": confidence,
    }, None, fixups


def ingest_batch(
    db,
    path: Path,
    valid_subs: set[str],
    dry_run: bool,
    valid_parents: set[str] | None = None,
) -> tuple[int, int, int, list[str]]:
    """Returns (inserted, updated, rejected_count, audit_lines)."""
    raw = json.loads(path.read_text(encoding="utf-8"))
    classifications = raw.get("classifications") or raw  # tolerate bare list
    if not isinstance(classifications, list):
        log.error(f"  {path.name}: not a list — got {type(classifications).__name__}")
        return 0, 0, 0, [f"{path.name}: top-level not a list"]

    audit: list[str] = []
    inserted = updated = rejected = 0
    fixed_up = 0
    seen_in_file: set[str] = set()

    # Pre-load existing rows ONCE per batch (faster than per-row queries)
    existing = {
        row.normalized_name: row
        for row in db.query(ArtistGenre).all()
    }

    for entry in classifications:
        clean, reason, fixups = validate_classification(entry, valid_subs, valid_parents)
        if clean is None:
            audit.append(f"{path.name} REJECT: {reason}  ↦  {entry!r}")
            rejected += 1
            continue
        if fixups:
            fixed_up += 1
            audit.append(
                f"{path.name} FIXUP {clean['artist']!r}: {'; '.join(fixups)}"
            )

        norm = normalize_name(clean["artist"])
        if norm in seen_in_file:
            audit.append(f"{path.name} DUP-IN-FILE: {clean['artist']!r}")
            rejected += 1
            continue
        seen_in_file.add(norm)

        row = existing.get(norm)
        if row is None:
            if not dry_run:
                db.add(ArtistGenre(
                    artist_name=clean["artist"],
                    normalized_name=norm,
                    primary_genre=clean["primary"],
                    secondary_1=clean["secondary_1"],
                    secondary_2=clean["secondary_2"],
                    confidence=clean["confidence"],
                    source="gemini",
                ))
            inserted += 1
        else:
            changed = (
                row.primary_genre != clean["primary"]
                or row.secondary_1   != clean["secondary_1"]
                or row.secondary_2   != clean["secondary_2"]
                or row.confidence    != clean["confidence"]
            )
            if changed and not dry_run:
                row.artist_name = clean["artist"]
                row.primary_genre = clean["primary"]
                row.secondary_1   = clean["secondary_1"]
                row.secondary_2   = clean["secondary_2"]
                row.confidence    = clean["confidence"]
                row.source = "gemini"
            if changed:
                updated += 1

    if not dry_run:
        db.commit()
    return inserted, updated, rejected, audit


# ── Driver ───────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--taxonomy", default="taxonomy.json",
                    help="Filename inside gemini_responses/ (default: taxonomy.json)")
    ap.add_argument("--batches", nargs="*", default=None,
                    help="Specific batch files (relative to gemini_responses/). "
                         "Default: all batch_*.json")
    ap.add_argument("--taxonomy-only", action="store_true",
                    help="Load taxonomy but skip batches.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Validate everything, write nothing.")
    args = ap.parse_args()

    RESP_DIR.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(bind=engine)  # ensure tables exist

    db = SessionLocal()
    try:
        # ── 1. Taxonomy ─────────────────────────────────────────────────────
        tax_path = RESP_DIR / args.taxonomy
        sub_to_parent: dict[str, str] = {}
        if tax_path.is_file():
            sub_to_parent = load_taxonomy_file(tax_path)
            inserted, updated = write_taxonomy(db, sub_to_parent, args.dry_run)
            log.info(f"taxonomy → inserted={inserted}, updated={updated}"
                     f"{' (dry-run)' if args.dry_run else ''}")
        else:
            log.info(f"no {args.taxonomy} found — using existing DB taxonomy")

        # Build the valid-set. In dry-run mode write_taxonomy didn't persist,
        # so prefer the in-memory taxonomy we just parsed; otherwise read from DB.
        valid_subs: set[str] = set()
        valid_parents: set[str] = set()
        if args.dry_run and sub_to_parent:
            valid_subs = set(sub_to_parent.keys())
            valid_parents = set(sub_to_parent.values())
        else:
            rows = db.query(GenreTaxonomy).all()
            valid_subs = {row.sub_genre for row in rows}
            valid_parents = {row.parent_genre for row in rows}
        if not valid_subs:
            log.error("genre_taxonomy is empty — load taxonomy.json before batches.")
            return 1
        log.info(
            f"taxonomy in DB: {len(valid_subs)} sub-genres under "
            f"{len(valid_parents)} parents available for validation"
        )

        if args.taxonomy_only:
            log.info("--taxonomy-only set, exiting.")
            return 0

        # ── 2. Batches ──────────────────────────────────────────────────────
        if args.batches:
            batch_files = [RESP_DIR / b for b in args.batches]
        else:
            batch_files = sorted(RESP_DIR.glob("batch_*.json"))
        if not batch_files:
            log.warning(f"no batch_*.json files in {RESP_DIR} — nothing to ingest")
            return 0

        all_audit: list[str] = []
        totals = [0, 0, 0]   # inserted, updated, rejected
        for path in batch_files:
            if not path.is_file():
                log.warning(f"  {path.name}: not found, skipped")
                continue
            ins, upd, rej, audit = ingest_batch(db, path, valid_subs, args.dry_run, valid_parents)
            totals[0] += ins; totals[1] += upd; totals[2] += rej
            all_audit.extend(audit)
            log.info(f"  {path.name}: +{ins} new, ~{upd} updated, ✗{rej} rejected")

        log.info(
            f"\nTOTALS: inserted={totals[0]}, updated={totals[1]}, "
            f"rejected={totals[2]}{' (dry-run)' if args.dry_run else ''}"
        )

        if all_audit:
            AUDIT_PATH.write_text("\n".join(all_audit) + "\n", encoding="utf-8")
            log.info(f"audit details → {AUDIT_PATH}")

        # Quick sanity stats
        if not args.dry_run:
            n_total   = db.query(ArtistGenre).count()
            n_unknown = db.query(ArtistGenre).filter(ArtistGenre.primary_genre == UNKNOWN).count()
            log.info(f"DB now has {n_total:,} artist_genre rows ({n_unknown:,} UNKNOWN)")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
