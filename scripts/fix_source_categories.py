"""
Apply source-based category overrides for scrapers whose events
have no artist_name (so performer lookup never fires).

Rules:
  barby        → Music / Concert       (music venue, Hebrew artists)
  cameri       → Art / Play/Drama      (theater)
  tel_aviv_venues → Music / Concert   (music venues)
  leaan        → use name/desc keywords, else keep existing

Run after recategorize_from_performers.py and refine_concerts.py.

Usage:
    python3 scripts/fix_source_categories.py
    python3 scripts/fix_source_categories.py --dry-run
"""
import argparse
import os
import re
import sys
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models import Event, EventType

# ── hard-coded source → type overrides ───────────────────────────────────────
SOURCE_OVERRIDES = {
    "barby":           ("Music", "Concert"),
    "tel_aviv_venues": ("Music", "Concert"),
    "cameri":          ("Art", "Play / Drama"),
}

# ── Hebrew + English keyword rules for leaan events (no artist_name) ─────────
# First match wins
LEAAN_RULES = [
    ("Art", "Play / Drama", [
        r"תיאטרון", r"תאטרון", r"מחזה", r"הצגה", r"theatre", r"theater", r"play\b", r"drama",
        r"broadway", r"מונולוג", r"קומדיה\s*מוזיקלית",
    ]),
    ("Music", "Electronic / DJ Set", [
        r"rave", r"dj\s*set", r"electornic", r"edm", r"techno", r"house",
        r"מסיבה", r"ריקוד",
    ]),
    ("Music", "Jazz Concert", [
        r"jazz", r"ג'אז", r"blues",
    ]),
    ("Music", "Classical Concert", [
        r"orchestra", r"תזמורת", r"symphony", r"סימפוני", r"פילהרמונ",
        r"chamber", r"quartet", r"trio\b",
    ]),
    ("Comedy", "Comedy Club Headliners", [
        r"stand.?up", r"comedy", r"קומדי", r"סטנד.?אפ", r"עומד\s*על\s*הבמה",
    ]),
    ("Dance", "Ballet Performance", [
        r"ballet", r"בלט", r"מחול", r"dance\s*show", r"כורא",
    ]),
    ("Music", "Concert", [
        r"לייב", r"live\b", r"הופעה", r"מופע", r"concert", r"פסטיבל",
        r"festival", r"גיטרה", r"תופים", r"קלידים",
    ]),
]

LEAAN_COMPILED = [
    (cat, typ, [re.compile(p, re.IGNORECASE) for p in pats])
    for cat, typ, pats in LEAAN_RULES
]


def classify_leaan(event: "Event") -> Optional[tuple]:
    """Return (category, type_name) from event text, or None."""
    text = " ".join(filter(None, [
        event.name or "",
        event.description or "",
        event.venue_name or "",
    ]))
    for cat, typ, patterns in LEAAN_COMPILED:
        for pat in patterns:
            if pat.search(text):
                return cat, typ
    return None


def run(dry_run: bool = False):
    db = SessionLocal()
    try:
        et_by_name = {et.name: et for et in db.query(EventType).all()}

        updated = 0
        skipped = 0
        breakdown: dict[str, int] = {}

        # ── 1. Hard-coded source overrides ────────────────────────────────────
        for source, (cat, type_name) in SOURCE_OVERRIDES.items():
            et = et_by_name.get(type_name)
            if not et:
                print(f"WARNING: event type {type_name!r} not found in DB")
                continue

            events = (
                db.query(Event)
                .filter(Event.scrape_source == source)
                .all()
            )

            for event in events:
                current = [t.name for t in event.event_types]
                if current == [type_name]:
                    skipped += 1
                    continue
                if not dry_run:
                    event.event_types = [et]
                updated += 1
                breakdown[f"{source} → {type_name}"] = (
                    breakdown.get(f"{source} → {type_name}", 0) + 1
                )

        # ── 2. Leaan events without artist_name ───────────────────────────────
        leaan_no_artist = (
            db.query(Event)
            .filter(Event.scrape_source == "leaan")
            .filter(
                (Event.artist_name == None) |  # noqa: E711
                (Event.artist_name == "")
            )
            .all()
        )

        for event in leaan_no_artist:
            result = classify_leaan(event)
            if not result:
                skipped += 1
                continue
            cat, type_name = result
            et = et_by_name.get(type_name)
            if not et:
                skipped += 1
                continue
            current = [t.name for t in event.event_types]
            if current == [type_name]:
                skipped += 1
                continue
            if not dry_run:
                event.event_types = [et]
            updated += 1
            breakdown[f"leaan (kw) → {type_name}"] = (
                breakdown.get(f"leaan (kw) → {type_name}", 0) + 1
            )

        if not dry_run:
            db.commit()

        print(f"Updated: {updated:,}  |  Unchanged/skipped: {skipped:,}")
        if breakdown:
            print("\nBreakdown:")
            for k, v in sorted(breakdown.items(), key=lambda x: -x[1]):
                print(f"  {k:<45} {v:>5,}")

        if dry_run:
            print("\n[DRY RUN — nothing committed]")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
