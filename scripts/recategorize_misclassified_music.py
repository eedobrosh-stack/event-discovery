"""One-off cleanup: re-classify events that the pre-fix _classify()
order tagged as Music/Concert via the artist-implies-music fallback,
but whose name actually matches a non-music keyword.

Background — until 48c49cb (2026-05-31), scripts/categorize_events.py's
_classify() ran the music_default fallback ('artist exists but unknown
performer → tag as Concert') BEFORE the keyword classifier. So
conferences / lectures / workshops with speaker names extracted into
artist_name from JSON-LD ('Jari Hakanen, Sabine Sonnentag' on the
EAOHP conference) got auto-tagged as Music/Concert and never reached
the keyword check that would have spotted 'conference' in the name.

This script:
  1. Finds events currently tagged with 'Concert' or 'Pop Concert'
     event_type (the two music_default paths).
  2. Runs keyword_match on each event's name + description.
  3. If keyword_match returns a NON-music type, strips the music tag
     and assigns the matched type. Themes are NOT re-applied here —
     theme_match was already run by the hourly cron and is
     idempotent against re-runs; phase 2's THEME_KEYWORDS handle the
     theme assignment separately.

Convention: dry-run by default, --apply to commit.

Usage:
    PYTHONPATH=. python3 scripts/recategorize_misclassified_music.py
    PYTHONPATH=. python3 scripts/recategorize_misclassified_music.py --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("recategorize_misclassified_music")

from app.database import SessionLocal  # noqa: E402
from app.models import Event, EventType  # noqa: E402
from scripts.categorize_events import keyword_match  # noqa: E402


# Event types that the old music_default path could assign. Events
# carrying ONLY these types are candidates for re-classification.
MUSIC_DEFAULT_TYPES = ("Concert", "Pop Concert")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Actually mutate the DB (default is dry-run).")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        et_by_name = {et.name: et for et in db.query(EventType).all()}
        music_defaults = [et_by_name[n] for n in MUSIC_DEFAULT_TYPES if n in et_by_name]
        if not music_defaults:
            log.warning("Neither 'Concert' nor 'Pop Concert' exists — nothing to scan.")
            return 0

        music_default_ids = {et.id for et in music_defaults}
        log.info(f"Music-default type ids: {music_default_ids}")

        # Pull every event that has Concert or Pop Concert in its
        # event_types list. The candidate pool is big (~50K events
        # tagged Concert) so we iterate and filter in Python on the
        # name+desc keyword pass — checking against a large LIKE
        # union would be wasteful when most events legitimately are
        # music.
        candidates = (
            db.query(Event)
            .filter(Event.event_types.any(EventType.id.in_(music_default_ids)))
            .all()
        )
        log.info(f"Candidates (events with Concert/Pop Concert tag): {len(candidates):,}")

        stats = Counter()
        flips = 0
        for ev in candidates:
            text = " ".join(filter(None, [ev.name or "", ev.venue_name or "", ev.description or ""]))
            kw_type_name = keyword_match(text)
            if not kw_type_name:
                stats["no_kw_match"] += 1
                continue

            # If the keyword matched a MUSIC type (Jazz Concert, Rock
            # Concert, etc.), leave it alone — the music_default
            # was probably correct for this event, it just got the
            # generic 'Concert' tag instead of the specific subtype.
            # The full re-categorize CLI can refine those separately;
            # here we only flip clearly non-music misclassifications.
            kw_type = et_by_name.get(kw_type_name)
            if kw_type is None or kw_type.category == "Music":
                stats["kw_is_music_or_missing"] += 1
                continue

            # Flip: strip music defaults, add the keyword-matched type.
            stats[f"flip:{kw_type_name}"] += 1
            flips += 1
            if args.apply:
                ev.event_types = [
                    t for t in ev.event_types if t.id not in music_default_ids
                ] + [kw_type]

        log.info(f"Stats: {dict(stats)}")
        log.info(f"Total flips: {flips}")

        if args.apply:
            db.commit()
            log.info("APPLIED — re-categorization complete.")
        else:
            db.rollback()
            log.info(f"DRY-RUN — would re-categorize {flips} events. Re-run with --apply.")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
