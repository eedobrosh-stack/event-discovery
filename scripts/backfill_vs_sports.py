"""
One-shot backfill: re-tag legacy "X vs Y" / "X vs. Y" events as Sports.

College sports (Stanford Cardinal Baseball vs. Florida State …) and other
Ticketmaster "Sports" segment items ingested before the TM-sports-segment
fix landed ended up tagged as Music / Fitness / Marathons because the home
team was stored as artist_name, which routed them through the music
enrichment path.

This script:
  - Finds events with "vs"/"vs." in the name and sport=NULL that aren't
    already tagged Sports
  - Infers the specific sport from a name keyword (baseball, basketball, …)
  - Clears the bogus artist_name
  - Strips non-Sports event_types and adds the correct Sports type
  - Synthesizes a YouTube highlights search URL from derived home/away

Why a script and not a startup hook: in commit 5957c44 this ran at app
startup (inside _fix_sports_categories) and contributed to a prod hang /
OOM-restart loop on Render. Moving it out of the hot path.

Usage:
    python3 scripts/backfill_vs_sports.py --dry-run
    python3 scripts/backfill_vs_sports.py                # default: limit 2000
    python3 scripts/backfill_vs_sports.py --limit 10000
    python3 scripts/backfill_vs_sports.py --batch-size 200
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from urllib.parse import quote_plus

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import or_

from app.database import SessionLocal
from app.models import Event, EventType


# Name-keyword → (sport column value, EventType.name to attach)
_VS_SPORT_KEYWORDS = [
    ("baseball",   "Baseball",          "Baseball Game"),
    ("softball",   "Baseball",          "Baseball Game"),  # no Softball type — group under baseball
    ("basketball", "Basketball",        "Basketball Game"),
    ("hockey",     "Ice Hockey",        "Hockey Game"),
    ("football",   "American Football", "American Football Game"),
    ("soccer",     "Soccer",            "Soccer Match"),
    ("tennis",     "Tennis",            "Tennis Match"),
]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
)
log = logging.getLogger("backfill_vs_sports")


def _resolve_et(db, name: str) -> EventType | None:
    return db.query(EventType).filter(EventType.name == name).first()


def run(limit: int, batch_size: int, dry_run: bool) -> None:
    db = SessionLocal()
    try:
        vs_events = (
            db.query(Event)
            .filter(
                or_(
                    Event.name.ilike("% vs %"),
                    Event.name.ilike("% vs. %"),
                ),
                Event.sport.is_(None),
            )
            .limit(limit)
            .all()
        )

        log.info(f"Found {len(vs_events)} candidate events (sport=NULL, '% vs %' or '% vs. %')")

        # Preload the 6 Sports EventTypes we might attach, so the per-row loop
        # doesn't re-query the same rows thousands of times.
        sports_et_cache = {}
        for _, _, et_name in _VS_SPORT_KEYWORDS:
            sports_et_cache[et_name] = _resolve_et(db, et_name)
        sports_et_cache["Sports Event"] = _resolve_et(db, "Sports Event")

        fixed = 0
        in_batch = 0
        for ev in vs_events:
            # Skip if already sports-tagged
            if ev.event_types and any(
                et.category == "Sports" for et in ev.event_types
            ):
                continue

            lower_name = (ev.name or "").lower()
            # Pick the most specific sport type by name keyword
            sport_val = None
            et_name = "Sports Event"
            for kw, sv, et_n in _VS_SPORT_KEYWORDS:
                if kw in lower_name:
                    sport_val = sv
                    et_name = et_n
                    break

            sports_et = sports_et_cache.get(et_name)
            if not sports_et:
                continue

            dirty = False
            if sport_val and ev.sport != sport_val:
                ev.sport = sport_val
                dirty = True
            if ev.artist_name:
                # home team was stored as artist_name — clear it
                ev.artist_name = None
                dirty = True
            # Strip Music/Comedy/Fitness types; add the Sports one.
            non_sports_ids = {
                et.id for et in (ev.event_types or [])
                if et.category != "Sports"
            }
            if non_sports_ids:
                ev.event_types = [
                    et for et in ev.event_types if et.id not in non_sports_ids
                ]
                dirty = True
            if sports_et not in (ev.event_types or []):
                ev.event_types.append(sports_et)
                dirty = True
            # Derive home/away from name for the YouTube highlights URL
            if not ev.artist_youtube_channel and (" vs " in lower_name or " vs. " in lower_name):
                sep = " vs. " if " vs. " in lower_name else " vs "
                try:
                    home, away = ev.name.split(sep, 1)
                    q = quote_plus(f"{home.strip()} vs {away.strip()} highlights")
                    ev.artist_youtube_channel = (
                        f"https://www.youtube.com/results?search_query={q}"
                    )
                    dirty = True
                except ValueError:
                    pass

            if dirty:
                fixed += 1
                in_batch += 1
                log.debug(f"  [{fixed}] id={ev.id} {ev.name!r} → sport={ev.sport} et={et_name}")

            # Incremental commit so we don't hold 2000 dirty rows in memory
            # (and so a crash mid-run still saves progress). Skipped in dry-run.
            if not dry_run and in_batch >= batch_size:
                db.commit()
                in_batch = 0

        if dry_run:
            log.info(f"[dry-run] Would repair {fixed} events. Rolling back.")
            db.rollback()
        else:
            if in_batch:
                db.commit()
            if fixed:
                log.info(f"Repaired {fixed} events")
            else:
                log.info("Nothing to repair")

    except Exception as e:
        log.exception(f"Backfill failed: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--limit", type=int, default=2000, help="Max events to scan (default 2000)")
    p.add_argument("--batch-size", type=int, default=200, help="Commit every N repairs (default 200)")
    p.add_argument("--dry-run", action="store_true", help="Show counts only; do not commit")
    args = p.parse_args()

    run(limit=args.limit, batch_size=args.batch_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
