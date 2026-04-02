"""
Scan all distinct artist names in the events table, look each up on MusicBrainz,
and populate the performers table with category + event_type_name.

Usage:
    python3 scripts/enrich_performers.py            # process all un-looked-up artists
    python3 scripts/enrich_performers.py --limit 100  # stop after N new lookups
    python3 scripts/enrich_performers.py --refetch  # re-lookup everyone (slow)

Rate: ~2 MusicBrainz calls per artist (search + detail), 1.1 s between each.
Estimated time for 11 000 artists: ~6–7 hours.  Run with nohup or in a screen session.
Progress is printed every 50 artists and always resumable.
"""
import argparse
import asyncio
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.database import Base, engine, SessionLocal
from app.models import Event, Performer
from app.services.performer_lookup import lookup_musicbrainz, normalize

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def ensure_table():
    """Create performers table if it doesn't exist yet."""
    Base.metadata.create_all(bind=engine)


def get_pending_artists(db, refetch: bool) -> list[str]:
    """Return distinct artist names not yet in the performers table."""
    all_artists = (
        db.query(Event.artist_name)
        .filter(Event.artist_name.isnot(None))
        .filter(Event.artist_name != "")
        .distinct()
        .all()
    )
    all_names = [r[0].strip() for r in all_artists if r[0] and r[0].strip()]

    if refetch:
        return all_names

    # Filter out already-processed names
    existing_norms = {
        r[0] for r in db.query(Performer.normalized_name).all()
    }
    return [n for n in all_names if normalize(n) not in existing_norms]


async def run(limit: int, refetch: bool):
    ensure_table()
    db = SessionLocal()

    try:
        pending = get_pending_artists(db, refetch)
        total = len(pending)

        if limit:
            pending = pending[:limit]

        logger.info(f"Artists to process: {len(pending)} (total pending: {total})")

        if not pending:
            logger.info("Nothing to do — all artists already looked up.")
            return

        new_count = 0
        skip_count = 0

        async with httpx.AsyncClient() as http:
            for i, name in enumerate(pending, 1):
                norm = normalize(name)

                # Double-check in DB (in case of parallel runs)
                existing = (
                    db.query(Performer)
                    .filter(Performer.normalized_name == norm)
                    .first()
                )
                if existing and not refetch:
                    skip_count += 1
                    continue

                result = await lookup_musicbrainz(name, http)

                if existing:
                    # Update
                    existing.category = result["category"]
                    existing.event_type_name = result["event_type_name"]
                    existing.genres = json.dumps(result["genres"])
                    existing.mb_id = result["mb_id"]
                    existing.mb_type = result["mb_type"]
                    existing.source = result["source"]
                    existing.confidence = result["confidence"]
                else:
                    performer = Performer(
                        name=name,
                        normalized_name=norm,
                        category=result["category"],
                        event_type_name=result["event_type_name"],
                        genres=json.dumps(result["genres"]),
                        mb_id=result["mb_id"],
                        mb_type=result["mb_type"],
                        source=result["source"],
                        confidence=result["confidence"],
                    )
                    db.add(performer)

                new_count += 1

                # Commit every 50 to avoid big transactions
                if new_count % 50 == 0:
                    try:
                        db.commit()
                    except IntegrityError:
                        db.rollback()
                        logger.debug(f"Skipping duplicate batch around {name!r}")
                    logger.info(
                        f"  [{i}/{len(pending)}] Saved {new_count} performers "
                        f"(last: {name!r} → {result['event_type_name']})"
                    )

                # Log each one at DEBUG level
                logger.debug(
                    f"  {name!r} → {result['event_type_name']} "
                    f"[{result['source']}, {result['confidence']:.0%}] "
                    f"tags={result['genres'][:5]}"
                )

        try:
            db.commit()
        except IntegrityError:
            db.rollback()
        logger.info(
            f"\nDone!  New/updated: {new_count}  |  Skipped (already done): {skip_count}"
        )

        # Quick stats
        total_performers = db.query(Performer).count()
        logger.info(f"Total performers in DB: {total_performers}")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich performers from MusicBrainz")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max number of new lookups (0 = all)")
    parser.add_argument("--refetch", action="store_true",
                        help="Re-lookup all artists, even if already processed")
    args = parser.parse_args()

    asyncio.run(run(limit=args.limit, refetch=args.refetch))
