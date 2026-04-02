#!/usr/bin/env python3
"""
Enrich artist_youtube_channel for all events that have an artist_name
but no YouTube link.

Strategy:
  - Deduplicate: one lookup per unique artist_name
  - Update ALL events sharing that artist_name in one shot
  - Uses youtube_lookup.py (API first → scrape fallback)

Usage:
    caffeinate -i python3 scripts/enrich_youtube.py
    python3 scripts/enrich_youtube.py --limit 200   # cap at N artists (for testing)
"""
import sys
import os
import asyncio
import logging
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import func, or_
from app.database import SessionLocal
from app.models import Event
from app.services.youtube_lookup import lookup_youtube_video

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


async def enrich(limit: int = 0) -> None:
    db = SessionLocal()
    try:
        # Collect unique artist names that need a YouTube link
        q = (
            db.query(Event.artist_name)
            .filter(
                Event.artist_name.isnot(None),
                Event.artist_name != "",
                or_(
                    Event.artist_youtube_channel.is_(None),
                    Event.artist_youtube_channel == "",
                ),
            )
            .group_by(Event.artist_name)
            .order_by(func.count(Event.id).desc())   # most-common artists first
        )
        if limit:
            q = q.limit(limit)

        artists = [row[0] for row in q.all()]
        total = len(artists)
        logger.info("Artists to enrich: %d", total)

        found = 0
        failed = 0
        start = time.time()

        for idx, artist in enumerate(artists, 1):
            url = await lookup_youtube_video(artist)

            if url:
                # Update every event with this artist name
                updated = (
                    db.query(Event)
                    .filter(Event.artist_name == artist)
                    .update({"artist_youtube_channel": url}, synchronize_session=False)
                )
                db.commit()
                found += 1
                logger.info("[%d/%d] ✓ %s  →  %s  (%d events)", idx, total, artist, url, updated)
            else:
                failed += 1
                logger.info("[%d/%d] ✗ %s  (no result)", idx, total, artist)

            # Progress ETA every 50 artists
            if idx % 50 == 0:
                elapsed = time.time() - start
                rate = idx / elapsed
                eta_s = (total - idx) / rate if rate else 0
                eta_m = eta_s / 60
                logger.info(
                    "── Progress: %d/%d  |  found=%d  failed=%d  |  ETA %.1f min ──",
                    idx, total, found, failed, eta_m,
                )

        elapsed_m = (time.time() - start) / 60
        logger.info(
            "Done in %.1f min — found=%d  failed=%d  total=%d",
            elapsed_m, found, failed, total,
        )

    finally:
        db.close()


if __name__ == "__main__":
    limit = 0
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--limit" and i + 1 < len(sys.argv[1:]):
            limit = int(sys.argv[i + 2])

    asyncio.run(enrich(limit=limit))
