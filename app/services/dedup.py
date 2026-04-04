"""
Cross-source event deduplication.

Strategy:
  1. Group events by (venue_id, start_date) — only same venue + same day can be dupes
  2. Within each group, cluster by name similarity (>80%)
  3. In each cluster keep the "best" event; delete the rest
  4. "Best" = highest source priority + most fields filled

Source priority (higher = preferred):
  ticketmaster > resident_advisor > bandsintown > scraper > venue_web > (anything else)
"""
from __future__ import annotations

import logging
from difflib import SequenceMatcher
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models import Event

logger = logging.getLogger(__name__)

SOURCE_PRIORITY: dict[str, int] = {
    "ticketmaster": 10,
    "resident_advisor": 9,
    "bandsintown": 8,
    "scraper": 5,
    "venue_web": 3,
}


def _priority(event: Event) -> int:
    return SOURCE_PRIORITY.get(event.scrape_source or "", 1)


def _completeness(event: Event) -> int:
    """Score based on how many useful fields are populated."""
    return sum([
        bool(event.purchase_link),
        bool(event.price),
        bool(event.image_url),
        bool(event.start_time),
        bool(event.artist_name),
        bool(event.description),
        bool(event.venue_id),
    ])


def _best(events: list[Event]) -> Event:
    return max(events, key=lambda e: (_priority(e), _completeness(e)))


def _similar(a: str, b: str) -> bool:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() > 0.80


def _cluster(events: list[Event]) -> list[list[Event]]:
    """Greedy clustering: merge events whose names are similar."""
    clusters: list[list[Event]] = []
    for event in events:
        placed = False
        for cluster in clusters:
            if any(_similar(event.name, e.name) for e in cluster):
                cluster.append(event)
                placed = True
                break
        if not placed:
            clusters.append([event])
    return clusters


def dedup_events(db: Session) -> dict:
    """
    Find and remove duplicate events across sources.
    Returns {"groups_checked": int, "duplicates_removed": int}.
    """
    # Find venue+date combos that have more than one event
    groups = (
        db.query(Event.venue_id, Event.start_date)
        .filter(Event.venue_id.isnot(None))
        .group_by(Event.venue_id, Event.start_date)
        .having(func.count(Event.id) > 1)
        .all()
    )

    groups_checked = 0
    removed = 0

    for venue_id, start_date in groups:
        events = (
            db.query(Event)
            .filter_by(venue_id=venue_id, start_date=start_date)
            .all()
        )
        if len(events) < 2:
            continue

        groups_checked += 1
        clusters = _cluster(events)

        for cluster in clusters:
            if len(cluster) < 2:
                continue
            keeper = _best(cluster)
            # Merge any missing fields from lower-priority dupes into keeper
            for dupe in cluster:
                if dupe.id == keeper.id:
                    continue
                if not keeper.purchase_link and dupe.purchase_link:
                    keeper.purchase_link = dupe.purchase_link
                if not keeper.price and dupe.price:
                    keeper.price = dupe.price
                    keeper.price_currency = dupe.price_currency
                if not keeper.image_url and dupe.image_url:
                    keeper.image_url = dupe.image_url
                if not keeper.start_time and dupe.start_time:
                    keeper.start_time = dupe.start_time
                if not keeper.artist_name and dupe.artist_name:
                    keeper.artist_name = dupe.artist_name
                db.delete(dupe)
                removed += 1

    db.commit()
    logger.info(f"Dedup complete: {groups_checked} groups checked, {removed} duplicates removed")
    return {"groups_checked": groups_checked, "duplicates_removed": removed}
