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
import re
import unicodedata
from datetime import date
from difflib import SequenceMatcher
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models import Event

logger = logging.getLogger(__name__)

_NON_WORD = re.compile(r"[^\w\s]+")
_WS = re.compile(r"\s+")


def normalize_title(name: str | None) -> str:
    r"""Case- and punctuation-insensitive key for an event title.

    NFKC-fold, lowercase, replace every non-alphanumeric run with a space,
    collapse whitespace. "TECHSPO Philadelphia 2026!" and "techspo
    philadelphia, 2026" both become "techspo philadelphia 2026". Unicode
    letters survive (\w is unicode-aware), so Hebrew/German titles key
    correctly. Returns "" for empty input."""
    if not name:
        return ""
    s = unicodedata.normalize("NFKC", name).lower()
    s = _NON_WORD.sub(" ", s).replace("_", " ")
    return _WS.sub(" ", s).strip()


def _times_compatible(a: str | None, b: str | None) -> bool:
    """False only when both times are set and differ (matinee vs evening)."""
    ta = (a or "").strip()
    tb = (b or "").strip()
    return not (ta and tb and ta != tb)


def find_null_venue_duplicate(
    db: Session,
    name: str | None,
    start_date: date | None,
    start_time: str | None = None,
) -> Event | None:
    """Return an existing venue-less Event with the same normalised title
    and start_date (and a non-contradicting start_time), or None.

    Used at ingest for incoming events that have no venue / are online:
    the LLM extractor and jsonld recipes save the same virtual event once
    per source page, each with its own source_id, so the (scrape_source,
    source_id) check never catches them and the venue-keyed similarity
    check cannot run. Only rows with ``venue_id IS NULL`` are candidates,
    so same-named shows that *have* a venue are never affected.

    Candidates are fetched as light tuples (id, name, start_time) for the
    date and compared in Python — the null-venue pool per date is a few
    hundred rows at most, and SQL cannot do the punctuation folding."""
    key = normalize_title(name)
    if not key or start_date is None:
        return None
    rows = (
        db.query(Event.id, Event.name, Event.start_time)
        .filter(Event.start_date == start_date, Event.venue_id.is_(None))
        .all()
    )
    for ev_id, ev_name, ev_time in rows:
        if normalize_title(ev_name) == key and _times_compatible(start_time, ev_time):
            return db.get(Event, ev_id)
    return None

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
