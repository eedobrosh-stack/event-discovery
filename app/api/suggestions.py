from datetime import date, datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Event, EventType, Venue

router = APIRouter(prefix="/api/suggestions", tags=["suggestions"])

# ── In-memory suggestions cache (5-min TTL, keyed by query string) ────────────
_cache: dict = {}
_CACHE_TTL = 300  # seconds


def _cache_get(q: str) -> Optional[list]:
    entry = _cache.get(q)
    if entry and (datetime.utcnow() - entry["ts"]).total_seconds() < _CACHE_TTL:
        return entry["data"]
    return None


def _cache_set(q: str, data: list) -> None:
    _cache[q] = {"data": data, "ts": datetime.utcnow()}
    # Evict old entries if cache grows too large
    if len(_cache) > 500:
        cutoff = datetime.utcnow()
        stale = [k for k, v in _cache.items()
                 if (cutoff - v["ts"]).total_seconds() >= _CACHE_TTL]
        for k in stale:
            _cache.pop(k, None)


@router.get("")
def get_suggestions(
    q: str = Query(..., min_length=1),
    limit: int = Query(12, le=30),
    db: Session = Depends(get_db),
):
    """
    Returns autocomplete suggestions mixing:
      - Event categories  (badge: "Category")
      - Event types       (badge: "Type")
      - Artist names      (badge: "Artist")  — from future events only
      - Venues            (badge: "Venue")   — from future events only
    """
    cached = _cache_get(q)
    if cached is not None:
        return cached[:limit]

    q_like = f"%{q}%"
    today = date.today()
    PER_TYPE = 3

    # 1. Categories
    cats = (
        db.query(EventType.category)
        .filter(EventType.category.ilike(q_like))
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    categories = [{"kind": "category", "value": cat, "label": cat, "badge": "Category"}
                  for (cat,) in cats]

    # 2. Event types
    types = (
        db.query(EventType.name, EventType.category)
        .filter(EventType.name.ilike(q_like))
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    event_types = [{"kind": "event_type", "value": name, "label": name, "badge": "Type"}
                   for name, _ in types]

    # 3. Artists — query Event.artist_name directly (avoids correlated EXISTS on
    #    Performer table which is slow on large event sets).
    #    Only consider future events so stale artists don't pollute results.
    artist_rows = (
        db.query(Event.artist_name)
        .filter(
            Event.artist_name.isnot(None),
            Event.artist_name.ilike(q_like),
            Event.start_date >= today,
        )
        .distinct()
        .limit(PER_TYPE + 2)
        .all()
    )
    artists = [{"kind": "performer", "value": name, "label": name, "badge": "Artist"}
               for (name,) in artist_rows if name]

    # 4. Venues — JOIN to events instead of correlated EXISTS; filter future events.
    venue_rows = (
        db.query(Venue.name, Venue.physical_city)
        .join(Event, Event.venue_id == Venue.id)
        .filter(
            Venue.name.ilike(q_like),
            Event.start_date >= today,
        )
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    venue_results = [
        {"kind": "venue", "value": name,
         "label": f"{name} — {city}" if city else name, "badge": "Venue"}
        for name, city in venue_rows
    ]

    results = (categories + event_types + artists + venue_results)[:limit]
    _cache_set(q, results)
    return results
