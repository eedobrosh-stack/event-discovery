from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Event, EventType, Venue

# Sports event names follow the pattern "League - Home vs Away".
# When a query exactly matches a league prefix (e.g. "NBA", "EuroLeague"),
# we return only the sport suggestion and suppress all other completions.
_MIN_SPORT_QUERY_LEN = 2

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

    q_stripped = q.strip()
    q_like = f"%{q_stripped}%"
    PER_TYPE = 3

    # ── Sports league early-exit ────────────────────────────────────────────────
    # Sports events are named "League - Home vs Away" (e.g. "NBA - Spurs vs Lakers").
    # If the query matches a league prefix, return only that sport suggestion so
    # users can build a clean "NBA calendar" without noisy autocomplete mixing in.
    if len(q_stripped) >= _MIN_SPORT_QUERY_LEN:
        league_prefix = f"{q_stripped} -%"
        sport_name_rows = (
            db.query(Event.name, Event.sport)
            .filter(
                Event.sport.isnot(None),
                Event.name.ilike(league_prefix),
            )
            .distinct(Event.name)
            .limit(10)
            .all()
        )
        if sport_name_rows:
            # Extract unique league labels from event names (everything before " - ")
            leagues: dict[str, str] = {}  # league_label → sport value
            for name, sport in sport_name_rows:
                if " - " in name:
                    label = name.split(" - ")[0].strip()
                    if label.lower().startswith(q_stripped.lower()):
                        leagues[label] = label  # use label as the search value
            if leagues:
                results = [
                    {"kind": "sport", "value": label, "label": label, "badge": "Sport"}
                    for label in sorted(leagues)
                ][:limit]
                _cache_set(q, results)
                return results

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

    # 3. Artists — simple ilike on artist_name, no date filtering for speed
    artist_rows = (
        db.query(Event.artist_name)
        .filter(
            Event.artist_name.isnot(None),
            Event.artist_name.ilike(q_like),
        )
        .distinct()
        .limit(PER_TYPE + 2)
        .all()
    )
    artists = [{"kind": "performer", "value": name, "label": name, "badge": "Artist"}
               for (name,) in artist_rows if name]

    # 4. Venues — direct query, no event JOIN needed for speed
    venue_rows = (
        db.query(Venue.name, Venue.physical_city)
        .filter(Venue.name.ilike(q_like))
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
