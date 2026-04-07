import time
from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.models import City
from app.schemas.city import CityOut

router = APIRouter(prefix="/api/cities", tags=["cities"])

# In-memory cache — refreshes every 30 minutes.
# The city list changes only when new venues/events are scraped.
_cache: List = []
_cache_ts: float = 0.0
_TTL = 1800  # 30 minutes


def _build_city_list(db: Session) -> List:
    """
    Raw SQL: two cheap indexed lookups instead of a correlated subquery.
    1. Collect distinct venue_ids that have events  (uses ix_events_venue index)
    2. Collect distinct city_ids from those venues
    3. Return matching City rows ordered by name
    """
    rows = db.execute(text("""
        SELECT c.id, c.name, c.country, c.state, c.timezone, c.latitude, c.longitude
        FROM cities c
        WHERE c.id IN (
            SELECT DISTINCT v.city_id
            FROM venues v
            WHERE v.city_id IS NOT NULL
              AND v.id IN (
                  SELECT DISTINCT e.venue_id
                  FROM events e
                  WHERE e.venue_id IS NOT NULL
              )
        )
        ORDER BY c.name
    """)).fetchall()
    # Convert raw rows to City-like dicts the schema can serialise
    return [
        City(id=r[0], name=r[1], country=r[2], state=r[3],
             timezone=r[4], latitude=r[5], longitude=r[6])
        for r in rows
    ]


def warm_cities_cache():
    """Call once at startup so the first user request is instant."""
    global _cache, _cache_ts
    db = SessionLocal()
    try:
        _cache = _build_city_list(db)
        _cache_ts = time.time()
    finally:
        db.close()


@router.get("", response_model=List[CityOut])
def list_cities(db: Session = Depends(get_db)):
    global _cache, _cache_ts
    if _cache and (time.time() - _cache_ts) < _TTL:
        return _cache
    _cache = _build_city_list(db)
    _cache_ts = time.time()
    return _cache
