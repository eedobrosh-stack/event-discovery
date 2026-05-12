import time
from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.models import City
from app.schemas.city import CityOut
from app.api._us_states import normalize as normalize_us_state

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

    State values for US rows are normalised to their canonical full
    name (e.g. "CA" → "California") at API-output time. The DB still
    stores 2-letter codes (with two known full-name outliers); the
    frontend should never have to know about either format.
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
    out: List = []
    for r in rows:
        state = r[3]
        if r[2] == "United States" and state:
            state = normalize_us_state(state)
        out.append(City(id=r[0], name=r[1], country=r[2], state=state,
                        timezone=r[4], latitude=r[5], longitude=r[6]))
    return out


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


# ── Countries endpoint ─────────────────────────────────────────────────────────

_country_cache: List = []
_country_cache_ts: float = 0.0


@router.get("/countries")
def list_countries(db: Session = Depends(get_db)):
    """Return distinct countries that have cities with events, ordered by event count."""
    global _country_cache, _country_cache_ts
    if _country_cache and (time.time() - _country_cache_ts) < _TTL:
        return _country_cache

    rows = db.execute(text("""
        SELECT c.country, COUNT(DISTINCT c.id) as city_count, COUNT(DISTINCT e.id) as event_count
        FROM cities c
        JOIN venues v ON v.city_id = c.id
        JOIN events e ON e.venue_id = v.id
        WHERE c.country IS NOT NULL AND c.country != ''
        GROUP BY c.country
        ORDER BY event_count DESC
    """)).fetchall()

    result = [
        {"name": r[0], "city_count": r[1], "event_count": r[2]}
        for r in rows
    ]
    _country_cache = result
    _country_cache_ts = time.time()
    return result


# ── States endpoint (US-only) ──────────────────────────────────────────────

_state_cache: List = []
_state_cache_ts: float = 0.0


@router.get("/states")
def list_states(db: Session = Depends(get_db)):
    """Return distinct US states that have cities with events.

    State codes (CA / NY / …) and the two known full-name outliers
    (``"Ohio"`` / ``"West Virginia"``) are aggregated under their
    canonical full display name in Python — we can't rely on the DB
    GROUP BY because the two storage forms are different strings.
    Output shape mirrors /countries:
        {name, country, city_count, event_count}
    Ordered by event_count desc so heavyweight states surface first
    in autocomplete.
    """
    global _state_cache, _state_cache_ts
    if _state_cache and (time.time() - _state_cache_ts) < _TTL:
        return _state_cache

    raw_rows = db.execute(text("""
        SELECT c.state, COUNT(DISTINCT c.id) AS city_count,
               COUNT(DISTINCT e.id) AS event_count
        FROM cities c
        JOIN venues v ON v.city_id = c.id
        JOIN events e ON e.venue_id = v.id
        WHERE c.country = 'United States'
          AND c.state IS NOT NULL AND c.state != ''
        GROUP BY c.state
    """)).fetchall()

    # Aggregate by canonical full name (handles the OH/Ohio split).
    by_name: dict[str, dict] = {}
    for state_value, city_count, event_count in raw_rows:
        canonical = normalize_us_state(state_value)
        if not canonical:
            continue
        bucket = by_name.setdefault(
            canonical,
            {"name": canonical, "country": "United States",
             "city_count": 0, "event_count": 0},
        )
        bucket["city_count"] += int(city_count or 0)
        bucket["event_count"] += int(event_count or 0)

    result = sorted(by_name.values(), key=lambda r: -r["event_count"])
    _state_cache = result
    _state_cache_ts = time.time()
    return result


# ── Continents + sub-continents endpoints ──────────────────────────────────
# Layered above Country in the location autocomplete cascade. Like
# /states, both endpoints return aggregated event-counts so the
# frontend can rank chips by relevance. Selection is implemented
# client-side via the multi-city-id plumbing — same code path the
# states layer uses.

_continent_cache: List = []
_continent_cache_ts: float = 0.0
_subcontinent_cache: List = []
_subcontinent_cache_ts: float = 0.0


def _aggregate_geo(db: Session, by_sub_continent: bool):
    """Shared aggregation: country → continent (or sub_continent) →
    {city_count, event_count, country_count}. One SQL round-trip
    over the (country, COUNT) pairs we already cache for /countries,
    rolled up in Python via the COUNTRY_TO_CONTINENT map."""
    from app.api._continents import COUNTRY_TO_CONTINENT
    rows = db.execute(text("""
        SELECT c.country, COUNT(DISTINCT c.id) AS city_count,
               COUNT(DISTINCT e.id) AS event_count
        FROM cities c
        JOIN venues v ON v.city_id = c.id
        JOIN events e ON e.venue_id = v.id
        WHERE c.country IS NOT NULL AND c.country != ''
        GROUP BY c.country
    """)).fetchall()

    by_key: dict[str, dict] = {}
    for country, city_count, event_count in rows:
        rec = COUNTRY_TO_CONTINENT.get(country)
        if not rec:
            continue
        cont, sub = rec
        key = sub if by_sub_continent else cont
        if not key:
            continue
        bucket = by_key.setdefault(key, {
            "name": key,
            "city_count": 0,
            "event_count": 0,
            "country_count": 0,
        })
        bucket["city_count"] += int(city_count or 0)
        bucket["event_count"] += int(event_count or 0)
        bucket["country_count"] += 1

    return sorted(by_key.values(), key=lambda r: -r["event_count"])


@router.get("/continents")
def list_continents(db: Session = Depends(get_db)):
    """Continents with their aggregate event counts.

    Returns: [{name, city_count, event_count, country_count}, ...]
    sorted by event_count desc.
    """
    global _continent_cache, _continent_cache_ts
    if _continent_cache and (time.time() - _continent_cache_ts) < _TTL:
        return _continent_cache
    result = _aggregate_geo(db, by_sub_continent=False)
    _continent_cache = result
    _continent_cache_ts = time.time()
    return result


@router.get("/sub-continents")
def list_sub_continents(db: Session = Depends(get_db)):
    """Sub-continents (Northern Europe, East Asia, Northern America,
    etc.) with aggregate counts.

    Returns: [{name, city_count, event_count, country_count}, ...]
    sorted by event_count desc.
    """
    global _subcontinent_cache, _subcontinent_cache_ts
    if _subcontinent_cache and (time.time() - _subcontinent_cache_ts) < _TTL:
        return _subcontinent_cache
    result = _aggregate_geo(db, by_sub_continent=True)
    _subcontinent_cache = result
    _subcontinent_cache_ts = time.time()
    return result
