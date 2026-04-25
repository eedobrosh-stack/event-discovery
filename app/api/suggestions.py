from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import City, Event, EventType, Venue

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
    #
    # SQL filter is a plain prefix (`{q}%`) rather than `{q} -%` so partial
    # typing works — "Euro" needs to match "Euroleague - Real Madrid vs …",
    # not just "Euroleague - …". The Python loop below extracts the league
    # label (everything before " - ") and keeps only labels whose own prefix
    # matches `q`, so non-sports names like "Eurovision" won't slip through
    # unless they actually use the "League - Home vs Away" pattern.
    if len(q_stripped) >= _MIN_SPORT_QUERY_LEN:
        league_prefix = f"{q_stripped}%"
        sport_name_rows = (
            db.query(Event.name, Event.sport)
            .filter(
                Event.sport.isnot(None),
                Event.name.ilike(league_prefix),
            )
            .distinct(Event.name)
            .order_by(Event.name)
            .limit(50)
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

    # 3. Sports teams — `home_team` / `away_team` are dedicated columns on sport
    # events (e.g. "Real Madrid", "Portland Trail Blazers"). Without this block
    # a query like "Real Madrid" matches nothing — league early-exit requires a
    # prefix on Event.name, and artist_name is NULL on sport rows.
    sport_teams: list = []
    if len(q_stripped) >= _MIN_SPORT_QUERY_LEN:
        team_like = f"%{q_stripped}%"
        home_rows = (
            db.query(Event.home_team)
            .filter(Event.sport.isnot(None), Event.home_team.ilike(team_like))
            .distinct()
            .limit(20)
            .all()
        )
        away_rows = (
            db.query(Event.away_team)
            .filter(Event.sport.isnot(None), Event.away_team.ilike(team_like))
            .distinct()
            .limit(20)
            .all()
        )
        # Merge home + away, dedupe by lowercase, keep canonical casing.
        seen: dict[str, str] = {}
        for (t,) in home_rows + away_rows:
            if t:
                seen.setdefault(t.lower(), t)
        sport_teams = [
            {"kind": "sport_team", "value": t, "label": t, "badge": "Team"}
            for t in sorted(seen.values())
        ][:PER_TYPE]

    # 4. Artists — simple ilike on artist_name, no date filtering for speed
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

    # 5. Venues — match against venue name OR its physical_city, so an English
    # query like "Tel Aviv" surfaces Hebrew-named venues whose physical_city is
    # "Tel Aviv". Without the physical_city branch, only literal name hits
    # qualify and Hebrew-named IL venues are unreachable from English queries.
    venue_rows = (
        db.query(Venue.name, Venue.physical_city)
        .filter(or_(Venue.name.ilike(q_like), Venue.physical_city.ilike(q_like)))
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    venue_results = [
        {"kind": "venue", "value": name,
         "label": f"{name} — {city}" if city else name, "badge": "Venue"}
        for name, city in venue_rows
    ]

    # 6. Cities — direct match on City.name, returns the cityId so the
    # frontend can populate the city filter (NOT a type-search chip — those
    # would just look for the literal city string in event/venue/type names
    # and miss Hebrew-named venues entirely).
    city_rows = (
        db.query(City.id, City.name, City.country)
        .filter(City.name.ilike(q_like))
        .order_by(City.name)
        .limit(PER_TYPE)
        .all()
    )
    city_results = [
        {"kind": "city",
         "value": str(cid),
         "label": f"{name}, {country}" if country else name,
         "badge": "City"}
        for cid, name, country in city_rows
    ]

    # 7. Event names — covers performers / conferences whose name lives only
    # in Event.name. Mevalim stores the comedian's name there (with
    # artist_name=NULL); techconf stores the conference name. Without this
    # branch, autocomplete is blind to anything those collectors produce —
    # "שחר חסון" and "Stir Trek" both return [] otherwise.
    #
    # Dedupe against the artist branch so a Spotify-enriched performer
    # doesn't appear twice (once as Artist, once as Event with the same
    # label). Filter to upcoming events so expired shows don't pollute.
    artist_names_seen = {a["value"].lower() for a in artists if a.get("value")}
    today = datetime.utcnow().date()
    # Rank by relevance so an exact match like "JAX" beats noisy substring
    # hits ("2026 Jax Waves...", "OneJax Awards"...) which would otherwise
    # sort alphabetically ahead of it and crowd out the small PER_TYPE slot.
    q_lower = q_stripped.lower()
    relevance = case(
        (func.lower(Event.name) == q_lower, 0),                # exact
        (Event.name.ilike(f"{q_stripped}%"), 1),               # prefix
        else_=2,                                                # substring
    )
    event_name_rows = (
        db.query(Event.name)
        .filter(
            Event.name.ilike(q_like),
            Event.start_date >= today,
        )
        .distinct()
        .order_by(relevance, Event.start_date, Event.name)
        .limit(PER_TYPE + 5)  # buffer for the dedupe + sport-name filter below
        .all()
    )
    event_results = [
        {"kind": "event", "value": name, "label": name, "badge": "Event"}
        for (name,) in event_name_rows
        if name and name.lower() not in artist_names_seen
        # Skip "League - Home vs Away" rows — those are sport events already
        # surfaced as Team / Sport suggestions; raw event-name listings here
        # would just be noise ("NBA - Spurs vs Lakers" et al).
        and " - " not in name
    ][:PER_TYPE]

    # Order: cities first (a city click is the strongest navigational
    # signal — "Karmiel" almost always means "show me Karmiel's calendar"
    # rather than "find an event with Karmiel in its name"), then sports
    # teams, then event-name matches (high-confidence: a literal title hit),
    # then everything else.
    results = (
        city_results + sport_teams + event_results
        + categories + event_types + artists + venue_results
    )[:limit]
    _cache_set(q, results)
    return results
