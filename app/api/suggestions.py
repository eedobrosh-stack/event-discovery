from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, or_
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Event, EventType, Venue
from app.models.genre import GenreTaxonomy
from app.api._search_filters import name_match_ilike

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
    # Tightened matching: name_match_ilike tightens to word-start hits for
    # long terms ("sting" → "Stinging" yes, "testing" no) and whole-word for
    # short terms ("JAX" → "JAX Conf" yes, "Ajax" no). Used by every text
    # branch below except sports detection (which has its own prefix/whole
    # word logic) and the relevance CASE expression (uses raw prefix/exact).
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
        .filter(name_match_ilike(EventType.category, q_stripped))
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    categories = [{"kind": "category", "value": cat, "label": cat, "badge": "Category"}
                  for (cat,) in cats]

    # 2. Event types
    types = (
        db.query(EventType.name, EventType.category)
        .filter(name_match_ilike(EventType.name, q_stripped))
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
        home_rows = (
            db.query(Event.home_team)
            .filter(Event.sport.isnot(None), name_match_ilike(Event.home_team, q_stripped))
            .distinct()
            .limit(20)
            .all()
        )
        away_rows = (
            db.query(Event.away_team)
            .filter(Event.sport.isnot(None), name_match_ilike(Event.away_team, q_stripped))
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

    # 3b. Genres — match the query against parent OR sub-genre names, but
    # always present the *parent* as the suggestion. Users never see the
    # 92-row sub-genre list directly: typing "techno" surfaces "Electronic",
    # typing "indie" surfaces "Rock"/"Pop", typing "rock" surfaces "Rock".
    #
    # Two-pass ranking so a direct parent hit isn't drowned out by accidental
    # sub-genre word matches. If the query matches any parent name directly,
    # use those parents only — otherwise "rock" would pull in Country (because
    # of the Country Rock sub-genre), polluting the obvious case. Only when
    # there is no parent hit do we fan out across sub-genre matches.
    parent_hit_rows = (
        db.query(GenreTaxonomy.parent_genre)
        .filter(name_match_ilike(GenreTaxonomy.parent_genre, q_stripped))
        .distinct()
        .all()
    )
    if parent_hit_rows:
        genre_parents = [r[0] for r in parent_hit_rows if r[0]]
    else:
        sub_hit_rows = (
            db.query(GenreTaxonomy.parent_genre)
            .filter(name_match_ilike(GenreTaxonomy.sub_genre, q_stripped))
            .distinct()
            .all()
        )
        genre_parents = [r[0] for r in sub_hit_rows if r[0]]

    seen_parents: set[str] = set()
    genres_results: list = []
    for parent in genre_parents:
        if parent not in seen_parents:
            seen_parents.add(parent)
            genres_results.append({
                "kind": "genre", "value": parent, "label": parent, "badge": "Genre",
            })
    genres_results = genres_results[:PER_TYPE]

    # 4. Artists — word-aware match on artist_name, no date filtering for speed.
    # Rank by exact > prefix > word-start so "Sting" beats "Stingrays" in the
    # Artist slot; alphabetical otherwise would let plurals/extensions sort
    # ahead of the literal hit the user almost certainly wants.
    q_lower_artist = q_stripped.lower()
    artist_relevance = case(
        (func.lower(Event.artist_name) == q_lower_artist, 0),    # exact
        (Event.artist_name.ilike(f"{q_stripped}%"), 1),          # prefix
        else_=2,                                                  # word-start
    )
    artist_rows = (
        db.query(Event.artist_name)
        .filter(
            Event.artist_name.isnot(None),
            name_match_ilike(Event.artist_name, q_stripped),
        )
        .distinct()
        .order_by(artist_relevance, Event.artist_name)
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
        .filter(or_(
            name_match_ilike(Venue.name, q_stripped),
            name_match_ilike(Venue.physical_city, q_stripped),
        ))
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    venue_results = [
        {"kind": "venue", "value": name,
         "label": f"{name} — {city}" if city else name, "badge": "Venue"}
        for name, city in venue_rows
    ]

    # 6. Event names — covers performers / conferences whose name lives only
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
            name_match_ilike(Event.name, q_stripped),
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

    # Order: artists first (the dominant intent for music-driven searches —
    # "stin" → user almost always means Sting), then sports teams (won't
    # collide with artists since teams aren't named "Sting"), then literal
    # event-name hits, then everything else. Cities are deliberately NOT
    # surfaced here — the dedicated Location box on both home and results
    # pages owns city navigation; mixing cities into the type/performer
    # suggestions just leaks irrelevant rows like "Blowing Rock" when a user
    # types "rock".
    # Order: artists first (the dominant intent for music-driven searches),
    # then genres (tolerant fallback when the user typed a sub-genre or a
    # vague genre word), then sports teams, then everything else.
    results = (
        artists + genres_results + sport_teams + event_results
        + categories + event_types + venue_results
    )[:limit]
    _cache_set(q, results)
    return results
