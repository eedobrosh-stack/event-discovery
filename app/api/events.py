from typing import Optional, List
from datetime import date, datetime
from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy.orm import Session, joinedload, selectinload

from app.database import get_db
from app.models import Event, EventType, Venue, Performer, event_event_types, ZeroResultSearch
from app.schemas.event import EventOut, ZeroResultSearchRequest
from app.api._search_filters import (
    word_boundary_ilike,
    name_match_ilike,
    resolve_genre_artist_names,
    build_genre_format_event_type_subquery,
    build_classified_artists_subquery,
)

router = APIRouter(prefix="/api/events", tags=["events"])

# Exact league labels — when a search term matches one of these exactly
# (case-insensitive), we use a strict prefix match ("NBA - %") instead of
# a substring match ("%NBA%") to block WNBA, artist names, etc.
def _get_sport_league_labels() -> frozenset[str]:
    try:
        from app.services.collectors.scrapers.sports.leagues import ESPN_LEAGUES
        labels = {cfg.label.lower() for cfg in ESPN_LEAGUES}
    except Exception:
        labels = set()
    # Add non-ESPN leagues
    labels.update({
        "mlb", "formula 1", "cricket", "euroleague", "eurocup",
        "euroleague basketball", "eurocup basketball",
    })
    return frozenset(labels)

_SPORT_LEAGUE_LABELS: frozenset[str] = _get_sport_league_labels()


# Search-filter helpers (word_boundary_ilike, name_match_ilike) live in
# app/api/_search_filters.py and are reused by suggestions.py too.


def _build_filter_query(db: Session, query, categories, type_search, city_ids, start_date, end_date, search, country=None, artist_exact=None, genres=None):
    """Shared filter logic used by both list and count endpoints."""
    from sqlalchemy import or_, func, select
    from app.models import City

    # Strict artist filter — used when the user clicked an "Artist" suggestion
    # in autocomplete. Exact case-insensitive match on artist_name so "Sting"
    # returns ONLY Sting events, never "Stingrays" or "DJ Stingray". Multiple
    # values (comma-separated) are OR'd.
    if artist_exact:
        names = [n.strip() for n in artist_exact.split(",") if n.strip()]
        if names:
            lowered = [n.lower() for n in names]
            query = query.filter(
                Event.artist_name.isnot(None),
                func.lower(Event.artist_name).in_(lowered),
            )

    # Genre filter — chip values are *parent* genres (e.g. "Rock", "Electronic").
    # Two complementary match paths combined as OR:
    #
    #   1. Artist-genre match (the canonical one).
    #      Parent → sub-genres in genre_taxonomy → artists tagged with any
    #      sub-genre via artist_genre.primary/secondary. Surfaces events by
    #      classified artists.
    #
    #   2. Format-fallback match (the safety net).
    #      For events whose artist isn't classified (null artist_name OR not
    #      in artist_genre OR primary='UNKNOWN'), match by event_type name
    #      via build_genre_format_event_type_subquery. This rescues "Jazz
    #      Night at Shablul Jazz Club" with artist=null when user searches
    #      Genre=Jazz — without it, we'd only return the ~2 jazz artists
    #      whose name happens to be classified.
    #
    # Together they give the same recall as a free-text "jazz" search but
    # with the precision of the taxonomy (no Rock Climbing under Rock).
    artist_norms = resolve_genre_artist_names(db, genres)
    if artist_norms is not None:
        from sqlalchemy import and_
        format_subq = build_genre_format_event_type_subquery(db, genres)
        conditions = []
        if artist_norms:
            conditions.append(
                func.lower(Event.artist_name).in_(artist_norms)
            )
        if format_subq is not None:
            classified_subq = build_classified_artists_subquery(db)
            conditions.append(and_(
                or_(
                    Event.artist_name.is_(None),
                    func.lower(Event.artist_name).notin_(classified_subq),
                ),
                Event.id.in_(format_subq),
                # Guardrail: never surface sports rows under a music genre.
                # The existing event_type classifier sometimes mis-tags
                # basketball games as "Rock Concert" — that's a data-quality
                # bug upstream, but it would be very visible here without
                # this filter. Cheap to enforce; nothing legitimate is lost
                # since music genres don't apply to sports anyway.
                Event.sport.is_(None),
            ))
        if conditions:
            query = query.filter(or_(*conditions))
        else:
            # Defensive — should not happen since autocomplete only emits
            # real parents and every parent has either tagged artists or a
            # format mapping. Treat as no-match.
            query = query.filter(False)

    # Legacy: exact category filter
    if categories:
        cat_list = [c.strip() for c in categories.split(",")]
        type_ids = (
            db.query(EventType.id)
            .filter(EventType.category.in_(cat_list))
            .subquery()
        )
        query = query.join(event_event_types).filter(
            event_event_types.c.event_type_id.in_(
                db.query(type_ids.c.id)
            )
        )

    if type_search:
        terms = [t.strip() for t in type_search.split(",") if t.strip()]
        for term in terms:
            # Word-aware matching: ≥4 chars → word-start ("sting" matches
            # "Stinging" but not "testing"); <4 chars → strict whole-word
            # ("JAX" matches "JAX Conf" but not "Ajax Amsterdam"). Replaces
            # the previous %term% substring match that surfaced "testing"
            # when the user searched for "sting".
            type_matched_event_ids = (
                select(event_event_types.c.event_id)
                .join(EventType, EventType.id == event_event_types.c.event_type_id)
                .where(or_(
                    name_match_ilike(EventType.name, term),
                    name_match_ilike(EventType.category, term),
                ))
                .scalar_subquery()
            )
            venue_matched_event_ids = (
                select(Event.id)
                .join(Venue, Event.venue_id == Venue.id)
                .where(name_match_ilike(Venue.name, term))
                .scalar_subquery()
            )
            # Exact league label → strict prefix, same as `search` param
            if term.lower() in _SPORT_LEAGUE_LABELS:
                prefix_like = f"{term} -%"
                query = query.filter(Event.name.ilike(prefix_like))
            else:
                # Whole-word check so "JAX" doesn't match "Ajax Amsterdam"
                # and lock the entire result set to sports.
                is_sports_term = (
                    db.query(Event.id)
                    .filter(
                        Event.sport.isnot(None),
                        word_boundary_ilike(Event.name, term),
                    )
                    .limit(1)
                    .scalar()
                )
                if is_sports_term:
                    query = query.filter(
                        Event.sport.isnot(None),
                        name_match_ilike(Event.name, term),
                    )
                else:
                    query = query.filter(or_(
                        Event.id.in_(type_matched_event_ids),
                        name_match_ilike(Event.artist_name, term),
                        name_match_ilike(Event.name, term),
                        Event.id.in_(venue_matched_event_ids),
                    ))

    if city_ids:
        ids = [int(x.strip()) for x in city_ids.split(",") if x.strip().isdigit()]
        query = query.join(Venue, Event.venue_id == Venue.id).filter(
            Venue.city_id.in_(ids)
        )
    elif country:
        # Country filter — join through venue→city and match country name
        query = (
            query
            .join(Venue, Event.venue_id == Venue.id)
            .join(City, Venue.city_id == City.id)
            .filter(City.country.ilike(country))
        )

    query = query.filter(Event.start_date >= date.today())
    if start_date:
        query = query.filter(Event.start_date >= start_date)
    if end_date:
        query = query.filter(Event.start_date <= end_date)
    if search:
        # Exact league label (e.g. "NBA", "Champions League") → strict prefix
        # match so "WNBA" or music artists don't bleed into the results.
        # No sport IS NOT NULL requirement — old events may have sport=NULL
        # before the backfill runs; the prefix pattern is specific enough.
        if search.strip().lower() in _SPORT_LEAGUE_LABELS:
            prefix_like = f"{search.strip()} -%"
            query = query.filter(Event.name.ilike(prefix_like))
        else:
            term = search.strip()
            # If the search term matches any *sports* event name as a whole
            # word, restrict the result set to sports events. Whole-word
            # matching prevents short tokens like "JAX" (substring of Ajax),
            # "FOX" (Foxes), or "PSG" (PSGs) from auto-locking results into
            # sports-only mode and hiding the JAX tech conference et al.
            is_sports_term = (
                db.query(Event.id)
                .filter(
                    Event.sport.isnot(None),
                    word_boundary_ilike(Event.name, term),
                )
                .limit(1)
                .scalar()
            )
            if is_sports_term:
                query = query.filter(
                    Event.sport.isnot(None),
                    name_match_ilike(Event.name, term),
                )
            else:
                query = query.filter(name_match_ilike(Event.name, term))

    return query


@router.get("/count")
def count_events(
    categories: Optional[str] = Query(None),
    type_search: Optional[str] = Query(None),
    city_ids: Optional[str] = Query(None),
    country: Optional[str] = Query(None, description="Filter by country name"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None,
    artist_exact: Optional[str] = Query(None, description="Comma-separated exact artist names (case-insensitive)"),
    genres: Optional[str] = Query(None, description="Comma-separated parent genre names (Rock, Electronic, …); expanded to all sub-genres' artists."),
    db: Session = Depends(get_db),
):
    from sqlalchemy import func
    query = _build_filter_query(
        db, db.query(func.count(Event.id.distinct())),
        categories, type_search, city_ids, start_date, end_date, search, country,
        artist_exact=artist_exact, genres=genres,
    )
    return {"total": query.scalar() or 0}


@router.get("", response_model=List[EventOut])
def list_events(
    categories: Optional[str] = Query(None, description="Comma-separated category names (legacy)"),
    type_search: Optional[str] = Query(None, description="Comma-separated terms; OR-searches event type name, category, and artist name"),
    city_ids: Optional[str] = Query(None, description="Comma-separated city IDs"),
    country: Optional[str] = Query(None, description="Filter by country name"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None,
    artist_exact: Optional[str] = Query(None, description="Comma-separated exact artist names (case-insensitive)"),
    genres: Optional[str] = Query(None, description="Comma-separated parent genre names; expanded to all sub-genres' artists."),
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    base_query = db.query(Event).options(
        joinedload(Event.venue),
        selectinload(Event.event_types),
    )
    query = _build_filter_query(
        db, base_query, categories, type_search, city_ids, start_date, end_date, search, country,
        artist_exact=artist_exact, genres=genres,
    )

    events = (
        query.order_by(Event.start_date, Event.start_time)
        .offset(offset)
        .limit(limit)
        .all()
    )

    # Bulk-fetch artist genres for this page in one shot. Avoids per-row
    # lookups; ~50 artists per page × O(1) hash lookup is trivially fast.
    artist_genre_map: dict[str, str] = {}
    artist_lowered = {
        e.artist_name.lower()
        for e in events
        if e.artist_name
    }
    if artist_lowered:
        from app.models.genre import ArtistGenre
        rows = (
            db.query(ArtistGenre.normalized_name, ArtistGenre.primary_genre)
            .filter(ArtistGenre.normalized_name.in_(artist_lowered))
            .all()
        )
        # Skip "UNKNOWN" — surfacing it as a label is noise; null is cleaner.
        artist_genre_map = {
            n: g for (n, g) in rows
            if g and g != "UNKNOWN"
        }

    results = []
    for e in events:
        out = EventOut.model_validate(e)
        types = e.event_types or []
        out.categories = list(dict.fromkeys(et.category for et in types if et.category))
        out.event_types = [et.name for et in types if et.name]
        if e.artist_name:
            out.artist_genre = artist_genre_map.get(e.artist_name.lower())
        if e.venue and e.venue.timezone:
            out.venue_timezone = e.venue.timezone
        if e.venue and e.venue.website_url:
            out.venue_website_url = e.venue.website_url
        if e.venue and e.venue.physical_city:
            out.venue_city = e.venue.physical_city
        if e.venue and e.venue.physical_country:
            out.venue_country = e.venue.physical_country

        # Synthesize a YouTube highlights search URL for sports events
        # (sports rows have artist_name=None, so there is no performer
        # channel to pull from).  Link target is the YouTube results page
        # for "<Home> vs <Away> highlights".
        if not out.artist_youtube_channel and e.sport and e.home_team and e.away_team:
            from urllib.parse import quote_plus
            q = quote_plus(f"{e.home_team} vs {e.away_team} highlights")
            out.artist_youtube_channel = f"https://www.youtube.com/results?search_query={q}"

        results.append(out)
    return results


@router.post("/zero-result")
def log_zero_result_search(
    payload: ZeroResultSearchRequest,
    request: Request,
    db: Session = Depends(get_db),
):
    """Persist a search that came up empty even after lookahead.

    The frontend calls this after a search returns 0 events AND the
    end-date-extended retry also returns 0. The row goes into
    `zero_result_searches` for offline scanning ("what's the catalog
    missing that users want?"). Cheap append-only — no dedupe at
    write-time; aggregation happens at read-time via SQL.

    Returns {"ok": true} regardless of payload contents (best-effort).
    """
    try:
        db.add(ZeroResultSearch(
            genres=payload.genres,
            artists=payload.artists,
            type_search=payload.type_search,
            free_search=payload.free_search,
            city_ids=payload.city_ids,
            country=payload.country,
            start_date=payload.start_date,
            end_date=payload.end_date,
            user_agent=(request.headers.get("user-agent") or "")[:500] or None,
        ))
        db.commit()
    except Exception:
        # Best-effort logging — never break the user's empty-state UX.
        db.rollback()
    return {"ok": True}
