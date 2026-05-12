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
    build_genre_venue_name_subquery,
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
    from sqlalchemy import or_, and_, func, select
    from app.models import City

    # Strict artist filter — used when the user clicked an "Artist" suggestion
    # in autocomplete. Exact case-insensitive match on artist_name so "Sting"
    # returns ONLY Sting events, never "Stingrays" or "DJ Stingray". Multiple
    # values (comma-separated) are OR'd.
    #
    # Two-column match: artist_name OR (name AND empty/null artist_name).
    # The second branch catches sources that put the performer's name in
    # Event.name with no artist_name set — chiefly mevalim (Hebrew
    # comedians like שחר חסון) and techconf (conference speaker names).
    # Without this branch, clicking the "Artist · שחר חסון" chip would
    # miss the dozens of mevalim shows that ARE for him but were
    # ingested with artist_name=''. We constrain the second branch to
    # rows where artist_name is null/empty so we never accidentally
    # capture another artist's event whose name happens to equal the
    # query (e.g. a tribute event titled "Sting Tribute" by some other
    # band).
    if artist_exact:
        names = [n.strip() for n in artist_exact.split(",") if n.strip()]
        if names:
            lowered = [n.lower() for n in names]
            query = query.filter(or_(
                func.lower(Event.artist_name).in_(lowered),
                and_(
                    or_(Event.artist_name.is_(None), Event.artist_name == ""),
                    func.lower(Event.name).in_(lowered),
                ),
            ))

    # Genre filter — chip values are *parent* genres (e.g. "Rock", "Electronic").
    # Three complementary match paths combined as OR:
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
    #   3. Venue-name match (the venue-identity safety net).
    #      Events at a venue whose name contains the genre keyword
    #      (e.g. "Shablul Jazz Club", "Comedy Cellar"). Catches the case
    #      where the per-event event_type is generic ("Concert") or
    #      mis-tagged (Hebrew-Shablul events tagged "Pop Concert" by
    #      their collector). NOT guarded by "artist unclassified" — the
    #      venue identity is a strong genre signal that should win over
    #      generic per-event tags. Conservative keyword set in
    #      _GENRE_VENUE_NAME_MATCH; only well-disambiguated genres
    #      (Jazz/Comedy/Classical) participate.
    #
    # Together they give the same recall as a free-text "jazz" search but
    # with the precision of the taxonomy (no Rock Climbing under Rock).
    artist_norms = resolve_genre_artist_names(db, genres)
    if artist_norms is not None:
        from sqlalchemy import and_
        format_subq = build_genre_format_event_type_subquery(db, genres)
        venue_subq = build_genre_venue_name_subquery(db, genres)
        # The classified-artists subquery is the gate both fallback
        # legs use: "fire only when the artist is null or unclassified
        # — if the artist IS classified, path 1 has already handled
        # them (correctly when they match the requested genre,
        # silently when they don't, which is what we want)." Built
        # once and shared so we don't pay the planning cost twice.
        classified_subq = (
            build_classified_artists_subquery(db)
            if (format_subq is not None or venue_subq is not None)
            else None
        )
        conditions = []
        if artist_norms:
            conditions.append(
                func.lower(Event.artist_name).in_(artist_norms)
            )
        if format_subq is not None:
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
        if venue_subq is not None:
            # Precision gate: exclude events whose artist is already
            # classified into ANY genre. Without this, a non-jazz artist
            # booked at "Shablul Jazz Club" (Israeli jazz venue that
            # occasionally hosts pop tributes / classical crossover
            # nights) would falsely surface under Genre=Jazz purely
            # because of the venue name. With the gate, classified
            # artists are routed exclusively through path 1: they
            # surface ONLY under genres their classification supports.
            # Unclassified artists and null-artist rows still benefit
            # from the venue-name signal — that's the recall win the
            # path was added for.
            conditions.append(and_(
                or_(
                    Event.artist_name.is_(None),
                    func.lower(Event.artist_name).notin_(classified_subq),
                ),
                Event.id.in_(venue_subq),
                # Same sports guardrail — a "Comedy Cellar" venue could
                # technically host a sports trivia event tagged with
                # sport != null. Keep the genre filter clean of those.
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
                # Broad search: OR across every column the UI displays.
                # Pre-2026-05-12 this was just `name_match_ilike(Event.name, term)`,
                # which led to user-visible mismatches like searching "Concert"
                # finding 0 events even when Format=Concert was rendered in the
                # table, or "Psychedelic Rock" finding 0 even when Genre=
                # Psychedelic Rock was rendered. The display surface and the
                # search surface must be the same set of columns — otherwise
                # the UI is lying about what's findable.
                from app.models.genre import ArtistGenre as _AG
                # Artists whose primary_genre matches the term — lowercase
                # the artist's normalized_name so we can join Event.artist_name
                # via func.lower() the same way the genre filter does.
                artist_by_genre_subq = (
                    db.query(_AG.normalized_name)
                    .filter(name_match_ilike(_AG.primary_genre, term))
                    .scalar_subquery()
                )
                # Events whose attached EventType name or category matches.
                et_subq = (
                    db.query(event_event_types.c.event_id)
                    .join(EventType, EventType.id == event_event_types.c.event_type_id)
                    .filter(or_(
                        name_match_ilike(EventType.name, term),
                        name_match_ilike(EventType.category, term),
                    ))
                    .scalar_subquery()
                )
                # Venues whose name matches → match all events at those venues.
                venue_subq = (
                    db.query(Venue.id)
                    .filter(name_match_ilike(Venue.name, term))
                    .scalar_subquery()
                )
                query = query.filter(or_(
                    name_match_ilike(Event.name, term),
                    name_match_ilike(Event.artist_name, term),
                    func.lower(Event.artist_name).in_(artist_by_genre_subq),
                    Event.id.in_(et_subq),
                    Event.venue_id.in_(venue_subq),
                ))

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
    from sqlalchemy import func, case as _case, exists, select
    from app.models.genre import ArtistGenre as _AG

    # Single multi-aggregate over the FILTERED event set. The frontend
    # uses these per-column presence ratios to decide which columns to
    # hide (sparse-column logic) — pulling the truth from the full
    # result set instead of evaluating just the first 50 rendered rows
    # (which caused the visible column set to depend on page-order
    # rather than the search context).
    has_event_type = exists(
        select(1)
        .select_from(event_event_types)
        .where(event_event_types.c.event_id == Event.id)
    )
    artist_genre_known = exists(
        select(1)
        .select_from(_AG)
        .where(
            func.lower(Event.artist_name) == _AG.normalized_name,
            _AG.primary_genre.isnot(None),
            _AG.primary_genre != "UNKNOWN",
        )
    )
    base = db.query(
        func.count(Event.id.distinct()).label("total"),
        # `artist` presence must match the frontend's render rule:
        # artistHtml is "-" when artist_name == name (case-insensitive)
        # because rendering the same string twice is noise. Backend
        # used to count any non-null artist_name as present, so events
        # like "Brian Jackson" (name=artist_name) showed up as 100%
        # artist-present even though the UI rendered "-" in the
        # column. The threshold logic then kept the column visible.
        # 2026-05-12 image-13 bug.
        func.sum(_case(
            (
                Event.artist_name.isnot(None)
                & (Event.artist_name != "")
                & (func.lower(Event.artist_name) != func.lower(Event.name)),
                1,
            ),
            else_=0,
        )).label("artist"),
        func.sum(_case(
            (Event.artist_youtube_channel.isnot(None) & (Event.artist_youtube_channel != ""), 1),
            else_=0,
        )).label("youtube"),
        func.sum(_case((Event.price.isnot(None), 1), else_=0)).label("price"),
        func.sum(_case((Event.tv_channels.isnot(None), 1), else_=0)).label("tv"),
        func.sum(_case(
            (Event.purchase_link.isnot(None) & (Event.purchase_link != ""), 1),
            else_=0,
        )).label("link"),
        func.sum(_case((has_event_type, 1), else_=0)).label("format_and_category"),
        func.sum(_case((artist_genre_known, 1), else_=0)).label("genre"),
    )
    query = _build_filter_query(
        db, base,
        categories, type_search, city_ids, start_date, end_date, search, country,
        artist_exact=artist_exact, genres=genres,
    )
    row = query.first()
    if not row or not row.total:
        return {"total": 0, "column_presence": {}}

    total = row.total
    presence = {
        "artist":   (row.artist or 0) / total,
        "youtube":  (row.youtube or 0) / total,
        "price":    (row.price or 0) / total,
        "tv":       (row.tv or 0) / total,
        "link":     (row.link or 0) / total,
        # Format and category are populated from the same event_types
        # m2m — when at least one EventType row is attached, both
        # category and format render. Use one ratio for both columns.
        "category": (row.format_and_category or 0) / total,
        "format":   (row.format_and_category or 0) / total,
        "genre":    (row.genre or 0) / total,
    }
    return {"total": total, "column_presence": presence}


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
