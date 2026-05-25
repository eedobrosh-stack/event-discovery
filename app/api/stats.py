from collections import defaultdict
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import case, func, text
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import (
    City, Venue, Event, EventType, event_event_types,
    SpotifyArtist, SpotifyBraveAttempt, LLMSource, Performer,
)
from app.models.scan_log import ScanLog
from app.services.collectors.scrapers.city_guides import CITY_GUIDES

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("/cities")
def city_coverage(db: Session = Depends(get_db)):
    """Return venue + upcoming-event counts per city, ordered by venue count desc."""
    today = date.today()
    rows = (
        db.query(
            City.name,
            City.country,
            func.count(func.distinct(Venue.id)).label("venues"),
            # Count only upcoming events (CASE keeps outer-join cities with 0 events)
            func.count(func.distinct(
                case((Event.start_date >= today, Event.id), else_=None)
            )).label("events"),
            func.min(case((Event.start_date >= today, Event.start_date), else_=None)).label("earliest"),
            func.max(case((Event.start_date >= today, Event.start_date), else_=None)).label("latest"),
        )
        .join(Venue, Venue.city_id == City.id, isouter=True)
        .join(Event, Event.venue_id == Venue.id, isouter=True)
        .group_by(City.id)
        .having(func.count(func.distinct(Venue.id)) > 0)
        .order_by(func.count(func.distinct(Venue.id)).desc())
        .all()
    )

    total_venues = sum(r.venues for r in rows)

    # Authoritative upcoming count: direct filter, no join (avoids excluding
    # events whose venue_id is NULL or not yet linked to a city record)
    total_upcoming = (
        db.query(func.count(Event.id))
        .filter(Event.start_date >= today)
        .scalar() or 0
    )

    return {
        "summary": {
            "cities": len(rows),
            "venues": total_venues,
            "events": total_upcoming,
        },
        "cities": [
            {
                "city": r.name,
                "country": r.country,
                "venues": r.venues,
                "events": r.events,
                "earliest": r.earliest,
                "latest": r.latest,
            }
            for r in rows
        ],
    }


@router.get("/coverage")
def coverage_health(db: Session = Depends(get_db)):
    """Return enrichment coverage metrics and per-source scan health."""

    def pct(n: int, total: int) -> int:
        return round(n * 100 / total) if total > 0 else 0

    today = date.today()

    # ── Upcoming-event enrichment ─────────────────────────────────────────────
    total = db.query(func.count(Event.id)).filter(
        Event.start_date >= today
    ).scalar() or 0

    with_youtube = db.query(func.count(Event.id)).filter(
        Event.start_date >= today,
        Event.artist_youtube_channel.isnot(None),
        Event.artist_youtube_channel != "",
    ).scalar() or 0

    with_price = db.query(func.count(Event.id)).filter(
        Event.start_date >= today,
        Event.price.isnot(None),
    ).scalar() or 0

    with_artist = db.query(func.count(Event.id)).filter(
        Event.start_date >= today,
        Event.artist_name.isnot(None),
        Event.artist_name != "",
    ).scalar() or 0

    # Events that have at least one event-type assigned (via association table)
    with_type = db.execute(
        text(
            "SELECT COUNT(DISTINCT e.id) FROM events e "
            "JOIN event_event_types eet ON eet.event_id = e.id "
            "WHERE e.start_date >= :today"
        ),
        {"today": today.isoformat()},
    ).scalar() or 0

    # ── Venue enrichment ─────────────────────────────────────────────────────
    total_venues = db.query(func.count(Venue.id)).scalar() or 0
    venues_with_url = db.query(func.count(Venue.id)).filter(
        Venue.website_url.isnot(None),
        Venue.website_url != "",
    ).scalar() or 0

    # ── Source health — last 5 runs per (job_name, detail) ───────────────────
    recent_logs = (
        db.query(ScanLog)
        .filter(ScanLog.status.in_(["success", "failed"]))
        .order_by(ScanLog.started_at.desc())
        .limit(400)
        .all()
    )

    sources_map: dict[tuple, list] = defaultdict(list)
    for log in recent_logs:
        key = (log.job_name, log.detail or "")
        if len(sources_map[key]) < 5:
            sources_map[key].append(log)

    sources = []
    for (job, detail), logs in sorted(sources_map.items()):
        last = logs[0]
        # events_found = total fetched from the source API (fixed key in jobs.py)
        # events_saved = net-new events written to DB
        # A source is stale when it fetched 0 on its last 3+ runs but previously had data
        consec_fetch_zeros = sum(1 for lg in logs if (lg.events_found or 0) == 0)
        ever_fetched = any((lg.events_found or 0) > 0 for lg in logs)
        alert = consec_fetch_zeros >= 3 and ever_fetched
        sources.append(
            {
                "job": job,
                "detail": detail,
                "last_run": last.started_at.isoformat() if last.started_at else None,
                "last_status": last.status,
                "last_fetched": last.events_found or 0,   # events pulled from source
                "last_saved": last.events_saved or 0,     # net-new to DB
                "consecutive_zeros": consec_fetch_zeros,
                "alert": alert,
            }
        )

    # ── Cities with thin coverage (< 10 upcoming events) ─────────────────────
    thin_cities = (
        db.query(
            City.name,
            City.country,
            func.count(func.distinct(Event.id)).label("upcoming"),
        )
        .join(Venue, Venue.city_id == City.id)
        .join(Event, Event.venue_id == Venue.id)
        .filter(Event.start_date >= today)
        .group_by(City.id)
        .having(func.count(func.distinct(Event.id)) < 10)
        .order_by(func.count(func.distinct(Event.id)).asc())
        .all()
    )

    return {
        "events": {
            "total": total,
            "with_type": with_type,
            "with_youtube": with_youtube,
            "with_price": with_price,
            "with_artist": with_artist,
            "type_pct": pct(with_type, total),
            "youtube_pct": pct(with_youtube, total),
            "price_pct": pct(with_price, total),
            "artist_pct": pct(with_artist, total),
        },
        "venues": {
            "total": total_venues,
            "with_url": venues_with_url,
            "url_pct": pct(venues_with_url, total_venues),
        },
        "sources": sources,
        "thin_cities": [
            {"city": r.name, "country": r.country, "upcoming": r.upcoming}
            for r in thin_cities
        ],
    }


@router.get("/upcoming-breakdown")
def upcoming_breakdown(db: Session = Depends(get_db)):
    """
    Breakdown of upcoming events along three orthogonal dimensions:
      • by_category — EventType.category   (top-level bucket)
      • by_format   — EventType.name        (specific format/type)
      • by_genre    — GenreTaxonomy.parent_genre via the artist's primary
                      classification

    An event can have multiple types assigned (via event_event_types), so we
    use COUNT(DISTINCT event_id) in each bucket — an event with two jazz
    types counted as one "Jazz" rather than two.

    Events with no type assigned are excluded from the category/format
    buckets. Events with no classified artist (or whose artist's primary
    genre is UNKNOWN) are excluded from the genre bucket. Each dimension
    has its own denominator (`total_classified` for type-based, and
    `total_with_genre` for genre-based) so card percentages stay
    internally consistent.

    Backwards-compatibility: `by_type` is kept as an alias of `by_format`
    so older clients keep working until we deprecate it.
    """
    today = date.today()
    upcoming = Event.start_date >= today

    # Shared join expression — events → event_event_types → event_types,
    # restricted to future-dated events.
    def _breakdown(group_col):
        return (
            db.query(group_col, func.count(func.distinct(Event.id)).label("n"))
            .join(event_event_types, event_event_types.c.event_id == Event.id)
            .join(EventType, EventType.id == event_event_types.c.event_type_id)
            .filter(upcoming, group_col.isnot(None), group_col != "")
            .group_by(group_col)
            .order_by(func.count(func.distinct(Event.id)).desc())
            .all()
        )

    by_category = _breakdown(EventType.category)
    by_format   = _breakdown(EventType.name)

    # Distinct upcoming events with at least one type — denominator for the
    # "% of classified upcoming" stat. Matches the `with_type` metric in
    # /api/stats/coverage so the two cards tell a consistent story.
    classified_total = (
        db.query(func.count(func.distinct(Event.id)))
        .join(event_event_types, event_event_types.c.event_id == Event.id)
        .filter(upcoming)
        .scalar() or 0
    )

    # Genre breakdown — different join path. Events match ArtistGenre via
    # lowercased artist_name, then ArtistGenre.primary_genre (a sub-genre)
    # rolls up to GenreTaxonomy.parent_genre. UNKNOWN classifications are
    # excluded — they would only inflate the bucket without conveying
    # information. Locally imported because models/genre.py is registered
    # late and a top-level import would force a circular dance with
    # app.models.__init__.
    from app.models.genre import ArtistGenre, GenreTaxonomy

    artist_join = ArtistGenre.normalized_name == func.lower(func.trim(Event.artist_name))
    parent_join = GenreTaxonomy.sub_genre == ArtistGenre.primary_genre
    genre_filters = (
        upcoming,
        Event.artist_name.isnot(None),
        Event.artist_name != "",
        ArtistGenre.primary_genre.isnot(None),
        ArtistGenre.primary_genre != "UNKNOWN",
    )

    by_genre = (
        db.query(GenreTaxonomy.parent_genre, func.count(func.distinct(Event.id)).label("n"))
        .select_from(Event)
        .join(ArtistGenre, artist_join)
        .join(GenreTaxonomy, parent_join)
        .filter(*genre_filters)
        .group_by(GenreTaxonomy.parent_genre)
        .order_by(func.count(func.distinct(Event.id)).desc())
        .all()
    )

    genre_total = (
        db.query(func.count(func.distinct(Event.id)))
        .select_from(Event)
        .join(ArtistGenre, artist_join)
        .join(GenreTaxonomy, parent_join)
        .filter(*genre_filters)
        .scalar() or 0
    )

    by_format_payload = [{"name": r[0], "count": r[1]} for r in by_format]
    return {
        "total_classified": classified_total,
        "total_with_genre": genre_total,
        "by_category": [{"name": r[0], "count": r[1]} for r in by_category],
        "by_format":   by_format_payload,
        # Legacy alias — keep until the frontend rename has been deployed
        # for at least a release cycle.
        "by_type":     by_format_payload,
        "by_genre":    [{"name": r[0], "count": r[1]} for r in by_genre],
    }


@router.get("/genre-coverage")
def genre_coverage(db: Session = Depends(get_db)):
    """Artist/genre data-hygiene snapshot.

    Surfaces three things the dashboard needs to make data-quality
    decisions visible:

    1. Coverage — how many distinct upcoming-event artists have a
       known parent genre (the % gap is the lever for retrieval-
       augmented re-classification).
    2. Confidence — distribution of high/medium/low/unknown across
       all classified artists. Low+UNKNOWN ≈ "Gemini gave up", which
       is the bucket Brave-augmented context could rescue.
    3. Taxonomy — the full parent → sub-genre tree so we can see
       what categories exist and how they're populated.

    All counts use COUNT(DISTINCT lower(trim(artist_name))) for the
    upcoming-event side — same canonicalisation the join uses, so the
    numbers match what an actual query would resolve.
    """
    from app.models.genre import ArtistGenre, GenreTaxonomy

    today = date.today()
    upcoming = Event.start_date >= today

    # Distinct upcoming-event artists, canonicalised the same way the
    # ArtistGenre join canonicalises them.
    norm_artist = func.lower(func.trim(Event.artist_name))
    upcoming_artist_filters = (
        upcoming,
        Event.artist_name.isnot(None),
        Event.artist_name != "",
    )
    distinct_upcoming_artists = (
        db.query(func.count(func.distinct(norm_artist)))
        .filter(*upcoming_artist_filters)
        .scalar() or 0
    )

    # Same set, but only those that resolve to a non-UNKNOWN parent
    # genre via the taxonomy join.
    matched = (
        db.query(func.count(func.distinct(norm_artist)))
        .join(ArtistGenre, ArtistGenre.normalized_name == norm_artist)
        .join(GenreTaxonomy, GenreTaxonomy.sub_genre == ArtistGenre.primary_genre)
        .filter(
            *upcoming_artist_filters,
            ArtistGenre.primary_genre.isnot(None),
            ArtistGenre.primary_genre != "UNKNOWN",
        )
        .scalar() or 0
    )

    # ArtistGenre population stats — across all classified artists,
    # not just upcoming-event ones.
    total_classified = db.query(func.count(ArtistGenre.id)).scalar() or 0
    classified_known = (
        db.query(func.count(ArtistGenre.id))
        .filter(
            ArtistGenre.primary_genre.isnot(None),
            ArtistGenre.primary_genre != "UNKNOWN",
        )
        .scalar() or 0
    )

    # Confidence distribution. "low" tightly correlates with UNKNOWN —
    # the prompt mandates `confidence='low'` whenever Gemini can't
    # recognise the artist — so this gives a quick read on how much of
    # our classified pool is actually weak signal.
    conf_rows = (
        db.query(ArtistGenre.confidence, func.count(ArtistGenre.id))
        .group_by(ArtistGenre.confidence)
        .all()
    )
    confidence: dict[str, int] = {"high": 0, "medium": 0, "low": 0, "unset": 0}
    for c, n in conf_rows:
        key = (c or "").strip().lower()
        if key in ("high", "medium", "low"):
            confidence[key] += int(n)
        else:
            confidence["unset"] += int(n)

    # Full taxonomy: parent → [sub_genre, …], sorted alphabetically inside
    # each parent for stable rendering. Sub-genre counts shown as the
    # number of upcoming events whose primary-classified artist sits in
    # that sub-genre — gives a quick read on which sub-genres are
    # populated vs. dormant.
    sub_event_counts = dict(
        db.query(ArtistGenre.primary_genre, func.count(func.distinct(Event.id)))
        .select_from(Event)
        .join(ArtistGenre, ArtistGenre.normalized_name == norm_artist)
        .filter(
            *upcoming_artist_filters,
            ArtistGenre.primary_genre.isnot(None),
            ArtistGenre.primary_genre != "UNKNOWN",
        )
        .group_by(ArtistGenre.primary_genre)
        .all()
    )

    taxonomy_rows = (
        db.query(GenreTaxonomy.parent_genre, GenreTaxonomy.sub_genre)
        .order_by(GenreTaxonomy.parent_genre, GenreTaxonomy.sub_genre)
        .all()
    )
    taxonomy: list[dict] = []
    by_parent: dict[str, list[dict]] = {}
    for parent, sub in taxonomy_rows:
        sub_n = int(sub_event_counts.get(sub, 0) or 0)
        by_parent.setdefault(parent, []).append({"sub_genre": sub, "events": sub_n})
    # Ranked by total events under each parent, descending — same order
    # as the by_genre breakdown card so the two views read consistently.
    parent_totals = {p: sum(s["events"] for s in subs) for p, subs in by_parent.items()}
    for parent in sorted(by_parent.keys(), key=lambda p: -parent_totals[p]):
        taxonomy.append({
            "parent_genre": parent,
            "events": parent_totals[parent],
            "sub_genres": by_parent[parent],
        })

    return {
        "upcoming_distinct_artists": distinct_upcoming_artists,
        "upcoming_with_genre": matched,
        "upcoming_without_genre": max(distinct_upcoming_artists - matched, 0),
        "coverage_pct": (
            round(matched * 100 / distinct_upcoming_artists, 1)
            if distinct_upcoming_artists else 0.0
        ),
        "artist_genre_total": total_classified,
        "artist_genre_known": classified_known,
        "artist_genre_unknown": max(total_classified - classified_known, 0),
        "confidence": confidence,
        "taxonomy": taxonomy,
        "taxonomy_parent_count": len(taxonomy),
        "taxonomy_sub_count": sum(len(p["sub_genres"]) for p in taxonomy),
    }


@router.get("/source-detail")
def source_detail(source: str, db: Session = Depends(get_db)):
    """City breakdown for a given scrape source over the last 24h."""
    since = datetime.utcnow() - timedelta(hours=24)
    rows = (
        db.query(
            City.name,
            City.country,
            func.count(Event.id).label("events"),
            func.sum(
                case((Event.created_at >= since, 1), else_=0)
            ).label("new_events"),
        )
        .join(Venue, Venue.city_id == City.id)
        .join(Event, Event.venue_id == Venue.id)
        .filter(Event.scrape_source == source, Event.created_at >= since)
        .group_by(City.id, City.name, City.country)
        .order_by(func.count(Event.id).desc())
        .all()
    )
    return {
        "source": source,
        "cities": [
            {"city": r.name, "country": r.country, "events": r.events}
            for r in rows
        ],
    }


@router.get("/daily")
def daily_pulse(db: Session = Depends(get_db)):
    """24-hour snapshot: new events / venues / artists / active cities by source,
    plus previous-24h totals so the frontend can show ↑↓ deltas."""
    now = datetime.utcnow()
    since = now - timedelta(hours=24)
    prev_since = now - timedelta(hours=48)
    today = date.today()

    def _count_events(after, before=None):
        q = db.query(func.count(Event.id)).filter(Event.created_at >= after)
        if before:
            q = q.filter(Event.created_at < before)
        return q.scalar() or 0

    def _count_venues(after, before=None):
        q = db.query(func.count(Venue.id)).filter(Venue.created_at >= after)
        if before:
            q = q.filter(Venue.created_at < before)
        return q.scalar() or 0

    def _count_artists(after, before=None):
        q = (
            db.query(func.count(func.distinct(Event.artist_name)))
            .filter(
                Event.created_at >= after,
                Event.artist_name.isnot(None),
                Event.artist_name != "",
            )
        )
        if before:
            q = q.filter(Event.created_at < before)
        return q.scalar() or 0

    def _count_cities(after, before=None):
        q = (
            db.query(func.count(func.distinct(City.id)))
            .join(Venue, Venue.city_id == City.id)
            .join(Event, Event.venue_id == Venue.id)
            .filter(Event.created_at >= after)
        )
        if before:
            q = q.filter(Event.created_at < before)
        return q.scalar() or 0

    # ── 1. Total upcoming events ──────────────────────────────────────────────
    total_upcoming = (
        db.query(func.count(Event.id))
        .filter(Event.start_date >= today)
        .scalar() or 0
    )
    prev_upcoming = (
        db.query(func.count(Event.id))
        .filter(Event.start_date >= today - timedelta(days=1))
        .scalar() or 0
    )

    # ── 1b. Sliding window balance ────────────────────────────────────────────
    yesterday = today - timedelta(days=1)

    # Events that aged out: start_date was yesterday → they've now taken place
    aged_out = (
        db.query(func.count(Event.id))
        .filter(Event.start_date == yesterday)
        .scalar() or 0
    )

    # Net-new upcoming events added in last 24 h (scraped & saved, still future)
    added_upcoming = (
        db.query(func.count(Event.id))
        .filter(Event.created_at >= since, Event.start_date >= today)
        .scalar() or 0
    )

    window_net = added_upcoming - aged_out

    # ── 1c. Dedup stats from scan_logs (collect_events jobs in last 24 h) ────
    dedup_row = (
        db.query(
            func.coalesce(func.sum(ScanLog.events_found), 0).label("fetched"),
            func.coalesce(func.sum(ScanLog.events_saved), 0).label("saved"),
        )
        .filter(
            ScanLog.job_name == "collect_events",
            ScanLog.started_at >= since,
            ScanLog.status == "success",
        )
        .first()
    )
    total_fetched = int(dedup_row.fetched) if dedup_row else 0
    total_dedup_saved = int(dedup_row.saved) if dedup_row else 0
    redundant = max(0, total_fetched - total_dedup_saved)
    redundant_pct = round(redundant * 100 / total_fetched) if total_fetched else 0

    # ── 2. New events in 24h by source ───────────────────────────────────────
    new_ev_rows = (
        db.query(Event.scrape_source, func.count(Event.id).label("n"))
        .filter(Event.created_at >= since)
        .group_by(Event.scrape_source)
        .order_by(func.count(Event.id).desc())
        .all()
    )

    # ── 3. New venues in 24h by source ───────────────────────────────────────
    new_venue_sq = db.query(Venue.id).filter(Venue.created_at >= since).subquery()
    new_v_by_src = (
        db.query(
            Event.scrape_source,
            func.count(func.distinct(Event.venue_id)).label("n"),
        )
        .join(new_venue_sq, Event.venue_id == new_venue_sq.c.id)
        .group_by(Event.scrape_source)
        .order_by(func.count(func.distinct(Event.venue_id)).desc())
        .all()
    )

    # ── 4. New artists in 24h by source ──────────────────────────────────────
    new_art_rows = (
        db.query(Event.scrape_source, func.count(func.distinct(Event.artist_name)).label("n"))
        .filter(
            Event.created_at >= since,
            Event.artist_name.isnot(None),
            Event.artist_name != "",
        )
        .group_by(Event.scrape_source)
        .order_by(func.count(func.distinct(Event.artist_name)).desc())
        .all()
    )

    # ── 5. Cities that received new events in 24h ─────────────────────────────
    new_city_rows = (
        db.query(
            City.name,
            City.country,
            Event.scrape_source,
            func.count(func.distinct(Event.id)).label("n"),
        )
        .join(Venue, Venue.city_id == City.id)
        .join(Event, Event.venue_id == Venue.id)
        .filter(Event.created_at >= since)
        .group_by(City.id, City.name, City.country, Event.scrape_source)
        .order_by(func.count(func.distinct(Event.id)).desc())
        .all()
    )

    def to_sources(rows):
        return [{"source": r[0] or "unknown", "count": r[1]} for r in rows]

    from collections import OrderedDict
    cities_map: dict = OrderedDict()
    for r in new_city_rows:
        key = f"{r.name}|{r.country}"
        if key not in cities_map:
            cities_map[key] = {"city": r.name, "country": r.country, "total": 0, "sources": []}
        cities_map[key]["total"] += r.n
        cities_map[key]["sources"].append({"source": r.scrape_source or "unknown", "count": r.n})

    # ── Previous-window totals for delta calculation ──────────────────────────
    prev_events  = _count_events(prev_since, since)
    prev_venues  = _count_venues(prev_since, since)
    prev_artists = _count_artists(prev_since, since)
    prev_cities  = _count_cities(prev_since, since)

    cur_events  = sum(r.n for r in new_ev_rows)
    cur_venues  = _count_venues(since)
    cur_artists = sum(r.n for r in new_art_rows)
    cur_cities  = len(cities_map)

    return {
        "as_of": now.isoformat(),
        "since": since.isoformat(),
        "total_upcoming": total_upcoming,
        "prev_upcoming": prev_upcoming,
        "window": {
            "added": added_upcoming,
            "aged_out": aged_out,
            "net": window_net,
        },
        "dedup": {
            "fetched": total_fetched,
            "saved": total_dedup_saved,
            "redundant": redundant,
            "redundant_pct": redundant_pct,
        },
        "new_events": {
            "total": cur_events,
            "prev": prev_events,
            "by_source": to_sources(new_ev_rows),
        },
        "new_venues": {
            "total": cur_venues,
            "prev": prev_venues,
            "by_source": to_sources(new_v_by_src),
        },
        "new_artists": {
            "total": cur_artists,
            "prev": prev_artists,
            "by_source": to_sources(new_art_rows),
        },
        "new_cities": {
            "total": cur_cities,
            "prev": prev_cities,
            "by_city": list(cities_map.values()),
        },
    }


@router.get("/source-matrix")
def source_matrix(db: Session = Depends(get_db)):
    """
    Two-dimensional source contribution matrix:
      - all_time: total events / distinct artists / distinct cities per source
      - recent: same metrics restricted to events created in the last 24 hours
    Totals are included so the frontend can compute percentages without an
    extra round-trip.
    """
    since = datetime.utcnow() - timedelta(hours=24)

    def _by_source(col_expr, extra_filters=None, recent=False):
        q = db.query(Event.scrape_source, col_expr.label("n"))
        if extra_filters:
            for f in extra_filters:
                q = q.filter(f)
        if recent:
            q = q.filter(Event.created_at >= since)
        return {(r[0] or "unknown"): r[1] for r in q.group_by(Event.scrape_source).all()}

    def _cities_by_source(recent=False):
        q = (
            db.query(Event.scrape_source, func.count(func.distinct(City.id)).label("n"))
            .join(Venue, Event.venue_id == Venue.id)
            .join(City, Venue.city_id == City.id)
        )
        if recent:
            q = q.filter(Event.created_at >= since)
        return {(r[0] or "unknown"): r[1] for r in q.group_by(Event.scrape_source).all()}

    # ── All-time ─────────────────────────────────────────────────────────────
    all_events  = _by_source(func.count(Event.id))
    all_artists = _by_source(
        func.count(func.distinct(Event.artist_name)),
        extra_filters=[Event.artist_name.isnot(None), Event.artist_name != ""],
    )
    all_cities  = _cities_by_source()

    total_events  = sum(all_events.values())  or 1
    total_artists = sum(all_artists.values()) or 1
    total_cities  = sum(all_cities.values())  or 1

    # ── Recent (last 24 h) ────────────────────────────────────────────────────
    rec_events  = _by_source(func.count(Event.id), recent=True)
    rec_artists = _by_source(
        func.count(func.distinct(Event.artist_name)),
        extra_filters=[Event.artist_name.isnot(None), Event.artist_name != ""],
        recent=True,
    )
    rec_cities  = _cities_by_source(recent=True)

    total_rec_events  = sum(rec_events.values())  or 1
    total_rec_artists = sum(rec_artists.values()) or 1
    total_rec_cities  = sum(rec_cities.values())  or 1

    # ── Merge into source rows ────────────────────────────────────────────────
    all_sources = sorted(
        set(all_events) | set(all_artists) | set(all_cities),
        key=lambda s: all_events.get(s, 0),
        reverse=True,
    )

    def pct(n, total):
        return round(n / total * 100, 1) if total else 0

    rows = []
    for src in all_sources:
        rows.append({
            "source": src,
            "all_time": {
                "events":  {"n": all_events.get(src, 0),  "pct": pct(all_events.get(src, 0),  total_events)},
                "artists": {"n": all_artists.get(src, 0), "pct": pct(all_artists.get(src, 0), total_artists)},
                "cities":  {"n": all_cities.get(src, 0),  "pct": pct(all_cities.get(src, 0),  total_cities)},
            },
            "recent": {
                "events":  {"n": rec_events.get(src, 0),  "pct": pct(rec_events.get(src, 0),  total_rec_events)},
                "artists": {"n": rec_artists.get(src, 0), "pct": pct(rec_artists.get(src, 0), total_rec_artists)},
                "cities":  {"n": rec_cities.get(src, 0),  "pct": pct(rec_cities.get(src, 0),  total_rec_cities)},
            },
        })

    return {
        "as_of": datetime.utcnow().isoformat(),
        "totals": {
            "all_time": {"events": total_events,      "artists": total_artists,      "cities": total_cities},
            "recent":   {"events": total_rec_events,  "artists": total_rec_artists,  "cities": total_rec_cities},
        },
        "sources": rows,
    }


@router.get("/city-guides")
def city_guides_index(db: Session = Depends(get_db)):
    """Return CITY_GUIDES config enriched with live event counts and last-run info."""
    today = date.today()

    # Upcoming event counts per source_tag (source_id starts with "{tag}:")
    tag_counts = {}
    for city_configs in CITY_GUIDES.values():
        for c in city_configs:
            tag = c.source_tag
            if not tag:
                continue
            n = db.query(func.count(Event.id)).filter(
                Event.scrape_source == "city_guide",
                Event.source_id.like(f"{tag}:%"),
                Event.start_date >= today,
            ).scalar() or 0
            tag_counts[tag] = n

    # Last run time per city from scan_logs (job_name="collect_events", detail=city_name)
    city_names = list(CITY_GUIDES.keys())
    last_runs = {}
    if city_names:
        logs = (
            db.query(ScanLog.detail, func.max(ScanLog.started_at).label("last"))
            .filter(
                ScanLog.job_name == "collect_events",
                ScanLog.detail.in_(city_names),
                ScanLog.status == "success",
            )
            .group_by(ScanLog.detail)
            .all()
        )
        last_runs = {r.detail: r.last.isoformat() if r.last else None for r in logs}

    result = []
    for city, configs in CITY_GUIDES.items():
        result.append({
            "city": city,
            "last_run": last_runs.get(city),
            "sources": [
                {
                    "url": c.base_url,
                    "source_tag": c.source_tag,
                    "max_pages": c.max_pages,
                    "upcoming_events": tag_counts.get(c.source_tag, 0),
                }
                for c in configs
            ],
        })
    return {"guides": result}


@router.get("/spotify")
def spotify_funnel(db: Session = Depends(get_db)):
    """End-to-end metrics for the Spotify → Brave → LLMSource → Performer funnel.

    Surfaces:
      coverage:
        seen          — cumulative SpotifyArtist rows
        matched       — found in our Performer DB on first encounter
        unmatched     — never matched (pending_brave or brave_done)
        coverage_pct  — matched / seen
      ab_test:
        per-variant trial count + avg new_llm_sources_registered
        winner — null while either variant is below the threshold
      funnel:
        new_artists       — SpotifyArtists in unmatched state
        new_websites      — LLMSources discovered via spotify_artist_query
        new_artists_via   — distinct artist_names on Events from those sources
                            (artist_name not in Performer at the time of the
                            event was registered → "new" in the funnel sense)
      recent_runs:
        last 7 spotify_scan + spotify_brave_query ScanLog rows for the card

    The card on stats.html consumes this whole payload as-is; keep keys stable.
    """
    # ── Coverage ───────────────────────────────────────────────────────
    seen = db.query(func.count(SpotifyArtist.id)).scalar() or 0
    matched = db.query(func.count(SpotifyArtist.id)).filter(
        SpotifyArtist.match_status == "matched"
    ).scalar() or 0
    pending = db.query(func.count(SpotifyArtist.id)).filter(
        SpotifyArtist.match_status == "pending_brave"
    ).scalar() or 0
    done = db.query(func.count(SpotifyArtist.id)).filter(
        SpotifyArtist.match_status == "brave_done"
    ).scalar() or 0
    unmatched = pending + done
    coverage_pct = (
        round(100 * matched / (matched + unmatched), 2)
        if (matched + unmatched) else 0.0
    )

    # ── A/B test ──────────────────────────────────────────────────────
    ab_rows = (
        db.query(
            SpotifyBraveAttempt.query_variant,
            func.count(SpotifyBraveAttempt.id).label("trials"),
            func.coalesce(func.sum(SpotifyBraveAttempt.new_llm_sources_registered), 0).label("new_sources"),
            func.coalesce(func.sum(SpotifyBraveAttempt.brave_results_count), 0).label("hits"),
        )
        .group_by(SpotifyBraveAttempt.query_variant)
        .all()
    )
    ab = {
        r.query_variant: {
            "trials": r.trials,
            "total_new_sources": r.new_sources,
            "total_brave_hits": r.hits,
            "avg_new_sources": round(r.new_sources / r.trials, 3) if r.trials else 0.0,
            "avg_brave_hits": round(r.hits / r.trials, 2) if r.trials else 0.0,
        } for r in ab_rows
    }
    a = ab.get("shows", {}).get("trials", 0)
    b = ab.get("upcoming_performances", {}).get("trials", 0)
    THRESHOLD = 100
    if a >= THRESHOLD and b >= THRESHOLD:
        avg_a = ab["shows"]["avg_new_sources"]
        avg_b = ab["upcoming_performances"]["avg_new_sources"]
        winner = "shows" if avg_a >= avg_b else "upcoming_performances"
    else:
        winner = None

    # ── Funnel ─────────────────────────────────────────────────────────
    new_websites = db.query(func.count(LLMSource.id)).filter(
        LLMSource.discovered_via == "spotify_artist_query"
    ).scalar() or 0

    # New artists via new websites: Events with llm_source_id pointing
    # to a spotify_artist_query LLMSource, whose artist_name doesn't
    # exist in Performer.normalized_name. Conservative — we may double-
    # count case variants, but the SUBSTRING gives the right rough number.
    # SQLite-compatible (no DISTINCT-on subqueries).
    rows = (
        db.query(func.lower(func.trim(Event.artist_name)).label("aname"))
        .join(LLMSource, Event.llm_source_id == LLMSource.id)
        .filter(
            LLMSource.discovered_via == "spotify_artist_query",
            Event.artist_name.isnot(None),
        )
        .distinct()
        .all()
    )
    candidate_artists = {r.aname for r in rows if r.aname}
    if candidate_artists:
        known = {
            n for (n,) in db.query(Performer.normalized_name)
            .filter(Performer.normalized_name.in_(list(candidate_artists)))
            .all()
        }
        new_artists_via_websites = len(candidate_artists - known)
    else:
        new_artists_via_websites = 0

    # ── Recent runs ─────────────────────────────────────────────────────
    recent = (
        db.query(ScanLog)
        .filter(ScanLog.job_name.in_(["spotify_scan", "spotify_brave_query"]))
        .order_by(ScanLog.started_at.desc())
        .limit(10)
        .all()
    )
    recent_runs = [
        {
            "job": r.job_name,
            "status": r.status,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "finished_at": r.finished_at.isoformat() if r.finished_at else None,
            "events_found": r.events_found,
            "events_saved": r.events_saved,
            "notes": r.notes,
        }
        for r in recent
    ]

    return {
        "coverage": {
            "seen": seen,
            "matched": matched,
            "unmatched": unmatched,
            "pending_brave": pending,
            "brave_done": done,
            "coverage_pct": coverage_pct,
        },
        "ab_test": {
            "threshold": THRESHOLD,
            "variants": ab,
            "winner": winner,
        },
        "funnel": {
            "new_artists_from_spotify": unmatched,
            "new_websites_registered": new_websites,
            "new_artists_via_websites": new_artists_via_websites,
        },
        "recent_runs": recent_runs,
    }
