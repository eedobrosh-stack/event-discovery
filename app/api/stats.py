from collections import defaultdict
from datetime import date, datetime, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import City, Venue, Event
from app.models.scan_log import ScanLog
from app.services.collectors.scrapers.city_guides import CITY_GUIDES

router = APIRouter(prefix="/api/stats", tags=["stats"])


@router.get("/cities")
def city_coverage(db: Session = Depends(get_db)):
    """Return venue + event counts per city, ordered by venue count desc."""
    rows = (
        db.query(
            City.name,
            City.country,
            func.count(func.distinct(Venue.id)).label("venues"),
            func.count(func.distinct(Event.id)).label("events"),
            func.min(Event.start_date).label("earliest"),
            func.max(Event.start_date).label("latest"),
        )
        .join(Venue, Venue.city_id == City.id, isouter=True)
        .join(Event, Event.venue_id == Venue.id, isouter=True)
        .group_by(City.id)
        .having(func.count(func.distinct(Venue.id)) > 0)
        .order_by(func.count(func.distinct(Venue.id)).desc())
        .all()
    )

    total_venues = sum(r.venues for r in rows)
    total_events = sum(r.events for r in rows)

    return {
        "summary": {
            "cities": len(rows),
            "venues": total_venues,
            "events": total_events,
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
                func.case((Event.created_at >= since, 1), else_=0)
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
def city_guides_index():
    """Return the CITY_GUIDES configuration as a structured list."""
    result = []
    for city, configs in CITY_GUIDES.items():
        result.append({
            "city": city,
            "sources": [
                {
                    "url": c.base_url,
                    "source_tag": c.source_tag,
                    "max_pages": c.max_pages,
                }
                for c in configs
            ],
        })
    return {"guides": result}
