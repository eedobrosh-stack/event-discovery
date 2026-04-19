from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.database import Base, engine
from app.api import auth, cities, event_types, events, export, admin, venues, stats, suggestions
from app.api import platform_venues as platform_venues_api
from app.api import metro_areas
from app.api.cities import warm_cities_cache
from app.api.metro_areas import warm_metro_cache
from app.scheduler.jobs import collect_all_events, cleanup_past_events, collect_venue_websites, run_dedup, collect_platform_venues, enrich_youtube_job, enrich_performers_job, enrich_venue_urls_job, discover_venues_job, collect_bandsintown_job, collect_techconf_job, enrich_spotify_job

scheduler = AsyncIOScheduler()


def _seed_platform_venues():
    """One-time migration: move hardcoded VenuePilot venues into the platform_venues table."""
    from app.database import SessionLocal
    from app.models.platform_venue import PlatformVenue
    from app.models import City
    from app.services.collectors.scrapers.venuepilot import VENUEPILOT_VENUES

    db = SessionLocal()
    try:
        for cfg in VENUEPILOT_VENUES:
            existing = db.query(PlatformVenue).filter(
                PlatformVenue.platform == "venuepilot",
                PlatformVenue.platform_id == str(cfg["account_id"]),
            ).first()
            if existing:
                continue  # already seeded
            # Resolve city: prefer the first city in run_for_cities that has a DB record
            city = None
            for city_name in cfg.get("run_for_cities", [cfg.get("city", "")]):
                city = db.query(City).filter(City.name.ilike(city_name)).first()
                if city:
                    break
            db.add(PlatformVenue(
                name=cfg["name"],
                city_id=city.id if city else None,
                platform="venuepilot",
                platform_id=str(cfg["account_id"]),
                website_url=cfg.get("website_url"),
                address=cfg.get("address"),
                active=True,
            ))
        db.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"_seed_platform_venues failed: {e}")
    finally:
        db.close()


def _seed_priority_cities():
    """Ensure every PRIORITY_CITIES entry exists in the cities table."""
    import logging
    from app.database import SessionLocal
    from app.models import City
    from app.scheduler.jobs import PRIORITY_CITIES

    # (name, country, state, timezone, lat, lon)
    CITY_META: dict[tuple[str, str], dict] = {
        # ── United States ────────────────────────────────────────────────
        ("New York",       "United States"): dict(state="NY", timezone="America/New_York",   latitude=40.7128,  longitude=-74.0060),
        ("Los Angeles",    "United States"): dict(state="CA", timezone="America/Los_Angeles", latitude=34.0522,  longitude=-118.2437),
        ("Chicago",        "United States"): dict(state="IL", timezone="America/Chicago",     latitude=41.8781,  longitude=-87.6298),
        ("San Francisco",  "United States"): dict(state="CA", timezone="America/Los_Angeles", latitude=37.7749,  longitude=-122.4194),
        ("Berkeley",       "United States"): dict(state="CA", timezone="America/Los_Angeles", latitude=37.8715,  longitude=-122.2730),
        # ── United Kingdom ───────────────────────────────────────────────
        ("London",         "United Kingdom"): dict(timezone="Europe/London",    latitude=51.5074,  longitude=-0.1278),
        ("Manchester",     "United Kingdom"): dict(timezone="Europe/London",    latitude=53.4808,  longitude=-2.2426),
        ("Edinburgh",      "United Kingdom"): dict(timezone="Europe/London",    latitude=55.9533,  longitude=-3.1883),
        # ── Germany ──────────────────────────────────────────────────────
        ("Berlin",         "Germany"):        dict(timezone="Europe/Berlin",    latitude=52.5200,  longitude=13.4050),
        ("Munich",         "Germany"):        dict(timezone="Europe/Berlin",    latitude=48.1351,  longitude=11.5820),
        # ── France ───────────────────────────────────────────────────────
        ("Paris",          "France"):         dict(timezone="Europe/Paris",     latitude=48.8566,  longitude=2.3522),
        # ── Italy ────────────────────────────────────────────────────────
        ("Rome",           "Italy"):          dict(timezone="Europe/Rome",      latitude=41.9028,  longitude=12.4964),
        ("Milan",          "Italy"):          dict(timezone="Europe/Rome",      latitude=45.4642,  longitude=9.1900),
        # ── Spain ────────────────────────────────────────────────────────
        ("Madrid",         "Spain"):          dict(timezone="Europe/Madrid",    latitude=40.4168,  longitude=-3.7038),
        ("Barcelona",      "Spain"):          dict(timezone="Europe/Madrid",    latitude=41.3851,  longitude=2.1734),
        # ── Netherlands ──────────────────────────────────────────────────
        ("Amsterdam",      "Netherlands"):    dict(timezone="Europe/Amsterdam", latitude=52.3676,  longitude=4.9041),
        # ── Portugal ─────────────────────────────────────────────────────
        ("Lisbon",         "Portugal"):       dict(timezone="Europe/Lisbon",    latitude=38.7169,  longitude=-9.1399),
        # ── Belgium ──────────────────────────────────────────────────────
        ("Brussels",       "Belgium"):        dict(timezone="Europe/Brussels",  latitude=50.8503,  longitude=4.3517),
        # ── Turkey ───────────────────────────────────────────────────────
        ("Istanbul",       "Turkey"):         dict(timezone="Europe/Istanbul",  latitude=41.0082,  longitude=28.9784),
        # ── Greece ───────────────────────────────────────────────────────
        ("Athens",         "Greece"):         dict(timezone="Europe/Athens",    latitude=37.9838,  longitude=23.7275),
        # ── Brazil ───────────────────────────────────────────────────────
        ("São Paulo",      "Brazil"):         dict(timezone="America/Sao_Paulo",    latitude=-23.5505, longitude=-46.6333),
        ("Rio de Janeiro", "Brazil"):         dict(timezone="America/Sao_Paulo",    latitude=-22.9068, longitude=-43.1729),
        # ── Argentina ────────────────────────────────────────────────────
        ("Buenos Aires",   "Argentina"):      dict(timezone="America/Argentina/Buenos_Aires", latitude=-34.6037, longitude=-58.3816),
        # ── Mexico ───────────────────────────────────────────────────────
        ("Mexico City",    "Mexico"):         dict(timezone="America/Mexico_City",  latitude=19.4326,  longitude=-99.1332),
        # ── Canada ───────────────────────────────────────────────────────
        ("Toronto",        "Canada"):         dict(timezone="America/Toronto",      latitude=43.6532,  longitude=-79.3832),
        ("Vancouver",      "Canada"):         dict(timezone="America/Vancouver",    latitude=49.2827,  longitude=-123.1207),
        # ── Australia ────────────────────────────────────────────────────
        ("Sydney",         "Australia"):      dict(timezone="Australia/Sydney",     latitude=-33.8688, longitude=151.2093),
        ("Melbourne",      "Australia"):      dict(timezone="Australia/Melbourne",  latitude=-37.8136, longitude=144.9631),
        ("Brisbane",       "Australia"):      dict(timezone="Australia/Brisbane",   latitude=-27.4698, longitude=153.0251),
        # ── Israel ───────────────────────────────────────────────────────
        ("Tel Aviv",       "Israel"):         dict(timezone="Asia/Jerusalem",       latitude=32.0853,  longitude=34.7818),
    }

    _log = logging.getLogger(__name__)
    db = SessionLocal()
    added = 0
    try:
        for name, country in PRIORITY_CITIES:
            exists = db.query(City).filter_by(name=name, country=country).first()
            if exists:
                continue
            meta = CITY_META.get((name, country), {})
            db.add(City(
                name=name,
                country=country,
                state=meta.get("state"),
                timezone=meta.get("timezone"),
                latitude=meta.get("latitude"),
                longitude=meta.get("longitude"),
            ))
            added += 1
        db.commit()
        if added:
            _log.info(f"_seed_priority_cities: added {added} new city records")
    except Exception as e:
        _log.warning(f"_seed_priority_cities failed: {e}")
        db.rollback()
    finally:
        db.close()


def _seed_event_types():
    """Insert any event types from seed data that are not yet in the DB."""
    from app.database import SessionLocal
    from app.models import EventType
    from app.seed.event_types import EVENT_TYPES

    db = SessionLocal()
    try:
        for et in EVENT_TYPES:
            exists = db.query(EventType).filter_by(name=et["name"]).first()
            if not exists:
                db.add(EventType(
                    name=et["name"],
                    category=et["category"],
                    keywords=et.get("keywords", ""),
                ))
        db.commit()
    except Exception as e:
        logging.getLogger(__name__).warning(f"_seed_event_types failed: {e}")
    finally:
        db.close()


def _run_migrations():
    """Apply incremental schema changes that create_all() won't handle."""
    from sqlalchemy import text, inspect
    insp = inspect(engine)

    existing_venue_cols = [c["name"] for c in insp.get_columns("venues")]
    if "default_event_type_id" not in existing_venue_cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE venues ADD COLUMN default_event_type_id INTEGER"))
            conn.commit()

    existing_performer_cols = [c["name"] for c in insp.get_columns("performers")]
    spotify_performer_cols = {
        "spotify_id":  "TEXT",
        "spotify_url": "TEXT",
        "image_url":   "TEXT",
        "popularity":  "INTEGER",
    }
    with engine.connect() as conn:
        for col, coltype in spotify_performer_cols.items():
            if col not in existing_performer_cols:
                conn.execute(text(f"ALTER TABLE performers ADD COLUMN {col} {coltype}"))
        conn.commit()

    existing_event_cols = [c["name"] for c in insp.get_columns("events")]
    artist_spotify_cols = {
        "artist_popularity": "INTEGER",
        "artist_spotify_url": "TEXT",
    }
    with engine.connect() as conn:
        for col, coltype in artist_spotify_cols.items():
            if col not in existing_event_cols:
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {col} {coltype}"))
        conn.commit()

    sports_cols = {
        "sport":       "TEXT",
        "home_team":   "TEXT",
        "away_team":   "TEXT",
        "tv_channels": "TEXT",   # JSON stored as TEXT in SQLite
    }
    with engine.connect() as conn:
        for col, coltype in sports_cols.items():
            if col not in existing_event_cols:
                conn.execute(text(f"ALTER TABLE events ADD COLUMN {col} {coltype}"))
        conn.commit()


def _fix_sports_categories():
    """
    One-time repair: events collected before the sports-categorization fix
    have sport=NULL, wrong artist_name (home team), and/or Music/Concert
    event types. Detect them by the "<League> - " name prefix and repair in
    place. Runs on every startup (idempotent) so newly-scraped or re-ingested
    events also get repaired if anything slipped through the registry.
    """
    import logging
    _log = logging.getLogger(__name__)
    from app.database import SessionLocal
    from app.models import EventType
    from app.models.event import Event

    # label → (sport value, preferred event-type name from seed)
    # Names MUST match app/seed/event_types.py — if a specific type doesn't
    # exist, we fall back to the generic "Sports Event".
    LEAGUE_MAP = {
        "NBA":                    ("Basketball",          "Basketball Game"),
        "WNBA":                   ("Basketball",          "Basketball Game"),
        "NHL":                    ("Ice Hockey",          "Hockey Game"),
        "NFL":                    ("American Football",   "American Football Game"),
        "MLS":                    ("Soccer",              "Soccer Match"),
        "MLB":                    ("Baseball",            "Baseball Game"),
        "AFL":                    ("Australian Football", "Sports Event"),
        "NRL":                    ("Rugby League",        "Sports Event"),
        "NBL":                    ("Basketball",          "Basketball Game"),
        "CFL":                    ("Canadian Football",   "American Football Game"),
        "EuroLeague":             ("Basketball",          "Basketball Game"),
        "EuroCup":                ("Basketball",          "Basketball Game"),
        "Premier League":         ("Soccer",              "Soccer Match"),
        "Bundesliga":             ("Soccer",              "Soccer Match"),
        "La Liga":                ("Soccer",              "Soccer Match"),
        "Serie A":                ("Soccer",              "Soccer Match"),
        "Ligue 1":                ("Soccer",              "Soccer Match"),
        "Eredivisie":             ("Soccer",              "Soccer Match"),
        "UEFA Champions League":  ("Soccer",              "Soccer Match"),
        "UEFA Europa League":     ("Soccer",              "Soccer Match"),
        "Formula 1":              ("Motorsport",          "Sports Event"),
    }

    db = SessionLocal()
    try:
        # Pre-resolve all event types once (IDs of Music/Comedy to remove,
        # and the Sports target for each league).
        music_et_ids = {
            row[0] for row in db.query(EventType.id)
            .filter(EventType.category.in_(["Music", "Comedy"]))
            .all()
        }
        sports_generic = db.query(EventType).filter_by(
            name="Sports Event", category="Sports"
        ).first()

        # Cache: event-type name → EventType instance
        et_cache: dict[str, EventType] = {}

        def _resolve_et(name: str):
            if name in et_cache:
                return et_cache[name]
            et = db.query(EventType).filter_by(name=name, category="Sports").first()
            # Fall back to the generic "Sports Event" if specific type missing
            if et is None:
                et = sports_generic
            et_cache[name] = et
            return et

        fixed = 0
        for label, (sport_val, et_name) in LEAGUE_MAP.items():
            prefix = f"{label} - %"
            # Match by name prefix regardless of current sport value — some
            # events were partially fixed (sport set) but still carry Music
            # event types from the original scrape.
            events = (
                db.query(Event)
                .filter(Event.name.ilike(prefix))
                .all()
            )
            if not events:
                continue

            sports_et = _resolve_et(et_name)

            for ev in events:
                dirty = False
                if ev.sport != sport_val:
                    ev.sport = sport_val
                    dirty = True
                if ev.artist_name:
                    # artist_name was set to home team in pre-fix events
                    ev.artist_name = None
                    dirty = True
                # Strip any Music/Comedy types; add the correct Sports one.
                current_ids = {et.id for et in (ev.event_types or [])}
                if current_ids & music_et_ids:
                    ev.event_types = [
                        et for et in ev.event_types if et.id not in music_et_ids
                    ]
                    dirty = True
                if sports_et and sports_et not in (ev.event_types or []):
                    ev.event_types.append(sports_et)
                    dirty = True
                # Backfill YouTube highlights search URL when missing
                if (
                    not ev.artist_youtube_channel
                    and ev.home_team
                    and ev.away_team
                ):
                    from urllib.parse import quote_plus
                    q = quote_plus(
                        f"{ev.home_team} vs {ev.away_team} highlights"
                    )
                    ev.artist_youtube_channel = (
                        f"https://www.youtube.com/results?search_query={q}"
                    )
                    dirty = True
                if dirty:
                    fixed += 1

        if fixed:
            db.commit()
            _log.info(f"_fix_sports_categories: repaired {fixed} events")
        else:
            _log.info("_fix_sports_categories: nothing to repair")
    except Exception as e:
        _log.warning(f"_fix_sports_categories failed: {e}")
        db.rollback()
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    import asyncio
    import logging
    _log = logging.getLogger(__name__)

    # Create tables on startup (fast, must complete before serving)
    Base.metadata.create_all(bind=engine)
    _run_migrations()

    # Move cache warming to background so Render's health check passes quickly
    async def _deferred_startup():
        await asyncio.sleep(2)  # let uvicorn bind the port first
        try:
            await asyncio.get_event_loop().run_in_executor(None, warm_cities_cache)
            _log.info("Cities cache warmed")
            await asyncio.get_event_loop().run_in_executor(None, warm_metro_cache)
            _log.info("Metro areas cache warmed")
        except Exception as e:
            _log.warning(f"Cache warm failed: {e}")
    asyncio.create_task(_deferred_startup())

    # Schedule jobs
    # Jobs are staggered (start_date offset) so they don't all fire simultaneously
    # and compete for memory on the same instance.
    from datetime import datetime as _dt, timedelta as _td
    _t = _dt.utcnow()

    # Jobs are staggered so they don't compete for memory on startup.
    # Heavy scraping starts at t+15 min — well after Render's health checks pass.
    scheduler.add_job(
        collect_all_events,
        IntervalTrigger(hours=settings.SCRAPE_INTERVAL_HOURS, start_date=_t + _td(minutes=15)),
        id="collect_events",
        replace_existing=True,
    )
    scheduler.add_job(
        cleanup_past_events,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=35)),
        id="cleanup_past",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_venue_websites,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=50)),
        id="collect_venue_websites",
        replace_existing=True,
    )
    scheduler.add_job(
        run_dedup,
        CronTrigger(day_of_week="sun", hour=5, minute=0),  # weekly Sunday 5am UTC
        id="dedup_events",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_platform_venues,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=65)),
        id="collect_platform_venues",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_youtube_job,
        IntervalTrigger(hours=2, start_date=_t + _td(minutes=95)),
        id="enrich_youtube",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_performers_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=125)),
        id="enrich_performers",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_spotify_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=140)),
        id="enrich_spotify",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_venue_urls_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=155)),
        id="enrich_venue_urls",
        replace_existing=True,
    )
    scheduler.add_job(
        discover_venues_job,
        IntervalTrigger(hours=48, start_date=_t + _td(minutes=185)),
        id="discover_venues",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_bandsintown_job,
        IntervalTrigger(hours=12, start_date=_t + _td(minutes=25)),
        id="collect_bandsintown",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_techconf_job,
        IntervalTrigger(hours=24, start_date=_t + _td(minutes=30)),
        id="collect_techconf",
        replace_existing=True,
    )
    scheduler.start()

    # Seed data in background (non-blocking)
    async def _deferred_seed():
        await asyncio.sleep(5)
        try:
            await asyncio.get_event_loop().run_in_executor(None, _seed_priority_cities)
            await asyncio.get_event_loop().run_in_executor(None, _seed_platform_venues)
            await asyncio.get_event_loop().run_in_executor(None, _seed_event_types)
            _log.info("Seeding complete")
        except Exception as e:
            _log.warning(f"Seeding failed: {e}")
    asyncio.create_task(_deferred_seed())

    async def _deferred_sports_fix():
        # Must run *after* _seed_event_types (which starts at t+5s) finishes
        # so "Sports Event" and "Basketball Game" etc. are available to pick.
        await asyncio.sleep(20)
        try:
            await asyncio.get_event_loop().run_in_executor(None, _fix_sports_categories)
            _log.info("Sports category repair complete")
        except Exception as e:
            _log.warning(f"Sports category repair failed: {e}")
    asyncio.create_task(_deferred_sports_fix())

    yield

    scheduler.shutdown()


app = FastAPI(title="Supercaly", lifespan=lifespan)


# Health-check endpoint — must respond instantly, no DB / blocking work
# Configure this path in Render → Settings → Health Check Path: /ping
@app.get("/ping", include_in_schema=False)
def ping():
    return {"status": "ok"}


# API routers
app.include_router(auth.router)
app.include_router(cities.router)
app.include_router(event_types.router)
app.include_router(events.router)
app.include_router(export.router)
app.include_router(admin.router)
app.include_router(venues.router)
app.include_router(stats.router)
app.include_router(suggestions.router)
app.include_router(platform_venues_api.router)
app.include_router(metro_areas.router)

# Explicit route for admin page (StaticFiles html=True doesn't reliably resolve /admin → admin.html)
@app.get("/admin")
def admin_page():
    return FileResponse("frontend/admin.html")

# Serve frontend
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
