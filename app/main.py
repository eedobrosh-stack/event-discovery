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
from app.scheduler.jobs import collect_all_events, cleanup_past_events, collect_venue_websites, run_dedup, collect_platform_venues, enrich_youtube_job, enrich_performers_job, enrich_venue_urls_job, discover_venues_job, collect_bandsintown_job, collect_techconf_job

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
    existing_cols = [c["name"] for c in insp.get_columns("venues")]
    if "default_event_type_id" not in existing_cols:
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE venues ADD COLUMN default_event_type_id INTEGER"))
            conn.commit()


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
        IntervalTrigger(hours=6, start_date=_t + _td(minutes=95)),
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
            await asyncio.get_event_loop().run_in_executor(None, _seed_platform_venues)
            await asyncio.get_event_loop().run_in_executor(None, _seed_event_types)
            _log.info("Seeding complete")
        except Exception as e:
            _log.warning(f"Seeding failed: {e}")
    asyncio.create_task(_deferred_seed())

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
