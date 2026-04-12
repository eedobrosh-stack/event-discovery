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
from app.api.cities import warm_cities_cache
from app.scheduler.jobs import collect_all_events, cleanup_past_events, collect_venue_websites, run_dedup, collect_platform_venues, enrich_youtube_job, enrich_performers_job, enrich_venue_urls_job, discover_venues_job

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
    # Create tables on startup
    Base.metadata.create_all(bind=engine)
    _run_migrations()

    # Pre-warm the cities cache so the first user request is instant
    import asyncio
    await asyncio.get_event_loop().run_in_executor(None, warm_cities_cache)

    # Schedule jobs
    scheduler.add_job(
        collect_all_events,
        IntervalTrigger(hours=settings.SCRAPE_INTERVAL_HOURS),
        id="collect_events",
        replace_existing=True,
    )
    scheduler.add_job(
        cleanup_past_events,
        IntervalTrigger(hours=24),
        id="cleanup_past",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_venue_websites,
        IntervalTrigger(hours=24),   # every 24h from startup — resilient to restarts
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
        IntervalTrigger(hours=24),
        id="collect_platform_venues",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_youtube_job,
        IntervalTrigger(hours=6),   # 4x daily, 100 artists/run
        id="enrich_youtube",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_performers_job,
        IntervalTrigger(hours=24),
        id="enrich_performers",
        replace_existing=True,
    )
    scheduler.add_job(
        enrich_venue_urls_job,
        IntervalTrigger(hours=24),
        id="enrich_venue_urls",
        replace_existing=True,
    )
    scheduler.add_job(
        discover_venues_job,
        IntervalTrigger(hours=48),   # every 2 days — Overpass is expensive
        id="discover_venues",
        replace_existing=True,
    )
    scheduler.start()

    # Seed Ashkenaz into platform_venues if it hasn't been added yet
    _seed_platform_venues()

    yield

    scheduler.shutdown()


app = FastAPI(title="Supercaly", lifespan=lifespan)

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

# Explicit route for admin page (StaticFiles html=True doesn't reliably resolve /admin → admin.html)
@app.get("/admin")
def admin_page():
    return FileResponse("frontend/admin.html")

# Serve frontend
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
