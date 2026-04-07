from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.database import Base, engine
from app.api import auth, cities, event_types, events, export, admin, venues, stats, suggestions
from app.api.cities import warm_cities_cache
from app.scheduler.jobs import collect_all_events, cleanup_past_events, collect_venue_websites, run_dedup

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup
    Base.metadata.create_all(bind=engine)

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
        CronTrigger(hour=3, minute=0),
        id="cleanup_past",
        replace_existing=True,
    )
    scheduler.add_job(
        collect_venue_websites,
        CronTrigger(hour=4, minute=0),   # daily at 4am UTC, after cleanup
        id="collect_venue_websites",
        replace_existing=True,
    )
    scheduler.add_job(
        run_dedup,
        CronTrigger(day_of_week="sun", hour=5, minute=0),  # weekly Sunday 5am UTC
        id="dedup_events",
        replace_existing=True,
    )
    scheduler.start()

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

# Serve frontend
app.mount("/", StaticFiles(directory="frontend", html=True), name="frontend")
