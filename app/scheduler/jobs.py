import logging
from datetime import date, timedelta

from app.database import SessionLocal
from app.models import City, Event, Venue
from app.config import settings
from app.services.collectors.registry import CollectorRegistry
from app.services.collectors.scrapers.venue_websites import scrape_venue_website
from app.services.dedup import dedup_events
from app.services.collectors.api.ticketmaster import TicketmasterCollector
from app.services.collectors.api.eventbrite import EventbriteCollector
from app.services.collectors.api.seatgeek import SeatGeekCollector
from app.services.collectors.api.predicthq import PredictHQCollector
from app.services.collectors.scrapers.nyc_venues import NYCVenueScraper
from app.services.collectors.scrapers.tel_aviv_venues import TelAvivVenueScraper
from app.services.collectors.scrapers.leaan import LeaanCollector
from app.services.collectors.api.resident_advisor import ResidentAdvisorCollector
from app.services.collectors.scrapers.dice import DiceCollector
from app.services.collectors.scrapers.cameri import CameriCollector
from app.services.collectors.scrapers.barby import BarbyCollector
from app.services.collectors.scrapers.israel_sites import IsraelSitesCollector
from app.services.collectors.scrapers.smarticket import SmartticketCollector
from app.services.collectors.scrapers.hatarbut import HatarbutCollector

logger = logging.getLogger(__name__)

registry = CollectorRegistry()
# Only register collectors that have credentials or are credential-free scrapers
if settings.TICKETMASTER_KEY:
    registry.register(TicketmasterCollector())
if settings.EVENTBRITE_TOKEN:
    registry.register(EventbriteCollector())
if settings.SEATGEEK_CLIENT_ID:
    registry.register(SeatGeekCollector())
if settings.PREDICTHQ_TOKEN:
    registry.register(PredictHQCollector())
# Credential-free scrapers — always register
registry.register(NYCVenueScraper())
registry.register(TelAvivVenueScraper())
registry.register(LeaanCollector())
registry.register(ResidentAdvisorCollector())
registry.register(DiceCollector())
registry.register(CameriCollector())
registry.register(BarbyCollector())
registry.register(IsraelSitesCollector())
registry.register(SmartticketCollector())
registry.register(HatarbutCollector())


PRIORITY_CITIES = [
    "New York", "Tel Aviv", "London", "Los Angeles", "Chicago",
    "San Francisco", "Berlin", "Paris", "Toronto", "Sydney",
]


async def collect_all_events():
    """Run all collectors for priority cities only to avoid memory spikes."""
    db = SessionLocal()
    try:
        cities = db.query(City).filter(City.name.in_(PRIORITY_CITIES)).all()
        if not cities:
            # Fallback: scrape up to 10 cities that already have events
            cities = (
                db.query(City)
                .join(City.events)
                .group_by(City.id)
                .limit(10)
                .all()
            )
        for city in cities:
            logger.info(f"Collecting events for {city.name}...")
            try:
                stats = await registry.collect_all(city, db)
                logger.info(f"{city.name} stats: {stats}")
            except Exception as e:
                logger.error(f"Error collecting {city.name}: {e}")
    finally:
        db.close()


async def collect_venue_websites():
    """Scrape each venue's own website for events. Runs every 24h."""
    import asyncio
    import httpx
    db = SessionLocal()
    try:
        venues = (
            db.query(Venue)
            .filter(Venue.website_url.isnot(None), Venue.website_url != "")
            .all()
        )
        logger.info(f"Venue website scraper: {len(venues)} venues to scan")
        sem = asyncio.Semaphore(8)
        total_found = 0
        total_saved = 0

        async with httpx.AsyncClient() as client:
            tasks = [
                scrape_venue_website(
                    client, sem,
                    v.name, v.physical_city or "", v.physical_country or "",
                    v.website_url,
                )
                for v in venues
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        for venue, result in zip(venues, results):
            if isinstance(result, Exception) or not result:
                continue
            total_found += len(result)
            if venue.city:
                saved = registry._save_events(result, venue.city, db)
                total_saved += saved

        logger.info(
            f"Venue website scraper done: {total_found} events found, {total_saved} saved"
        )
    except Exception as e:
        logger.error(f"Venue website scraper error: {e}")
    finally:
        db.close()


def run_dedup():
    """Weekly cross-source deduplication job."""
    db = SessionLocal()
    try:
        result = dedup_events(db)
        logger.info(f"Scheduled dedup: {result}")
    finally:
        db.close()


def cleanup_past_events():
    """Remove events older than CLEANUP_DAYS_AGO."""
    db = SessionLocal()
    try:
        cutoff = date.today() - timedelta(days=settings.CLEANUP_DAYS_AGO)
        deleted = db.query(Event).filter(Event.start_date < cutoff).delete()
        db.commit()
        logger.info(f"Cleaned up {deleted} past events")
    finally:
        db.close()
