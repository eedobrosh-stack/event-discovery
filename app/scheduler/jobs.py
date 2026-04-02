import logging
from datetime import date, timedelta

from app.database import SessionLocal
from app.models import City, Event
from app.config import settings
from app.services.collectors.registry import CollectorRegistry
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

logger = logging.getLogger(__name__)

registry = CollectorRegistry()
registry.register(TicketmasterCollector())
registry.register(EventbriteCollector())
registry.register(SeatGeekCollector())
registry.register(PredictHQCollector())
registry.register(NYCVenueScraper())
registry.register(TelAvivVenueScraper())
registry.register(LeaanCollector())
registry.register(ResidentAdvisorCollector())
registry.register(DiceCollector())
registry.register(CameriCollector())
registry.register(BarbyCollector())
registry.register(IsraelSitesCollector())


async def collect_all_events():
    """Run all collectors for all cities."""
    db = SessionLocal()
    try:
        cities = db.query(City).all()
        for city in cities:
            logger.info(f"Collecting events for {city.name}...")
            stats = await registry.collect_all(city, db)
            logger.info(f"{city.name} stats: {stats}")
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
