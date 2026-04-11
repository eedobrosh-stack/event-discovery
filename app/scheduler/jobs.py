import logging
from datetime import date, timedelta, datetime

from sqlalchemy import func
from app.database import SessionLocal
from app.models import City, Event, Venue, ScanLog
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
from app.services.collectors.scrapers.venuepilot import VenuePilotCollector

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
registry.register(VenuePilotCollector())


PRIORITY_CITIES = [
    "New York", "Tel Aviv", "London", "Los Angeles", "Chicago",
    "San Francisco", "Berkeley", "Berlin", "Paris", "Toronto", "Sydney",
]


async def collect_all_events():
    """Run all collectors for priority cities only to avoid memory spikes."""
    db = SessionLocal()
    try:
        cities = db.query(City).filter(City.name.in_(PRIORITY_CITIES)).all()
        if not cities:
            cities = (
                db.query(City)
                .join(City.events)
                .group_by(City.id)
                .limit(10)
                .all()
            )
        for city in cities:
            logger.info(f"Collecting events for {city.name}...")
            log = ScanLog(job_name="collect_events", detail=city.name, status="running")
            db.add(log)
            db.commit()
            db.refresh(log)
            try:
                stats = await registry.collect_all(city, db)
                logger.info(f"{city.name} stats: {stats}")
                log.status = "success"
                log.events_found = sum(v.get("fetched", 0) for v in stats.values() if isinstance(v, dict))
                log.events_saved = sum(v.get("saved", 0) for v in stats.values() if isinstance(v, dict))
                log.notes = str(stats)
            except Exception as e:
                logger.error(f"Error collecting {city.name}: {e}")
                log.status = "failed"
                log.notes = str(e)
            finally:
                log.finished_at = datetime.utcnow()
                db.commit()
                # Free all ORM objects accumulated during this city's scrape
                # so they don't pile up across cities.
                db.expire_all()
    finally:
        db.close()


async def collect_venue_websites():
    """Scrape each venue's own website for events. Runs every 24h."""
    import asyncio
    import httpx
    from sqlalchemy.orm import joinedload

    db = SessionLocal()
    log = ScanLog(job_name="venue_websites", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    try:
        # Eager-load city so we don't trigger N lazy queries inside the loop.
        # Cap at 500 venues per run to bound memory usage.
        venues = (
            db.query(Venue)
            .options(joinedload(Venue.city))
            .filter(Venue.website_url.isnot(None), Venue.website_url != "")
            .limit(500)
            .all()
        )
        logger.info(f"Venue website scraper: {len(venues)} venues to scan")
        sem = asyncio.Semaphore(3)
        total_found = 0
        total_saved = 0
        BATCH = 10

        async with httpx.AsyncClient() as client:
            for i in range(0, len(venues), BATCH):
                batch = venues[i:i + BATCH]
                tasks = [
                    scrape_venue_website(
                        client, sem,
                        v.name, v.physical_city or "", v.physical_country or "",
                        v.website_url,
                    )
                    for v in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for venue, result in zip(batch, results):
                    if isinstance(result, Exception) or not result:
                        continue
                    total_found += len(result)
                    if venue.city:
                        saved = registry._save_events(result, venue.city, db)
                        total_saved += saved
                # Flush accumulated ORM objects after each batch to keep
                # the session identity map small throughout the job.
                db.commit()
                db.expire_all()

        logger.info(f"Venue website scraper done: {total_found} found, {total_saved} saved")
        log.status = "success"
        log.events_found = total_found
        log.events_saved = total_saved
        log.detail = f"{len(venues)} venues scanned"
    except Exception as e:
        logger.error(f"Venue website scraper error: {e}")
        log.status = "failed"
        log.notes = str(e)
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()


def run_dedup():
    """Weekly cross-source deduplication job."""
    db = SessionLocal()
    log = ScanLog(job_name="dedup", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    try:
        result = dedup_events(db)
        logger.info(f"Scheduled dedup: {result}")
        log.status = "success"
        log.notes = str(result)
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()


async def collect_platform_venues():
    """Daily scrape for all active platform venues stored in the DB."""
    from app.models.platform_venue import PlatformVenue
    from app.services.platform_registry import fetch_platform_venue_events
    from datetime import datetime as dt

    db = SessionLocal()
    log = ScanLog(job_name="platform_venues", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    total_found = 0
    total_saved = 0

    try:
        pvs = db.query(PlatformVenue).filter(PlatformVenue.active == True).all()
        logger.info(f"Platform venue scraper: {len(pvs)} active venues to scan")

        for pv in pvs:
            try:
                city = db.query(City).filter(City.id == pv.city_id).first()
                if not city:
                    logger.warning(f"Platform venue '{pv.name}' has no linked city — skipping")
                    continue
                raw_events = await fetch_platform_venue_events(pv, city.name, city.country)
                saved = registry._save_events(raw_events, city, db)
                total_found += len(raw_events)
                total_saved += saved
                pv.last_scraped_at = dt.utcnow()
                db.commit()
                logger.info(
                    f"Platform venue '{pv.name}' ({pv.platform}): "
                    f"found={len(raw_events)}, saved={saved}"
                )
            except Exception as e:
                logger.error(f"Platform venue '{pv.name}' error: {e}")

        log.status = "success"
        log.events_found = total_found
        log.events_saved = total_saved
        log.detail = f"{len(pvs)} venues scanned"
    except Exception as e:
        logger.error(f"collect_platform_venues error: {e}")
        log.status = "failed"
        log.notes = str(e)
    finally:
        log.finished_at = dt.utcnow()
        db.commit()
        db.close()


async def enrich_youtube_job(batch: int = 100):
    """Find artists with no YouTube link and look them up. Runs every 6h."""
    from sqlalchemy import func as _func, or_
    from app.services.youtube_lookup import lookup_youtube_video

    db = SessionLocal()
    log = ScanLog(job_name="enrich_youtube", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    found = 0
    failed = 0
    try:
        artists = (
            db.query(Event.artist_name)
            .filter(
                Event.artist_name.isnot(None),
                Event.artist_name != "",
                or_(
                    Event.artist_youtube_channel.is_(None),
                    Event.artist_youtube_channel == "",
                ),
            )
            .group_by(Event.artist_name)
            .order_by(_func.count(Event.id).desc())   # most-event artists first
            .limit(batch)
            .all()
        )
        names = [r[0] for r in artists]
        logger.info(f"enrich_youtube: {len(names)} artists to enrich")

        for artist in names:
            try:
                url = await lookup_youtube_video(artist)
                if url:
                    db.query(Event).filter(Event.artist_name == artist).update(
                        {"artist_youtube_channel": url}, synchronize_session=False
                    )
                    db.commit()
                    found += 1
                else:
                    # Write empty string so we don't retry endlessly
                    db.query(Event).filter(
                        Event.artist_name == artist,
                        Event.artist_youtube_channel.is_(None),
                    ).update({"artist_youtube_channel": ""}, synchronize_session=False)
                    db.commit()
                    failed += 1
            except Exception as e:
                logger.warning(f"enrich_youtube: error for {artist!r}: {e}")
                failed += 1

        log.status = "success"
        log.events_found = len(names)
        log.events_saved = found
        log.notes = f"found={found} no_result={failed}"
        logger.info(f"enrich_youtube done: found={found} failed={failed}")
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
        logger.error(f"enrich_youtube error: {e}")
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()


async def enrich_performers_job(batch: int = 50):
    """MusicBrainz lookup for new artist names → performers table. Runs nightly."""
    import asyncio
    import httpx
    from app.models import Performer
    from app.services.performer_lookup import lookup_musicbrainz, normalize
    from sqlalchemy.exc import IntegrityError

    db = SessionLocal()
    log = ScanLog(job_name="enrich_performers", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    enriched = 0
    skipped = 0
    try:
        existing_norms = {r[0] for r in db.query(Performer.normalized_name).all()}
        all_artists = (
            db.query(Event.artist_name, func.count(Event.id).label("n"))
            .filter(Event.artist_name.isnot(None), Event.artist_name != "")
            .group_by(Event.artist_name)
            .order_by(func.count(Event.id).desc())
            .all()
        )
        pending = [r[0] for r in all_artists if normalize(r[0]) not in existing_norms][:batch]
        logger.info(f"enrich_performers: {len(pending)} new artists to look up")

        async with httpx.AsyncClient(timeout=15) as http:
            for artist in pending:
                try:
                    result = await lookup_musicbrainz(artist, http)
                    if result:
                        perf = Performer(
                            name=artist,
                            normalized_name=normalize(artist),
                            category=result.get("category"),
                            event_type_name=result.get("event_type_name"),
                            genres=result.get("genres"),
                            mb_id=result.get("mb_id"),
                            mb_type=result.get("mb_type"),
                            source="musicbrainz",
                            confidence=result.get("confidence", 1.0),
                        )
                        db.add(perf)
                        try:
                            db.commit()
                            enriched += 1
                        except IntegrityError:
                            db.rollback()
                    else:
                        # Insert a stub so we don't retry
                        stub = Performer(
                            name=artist,
                            normalized_name=normalize(artist),
                            source="not_found",
                            confidence=0.0,
                        )
                        db.add(stub)
                        try:
                            db.commit()
                        except IntegrityError:
                            db.rollback()
                        skipped += 1
                except Exception as e:
                    logger.warning(f"enrich_performers: error for {artist!r}: {e}")
                    skipped += 1

        log.status = "success"
        log.events_found = len(pending)
        log.events_saved = enriched
        log.notes = f"enriched={enriched} not_found={skipped}"
        logger.info(f"enrich_performers done: enriched={enriched} skipped={skipped}")
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
        logger.error(f"enrich_performers error: {e}")
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()


def cleanup_past_events():
    """Remove events older than CLEANUP_DAYS_AGO."""
    db = SessionLocal()
    log = ScanLog(job_name="cleanup", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    try:
        cutoff = date.today() - timedelta(days=settings.CLEANUP_DAYS_AGO)
        deleted = db.query(Event).filter(Event.start_date < cutoff).delete()
        db.commit()
        logger.info(f"Cleaned up {deleted} past events")
        log.status = "success"
        log.notes = f"Deleted {deleted} events older than {cutoff}"
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()
