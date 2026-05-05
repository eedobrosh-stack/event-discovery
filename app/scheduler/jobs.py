import asyncio
import logging
from datetime import date, timedelta, datetime

from sqlalchemy import func
from app.database import SessionLocal

# Global lock — only one heavy scraping/enrichment job runs at a time.
# This prevents two jobs from competing for the same 512 MB on Render.
_heavy_job_lock = asyncio.Lock()

# City batching: scrape CITY_BATCH_SIZE cities per run, rotating through
# PRIORITY_CITIES on each invocation. All ~34 cities are covered over ~48h
# at the default 6h scrape interval with 8 batches of 4 cities each.
#
# Batch size was lowered from 8→4 after repeated Render OOM kills during
# large cities like New York: the process would die mid-batch and restart,
# losing the in-memory cursor and starting over from Batch 1 every time.
CITY_BATCH_SIZE = 4
_BATCH_INDEX_KEY = "city_batch_index"
from app.models import City, Event, Venue, ScanLog, JobState
from app.config import settings
from app.services.collectors.registry import CollectorRegistry
from app.services.collectors.scrapers.venue_websites import scrape_venue_website
from app.services.dedup import dedup_events
from app.services.collectors.api.ticketmaster import TicketmasterCollector
from app.services.collectors.scrapers.eventbrite_web import EventbriteWebScraper
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
from app.services.collectors.scrapers.luma import LumaCollector
from app.services.collectors.scrapers.meetup import MeetupCollector
from app.services.collectors.api.bandsintown import BandsintownClient
from app.services.collectors.scrapers.songkick import SongkickCollector
from app.services.collectors.scrapers.skiddle import SkiddleCollector
from app.services.collectors.scrapers.xceed import XceedCollector
from app.services.collectors.scrapers.concreteplayground import ConcretePlaygroundCollector
from app.services.collectors.scrapers.allevents import AlleventsCollector
from app.services.collectors.scrapers.tickchak import TickchakCollector
from app.services.collectors.scrapers.city_guides import CityGuideCollector
from app.services.collectors.scrapers.sports.espn import EspnSportsCollector
from app.services.collectors.scrapers.sports.mlb import MlbStatsApiCollector
from app.services.collectors.scrapers.sports.openf1 import OpenF1Collector
from app.services.collectors.scrapers.sports.cricapi import CricApiCollector
from app.services.collectors.scrapers.sports.euroleague import EuroLeagueCollector
from app.services.collectors.scrapers.choosechicago import ChooseChicagoCollector

logger = logging.getLogger(__name__)

registry = CollectorRegistry()
# Only register collectors that have credentials or are credential-free scrapers
if settings.TICKETMASTER_KEY:
    registry.register(TicketmasterCollector())
# Eventbrite v3 events/search API is deprecated — always use the web scraper
# (EventbriteCollector kept in codebase for when Eventbrite restores API access)
registry.register(EventbriteWebScraper())
if settings.SEATGEEK_CLIENT_ID:
    registry.register(SeatGeekCollector())
if settings.PREDICTHQ_TOKEN:
    registry.register(PredictHQCollector())
# Credential-free scrapers — always register
registry.register(NYCVenueScraper())
registry.register(LumaCollector())
registry.register(MeetupCollector())
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
registry.register(SongkickCollector())
registry.register(SkiddleCollector())
registry.register(XceedCollector())
registry.register(ConcretePlaygroundCollector())
registry.register(AlleventsCollector())
registry.register(TickchakCollector())
registry.register(CityGuideCollector())
# Sports — ESPN hidden API (no key) + MLB official StatsAPI (no key) + OpenF1 (no key)
registry.register(EspnSportsCollector())
registry.register(MlbStatsApiCollector())
registry.register(OpenF1Collector())
# Cricket — CricAPI (free 100 req/day; set CRICAPI_KEY in .env to activate)
registry.register(CricApiCollector())
# EuroLeague + EuroCup basketball (official API, no key)
registry.register(EuroLeagueCollector())
# Chicago-specific — Choose Chicago TEC REST API (no key)
registry.register(ChooseChicagoCollector())


# (city_name, country) — must match City.country values exactly (full names).
# Specifying country prevents collecting UK events into "London, Canada" etc.
PRIORITY_CITIES = [
    # ── United States ───────────────────────────────────────────────────────
    ("New York",       "United States"),
    ("Los Angeles",    "United States"),
    ("Chicago",        "United States"),
    ("San Francisco",  "United States"),
    ("Berkeley",       "United States"),
    # ── United Kingdom ──────────────────────────────────────────────────────
    ("London",         "United Kingdom"),
    ("Manchester",     "United Kingdom"),
    ("Edinburgh",      "United Kingdom"),
    # ── Germany ─────────────────────────────────────────────────────────────
    ("Berlin",         "Germany"),
    ("Munich",         "Germany"),
    # ── France ──────────────────────────────────────────────────────────────
    ("Paris",          "France"),
    # ── Italy ───────────────────────────────────────────────────────────────
    ("Rome",           "Italy"),
    ("Milan",          "Italy"),
    # ── Spain ───────────────────────────────────────────────────────────────
    ("Madrid",         "Spain"),
    ("Barcelona",      "Spain"),
    # ── Netherlands ─────────────────────────────────────────────────────────
    ("Amsterdam",      "Netherlands"),
    # ── Portugal ────────────────────────────────────────────────────────────
    ("Lisbon",         "Portugal"),
    # ── Belgium ─────────────────────────────────────────────────────────────
    ("Brussels",       "Belgium"),
    # ── Turkey ──────────────────────────────────────────────────────────────
    ("Istanbul",       "Turkey"),
    # ── Brazil ──────────────────────────────────────────────────────────────
    ("São Paulo",      "Brazil"),
    ("Rio de Janeiro", "Brazil"),
    # ── Argentina ───────────────────────────────────────────────────────────
    ("Buenos Aires",   "Argentina"),
    # ── Mexico ──────────────────────────────────────────────────────────────
    ("Mexico City",    "Mexico"),
    # ── Canada ──────────────────────────────────────────────────────────────
    ("Toronto",        "Canada"),
    ("Vancouver",      "Canada"),
    # ── Australia ───────────────────────────────────────────────────────────
    ("Sydney",         "Australia"),
    ("Melbourne",      "Australia"),
    ("Brisbane",       "Australia"),
    # ── Greece ──────────────────────────────────────────────────────────────
    ("Athens",         "Greece"),
    # ── Israel ──────────────────────────────────────────────────────────────
    ("Tel Aviv",       "Israel"),
]


def _get_batch_index() -> int:
    """Read the rotating city-batch cursor from DB (0 if unset)."""
    try:
        with SessionLocal() as db:
            row = db.query(JobState).filter_by(key=_BATCH_INDEX_KEY).first()
            return int(row.value) if row and row.value.isdigit() else 0
    except Exception as e:
        logger.warning(f"_get_batch_index: DB read failed ({e}); defaulting to 0")
        return 0


def _set_batch_index(value: int) -> None:
    """Persist the next city-batch cursor so it survives process restarts."""
    try:
        with SessionLocal() as db:
            row = db.query(JobState).filter_by(key=_BATCH_INDEX_KEY).first()
            if row:
                row.value = str(value)
            else:
                db.add(JobState(key=_BATCH_INDEX_KEY, value=str(value)))
            db.commit()
    except Exception as e:
        logger.warning(f"_set_batch_index: DB write failed ({e}); cursor not persisted")


async def collect_all_events():
    """Scrape one batch of cities per run (CITY_BATCH_SIZE cities), rotating
    through PRIORITY_CITIES on each invocation so all cities are covered
    across multiple runs without ever loading all 34 into a single process.
    At the default 6h interval + batch size 4: all ~34 cities refresh ~every 48h.

    The batch cursor is persisted in the job_state table so an OOM-kill +
    restart doesn't reset rotation back to batch 1.
    """
    import gc
    from sqlalchemy import and_, or_

    if _heavy_job_lock.locked():
        logger.info("collect_all_events: another heavy job is running — skipping this run")
        return

    async with _heavy_job_lock:
        # Pick the current batch of city names
        total = len(PRIORITY_CITIES)
        cursor = _get_batch_index()
        start = cursor % total
        batch_names = [
            PRIORITY_CITIES[(start + i) % total]
            for i in range(min(CITY_BATCH_SIZE, total))
        ]
        # Persist *before* we start work — if we OOM mid-batch, the next
        # process run should skip ahead rather than replay the same batch.
        _set_batch_index((start + CITY_BATCH_SIZE) % total)
        logger.info(
            f"collect_all_events: batch {start//CITY_BATCH_SIZE + 1} — "
            f"{[c[0] for c in batch_names]}"
        )

        # Resolve city IDs in a short-lived session
        with SessionLocal() as id_db:
            city_ids = [
                row[0]
                for row in id_db.query(City.id).filter(
                    or_(*[
                        and_(City.name == name, City.country == country)
                        for name, country in batch_names
                    ])
                ).all()
            ]

        for city_id in city_ids:
            # Fresh session per city — nothing leaks across cities
            with SessionLocal() as db:
                city = db.query(City).get(city_id)
                if not city:
                    continue
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
            gc.collect()  # after session closes + all objects are released


async def collect_venue_websites():
    """Scrape each venue's own website for events. Runs every 24h."""
    import asyncio
    import httpx
    from sqlalchemy.orm import joinedload

    if _heavy_job_lock.locked():
        logger.info("collect_venue_websites: another heavy job is running — skipping this run")
        return

    db = SessionLocal()
    log = ScanLog(job_name="venue_websites", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    try:
        # Eager-load city so we don't trigger N lazy queries inside the loop.
        # Cap at 100 venues per run to keep memory pressure low.
        venues = (
            db.query(Venue)
            .options(joinedload(Venue.city))
            .filter(Venue.website_url.isnot(None), Venue.website_url != "")
            .limit(100)
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


async def enrich_youtube_job(batch: int = 50):
    """Find artists with no YouTube link and look them up. Runs every 4h.

    batch=50 (was 100, originally 300): the artist pool grew over time
    (every new event adds candidate artists) and at batch=100 the job
    again crossed the 2GB Render ceiling — OOM observed 2026-05-04
    06:22 IL on the 06:20 firing, same shape as the 2026-04-21/22
    incidents that drove the 300→100 reduction. Halving again pulls
    peak memory well clear, and re-firing every 4h instead of 2h
    reduces collision risk with the other long-lived async jobs.

    Daily throughput now: 6 × 50 = 300 artists/day (was 12 × 100 = 1200).
    Acceptable: artist enrichment is one-shot per artist (cached on the
    Event row once found), so steady-state coverage matters more than
    burst rate. New artists drift in slowly; 300/day comfortably keeps
    pace with the catalog growth rate.
    """
    from sqlalchemy import func as _func, or_
    from app.services.youtube_lookup import lookup_youtube_video

    db = SessionLocal()
    log = ScanLog(job_name="enrich_youtube", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    found = 0
    failed = 0

    def _select_pending():
        rows = (
            db.query(Event.artist_name)
            .filter(
                Event.artist_name.isnot(None),
                Event.artist_name != "",
                # Skip sports events — home_team names aren't music artists
                Event.sport.is_(None),
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
        return [r[0] for r in rows]

    def _persist(artist: str, url: str | None) -> int:
        """Sync DB write for one artist. Returns 1 on found, 0 on not-found."""
        if url:
            db.query(Event).filter(Event.artist_name == artist).update(
                {"artist_youtube_channel": url}, synchronize_session=False
            )
            db.commit()
            db.expire_all()
            return 1
        db.query(Event).filter(
            Event.artist_name == artist,
            Event.artist_youtube_channel.is_(None),
        ).update({"artist_youtube_channel": ""}, synchronize_session=False)
        db.commit()
        db.expire_all()
        return 0

    try:
        # Sync query off the event loop — NOT IN / GROUP BY on ~50k rows can take seconds.
        names = await asyncio.to_thread(_select_pending)
        logger.info(f"enrich_youtube: {len(names)} artists to enrich")

        for artist in names:
            try:
                url = await lookup_youtube_video(artist)
                # Persist sync DB write off the event loop.
                if await asyncio.to_thread(_persist, artist, url):
                    found += 1
                else:
                    failed += 1
            except Exception as e:
                logger.warning(f"enrich_youtube: error for {artist!r}: {e}")
                await asyncio.to_thread(db.rollback)
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
    import json
    import httpx
    from app.models import Performer
    from app.services.performer_lookup import lookup_musicbrainz, normalize
    from sqlalchemy.exc import IntegrityError

    if _heavy_job_lock.locked():
        logger.info("enrich_performers_job: another heavy job is running — skipping this run")
        return

    db = SessionLocal()
    log = ScanLog(job_name="enrich_performers", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    enriched = 0
    skipped = 0

    def _select_pending():
        # Push filtering to SQL — never load all artists or all performers into Python
        already_seen_sq = db.query(Performer.name).subquery()
        rows = (
            db.query(Event.artist_name, func.count(Event.id).label("n"))
            .filter(
                Event.artist_name.isnot(None),
                Event.artist_name != "",
                # Skip sports events — team names are not music artists
                Event.sport.is_(None),
                Event.artist_name.notin_(already_seen_sq),
            )
            .group_by(Event.artist_name)
            .order_by(func.count(Event.id).desc())
            .limit(batch)
            .all()
        )
        return [r[0] for r in rows]

    def _persist(artist: str, result: dict | None) -> tuple[int, int]:
        """Sync DB write. Returns (enriched_delta, skipped_delta)."""
        if result:
            # Performer.genres is a TEXT column — serialize the Python list
            # to JSON before binding. Empty list → NULL so we don't store "[]".
            genres_list = result.get("genres") or []
            genres_json = json.dumps(genres_list) if genres_list else None
            perf = Performer(
                name=artist,
                normalized_name=normalize(artist),
                category=result.get("category"),
                event_type_name=result.get("event_type_name"),
                genres=genres_json,
                mb_id=result.get("mb_id"),
                mb_type=result.get("mb_type"),
                source="musicbrainz",
                confidence=result.get("confidence", 1.0),
            )
            db.add(perf)
            try:
                db.commit()
                db.expire_all()
                return (1, 0)
            except IntegrityError:
                db.rollback()
                return (0, 0)
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
            db.expire_all()
            return (0, 1)
        except IntegrityError:
            db.rollback()
            return (0, 0)

    try:
        # Sync NOT IN + GROUP BY off the event loop.
        pending = await asyncio.to_thread(_select_pending)
        logger.info(f"enrich_performers: {len(pending)} new artists to look up")

        async with httpx.AsyncClient(timeout=15) as http:
            for artist in pending:
                try:
                    result = await lookup_musicbrainz(artist, http)
                    e_d, s_d = await asyncio.to_thread(_persist, artist, result)
                    enriched += e_d
                    skipped += s_d
                except Exception as e:
                    logger.warning(f"enrich_performers: error for {artist!r}: {e}")
                    await asyncio.to_thread(db.rollback)
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


async def enrich_venue_urls_job(batch: int = 50):
    """
    Fill missing website_url on existing venues using OSM Nominatim.
    Processes the `batch` venues with the most events first.
    Rate-limited to ≥1.1 s between Nominatim requests.
    """
    import asyncio
    import httpx
    from sqlalchemy import or_ as _or_
    from app.services.osm import find_venue_url

    db = SessionLocal()
    log = ScanLog(job_name="enrich_venue_urls", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    found = 0
    checked = 0
    try:
        # Venues with no URL, ordered by event count desc so the most-visited get filled first
        rows = (
            db.query(Venue.id, Venue.name, Venue.physical_city, Venue.physical_country)
            .outerjoin(Event, Event.venue_id == Venue.id)
            .filter(
                _or_(Venue.website_url.is_(None), Venue.website_url == "")
            )
            .group_by(Venue.id, Venue.name, Venue.physical_city, Venue.physical_country)
            .order_by(func.count(Event.id).desc())
            .limit(batch)
            .all()
        )
        logger.info(f"enrich_venue_urls: {len(rows)} venues to look up")

        serper_key = settings.SERPER_API_KEY
        async with httpx.AsyncClient(timeout=15) as client:
            for venue_id, name, city, country in rows:
                try:
                    url = await find_venue_url(
                        client, name, city or "", country or "", serper_key
                    )
                    checked += 1
                    if url:
                        db.query(Venue).filter(Venue.id == venue_id).update(
                            {"website_url": url}, synchronize_session=False
                        )
                        db.commit()
                        found += 1
                        logger.debug(f"enrich_venue_urls: {name!r} → {url}")
                    # Nominatim rate limit: ≥1.1 s between requests
                    await asyncio.sleep(1.1)
                    db.expire_all()
                except Exception as e:
                    logger.warning(f"enrich_venue_urls: error for {name!r}: {e}")
                    db.rollback()

        log.status = "success"
        log.events_found = checked
        log.events_saved = found
        log.notes = f"checked={checked} urls_found={found}"
        logger.info(f"enrich_venue_urls done: checked={checked} found={found}")
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
        logger.error(f"enrich_venue_urls error: {e}")
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()


async def discover_venues_job():
    """
    Use OSM Overpass API to find venue nodes/ways near each priority city
    and insert any that are not already in our DB.
    Processes CITY_BATCH_SIZE cities per run (same rotation as collect_all_events).
    """
    import asyncio
    import httpx
    from app.services.osm import overpass_discover_venues

    if _heavy_job_lock.locked():
        logger.info("discover_venues_job: another heavy job is running — skipping this run")
        return

    db = SessionLocal()
    log = ScanLog(job_name="discover_venues", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    new_venues = 0
    cities_checked = 0
    try:
        # Process only a small batch of cities per run to cap memory usage
        priority_names = [name for name, _country in PRIORITY_CITIES]
        cities = (
            db.query(City)
            .filter(City.name.in_(priority_names), City.latitude.isnot(None), City.longitude.isnot(None))
            .limit(CITY_BATCH_SIZE)
            .all()
        )
        logger.info(f"discover_venues: checking {len(cities)} priority cities")

        async with httpx.AsyncClient(timeout=50) as client:
            for city in cities:
                try:
                    candidates = await overpass_discover_venues(
                        client, city.latitude, city.longitude, city.name
                    )
                    cities_checked += 1
                    for v in candidates:
                        if not v.get("name"):
                            continue
                        # Case-insensitive match: skip if already in DB for this city
                        exists = (
                            db.query(Venue.id)
                            .filter(
                                Venue.city_id == city.id,
                                func.lower(Venue.name) == v["name"].lower(),
                            )
                            .first()
                        )
                        if exists:
                            continue
                        # Take URL from OSM if present; enrich_venue_urls_job fills the rest
                        website = v.get("website") or None
                        venue = Venue(
                            name=v["name"],
                            city_id=city.id,
                            physical_city=city.name,
                            physical_country=city.country,
                            latitude=v.get("lat"),
                            longitude=v.get("lon"),
                            street_address=v.get("address"),
                            website_url=website or None,
                            venue_type=v.get("venue_type"),
                        )
                        db.add(venue)
                        new_venues += 1
                    db.commit()
                    db.expire_all()
                    logger.info(
                        f"discover_venues: {city.name} — {len(candidates)} found, "
                        f"{new_venues} new total so far"
                    )
                except Exception as e:
                    logger.warning(f"discover_venues: error for {city.name}: {e}")
                    db.rollback()

        log.status = "success"
        log.events_found = cities_checked
        log.events_saved = new_venues
        log.notes = f"cities={cities_checked} new_venues={new_venues}"
        logger.info(f"discover_venues done: cities={cities_checked} new_venues={new_venues}")
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
        logger.error(f"discover_venues error: {e}")
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()


async def collect_bandsintown_job(batch: int = 150):
    """
    Artist-centric Bandsintown scan — queries the top `batch` performers
    by event count and saves any upcoming events returned by the API.
    Runs every 12 hours so the most-popular artists stay fresh.
    """
    if _heavy_job_lock.locked():
        logger.info("collect_bandsintown_job: another heavy job is running — skipping this run")
        return

    import asyncio as _asyncio
    from app.models import City, Venue, Event, Performer
    from app.services.collectors.base import RawEvent, default_end_time
    from datetime import date as _date
    import urllib.parse

    if not settings.BANDSINTOWN_APP_ID:
        logger.info("collect_bandsintown_job: BANDSINTOWN_APP_ID not set — skipping")
        return

    db = SessionLocal()
    log = ScanLog(job_name="bandsintown", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    found = saved = 0

    try:
        # Top performers by event count — most-booked artists first
        rows = (
            db.query(Performer.name, func.count(Event.id).label("n"))
            .outerjoin(Event, func.lower(Event.artist_name) == func.lower(Performer.name))
            .group_by(Performer.id, Performer.name)
            .order_by(func.count(Event.id).desc())
            .limit(batch)
            .all()
        )
        artist_names = [r[0] for r in rows]
        logger.info(f"collect_bandsintown_job: scanning {len(artist_names)} artists")

        client = BandsintownClient()
        today = _date.today()

        for artist in artist_names:
            try:
                events = await client.get_artist_events(artist)
                found += len(events)

                for ev in events:
                    try:
                        # Parse date
                        dt_str = ev.get("datetime") or ev.get("starts_at") or ""
                        from datetime import datetime as _dt
                        start_dt = _dt.fromisoformat(dt_str.replace("Z", "+00:00")) if dt_str else None
                        if not start_dt or start_dt.date() < today:
                            continue

                        # Resolve venue / city
                        venue_data = ev.get("venue") or {}
                        city_name    = venue_data.get("city") or ""
                        country_name = venue_data.get("country") or ""
                        venue_name   = venue_data.get("name") or ""

                        city = db.query(City).filter(
                            func.lower(City.name) == city_name.lower()
                        ).first()
                        if not city:
                            city = City(
                                name=city_name,
                                country=country_name,
                                latitude=venue_data.get("latitude"),
                                longitude=venue_data.get("longitude"),
                            )
                            db.add(city)
                            db.flush()

                        venue = db.query(Venue).filter(
                            Venue.city_id == city.id,
                            func.lower(Venue.name) == venue_name.lower(),
                        ).first()
                        if not venue:
                            venue = Venue(
                                name=venue_name,
                                city_id=city.id,
                                physical_city=city_name,
                                physical_country=country_name,
                                latitude=venue_data.get("latitude"),
                                longitude=venue_data.get("longitude"),
                            )
                            db.add(venue)
                            db.flush()

                        source_id = f"bandsintown:{ev.get('id', '')}"
                        if db.query(Event.id).filter_by(scrape_source="bandsintown", source_id=source_id).first():
                            continue

                        lineup = ev.get("lineup") or []
                        event_name = lineup[0] if lineup else artist

                        new_ev = Event(
                            name=event_name,
                            artist_name=artist,
                            start_date=start_dt.date(),
                            start_time=start_dt.strftime("%H:%M"),
                            venue_id=venue.id,
                            venue_name=venue_name,
                            purchase_link=ev.get("url"),
                            description=ev.get("description"),
                            scrape_source="bandsintown",
                            source_id=source_id,
                        )
                        db.add(new_ev)
                        saved += 1
                    except Exception as e:
                        logger.debug(f"bandsintown event error for {artist!r}: {e}")

                db.commit()
                db.expire_all()
                await _asyncio.sleep(1.1)  # Bandsintown rate limit

            except Exception as e:
                logger.warning(f"bandsintown artist error {artist!r}: {e}")
                db.rollback()

        log.status = "success"
        log.events_found = found
        log.events_saved = saved
        log.notes = f"artists={len(artist_names)} found={found} saved={saved}"
        logger.info(f"collect_bandsintown_job done: found={found} saved={saved}")
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
        logger.error(f"collect_bandsintown_job error: {e}")
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()


_TECHCONF_DISTRICT_TO_CITY: dict[str, str] = {
    # Singapore districts
    "marina bay":    "Singapore",
    "downtown core": "Singapore",
    "orchard":       "Singapore",
    "sentosa":       "Singapore",
    # Dubai / UAE
    "dubai media city": "Dubai",
    "dubai world trade centre": "Dubai",
    "dwtc":          "Dubai",
    "jbr":           "Dubai",
    # US venues often listed by venue name not city
    "las vegas convention center": "Las Vegas",
    # Netherlands
    "rai amsterdam": "Amsterdam",
}

# Country names that ARE city names (city-states / capitals used as country)
_TECHCONF_COUNTRY_IS_CITY: set[str] = {
    "singapore", "luxembourg", "monaco",
}


def _resolve_techconf_city(city_name: str, country_name: str, db) -> "City":
    """Resolve a conference city, handling districts and city-states."""
    from app.models import City as _City

    # 1. Direct exact match
    city = db.query(_City).filter(
        func.lower(_City.name) == city_name.lower()
    ).first()
    if city:
        return city

    # 2. District → known city mapping
    mapped = _TECHCONF_DISTRICT_TO_CITY.get(city_name.lower())
    if mapped:
        city = db.query(_City).filter(
            func.lower(_City.name) == mapped.lower()
        ).first()
        if city:
            return city
        city_name = mapped  # create with the proper name

    # 3. Country-as-city (Singapore, Luxembourg…)
    elif country_name.lower() in _TECHCONF_COUNTRY_IS_CITY:
        city = db.query(_City).filter(
            func.lower(_City.name) == country_name.lower()
        ).first()
        if city:
            return city
        city_name = country_name

    # 4. Create a new city record for cities we don't already track.
    #    Earlier behaviour fell back to "any city in the same country" via
    #    .first(), which caused e.g. "Palo Alto, US" events to be silently
    #    mis-shelved under whichever US city happened to sort first — so
    #    Stanford WebCamp would never show up when searching Palo Alto.
    new_city = _City(name=city_name, country=country_name)
    db.add(new_city)
    db.flush()
    return new_city


async def collect_techconf_job():
    """
    Scrape techconf.directory/conferences and save upcoming tech conferences.
    Runs daily — the directory is updated frequently with new events.
    """
    from app.services.collectors.scrapers.techconf_directory import scrape_techconf_directory
    from app.models import City, Venue, Event, EventType

    db = SessionLocal()
    log = ScanLog(job_name="techconf_directory", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    found = saved = 0

    try:
        conferences = await scrape_techconf_directory()
        found = len(conferences)

        # Look up the "Tech Conference" event type (seeded); fall back to
        # "AI Tech Conferences" for DBs not yet re-seeded after this deploy.
        tech_et = (
            db.query(EventType).filter_by(name="Tech Conference").first()
            or db.query(EventType).filter_by(name="AI Tech Conferences").first()
        )

        for conf in conferences:
            try:
                city_name    = conf["city"] or "Online"
                country_name = conf["country"] or ("Global" if conf["is_online"] else "")
                is_online    = conf["is_online"]

                # Resolve or create city (handles districts + city-states)
                city = _resolve_techconf_city(city_name, country_name, db)

                # Generic conference venue per city
                venue_name = f"{city_name} Conference" if not is_online else "Online"
                venue = db.query(Venue).filter(
                    Venue.city_id == city.id,
                    func.lower(Venue.name) == venue_name.lower(),
                ).first()
                if not venue:
                    venue = Venue(
                        name=venue_name,
                        city_id=city.id,
                        physical_city=city_name,
                        physical_country=country_name,
                    )
                    db.add(venue)
                    db.flush()

                # Dedup by source_id — but fix existing records saved with
                # the old broken parser (is_online=True, empty venue_name)
                source_id = f"techconf:{conf['url']}"
                existing = db.query(Event).filter_by(
                    scrape_source="techconf_directory", source_id=source_id
                ).first()
                if existing:
                    if existing.is_online and not is_online:
                        # Bad record from old parser — update in place
                        existing.is_online = False
                        existing.venue_id = venue.id
                        existing.venue_name = venue_name
                        existing.end_date = conf["end_date"]
                    # Backfill event type if missing (e.g. first run before seed)
                    if tech_et and tech_et not in existing.event_types:
                        existing.event_types.append(tech_et)
                    continue

                new_ev = Event(
                    name=conf["name"],
                    start_date=conf["start_date"],
                    end_date=conf["end_date"],
                    venue_id=venue.id,
                    venue_name=venue_name,
                    purchase_link=conf["url"],
                    scrape_source="techconf_directory",
                    source_id=source_id,
                    is_online=is_online,
                )
                db.add(new_ev)
                db.flush()

                # Assign Tech Conference event type
                if tech_et and tech_et not in new_ev.event_types:
                    new_ev.event_types.append(tech_et)

                saved += 1
            except Exception as e:
                logger.debug(f"collect_techconf_job event error {conf.get('name')!r}: {e}")

        db.commit()
        log.status = "success"
        log.events_found = found
        log.events_saved = saved
        log.notes = f"found={found} saved={saved}"
        logger.info(f"collect_techconf_job done: found={found} saved={saved}")
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
        logger.error(f"collect_techconf_job error: {e}")
        db.rollback()
    finally:
        log.finished_at = datetime.utcnow()
        db.commit()
        db.close()


async def enrich_spotify_job(batch: int = 150):
    """
    Enrich Performer records with Spotify data: genres, image, popularity, URL.
    Prioritises performers with no spotify_id yet, ordered by event count.
    Runs nightly after enrich_performers_job so MusicBrainz stubs exist first.
    """
    import json as _json
    import httpx as _httpx
    from app.models import Performer
    from app.services.spotify_lookup import lookup_spotify_artist

    if not settings.SPOTIFY_CLIENT_ID or not settings.SPOTIFY_CLIENT_SECRET:
        logger.info("enrich_spotify_job: SPOTIFY_CLIENT_ID not set — skipping")
        return

    if _heavy_job_lock.locked():
        logger.info("enrich_spotify_job: another heavy job is running — skipping this run")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="enrich_spotify", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)
        enriched = 0
        skipped = 0

        def _select_pending():
            # Prioritise performers with events but no Spotify data yet
            return (
                db.query(Performer.id, Performer.name, func.count(Event.id).label("n"))
                .outerjoin(Event, func.lower(Event.artist_name) == func.lower(Performer.name))
                .filter(
                    Performer.spotify_id.is_(None),
                    Performer.source != "not_found",   # skip confirmed-dead stubs
                )
                .group_by(Performer.id, Performer.name)
                .order_by(func.count(Event.id).desc())
                .limit(batch)
                .all()
            )

        def _persist(perf_id: int, name: str, result: dict | None) -> int:
            """Sync DB write. Returns 1 on enriched, 0 on skipped."""
            if not result:
                db.commit()
                db.expire_all()
                return 0
            perf_update: dict = {
                "spotify_id":  result["spotify_id"],
                "spotify_url": result["spotify_url"],
                "image_url":   result["image_url"],
                "popularity":  result["popularity"],
            }
            # Only overwrite genres/category/event_type if Spotify
            # gives us something more specific than what we have.
            if result["genres"]:
                perf_update["genres"] = _json.dumps(result["genres"])
            if result["event_type_name"]:
                perf_update["event_type_name"] = result["event_type_name"]
                perf_update["category"] = result["category"]
                perf_update["source"] = "spotify"

            db.query(Performer).filter(Performer.id == perf_id).update(
                perf_update, synchronize_session=False
            )

            # Propagate 1-10 popularity score + Spotify URL to all
            # events for this artist so the frontend can display it.
            raw_pop = result["popularity"] or 0
            score_1_10 = max(1, round(raw_pop / 10)) if raw_pop else None
            event_update: dict = {}
            if score_1_10:
                event_update["artist_popularity"] = score_1_10
            if result["spotify_url"]:
                event_update["artist_spotify_url"] = result["spotify_url"]
            # Fill missing event image with artist photo
            if result["image_url"]:
                db.query(Event).filter(
                    Event.artist_name == name,
                    Event.image_url.is_(None),
                ).update(
                    {"image_url": result["image_url"]},
                    synchronize_session=False,
                )
            if event_update:
                db.query(Event).filter(
                    Event.artist_name == name,
                ).update(event_update, synchronize_session=False)

            db.commit()
            db.expire_all()
            logger.debug(
                f"enrich_spotify: {name!r} → {result['event_type_name']} "
                f"(pop={result['popularity']})"
            )
            return 1

        try:
            # Sync query off the event loop
            rows = await asyncio.to_thread(_select_pending)
            logger.info(f"enrich_spotify_job: {len(rows)} performers to enrich")

            async with _httpx.AsyncClient(timeout=10) as http:
                for perf_id, name, _n in rows:
                    try:
                        result = await lookup_spotify_artist(
                            name,
                            settings.SPOTIFY_CLIENT_ID,
                            settings.SPOTIFY_CLIENT_SECRET,
                            http,
                        )
                        if await asyncio.to_thread(_persist, perf_id, name, result):
                            enriched += 1
                        else:
                            skipped += 1
                        await asyncio.sleep(0.2)   # ~5 req/s — well within Spotify limits

                    except Exception as e:
                        logger.warning(f"enrich_spotify: error for {name!r}: {e}")
                        await asyncio.to_thread(db.rollback)
                        skipped += 1

            log.status = "success"
            log.events_found = len(rows)
            log.events_saved = enriched
            log.notes = f"enriched={enriched} no_result={skipped}"
            logger.info(f"enrich_spotify_job done: enriched={enriched} skipped={skipped}")

        except Exception as e:
            log.status = "failed"
            log.notes = str(e)
            logger.error(f"enrich_spotify_job error: {e}")
        finally:
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()


# ---------------------------------------------------------------------------
# Mevalim (IL event aggregator) — dedicated multi-venue collector
# ---------------------------------------------------------------------------
# Mevalim lists shows across 40+ Israeli cities at the real venues they happen
# at. We CANNOT run it through the CollectorRegistry's city-loop because that
# pipeline pins every venue to the `city` param passed in (registry.py:601),
# which would mis-attach every Mevalim venue to Tel Aviv. Same shape as
# collect_techconf_job: resolve the real city per event, find-or-create the
# real venue, save directly.

# Raw category (from sitemap URL) → EventType name. These names match seeds
# in app/seed/event_types.py — verified present in prod DB.
_MEVALIM_CATEGORY_EVENT_TYPE: dict[str, str] = {
    "Music":    "Pop Concert",               # default concert bucket for mevalim
    "Comedy":   "Comedy Club Headliners",
    "Stand-up": "Comedy Club Headliners",
    "Theater":  "Play / Drama",
    "Family":   "Play / Drama",               # kids' shows are typically plays
    "Children": "Play / Drama",
}


def _resolve_mevalim_city(city_name: str, db) -> "City":
    """Find-or-create an Israeli city by canonical English name."""
    from app.models import City as _City

    city = db.query(_City).filter(
        func.lower(_City.name) == city_name.lower(),
        func.lower(_City.country) == "israel",
    ).first()
    if city:
        return city

    # Some legacy rows may have country NULL — match by name only as fallback.
    city = db.query(_City).filter(
        func.lower(_City.name) == city_name.lower()
    ).first()
    if city:
        if not city.country:
            city.country = "Israel"
        return city

    new_city = _City(name=city_name, country="Israel")
    db.add(new_city)
    db.flush()
    return new_city


async def collect_mevalim_job():
    """
    Scrape mevalim.co.il and save upcoming events across all IL cities.

    The Mevalim site is an event AGGREGATOR — each show happens at a real
    venue (not at "Mevalim"). This job parses every JSON-LD Event from the
    Yoast sitemap pages and attributes each event to its actual venue/city.
    Runs daily; full crawl takes ~2 min for ~1500 candidate URLs.
    """
    from app.services.collectors.scrapers.mevalim import scrape_mevalim
    from app.models import City, Venue, Event, EventType

    if _heavy_job_lock.locked():
        logger.info("collect_mevalim_job: another heavy job is running — skipping")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="mevalim", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)
        found = saved = updated = admitted_no_city = 0
        # Track venue names that the scraper couldn't resolve to a known city
        # so we can extend _HEBREW_CITIES on the next sweep instead of guessing.
        from collections import Counter
        unresolved_venues: Counter[str] = Counter()

        try:
            events = await scrape_mevalim()
            found = len(events)

            # Pre-fetch event type rows once — avoids per-event lookups in a
            # loop that can hit 500+ events.
            et_cache: dict[str, "EventType"] = {}
            for cat_name, et_name in _MEVALIM_CATEGORY_EVENT_TYPE.items():
                et = db.query(EventType).filter_by(name=et_name).first()
                if et:
                    et_cache[cat_name] = et

            for raw in events:
                try:
                    if not raw.venue_city:
                        # No city match → admit under a sentinel "Israel - Other"
                        # city instead of dropping. The event still surfaces in
                        # autocomplete + name searches; only city-filtered
                        # browses miss it. Beats the silent skip that hid
                        # ~50–130 real events per run.
                        unresolved_venues[raw.venue_name or "(unknown)"] += 1
                        city = _resolve_mevalim_city("Israel - Other", db)
                        admitted_no_city += 1
                    else:
                        city = _resolve_mevalim_city(raw.venue_city, db)

                    # Real venue (per event, city-pinned). If we've seen this
                    # venue name in this city before, reuse it.
                    venue = db.query(Venue).filter(
                        Venue.city_id == city.id,
                        func.lower(Venue.name) == raw.venue_name.lower(),
                    ).first()
                    if not venue:
                        venue = Venue(
                            name=raw.venue_name,
                            city_id=city.id,
                            street_address=raw.venue_address,
                            physical_city=raw.venue_city,
                            physical_country=raw.venue_country or "Israel",
                        )
                        db.add(venue)
                        db.flush()

                    # Dedup by canonical offer URL (scraper sets source_id to
                    # tickets.mevalim.co.il/event/{id}).
                    existing = db.query(Event).filter_by(
                        scrape_source="mevalim", source_id=raw.source_id
                    ).first()
                    if existing:
                        # Refresh core fields in case date/venue/price changed
                        existing.start_date   = raw.start_date
                        existing.start_time   = raw.start_time
                        existing.end_date     = raw.end_date
                        existing.end_time     = raw.end_time
                        existing.price        = raw.price
                        existing.price_currency = raw.price_currency
                        existing.purchase_link = raw.purchase_link
                        existing.venue_id     = venue.id
                        existing.venue_name   = raw.venue_name
                        updated += 1
                        continue

                    new_ev = Event(
                        name=raw.name,
                        start_date=raw.start_date,
                        start_time=raw.start_time,
                        end_date=raw.end_date,
                        end_time=raw.end_time,
                        price=raw.price,
                        price_currency=raw.price_currency,
                        purchase_link=raw.purchase_link,
                        image_url=raw.image_url,
                        venue_id=venue.id,
                        venue_name=raw.venue_name,
                        scrape_source="mevalim",
                        source_id=raw.source_id,
                        is_online=False,
                    )
                    db.add(new_ev)
                    db.flush()

                    # Assign event type from the first raw_category that has a
                    # mapping. Categories come from the sitemap URL prefix so
                    # they're authoritative for the show's genre.
                    for cat_name in (raw.raw_categories or []):
                        et = et_cache.get(cat_name)
                        if et and et not in new_ev.event_types:
                            new_ev.event_types.append(et)
                            break

                    saved += 1
                except Exception as e:
                    logger.debug(
                        f"collect_mevalim_job event error {raw.name!r}: {e}"
                    )
                    db.rollback()

            db.commit()
            log.status = "success"
            log.events_found = found
            log.events_saved = saved
            # Top-5 unresolved venue names — names that recur here are
            # high-value candidates to add to mevalim._HEBREW_CITIES so the
            # event lands in its true city next run.
            top_unresolved = ", ".join(
                f"{name!r}:{cnt}"
                for name, cnt in unresolved_venues.most_common(5)
            )
            unresolved_suffix = (
                f" top_unresolved=[{top_unresolved}]" if top_unresolved else ""
            )
            log.notes = (
                f"found={found} saved={saved} updated={updated} "
                f"admitted_no_city={admitted_no_city}{unresolved_suffix}"
            )
            logger.info(
                f"collect_mevalim_job done: found={found} saved={saved} "
                f"updated={updated} admitted_no_city={admitted_no_city}"
                f"{unresolved_suffix}"
            )
        except Exception as e:
            log.status = "failed"
            log.notes = str(e)
            logger.error(f"collect_mevalim_job error: {e}")
            db.rollback()
        finally:
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()


# Drift detection — sliding window + scoring helpers.
#
# We keep the last ``_DRIFT_WINDOW`` event counts on each LLMSource. Each
# run pushes a new count; oldest is dropped when the window is full.
#
# drift_score = (avg(recent_excluding_last) - last) / max(avg(...), 1)
#   ≤ 0   → no drift (events held steady or grew this run)
#   1.0   → total collapse (last run = 0)
#   0.5   → events halved vs. the prior average
#
# drift_flag fires only when:
#   • we have ``_DRIFT_MIN_HISTORY`` prior runs (avoid noise from small N)
#   • drift_score > ``_DRIFT_THRESHOLD``
#
# Move 2 of drift work (auto-action on flag) is deliberately deferred:
# v1 only surfaces the flag for the audit dashboard so we can calibrate
# the threshold against real noise before any state machine reacts.

_DRIFT_WINDOW = 10
# Minimum *prior* runs (so the flag earliest can fire on the 4th total
# run when we have 3 priors). 3 priors is the smallest sample where an
# average is meaningfully more than "the last value"; smaller and the
# flag becomes noise.
_DRIFT_MIN_HISTORY = 3
_DRIFT_THRESHOLD = 0.5      # >50% reduction vs prior average


def _update_drift_state(src, latest_event_count: int) -> None:
    """Mutates ``src`` in place: appends the latest count to the rolling
    window, recomputes drift_score + drift_flag.

    No-op cleanup: when latest is 0 due to a transient extractor error
    (method='error') the caller may want to skip drift updates entirely
    so we don't over-react to API hiccups. That gating is the caller's
    responsibility — this helper just does the math when called.
    """
    history = list(src.recent_event_counts or [])
    history.append(int(latest_event_count))
    history = history[-_DRIFT_WINDOW:]
    src.recent_event_counts = history

    if len(history) - 1 >= _DRIFT_MIN_HISTORY:
        prior = history[:-1]
        prior_avg = sum(prior) / len(prior)
        denom = max(prior_avg, 1.0)
        src.drift_score = (prior_avg - latest_event_count) / denom
        src.drift_flag = bool(src.drift_score > _DRIFT_THRESHOLD)
    else:
        src.drift_score = None
        src.drift_flag = False


async def llm_extract_recurring_job(
    max_sources_per_run: int = 20,
    min_hours_since_last: int = 18,
    auto_block_threshold: int = 3,
    auto_promote_threshold: int = 3,
):
    """Run the LLM extractor against active LLMSource rows.

    This is Cadence A of Route 1: keep already-onboarded sources fresh.
    Cadence B (discovery of new sources) is a separate job, not this one.

    Picks up to ``max_sources_per_run`` sources whose state is in
    {trial, recurring} and whose ``last_run_at`` is older than
    ``min_hours_since_last``. Sources with state=blocked or graduated
    are skipped — blocked is permanent until manually un-blocked,
    graduated means we wrote a custom collector and the LLM is
    redundant.

    Cost gate: hard cap on sources per fire. With max=20 and a 24h
    schedule, a registry of ≤20 sources gets daily coverage; larger
    registries require multiple fires per day to fully cycle (set the
    cron interval shorter accordingly, or raise the cap once we've
    observed real costs).

    Memory gate: sequential, with explicit gc between sources. Uses
    the same _heavy_job_lock the other long-lived jobs use, so it
    can't overlap with enrich_youtube etc — direct cause of the
    2026-04 / 2026-05 OOMs we tightened against.

    Auto-demote: ``consecutive_empty_runs >= auto_block_threshold``
    flips state to 'blocked' with a date-stamped note. This catches
    sources that have gone offline, redesigned past our extractor, or
    rate-limited us out.

    Auto-promote: ``consecutive_success_runs >= auto_promote_threshold``
    on a state='trial' source flips it to 'recurring' with a note.
    This finishes the source-graduation lifecycle without manual
    `--promote` flags. Reset symmetrically with consecutive_empty_runs:
    each run is either a success (events found, no error) or an empty
    (incrementing the demotion counter), never both.
    """
    import gc
    from sqlalchemy import or_
    from app.models import LLMSource, City

    if _heavy_job_lock.locked():
        logger.info("llm_extract_recurring: another heavy job is running — skipping this run")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="llm_extract_recurring", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)

        try:
            # Late import — avoids loading google-genai at scheduler-init time.
            # extract_auto tries JSON-LD parsing first (free, no API call) and
            # falls through to the LLM extractor only when JSON-LD doesn't yield
            # events. ExtractorUnconfigured is only raised if the LLM path is
            # actually needed — pure JSON-LD sources work without the API key.
            from app.extractors.llm_extractor import (
                extract_auto, resolve_template_urls, ExtractorUnconfigured,
            )
            from app.services.collectors.registry import CollectorRegistry

            cutoff = datetime.utcnow() - timedelta(hours=min_hours_since_last)
            sources = (
                db.query(LLMSource)
                .filter(
                    LLMSource.state.in_(["trial", "recurring"]),
                    or_(
                        LLMSource.last_run_at.is_(None),
                        LLMSource.last_run_at < cutoff,
                    ),
                )
                # Run never-run-before sources first; among those that have
                # run, prioritize the longest-untouched. Stable ordering
                # also helps reproducibility when debugging a slow cycle.
                .order_by(LLMSource.last_run_at.asc().nullsfirst(), LLMSource.id.asc())
                .limit(max_sources_per_run)
                .all()
            )

            if not sources:
                log.status = "success"
                log.notes = "no due sources"
                logger.info("llm_extract_recurring: no sources due for re-extraction")
                return

            registry = CollectorRegistry()
            total_events = 0
            total_saved = 0
            auto_blocked = 0
            extractor_unavailable = False

            for src in sources:
                # If the extractor was unconfigured on a previous source,
                # skip the rest — they'd all fail the same way.
                if extractor_unavailable:
                    break

                src_url = src.url

                # Resolve URL template if the source has one (Move 2). For
                # plain sources this is just [src.url]. For template
                # sources we iterate the expanded URL list, aggregate
                # events, and fold the per-URL signals into one result-
                # shaped object so the rest of the loop is unchanged.
                urls_to_scan = resolve_template_urls(
                    src.url,
                    template=src.url_template,
                    range_months=src.url_template_range_months,
                    values=list(src.url_template_values or []) if src.url_template_values else None,
                )

                aggregated_events = []
                aggregated_method = "html"   # overwritten below if any path differs
                aggregated_error = None
                aggregated_pagination = {"has_pagination": False, "signal": None, "next_page_url": None}
                aggregated_dropped = 0
                last_per_url_method = None

                try:
                    for u in urls_to_scan:
                        # Run extract_auto on a thread — sync (urllib + bs4
                        # + maybe Gemini SDK). JSON-LD path runs without
                        # the LLM key.
                        per_result = await asyncio.to_thread(
                            extract_auto, u,
                            source_name="llm_extractor",
                        )
                        aggregated_events.extend(per_result.events)
                        aggregated_dropped += per_result.dropped_for_hallucination
                        last_per_url_method = per_result.method
                        if per_result.error:
                            aggregated_error = per_result.error
                        if per_result.has_pagination:
                            aggregated_pagination = {
                                "has_pagination": True,
                                "signal": per_result.pagination_signal,
                                "next_page_url": per_result.next_page_url,
                            }
                except ExtractorUnconfigured as e:
                    extractor_unavailable = True
                    logger.warning(
                        f"llm_extract_recurring: extractor unconfigured "
                        f"({e}); skipping the rest of this run"
                    )
                    log.notes = f"GEMINI_API_KEY not set ({e})"
                    log.status = "failed"
                    break
                except Exception as e:
                    logger.warning(
                        f"llm_extract_recurring: {src_url} hard error: {e}"
                    )
                    src.last_error = f"{type(e).__name__}: {e}"[:500]
                    src.last_run_at = datetime.utcnow()
                    src.runs_total = (src.runs_total or 0) + 1
                    db.commit()
                    continue

                # Synthesize a single result object so downstream code
                # (drift update, streak counters, persistence) doesn't
                # need to know about iteration. method defaults to the
                # last per-URL method when ≥1 URL succeeded; "error"
                # only when every URL failed.
                from types import SimpleNamespace
                synthesized_method = (
                    last_per_url_method if aggregated_events
                    else (last_per_url_method or "error")
                )
                result = SimpleNamespace(
                    events=aggregated_events,
                    method=synthesized_method,
                    error=aggregated_error if not aggregated_events else None,
                    dropped_for_hallucination=aggregated_dropped,
                    has_pagination=aggregated_pagination["has_pagination"],
                    pagination_signal=aggregated_pagination["signal"],
                    next_page_url=aggregated_pagination["next_page_url"],
                )
                if len(urls_to_scan) > 1:
                    logger.info(
                        f"llm_extract_recurring: {src_url} template-expanded "
                        f"to {len(urls_to_scan)} URLs → {len(aggregated_events)} events"
                    )

                # Persist events via the same ingest pipeline collectors use.
                saved = 0
                if result.events:
                    city = None
                    if src.city_name:
                        q = db.query(City).filter(City.name == src.city_name)
                        if src.country:
                            q = q.filter(City.country == src.country)
                        city = q.first()
                    if city is None and src.country:
                        # Nationwide source fallback — pick any city in the
                        # country; venue_city per-event drives final assignment.
                        city = db.query(City).filter(
                            City.country == src.country
                        ).first()

                    if city is None:
                        logger.warning(
                            f"llm_extract_recurring: {src_url} no City "
                            f"resolvable (city={src.city_name!r}, "
                            f"country={src.country!r}) — events not persisted"
                        )
                    else:
                        try:
                            saved = registry._save_events(result.events, city, db)
                        except Exception as e:
                            logger.warning(
                                f"llm_extract_recurring: persist error for "
                                f"{src_url}: {e}"
                            )
                            db.rollback()
                            saved = 0

                # Update LLMSource row
                src.last_run_at = datetime.utcnow()
                src.runs_total = (src.runs_total or 0) + 1
                src.last_event_count = len(result.events)
                src.last_method = result.method
                src.last_error = result.error[:500] if result.error else None
                src.events_seen_total = (src.events_seen_total or 0) + len(result.events)
                src.events_saved_total = (src.events_saved_total or 0) + saved
                src.has_pagination = bool(result.has_pagination)
                src.pagination_signal = result.pagination_signal
                src.next_page_url = (result.next_page_url or "")[:1000] or None

                # Drift signal — only update when the run actually reached
                # an event-extraction phase. Transient extractor errors
                # (method='error', usually Gemini 5xx exhausted) shouldn't
                # poison the drift window with a fake "0 events" sample.
                if result.method != "error":
                    _update_drift_state(src, len(result.events))
                    if src.drift_flag:
                        logger.warning(
                            f"llm_extract_recurring: drift detected on {src_url} "
                            f"(score={src.drift_score:.2f}, "
                            f"recent={src.recent_event_counts})"
                        )

                # Track consecutive success / empty streaks. A run counts as
                # "successful" when events were extracted AND no error fired;
                # "empty" otherwise. Symmetric reset: incrementing one zeroes
                # the other so the streaks reflect the most-recent run-shape.
                run_was_successful = bool(result.events) and not result.error
                if run_was_successful:
                    src.consecutive_success_runs = (src.consecutive_success_runs or 0) + 1
                    src.consecutive_empty_runs = 0
                else:
                    src.consecutive_empty_runs = (src.consecutive_empty_runs or 0) + 1
                    src.consecutive_success_runs = 0

                # Auto-demote (skip already-blocked rows defensively).
                if (src.state in ("trial", "recurring")
                        and (src.consecutive_empty_runs or 0) >= auto_block_threshold):
                    src.state = "blocked"
                    stamp = f"[auto-blocked {datetime.utcnow().date()}] {auto_block_threshold}+ consecutive empty runs"
                    src.notes = (src.notes + "\n" + stamp) if src.notes else stamp
                    auto_blocked += 1
                    logger.info(
                        f"llm_extract_recurring: auto-blocked {src_url} "
                        f"(consecutive_empty={src.consecutive_empty_runs})"
                    )

                # Auto-promote trial → recurring after enough clean runs.
                # Recurring already runs on the same cadence; promotion is
                # an operational signal ("this source has earned its place
                # in the registry") rather than a behavior change. Operators
                # can still manually --block to override.
                if (src.state == "trial"
                        and (src.consecutive_success_runs or 0) >= auto_promote_threshold):
                    src.state = "recurring"
                    stamp = (
                        f"[auto-promoted {datetime.utcnow().date()}] "
                        f"{auto_promote_threshold}+ consecutive runs with events"
                    )
                    src.notes = (src.notes + "\n" + stamp) if src.notes else stamp
                    logger.info(
                        f"llm_extract_recurring: auto-promoted {src_url} "
                        f"trial → recurring (consecutive_success={src.consecutive_success_runs})"
                    )

                db.commit()
                total_events += len(result.events)
                total_saved += saved

                logger.info(
                    f"llm_extract_recurring: {src_url} → {len(result.events)} "
                    f"events, saved={saved}, method={result.method}"
                )

                # Free per-source memory before the next iteration.
                db.expire_all()
                gc.collect()

            if log.status != "failed":
                log.status = "success"
                log.events_found = total_events
                log.events_saved = total_saved
                log.notes = (
                    f"sources={len(sources)} events_found={total_events} "
                    f"saved={total_saved} auto_blocked={auto_blocked}"
                )
        except Exception as e:
            log.status = "failed"
            log.notes = str(e)
            logger.error(f"llm_extract_recurring error: {e}")
            db.rollback()
        finally:
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()


# Domains already covered by hand-coded collectors. Discovery skips
# candidates on these hosts so we don't pay LLM tokens to find inventory
# we already have via free / cheap paths. New collectors → add the
# host(s) here to keep discovery efficient.
_RESERVED_DISCOVERY_DOMAINS: frozenset[str] = frozenset({
    # API-based collectors
    "ticketmaster.com", "ticketmaster.co.uk", "ticketmaster.de",
    "seatgeek.com", "bandsintown.com", "predicthq.com",
    # Global web scrapers
    "eventbrite.com", "eventbrite.co.uk", "eventbrite.de",
    "lu.ma", "meetup.com", "ra.co", "dice.fm",
    "songkick.com", "skiddle.com", "xceed.me",
    "concreteplayground.com", "allevents.in", "venuepilot.com",
    # Israel-specific
    "tickchak.co.il", "leaan.co.il", "cameri.co.il",
    "barby.co.il", "smartticket.co.il", "hatarbut.org.il",
    # City-specific (where we have hand-coded support)
    "choosechicago.com",
    # Sports
    "espn.com", "mlb.com", "openf1.org", "cricapi.com",
    "euroleague.net",
})


def _is_reserved_discovery_url(url: str) -> bool:
    """True when ``url``'s host is (a subdomain of) a domain we already
    cover with a hand-coded collector. Discovery filters these out."""
    from urllib.parse import urlsplit
    try:
        host = (urlsplit(url).hostname or "").lower()
    except Exception:
        return False
    if not host:
        return False
    if host.startswith("www."):
        host = host[4:]
    if host in _RESERVED_DISCOVERY_DOMAINS:
        return True
    parts = host.split(".")
    for i in range(1, len(parts) - 1):
        if ".".join(parts[i:]) in _RESERVED_DISCOVERY_DOMAINS:
            return True
    return False


async def llm_discover_sources_job(
    candidates_per_city: int = 15,
    min_event_count_to_register: int = 3,
    max_cities_per_run: int = 10,
):
    """Cadence B of Route 1 — find new candidate event sources per city.

    For each priority city we haven't recently scanned: ask Gemini's
    grounded search for candidate event-listing URLs, probe each via the
    JSON-LD path (free), and register winners as new LLMSource rows in
    state='trial' so the recurring extraction job picks them up on its
    next cycle.

    LLM-eligible candidates (probe = no JSON-LD events but page is
    substantial) are NOT auto-registered — they need human review first
    to avoid hallucination noise. Operators add those via
    scripts/llm_run_source.py instead.

    Cost: 1 Gemini grounded call per city scanned (~$0.005 each on flash).
    With max_cities_per_run=10 and a daily cron, we make ~300 calls/month
    at ~$1.50/month — a comfortable trade for 10× faster source-inventory
    growth vs the original weekly cadence.

    The probe step is the natural hallucination guard: Gemini sometimes
    invents URLs (live.today/הופעות-היום… 404'd in early spike runs).
    Bad URLs return no events from _fetch_html → not registered.
    """
    import gc
    from sqlalchemy import or_
    from app.models import LLMSource

    if _heavy_job_lock.locked():
        logger.info("llm_discover_sources: another heavy job running — skipping")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="llm_discover_sources", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)

        try:
            from app.extractors.discovery import discover_via_gemini, DiscoveryError
            from app.extractors.llm_extractor import _fetch_html
            from app.services.collectors._jsonld import iter_events, detect_pagination

            # Pick cities longest-untouched first (or never-touched).
            # We track that via a synthetic "discovery cursor" stored as
            # the most-recent created_at on any LLMSource row scoped to
            # that (city_name, country) tuple. New cities (no rows yet)
            # come first — they need the most help.
            city_pool = list(PRIORITY_CITIES)
            city_pool.sort(key=lambda cc: _city_last_discovered_at(db, *cc) or "")
            cities_to_scan = city_pool[:max_cities_per_run]

            stats = {"scanned": 0, "registered": 0, "skipped_existing": 0,
                     "skipped_reserved": 0,
                     "no_events": 0, "fetch_errors": 0}

            for city_name, country in cities_to_scan:
                try:
                    candidates = await asyncio.to_thread(
                        discover_via_gemini, city_name, candidates_per_city,
                    )
                except DiscoveryError as e:
                    log.notes = f"discovery unavailable: {e}"
                    log.status = "failed"
                    logger.warning(f"llm_discover_sources: {e}")
                    return
                except Exception as e:
                    logger.warning(f"discover_via_gemini({city_name!r}) error: {e}")
                    candidates = []
                stats["scanned"] += 1

                for cand in candidates:
                    url = (cand.get("url") or "").strip()
                    if not url:
                        continue

                    # Skip URLs covered by hand-coded collectors — paying
                    # LLM tokens for inventory we already have is a waste.
                    if _is_reserved_discovery_url(url):
                        stats["skipped_reserved"] += 1
                        logger.info(
                            f"discovery: skipping reserved-domain {url} "
                            f"(already covered by a hand-coded collector)"
                        )
                        continue

                    # Skip URLs already in the registry — re-discovery
                    # shouldn't reset state or counters.
                    existing = db.query(LLMSource).filter(LLMSource.url == url).first()
                    if existing:
                        stats["skipped_existing"] += 1
                        continue

                    # Probe via JSON-LD: cheap, no LLM call. Only register
                    # sources that pass this bar.
                    raw_html = await asyncio.to_thread(_fetch_html, url)
                    if not raw_html:
                        stats["fetch_errors"] += 1
                        continue
                    ld_events = list(iter_events(raw_html, future_only=True))
                    if len(ld_events) < min_event_count_to_register:
                        stats["no_events"] += 1
                        continue

                    pag = detect_pagination(raw_html, base_url=url)
                    note = (
                        f"[discovered {datetime.utcnow().date()}] "
                        f"{cand.get('source_type', '?')} / "
                        f"{cand.get('language', '?')} — "
                        f"{cand.get('why_relevant', '')[:200]}"
                    )
                    new_src = LLMSource(
                        url=url,
                        city_name=city_name,
                        country=country,
                        state="trial",
                        runs_total=0,
                        events_seen_total=0,
                        events_saved_total=0,
                        has_pagination=bool(pag["has_pagination"]),
                        pagination_signal=pag["signal"],
                        next_page_url=(pag["next_page_url"] or "")[:1000] or None,
                        notes=note,
                    )
                    db.add(new_src)
                    db.commit()
                    stats["registered"] += 1
                    logger.info(
                        f"discovered: {url} ({len(ld_events)} JSON-LD events) "
                        f"→ LLMSource state=trial for {city_name}"
                    )

                # Free per-city memory before the next city's batch.
                db.expire_all()
                gc.collect()

            log.status = "success" if log.status != "failed" else log.status
            log.events_found = stats["registered"]
            log.events_saved = stats["registered"]
            log.notes = (
                f"cities={stats['scanned']} registered={stats['registered']} "
                f"skipped_reserved={stats['skipped_reserved']} "
                f"skipped_existing={stats['skipped_existing']} "
                f"no_events={stats['no_events']} fetch_errors={stats['fetch_errors']}"
            )
            logger.info(f"llm_discover_sources: {log.notes}")
        except Exception as e:
            log.status = "failed"
            log.notes = str(e)
            logger.error(f"llm_discover_sources error: {e}")
            db.rollback()
        finally:
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()


def _city_last_discovered_at(db, city_name: str, country: str):
    """Most-recent LLMSource.created_at for (city, country), or None.

    Used to schedule discovery LRU-style: cities that have never been
    scanned (or were scanned longest ago) come first. None sorts before
    any string under the heuristic ordering used in the discovery job.
    """
    from app.models import LLMSource
    row = (
        db.query(LLMSource.created_at)
        .filter(LLMSource.city_name == city_name, LLMSource.country == country)
        .order_by(LLMSource.created_at.desc())
        .first()
    )
    if not row:
        return None
    return row[0].isoformat() if row[0] else None


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
