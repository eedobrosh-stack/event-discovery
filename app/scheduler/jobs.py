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
from app.services.collectors.scrapers.allevents import AlleventsCollector
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
registry.register(AlleventsCollector())
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


async def collect_all_events():
    """Run all collectors for priority cities only to avoid memory spikes."""
    import gc
    from sqlalchemy import and_, or_
    db = SessionLocal()
    try:
        cities = db.query(City).filter(
            or_(*[
                and_(City.name == name, City.country == country)
                for name, country in PRIORITY_CITIES
            ])
        ).all()
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
                gc.collect()
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


async def enrich_youtube_job(batch: int = 300):
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
        names = [r[0] for r in artists]
        logger.info(f"enrich_youtube: {len(names)} artists to enrich")

        for artist in names:
            try:
                url = await lookup_youtube_video(artist)
                if url:
                    db.query(Event).filter(Event.artist_name == artist).update(
                        {"artist_youtube_channel": url}, synchronize_session=False
                    )
                    found += 1
                else:
                    # Write empty string so we don't retry endlessly
                    db.query(Event).filter(
                        Event.artist_name == artist,
                        Event.artist_youtube_channel.is_(None),
                    ).update({"artist_youtube_channel": ""}, synchronize_session=False)
                    failed += 1
                db.commit()
                db.expire_all()   # release session identity map after each artist
            except Exception as e:
                logger.warning(f"enrich_youtube: error for {artist!r}: {e}")
                db.rollback()
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
        # Push filtering to SQL — never load all artists or all performers into Python
        already_seen_sq = db.query(Performer.name).subquery()
        pending_rows = (
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
        pending = [r[0] for r in pending_rows]
        del pending_rows   # free immediately
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
                            skipped += 1
                        except IntegrityError:
                            db.rollback()
                    db.expire_all()   # release session identity map after each artist
                except Exception as e:
                    logger.warning(f"enrich_performers: error for {artist!r}: {e}")
                    db.rollback()
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
    """
    import asyncio
    import httpx
    from app.services.osm import overpass_discover_venues

    db = SessionLocal()
    log = ScanLog(job_name="discover_venues", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    new_venues = 0
    cities_checked = 0
    try:
        # Only run for cities that have coordinates stored
        priority_names = [name for name, _country in PRIORITY_CITIES]
        cities = (
            db.query(City)
            .filter(City.name.in_(priority_names), City.latitude.isnot(None), City.longitude.isnot(None))
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

        # Look up the "Tech Conference" event type once
        tech_et = db.query(EventType).filter_by(name="Tech Conference").first()

        for conf in conferences:
            try:
                city_name    = conf["city"] or "Online"
                country_name = conf["country"] or ("Global" if conf["is_online"] else "")
                is_online    = conf["is_online"]

                # Resolve or create city
                city = db.query(City).filter(
                    func.lower(City.name) == city_name.lower()
                ).first()
                if not city:
                    city = City(name=city_name, country=country_name)
                    db.add(city)
                    db.flush()

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
