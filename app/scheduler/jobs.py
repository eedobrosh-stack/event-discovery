import asyncio
import logging
import os
from datetime import date, timedelta, datetime

from sqlalchemy import func
from app.database import SessionLocal

# Global lock — only one heavy scraping/enrichment job runs at a time.
# This prevents two jobs from competing for the same 512 MB on Render.
_heavy_job_lock = asyncio.Lock()

# City batching: scrape CITY_BATCH_SIZE cities per run, rotating through
# PRIORITY_CITIES on each invocation. As of the 2026-05-23 Tier-2
# expansion there are ~91 cities; at 4 cities/fire and the 6h scrape
# interval the full set covers in ~6 days (longer when _heavy_job_lock
# contention drops a fire). Each city is processed in its own session
# with a gc.collect() between, so peak memory is one-city-at-a-time —
# the batch size does NOT scale memory, only fires-per-cycle.
#
# Batch size was lowered from 8→4 after repeated Render OOM kills during
# large cities like New York: the process would die mid-batch and restart,
# losing the in-memory cursor and starting over from Batch 1 every time.
# Kept at 4 through the Tier-2 expansion to stay clear of that history.
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
from app.services.collectors.scrapers.sports.tennis import TennisCollector
from app.services.collectors.scrapers.sports.diamond_league import DiamondLeagueCollector
from app.services.collectors.scrapers.sports.world_aquatics import WorldAquaticsCollector
from app.services.collectors.scrapers.sports.uci_worldtour import UCIWorldTourCollector
from app.services.collectors.scrapers.sports.bsl_winner import BSLWinnerLeagueCollector
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
# ATP + WTA tennis tournaments (ESPN scoreboard, no key)
registry.register(TennisCollector())
# Wanda Diamond League athletics (per-meeting subdomain pages, no key)
registry.register(DiamondLeagueCollector())
# World Aquatics / FINA — swimming, diving, water polo, etc. (public API)
registry.register(WorldAquaticsCollector())
# UCI WorldTour road cycling (Wikipedia season table)
registry.register(UCIWorldTourCollector())
# Israeli Basketball Premier League (ליגת Winner / BSL) — basket.co.il
registry.register(BSLWinnerLeagueCollector())
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
    ("Jerusalem",      "Israel"),
    ("Haifa",          "Israel"),
    ("Eilat",          "Israel"),
    # ── Tier-2 expansion (2026-05-23) ──────────────────────────────────
    # 58 cities verified against the live cities table (exact name +
    # country spelling). Most are already in the hardcoded allowlists of
    # the city-based collectors (AllEvents, Eventbrite, Songkick, Meetup,
    # Luma, ResidentAdvisor, Skiddle, Xceed) — so adding them here
    # activates 6+ collectors per city with no collector-code change.
    # Goal: grow the future-events pool toward 1M. Duplicate-spelling
    # rows (Montreal/Montréal, Zurich/Zürich, etc.) are both included on
    # purpose — each spelling fires its own collectors and events dedup
    # at the (scrape_source, source_id) index, so no double-counting.
    # ── United States ──────────────────────────────────────────────────
    ("Miami",          "United States"),
    ("Austin",         "United States"),
    ("Seattle",        "United States"),
    ("Boston",         "United States"),
    ("Nashville",      "United States"),
    ("Denver",         "United States"),
    ("Atlanta",        "United States"),
    ("Philadelphia",   "United States"),
    ("Portland",       "United States"),
    ("Las Vegas",      "United States"),
    ("Houston",        "United States"),
    ("Dallas",         "United States"),
    ("Phoenix",        "United States"),
    ("Washington",     "United States"),
    ("San Diego",      "United States"),
    ("New Orleans",    "United States"),
    # ── Canada ─────────────────────────────────────────────────────────
    ("Montreal",       "Canada"),
    ("Montréal",       "Canada"),
    # ── United Kingdom ─────────────────────────────────────────────────
    ("Birmingham",     "United Kingdom"),
    ("Glasgow",        "United Kingdom"),
    ("Bristol",        "United Kingdom"),
    ("Leeds",          "United Kingdom"),
    ("Liverpool",      "United Kingdom"),
    ("Belfast",        "United Kingdom"),
    # ── Ireland ────────────────────────────────────────────────────────
    ("Dublin",         "Ireland"),
    ("Cork",           "Ireland"),
    # ── Continental Europe ─────────────────────────────────────────────
    ("Vienna",         "Austria"),
    ("Prague",         "Czech Republic"),
    ("Prague",         "Czechia"),
    ("Budapest",       "Hungary"),
    ("Zurich",         "Switzerland"),
    ("Zürich",         "Switzerland"),
    ("Copenhagen",     "Denmark"),
    ("Stockholm",      "Sweden"),
    ("Oslo",           "Norway"),
    ("Helsinki",       "Finland"),
    ("Warsaw",         "Poland"),
    ("Hamburg",        "Germany"),
    ("Cologne",        "Germany"),
    ("Köln",           "Germany"),
    ("Frankfurt",      "Germany"),
    # ── Asia / Middle East / Pacific ───────────────────────────────────
    ("Tokyo",          "Japan"),
    ("Seoul",          "South Korea"),
    ("Singapore",      "Singapore"),
    ("Bangkok",        "Thailand"),
    ("Dubai",          "United Arab Emirates"),
    ("Auckland",       "New Zealand"),
    ("Wellington",     "New Zealand"),
    # ── Spain (Xceed coverage) ─────────────────────────────────────────
    ("Valencia",       "Spain"),
    ("Seville",        "Spain"),
    ("Sevilla",        "Spain"),
    ("Bilbao",         "Spain"),
    ("Málaga",         "Spain"),
    ("Ibiza",          "Spain"),
    ("Alicante",       "Spain"),
    # ── Italy / Portugal (Xceed coverage) ──────────────────────────────
    ("Turin",          "Italy"),
    ("Torino",         "Italy"),
    ("Porto",          "Portugal"),
]


# Discovery bias — cities to push to the front of the LRU rotation
# until they've been "saturated" (no new event-listing aggregators
# found across consecutive runs). Cadence B's per-fire cap of 10
# cities is unchanged; biased cities take the first slots, the
# remaining slots fall through to normal LRU.
#
# Curated by hand — when a biased city stops yielding new
# registrations across 2-3 consecutive runs, drop it from this set
# (no auto-detection: the cost of a wrong detection is bigger than
# the cost of one extra fire). Smaller sets keep the bias focused.
#
# Current focus (set 2026-05-10): Israel — easier to ground-truth
# against real-life venues for a Tel Aviv-based operator.
DISCOVERY_BIAS_CITIES: set[tuple[str, str]] = {
    ("Tel Aviv",   "Israel"),
    ("Jerusalem",  "Israel"),
    ("Haifa",      "Israel"),
    ("Eilat",      "Israel"),
}


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


async def collect_bandsintown_job(batch: int = 300):
    """
    Artist-centric Bandsintown scan — queries the top `batch` performers
    by event count and saves any upcoming events returned by the API.
    Runs every 8 hours; the offset advances after 2 fires at any given
    band so we sweep through the full performer pool (~13.8K) in ~15
    days rather than the prior ~46-day cycle.
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
        # Pool rotation: each offset band saturates on dedupe within a
        # couple of fires (the first fire at a fresh offset typically
        # finds ~25% new, the second <1%). Rotate after 2 fires at the
        # same offset regardless of save rate — the prior "wait for 3
        # consecutive <1% fires" rule wasted ~3-4 fires per band, so
        # the pool advanced only ~7 bands per month (1,050 of 13.8K
        # performers). The offset is persisted in the previous run's
        # `notes` string ("offset=N") — no new state table needed.
        import re as _re
        prev_logs = (
            db.query(ScanLog)
            .filter(ScanLog.job_name == "bandsintown", ScanLog.status == "success")
            .order_by(ScanLog.id.desc())
            .limit(3)
            .all()
        )
        prev_offset = 0
        if prev_logs:
            m = _re.search(r"offset=(\d+)", prev_logs[0].notes or "")
            if m:
                prev_offset = int(m.group(1))
        fires_at_current = sum(
            1 for l in prev_logs
            if l.notes and f"offset={prev_offset}" in l.notes
        )
        should_rotate = fires_at_current >= 2
        total_perf = db.query(Performer).count()
        offset = (
            (prev_offset + batch) % max(total_perf, 1)
            if should_rotate else prev_offset
        )
        if should_rotate:
            logger.info(
                f"collect_bandsintown_job: 2 fires at offset={prev_offset} "
                f"— rotating to offset {offset}"
            )

        # Performers by event count, starting at the rotation offset
        rows = (
            db.query(Performer.name, func.count(Event.id).label("n"))
            .outerjoin(Event, func.lower(Event.artist_name) == func.lower(Performer.name))
            .group_by(Performer.id, Performer.name)
            .order_by(func.count(Event.id).desc())
            .offset(offset)
            .limit(batch)
            .all()
        )
        artist_names = [r[0] for r in rows]
        logger.info(f"collect_bandsintown_job: scanning {len(artist_names)} artists (offset={offset})")

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
        log.notes = f"artists={len(artist_names)} found={found} saved={saved} offset={offset}"
        logger.info(f"collect_bandsintown_job done: found={found} saved={saved} offset={offset}")
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


# enrich_spotify_job removed 2026-05-25 — Spotify gutted popularity/genres
# /followers in late 2024 so the lookup had been writing popularity=0 +
# genres=[] for every row. The frontend never rendered the fields anyway
# (no references in app.js/home.js). Superseded by spotify_scan_job +
# spotify_brave_query_job below.


# ===========================================================================
# Spotify funnel — Phase 2 (scan) and Phase 3 (Brave A/B + LLMSource register)
# ===========================================================================
# Goal: use Spotify's editorial curation as a source of "artists who matter"
# (anybody-who's-anybody = anybody with a Spotify artist page who's surfaced
# on a chart, featured playlist, new release, or browse-category curation).
# Two-step pipeline:
#
#   spotify_scan_job (daily, +300min from boot)
#     Rotate through 10 Spotify markets/day (full ~75-market pass per week).
#     Walk Featured Playlists + Top 50 / Viral 50 + New Releases per market,
#     plus a global Browse Categories pass. Upsert into spotify_artists. For
#     each new row, match by lower(name) against Performer.normalized_name:
#         matched         → coverage; nothing else to do.
#         unmatched       → match_status='pending_brave', queued for phase 3.
#     Daily roll-up written as a ScanLog row with structured notes.
#
#   spotify_brave_query_job (every 2h, batch=25 from pending_brave queue)
#     For each artist, run the A/B query pair until each variant has
#     ≥AB_TRIAL_THRESHOLD attempts globally; thereafter only the winner.
#     For each Brave result URL: domain-dedupe against existing llm_sources,
#     classify-survivors via the artist-tour-page Gemini filter, and
#     register passers as LLMSource(state='trial',
#     discovered_via='spotify_artist_query', spotify_artist_id=…). Cadence A
#     picks them up on its next tick — funnel closes through Event.llm_source_id.

# Last.fm chart countries. Last.fm's geo.gettopartists takes the English
# name (NOT an ISO code), so we hardcode names. Covers the same markets
# the Spotify-era design used, plus a few extras where Last.fm data is
# strong (Russia, Ukraine, Czechia under that spelling). Order is rough
# data-volume weight so the LRU rotation hits the heaviest charts first.
_LASTFM_COUNTRIES: tuple[str, ...] = (
    "United States", "United Kingdom", "Germany", "France", "Italy",
    "Spain", "Brazil", "Mexico", "Canada", "Australia",
    "Japan", "Korea, Republic of", "Netherlands", "Sweden", "Norway",
    "Denmark", "Finland", "Poland", "Ireland", "Portugal",
    "Belgium", "Austria", "Switzerland", "Greece", "Czech Republic",
    "Hungary", "Romania", "Turkey", "Israel", "United Arab Emirates",
    "Saudi Arabia", "Egypt", "South Africa", "Nigeria", "Kenya",
    "Morocco", "Argentina", "Chile", "Colombia", "Peru",
    "Uruguay", "Venezuela", "Dominican Republic", "Guatemala", "Costa Rica",
    "Panama", "Ecuador", "Bolivia", "Paraguay", "India",
    "Indonesia", "Malaysia", "Philippines", "Singapore", "Thailand",
    "Vietnam", "Taiwan", "Hong Kong", "New Zealand", "Bulgaria",
    "Croatia", "Estonia", "Latvia", "Lithuania", "Slovakia",
    "Slovenia", "Iceland", "Luxembourg", "Malta", "Cyprus",
    "Jordan", "Kuwait", "Qatar", "Oman", "Bahrain",
)

# How many countries we visit per daily run. 10/day × 7-8 days ≈ a full
# rotation. Each call is one HTTP request to Last.fm; we also fire one
# global call per run regardless. Total ~11 Last.fm calls/day.
SPOTIFY_MARKETS_PER_RUN = 10

# A/B test: switch to winner-only after each variant has this many
# trials. Matches the spec ("after 100 attempts, keep the stronger").
AB_TRIAL_THRESHOLD = 100
# Max pending_brave artists processed per scheduler tick. Each costs
# 1-2 Brave queries (2 during A/B, 1 after) so batch=25 → ≤50
# Brave calls / 2h ≈ 600/day in the worst case, similar to the
# existing enrich_youtube_via_brave budget.
SPOTIFY_BRAVE_BATCH = 25
# Hits per Brave query to consider. Brave's free tier supports up
# to 20 per call but we only need the top ~10 — long-tail results
# rarely classify as event-listing pages.
SPOTIFY_BRAVE_HITS_PER_QUERY = 10


def _spotify_market_cursor(db: "Session") -> tuple[list[str], int]:
    """Return (countries-to-walk-this-run, next-cursor).

    Uses JobState['spotify_market_cursor'] as the LRU pointer. Wraps
    naturally at the end of _LASTFM_COUNTRIES. Key name kept as
    'spotify_market_cursor' for continuity with the prior design — the
    cursor value advances over the new Last.fm country list now, and
    re-using the key avoids a JobState row migration.
    """
    from app.models import JobState
    key = "spotify_market_cursor"
    state = db.query(JobState).filter(JobState.key == key).first()
    cursor = int(state.value) if state and state.value.isdigit() else 0
    n = len(_LASTFM_COUNTRIES)
    cursor = cursor % n
    picks = [_LASTFM_COUNTRIES[(cursor + i) % n] for i in range(SPOTIFY_MARKETS_PER_RUN)]
    next_cursor = (cursor + SPOTIFY_MARKETS_PER_RUN) % n
    if state:
        state.value = str(next_cursor)
    else:
        db.add(JobState(key=key, value=str(next_cursor)))
    return picks, next_cursor


async def spotify_scan_job():
    """Walk Last.fm charts (global + SPOTIFY_MARKETS_PER_RUN rotating
    countries) and upsert every artist into spotify_artists.

    Why Last.fm and not Spotify: Spotify deprecated featured-playlists,
    new-releases, categories, and even editorial-playlist track reads
    for Client Credentials apps in late 2024 — first prod run returned
    0 artists because every endpoint we hit responded 403. Last.fm's
    chart API is free, unauthenticated (api key only), and effectively
    measures the same thing the Spotify charts would have (Spotify
    listening feeds Last.fm scrobbles, so its global chart IS the
    cross-platform popularity ranking).

    PK strategy: prefer MusicBrainz ID (from Last.fm response), fall
    back to SHA1(lowercase name) — both fit in the existing
    SpotifyArtist.id String(40) column. See _normalize_artist_id in
    lastfm_chart.py for details.

    Match against Performer.normalized_name on first encounter:
        matched         → coverage; nothing else to do.
        pending_brave   → queued for spotify_brave_query_job.

    Writes a ScanLog row with structured notes consumed by
    /api/stats/spotify (json:// prefix triggers the dashboard parser).
    """
    import httpx as _httpx
    import json as _json
    from app.models import SpotifyArtist, Performer
    from app.services.lastfm_chart import fetch_global_top, fetch_country_top

    if not settings.LASTFM_API_KEY:
        logger.info("spotify_scan_job: LASTFM_API_KEY not set — skipping")
        return

    if _heavy_job_lock.locked():
        logger.info("spotify_scan_job: another heavy job is running — skipping this run")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="spotify_scan", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)
        artists_seen = 0
        artists_new = 0
        artists_matched = 0
        artists_unmatched = 0
        markets_walked: list[str] = []

        # Pull the country batch + advance cursor in one transaction so
        # a mid-run crash doesn't double-walk on next fire.
        try:
            picks, _next = _spotify_market_cursor(db)
            db.commit()
        except Exception as e:
            logger.error(f"spotify_scan_job: cursor advance failed: {e}")
            log.status = "failed"
            log.notes = f"cursor_advance_failed: {e}"
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()
            return

        logger.info(f"spotify_scan_job: walking countries {picks} + global")

        # Aggregate artists across global + per-country charts. Single
        # upsert pass commits at the end.
        combined: dict[str, dict] = {}

        try:
            async with _httpx.AsyncClient(timeout=20) as http:
                # Global chart first — heaviest single signal.
                glob = await fetch_global_top(http, settings.LASTFM_API_KEY)
                for aid, meta in glob.items():
                    combined[aid] = {
                        "name": meta["name"],
                        "external_url": meta.get("external_url"),
                        "markets": set(meta.get("markets") or set()),
                    }
                if glob:
                    markets_walked.append("global")
                # Then per-country rotation.
                for country in picks:
                    per = await fetch_country_top(http, settings.LASTFM_API_KEY, country)
                    if per:
                        markets_walked.append(country)
                    for aid, meta in per.items():
                        slot = combined.setdefault(
                            aid,
                            {"name": meta["name"], "external_url": meta.get("external_url"), "markets": set()},
                        )
                        slot["markets"] |= meta.get("markets") or set()
        except Exception as e:
            logger.exception(f"spotify_scan_job: walk failed: {e}")
            log.status = "failed"
            log.notes = f"walk_failed: {type(e).__name__}: {e}"[:255]
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()
            return

        artists_seen = len(combined)
        logger.info(f"spotify_scan_job: combined {artists_seen} unique Spotify artists")

        # ── Upsert + match in batches. We commit per N to keep the
        # identity map small and bound rollback cost on errors.
        try:
            now = datetime.utcnow()
            BATCH = 200
            ids = list(combined.keys())
            for i in range(0, len(ids), BATCH):
                chunk_ids = ids[i:i + BATCH]
                existing_rows = {
                    r.id: r for r in db.query(SpotifyArtist)
                    .filter(SpotifyArtist.id.in_(chunk_ids)).all()
                }
                # Bulk performer lookup for unmatched candidates only —
                # existing SpotifyArtist rows already know their match
                # status, no need to re-check.
                fresh_ids = [a for a in chunk_ids if a not in existing_rows]
                names_lower = {
                    a: combined[a]["name"].strip().lower()
                    for a in fresh_ids
                    if combined[a]["name"]
                }
                perf_hits: dict[str, int] = {}
                if names_lower:
                    rows = (
                        db.query(Performer.id, Performer.normalized_name)
                        .filter(Performer.normalized_name.in_(list(set(names_lower.values()))))
                        .all()
                    )
                    norm_to_pid = {nn: pid for pid, nn in rows}
                    for aid, lname in names_lower.items():
                        if lname in norm_to_pid:
                            perf_hits[aid] = norm_to_pid[lname]

                for aid in chunk_ids:
                    meta = combined[aid]
                    markets_str = ",".join(sorted(meta["markets"])) if meta["markets"] else None
                    if aid in existing_rows:
                        row = existing_rows[aid]
                        row.last_seen_at = now
                        # Union of markets_surfaced_in, capped at 4K chars
                        # to avoid runaway growth on cumulative scans.
                        prev_markets = set((row.markets_surfaced_in or "").split(",")) if row.markets_surfaced_in else set()
                        prev_markets.discard("")
                        merged = sorted(prev_markets | meta["markets"])
                        row.markets_surfaced_in = ",".join(merged)[:4000]
                    else:
                        artists_new += 1
                        matched_pid = perf_hits.get(aid)
                        if matched_pid:
                            artists_matched += 1
                            status = "matched"
                        else:
                            artists_unmatched += 1
                            status = "pending_brave"
                        db.add(SpotifyArtist(
                            id=aid,
                            name=meta["name"][:500],
                            external_url=meta["external_url"],
                            first_seen_at=now,
                            last_seen_at=now,
                            match_status=status,
                            matched_performer_id=matched_pid,
                            markets_surfaced_in=markets_str,
                        ))
                db.commit()
        except Exception as e:
            logger.exception(f"spotify_scan_job: upsert failed: {e}")
            db.rollback()
            log.status = "failed"
            log.notes = f"upsert_failed: {type(e).__name__}: {e}"[:255]
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()
            return

        # Coverage % for the new-this-run cohort. Cumulative coverage
        # lives on the SpotifyArtist table and the stats endpoint
        # computes it on read.
        coverage_pct = (
            round(100 * artists_matched / (artists_matched + artists_unmatched), 1)
            if (artists_matched + artists_unmatched) else 0.0
        )

        log.status = "success"
        log.events_found = artists_seen
        log.events_saved = artists_new
        # Structured-but-parseable notes — the stats endpoint json-decodes
        # the json://… prefix slice. Plain k=v format kept for parity with
        # other jobs that don't need structure.
        log.notes = "json://" + _json.dumps({
            "markets": markets_walked,
            "artists_seen": artists_seen,
            "artists_new": artists_new,
            "artists_matched": artists_matched,
            "artists_unmatched": artists_unmatched,
            "coverage_pct_new_cohort": coverage_pct,
        })
        log.finished_at = datetime.utcnow()
        db.commit()
        logger.info(
            f"spotify_scan_job done: seen={artists_seen} new={artists_new} "
            f"matched={artists_matched} unmatched={artists_unmatched} "
            f"coverage_new={coverage_pct}%"
        )
        db.close()


# Cache the current A/B winner across job runs so the brave loop doesn't
# requery the DB for it on every artist. Reset on process restart.
_AB_WINNER_CACHE: dict = {"winner": None, "computed_at": None}


def _compute_ab_winner(db: "Session") -> str | None:
    """Return the winning query variant once both have ≥AB_TRIAL_THRESHOLD
    attempts, else None.

    Winner = variant with the higher mean new_llm_sources_registered.
    """
    from app.models import SpotifyBraveAttempt
    from sqlalchemy import func as _f
    rows = (
        db.query(
            SpotifyBraveAttempt.query_variant,
            _f.count(SpotifyBraveAttempt.id).label("n"),
            _f.coalesce(_f.sum(SpotifyBraveAttempt.new_llm_sources_registered), 0).label("total"),
        )
        .group_by(SpotifyBraveAttempt.query_variant)
        .all()
    )
    counts = {r.query_variant: (r.n, r.total) for r in rows}
    a = counts.get("shows", (0, 0))
    b = counts.get("upcoming_performances", (0, 0))
    if a[0] < AB_TRIAL_THRESHOLD or b[0] < AB_TRIAL_THRESHOLD:
        return None
    avg_a = a[1] / a[0]
    avg_b = b[1] / b[0]
    if avg_a >= avg_b:
        return "shows"
    return "upcoming_performances"


async def spotify_brave_query_job(batch: int = SPOTIFY_BRAVE_BATCH):
    """For each pending_brave SpotifyArtist, fire Brave query (or both
    during A/B) and register new LLMSources.

    Until both query variants have ≥AB_TRIAL_THRESHOLD attempts, runs
    BOTH variants on every artist. Thereafter runs only the winner —
    determined by mean new_llm_sources_registered per variant.

    Each Brave result is:
      1. Reserved-domain filtered (same list Cadence B uses).
      2. Existing-LLMSource filtered (dedup on registered_domain).
      3. Gemini-classified as an event-listing page.
      4. Registered as LLMSource(state='trial', discovered_via=
         'spotify_artist_query', spotify_artist_id=…) on accept.

    Cadence A then picks the new sources up on its next tick.
    """
    import gc
    from app.models import SpotifyArtist, SpotifyBraveAttempt, LLMSource
    from app.extractors.discovery_search import (
        brave_search, filter_artist_tour_pages_via_llm, SearchHit,
    )

    if _heavy_job_lock.locked():
        logger.info("spotify_brave_query_job: another heavy job is running — skipping")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="spotify_brave_query", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)

        try:
            # Refresh the A/B winner cache on each run — cheap query,
            # avoids stale-winner-after-restart corner case.
            winner = _compute_ab_winner(db)
            _AB_WINNER_CACHE["winner"] = winner
            _AB_WINNER_CACHE["computed_at"] = datetime.utcnow()
            logger.info(
                f"spotify_brave_query_job: A/B winner={winner!r}; "
                f"{'winner-only' if winner else 'both variants (A/B in progress)'}"
            )

            # Pick batch from the pending_brave queue, oldest-first
            # so artists don't get stuck waiting forever.
            artists = (
                db.query(SpotifyArtist)
                .filter(SpotifyArtist.match_status == "pending_brave")
                .order_by(SpotifyArtist.first_seen_at.asc())
                .limit(batch)
                .all()
            )
            if not artists:
                log.status = "success"
                log.notes = "no pending artists"
                log.finished_at = datetime.utcnow()
                db.commit()
                db.close()
                logger.info("spotify_brave_query_job: nothing to do")
                return

            # Build the existing-LLMSource domain set once per run.
            existing_domains: set[str] = set()
            for (url,) in db.query(LLMSource.url).all():
                existing_domains.add(_registered_domain(url))

            variants_to_run = (
                ["shows", "upcoming_performances"] if not winner else [winner]
            )

            total_attempts = 0
            total_new_sources = 0
            artists_processed = 0

            for artist in artists:
                # Each variant produces one SpotifyBraveAttempt row.
                attempts_this_artist = 0
                new_sources_this_artist = 0
                for variant in variants_to_run:
                    query = (
                        f"{artist.name} shows"
                        if variant == "shows"
                        else f"{artist.name} upcoming performances"
                    )
                    try:
                        hits: list[SearchHit] = await asyncio.to_thread(
                            brave_search, query, SPOTIFY_BRAVE_HITS_PER_QUERY
                        )
                    except Exception as e:
                        logger.warning(
                            f"spotify_brave_query: brave_search failed for "
                            f"{artist.name!r} ({variant!r}): {e}"
                        )
                        hits = []
                    attempts_this_artist += 1

                    # Filter out reserved domains and already-known
                    # LLMSource domains BEFORE classifier — saves
                    # Gemini calls.
                    novel: list[SearchHit] = []
                    for h in hits:
                        if _is_reserved_discovery_url(h.url):
                            continue
                        dom = _registered_domain(h.url)
                        if not dom or dom in existing_domains:
                            continue
                        novel.append(h)

                    new_sources_this_variant = 0
                    if novel:
                        try:
                            candidates = await asyncio.to_thread(
                                filter_artist_tour_pages_via_llm,
                                novel,
                                artist.name,
                            )
                        except Exception as e:
                            logger.warning(
                                f"spotify_brave_query: classifier failed for "
                                f"{artist.name!r}: {e}"
                            )
                            candidates = []

                        for cand in candidates:
                            url = cand.get("url")
                            if not url:
                                continue
                            dom = _registered_domain(url)
                            if not dom or dom in existing_domains:
                                continue
                            existing_domains.add(dom)
                            note = (
                                f"[spotify_artist_query {datetime.utcnow().date()}] "
                                f"artist={artist.name!r} variant={variant!r} "
                                f"why={(cand.get('why_relevant') or '')[:120]}"
                            )
                            db.add(LLMSource(
                                url=url,
                                state="trial",
                                runs_total=0,
                                events_seen_total=0,
                                events_saved_total=0,
                                notes=note,
                                discovered_via="spotify_artist_query",
                                spotify_artist_id=artist.id,
                            ))
                            new_sources_this_variant += 1

                    # Record the attempt — even on zero-hit so the A/B
                    # mean is computed on the full denominator.
                    db.add(SpotifyBraveAttempt(
                        spotify_artist_id=artist.id,
                        query_variant=variant,
                        attempted_at=datetime.utcnow(),
                        brave_results_count=len(hits),
                        new_llm_sources_registered=new_sources_this_variant,
                    ))
                    new_sources_this_artist += new_sources_this_variant
                    db.commit()

                artist.brave_attempt_count = (artist.brave_attempt_count or 0) + attempts_this_artist
                artist.new_websites_found = (artist.new_websites_found or 0) + new_sources_this_artist
                artist.match_status = "brave_done"
                db.commit()

                artists_processed += 1
                total_attempts += attempts_this_artist
                total_new_sources += new_sources_this_artist
                # Re-check A/B winner if we're still in the trial phase —
                # could flip mid-batch as the 100th attempt of either
                # variant lands.
                if not winner:
                    winner = _compute_ab_winner(db)
                    if winner:
                        variants_to_run = [winner]
                        _AB_WINNER_CACHE["winner"] = winner
                        logger.info(
                            f"spotify_brave_query_job: A/B winner determined "
                            f"mid-run: {winner!r} — switching to winner-only"
                        )

                gc.collect()

            log.status = "success"
            log.events_found = total_attempts
            log.events_saved = total_new_sources
            log.notes = (
                f"artists={artists_processed} attempts={total_attempts} "
                f"new_sources={total_new_sources} winner={winner or 'tbd'}"
            )
            log.finished_at = datetime.utcnow()
            db.commit()
            logger.info(
                f"spotify_brave_query_job done: artists={artists_processed} "
                f"attempts={total_attempts} new_sources={total_new_sources} "
                f"winner={winner or 'tbd'}"
            )
        except Exception as e:
            logger.exception(f"spotify_brave_query_job failed: {e}")
            db.rollback()
            log.status = "failed"
            log.notes = f"error: {type(e).__name__}: {e}"[:255]
            log.finished_at = datetime.utcnow()
            db.commit()
        finally:
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
                        # Backfill artist_name on every refresh — older
                        # rows ingested before the mevalim collector
                        # populated this field will gradually get fixed
                        # as the daily job re-touches them, with no
                        # separate migration needed.
                        if raw.artist_name and not existing.artist_name:
                            existing.artist_name = raw.artist_name
                        updated += 1
                        continue

                    new_ev = Event(
                        name=raw.name,
                        artist_name=raw.artist_name,
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
    max_sources_per_run: int = 150,
    min_hours_since_last: int = 6,
    auto_block_threshold: int = 3,
    auto_promote_threshold: int = 1,
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

            for _src_idx, src in enumerate(sources, start=1):
                # If the extractor was unconfigured on a previous source,
                # skip the rest — they'd all fail the same way.
                if extractor_unavailable:
                    break

                src_url = src.url

                # Real-time progress signal — write to scan_logs.detail
                # *before* the fetch so admin LLM Pipeline tab shows
                # "currently scanning: X" while the slow work happens.
                # No commit here — the next per-source commit below
                # flushes it.
                log.detail = (
                    f"{_src_idx}/{len(sources)} fetching: "
                    + (src_url or "")[:120]
                )
                db.commit()

                # Defense-in-depth reserved-domain guard. Discovery
                # (Cadence B + seed_brave_from_zero_results) is
                # supposed to filter these out before they reach
                # LLMSource, but legacy rows or future bypasses
                # shouldn't waste a fetch + Gemini call. Auto-block
                # the source so it never gets picked up again.
                if _is_reserved_discovery_url(src_url):
                    if src.state != "blocked":
                        src.state = "blocked"
                        src.notes = (
                            (src.notes or "")
                            + f"\n[auto-blocked {datetime.utcnow().date()}] "
                            + "reserved-domain — covered by hand-coded collector"
                        ).strip()
                        db.commit()
                        auto_blocked += 1
                        logger.info(
                            f"llm_extract_recurring: reserved-domain skip "
                            f"+ auto-block: {src_url}"
                        )
                    continue

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
                            # Pass src.id so every Event row knows which
                            # LLMSource it came from — closes the funnel
                            # SpotifyArtist → LLMSource → Event → Performer
                            # used by the Spotify stats endpoint.
                            saved = registry._save_events(
                                result.events, city, db,
                                llm_source_id=src.id,
                            )
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

                # Roll running totals into the scan_log so the admin
                # LLM Pipeline tab can show live progress without
                # waiting for the whole 150-source fire to finish.
                # auto_blocked count is included in the detail so a
                # spike in blocks is visible immediately.
                log.events_found = total_events
                log.events_saved = total_saved
                log.detail = (
                    f"{_src_idx}/{len(sources)} done — "
                    f"found {total_events}, saved {total_saved}, "
                    f"blocked {auto_blocked}"
                )
                db.commit()

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
#
# 2026-05-18: conference-aggregator hosts were briefly added here
# (cbe9f19) then reverted (45e9f15) — the policy choice is to rely on
# the (a) Cadence-B query taxonomy rebalance (vertical_taxonomy.py,
# 2026-05-18) to lower conference-query share from 55% → 22%, and (b)
# existing drift / consecutive-empty-runs auto-block to handle junky
# sources organically. A blunt domain block was over-rotation; "focus
# more on events" doesn't require eliminating conferences.
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


def _registered_domain(url: str) -> str:
    """Bare hostname with leading 'www.' stripped, lowercased. Empty
    string when ``url`` doesn't parse to a netloc.

    Limitation: doesn't deconstruct subdomains, so ``news.timeout.com``
    and ``timeout.com`` are treated as separate domains. The discovery
    same-domain gate accepts that — we'd rather over-fetch a subdomain
    than miss a genuinely-different event-listing site that happens to
    sit on a public hosting provider's bare domain (e.g.
    ``something.eventbrite.com``)."""
    from urllib.parse import urlparse
    try:
        host = (urlparse(url).netloc or "").lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


# LLMSource states that count as "this domain has proven itself" for
# the same-domain gate. Once a domain has any row in one of these
# states, the gate stops blocking new URLs on that domain — fresh
# pages from a known-good source are welcome.
_DOMAIN_VERIFIED_STATES = frozenset({"recurring", "graduated"})


# ── seed_brave_from_zero_results ─────────────────────────────────────
# When a user search returns 0 events globally (even after the
# lookahead date-extend retry), /api/events/zero-result logs the
# query into zero_result_searches. This job feeds those dead-end
# queries into Brave Search as "{text} performances" / "{text}
# events 2026" / "{text} schedule", then registers the top results
# as LLMSource trial rows. Cadence A's auto-promote (=1 success) and
# auto-block (=3 empties) self-clean any noise — no LLM classifier
# needed at registration time.

# Pure-noise patterns we never want to feed back to Brave. Tournament
# bracket placeholders ("Semifinal 1 Winner", "Round of 32: 1A vs.
# TBD", "Group D Runners-up") sneak into the AC index from
# Ticketmaster's WC fixtures and end up here when users click them
# expecting a real event. Filter ruthlessly.
_DEAD_END_NOISE_PATTERNS = [
    "semifinal", "quarterfinal", "round of",
    "group winner", "group 2nd", "group runner", "runners-up", "runners up",
    "tbd", "tba", "winner", "third place",
]


def _is_query_meaningful_for_brave(q):
    """Cheap noise filter for dead-end queries before we spend Brave
    credits on them. Rejects empty, too-short, or tournament-placeholder
    strings that wouldn't yield useful discovery."""
    if not q:
        return False
    s = q.strip()
    if len(s) < 3:
        return False
    sl = s.lower()
    # Reject if the query is dominated by bracket-placeholder language.
    # We use 'in' rather than 'startswith' so "X v Group A Winner" also
    # gets filtered — the noise is the bracket vocab, wherever it sits.
    for pat in _DEAD_END_NOISE_PATTERNS:
        if pat in sl:
            return False
    return True


async def seed_brave_from_zero_results_job(
    max_queries_per_run: int = 5,
    brave_hits_per_query: int = 5,
    min_seen_count: int = 1,
):
    """Feed dead-end user searches into Brave to grow the LLM-source pool.

    Pulls zero_result_searches entries with seeded_brave_at IS NULL,
    groups by the actual query text (free_search if set, else
    type_search), counts occurrences in the last 90 days, sorts by
    count DESC + recency DESC, picks the top ``max_queries_per_run``
    after noise filtering, and emits 3 Brave variants per query:
    "{q} performances", "{q} events 2026", "{q} schedule".

    Each unique URL returned by Brave that isn't already an LLMSource
    becomes a new trial row. The matching zero_result_searches rows
    get stamped with seeded_brave_at so they aren't re-fired.

    Cost envelope (defaults):
      5 queries/fire × 3 Brave variants × 5 hits each = up to 75 hits/day
      → maybe 10-20 new LLMSource trials/day after dedup
      → ~$0.45/day in Brave (15 queries @ ~$3/1k = $0.045) — trivial.

    Cadence A's promote=1 / block=3 cycle handles trial-pool hygiene.
    """
    import gc
    from datetime import datetime
    from sqlalchemy import text
    from app.models import LLMSource, ZeroResultSearch
    from app.extractors.discovery_search import brave_search

    if _heavy_job_lock.locked():
        logger.info("seed_brave_from_zero_results: another heavy job running — skipping")
        return

    async with _heavy_job_lock:
        log = ScanLog(job_name="seed_brave_from_zero_results", status="running")
        db = SessionLocal()
        db.add(log)
        db.commit()
        stats = {"queries_processed": 0, "brave_hits": 0, "new_sources": 0, "stamped": 0}
        try:
            # Pull pending dead-ends, grouped + counted. We use the
            # raw rows query so the SAME row can be stamped after we
            # extract a query from it; an aggregate would lose the row
            # ids. Cap the raw fetch at 200 to bound work.
            pending = db.query(ZeroResultSearch).filter(
                ZeroResultSearch.seeded_brave_at.is_(None)
            ).order_by(ZeroResultSearch.timestamp.desc()).limit(200).all()

            # Group by the meaningful query text. A row contributes its
            # free_search if non-empty, else its type_search. Rows with
            # neither (genre-only or artist-chip-only dead-ends) are
            # ignored — we don't know what to ask Brave for them.
            by_query: dict[str, list[ZeroResultSearch]] = {}
            for row in pending:
                q = (row.free_search or "").strip() or (row.type_search or "").strip()
                if not _is_query_meaningful_for_brave(q):
                    continue
                by_query.setdefault(q, []).append(row)

            if not by_query:
                logger.info("seed_brave_from_zero_results: nothing pending")
                log.status = "success"
                log.finished_at = datetime.utcnow()
                log.notes = "no pending meaningful queries"
                db.commit()
                return

            # Rank: most-frequent (proxy: most-recent occurrences captured
            # in `pending`) wins. Most-recent within ties.
            ranked = sorted(
                by_query.items(),
                key=lambda kv: (-len(kv[1]), -max(r.id for r in kv[1])),
            )
            top_queries = [q for q, rows in ranked[:max_queries_per_run]
                           if len(rows) >= min_seen_count]

            logger.info(
                f"seed_brave_from_zero_results: {len(pending)} pending rows, "
                f"{len(by_query)} distinct queries, processing top {len(top_queries)}"
            )

            # Pre-load existing LLMSource URLs once to dedupe in-memory.
            existing_urls = {
                u for (u,) in db.query(LLMSource.url).all() if u
            }

            for q in top_queries:
                stats["queries_processed"] += 1
                rows_for_query = by_query[q]
                # Take city/country hint from the most recent row that
                # had one — it's the LLMSource's city_name/country
                # field, which Cadence A uses for downstream scoping.
                city_name = None
                country = None
                for r in sorted(rows_for_query, key=lambda x: x.id, reverse=True):
                    if r.city_ids and not city_name:
                        # city_ids is a comma-string of ids; first id is good enough.
                        first_id = (r.city_ids.split(",") or [""])[0].strip()
                        if first_id.isdigit():
                            row = db.execute(
                                text("SELECT name, country FROM cities WHERE id = :id"),
                                {"id": int(first_id)},
                            ).fetchone()
                            if row:
                                city_name = row[0]
                                country = row[1]
                                break
                    if r.country and not country:
                        country = r.country
                # Three query variants — different phrasing catches
                # different page shapes (artist pages, calendars,
                # season schedules).
                variants = [
                    f"{q} performances",
                    f"{q} events 2026",
                    f"{q} schedule",
                ]
                seen_for_query: set[str] = set()
                for variant in variants:
                    try:
                        hits = brave_search(variant, n=brave_hits_per_query)
                    except Exception as e:
                        logger.warning(
                            f"seed_brave_from_zero_results: brave_search({variant!r}) failed: {e}"
                        )
                        continue
                    for h in hits:
                        if not h.url or h.url in seen_for_query:
                            continue
                        seen_for_query.add(h.url)
                        stats["brave_hits"] += 1
                        if h.url in existing_urls:
                            continue
                        # Skip reserved domains — we already cover
                        # ticketmaster.com / espn.com / bandsintown.com
                        # via hand-coded collectors or API, so a Brave
                        # hit pointing at one of those URLs is pure
                        # noise (it would just race to auto-block).
                        if _is_reserved_discovery_url(h.url):
                            stats.setdefault("skipped_reserved", 0)
                            stats["skipped_reserved"] += 1
                            continue
                        existing_urls.add(h.url)
                        # Register as trial — Cadence A picks it up
                        # and either extracts events (auto-promote) or
                        # racks up 3 empties (auto-block).
                        note = (
                            f"[zero-result seed {datetime.utcnow().date()}] "
                            f"q={q!r} via {variant!r} — "
                            f"{(h.title or '')[:120]}"
                        )
                        db.add(LLMSource(
                            url=h.url,
                            city_name=city_name,
                            country=country,
                            state="trial",
                            runs_total=0,
                            events_seen_total=0,
                            events_saved_total=0,
                            notes=note,
                        ))
                        stats["new_sources"] += 1
                db.commit()

                # Stamp every zero_result_searches row that contributed
                # this query so we don't re-fire next cycle.
                now = datetime.utcnow()
                for r in rows_for_query:
                    r.seeded_brave_at = now
                stats["stamped"] += len(rows_for_query)
                db.commit()
                gc.collect()

            log.status = "success"
            log.finished_at = datetime.utcnow()
            log.events_found = stats["brave_hits"]
            log.events_saved = stats["new_sources"]
            log.notes = (
                f"queries={stats['queries_processed']} "
                f"hits={stats['brave_hits']} "
                f"new_sources={stats['new_sources']} "
                f"rows_stamped={stats['stamped']}"
            )
            db.commit()
            logger.info(f"seed_brave_from_zero_results: {log.notes}")
        except Exception as e:
            logger.exception(f"seed_brave_from_zero_results failed: {e}")
            log.status = "failed"
            log.finished_at = datetime.utcnow()
            log.notes = f"error: {type(e).__name__}: {e}"[:255]
            db.commit()
        finally:
            db.close()


def _run_vertical_geo_brave_phase(db, log, queries_per_run: int = 100,
                                  hits_per_query: int = 5):
    """Cadence-B phase 2: rotate through the vertical × geo matrix.

    Source taxonomy lives in app/extractors/vertical_taxonomy.py
    (operator-curated: 23 event categories, 28 conference verticals,
    ~46 target cities, plus distinct countries from the cities table).
    Total matrix ≈ 3,700 pairs.

    Each fire picks ``queries_per_run`` oldest-fired-or-never-fired
    pairs (NULL fired_at sorts first), runs the Brave query for each
    (e.g. "Art events in Detroit", "MarTech conferences in Norway"),
    registers up to ``hits_per_query`` hits as LLMSource trials, and
    stamps the coverage row. Reserved-domain filter applied per hit
    so already-covered hosts (ticketmaster.com, espn.com, etc.) don't
    pollute the trial pool.

    Cost envelope at the defaults:
      100 queries × ~$0.003 per Brave call ≈ $0.30/day
      Full matrix coverage: ~37 days → monthly refresh cadence.

    No LLM classifier on the hits (same design call as
    seed_brave_from_zero_results — auto-promote=1 / auto-block=3
    handles trial-pool hygiene). Returns a stats dict the caller folds
    into the parent job's ScanLog notes.
    """
    from sqlalchemy import text
    from app.models import BraveQueryCoverage, LLMSource
    from app.models.city import City  # noqa: F401 — kept for relationship resolution
    from app.extractors.vertical_taxonomy import (
        enumerate_pairs, render_query, compute_priority,
    )
    from app.extractors.discovery_search import brave_search

    stats = {
        "queries_fired": 0,
        "brave_hits": 0,
        "new_sources": 0,
        "skipped_reserved": 0,
        "skipped_existing": 0,
    }

    # Pull the geo axis from the live cities table. Cities = all
    # ~8K rows; countries = distinct country values. This is the
    # whole geography we collect events for — no curated subset.
    city_rows = db.execute(text(
        "SELECT name FROM cities WHERE name IS NOT NULL AND name != ''"
    )).fetchall()
    cities = sorted({(r[0] or "").strip() for r in city_rows if r[0]})

    country_rows = db.execute(text(
        "SELECT DISTINCT country FROM cities WHERE country IS NOT NULL AND country != ''"
    )).fetchall()
    countries = sorted({(r[0] or "").strip() for r in country_rows if r[0]})

    # Top-100 cities by event count drive Wave 1 priority. Recomputed
    # every fire because the leaderboard moves as we ingest. Empty
    # result set is fine (early-deploy state) — compute_priority
    # falls through to wave 0.
    top_city_rows = db.execute(text(
        "SELECT c.name, COUNT(e.id) AS ec "
        "FROM cities c "
        "LEFT JOIN venues v ON v.city_id = c.id "
        "LEFT JOIN events e ON e.venue_id = v.id "
        "WHERE c.name IS NOT NULL AND c.name != '' "
        "GROUP BY c.id, c.name "
        "ORDER BY ec DESC LIMIT 100"
    )).fetchall()
    top_cities: set[str] = {(r[0] or "").strip() for r in top_city_rows if r[0]}
    logger.info(
        f"vertical_geo: top_cities[0..5]="
        f"{[r[0] for r in top_city_rows[:5]]} ({len(top_cities)} total)"
    )

    # Upsert one row per pair into brave_query_coverage. With ~8K
    # cities × 51 verticals + 28 × ~50 countries + 23 × ~50 countries
    # this is ~410K rows on first build. We batch the insert and use
    # a set-based existence check so the first build is ~30s instead
    # of hours. Each row gets its computed priority on creation.
    pairs = enumerate_pairs(cities, countries)
    existing: dict[tuple[str, str, str], tuple[int, int]] = {}
    for r in db.execute(text(
        "SELECT kind, vertical, geo_name, id, priority FROM brave_query_coverage"
    )).fetchall():
        existing[(r[0], r[1], r[2])] = (r[3], r[4])

    pending_inserts = []
    priority_updates = []   # (id, new_priority) for rows whose tier shifted
    for kind, vertical, geo_type, geo_name in pairs:
        prio = compute_priority(kind, vertical, geo_name, top_cities)
        key = (kind, vertical, geo_name)
        if key in existing:
            row_id, old_prio = existing[key]
            if old_prio != prio:
                priority_updates.append({"id": row_id, "priority": prio})
            continue
        pending_inserts.append({
            "kind": kind, "vertical": vertical,
            "geo_type": geo_type, "geo_name": geo_name,
            "fired_at": None, "hits": 0, "new_sources": 0,
            "priority": prio,
        })
    if pending_inserts:
        # Batch bulk_insert_mappings at 5k/commit so a fresh prod box
        # building the full 410k-row matrix doesn't hold a single
        # write transaction for the whole job.
        CHUNK = 5000
        for i in range(0, len(pending_inserts), CHUNK):
            db.bulk_insert_mappings(
                BraveQueryCoverage, pending_inserts[i:i + CHUNK]
            )
            db.commit()
        logger.info(
            f"vertical_geo: added {len(pending_inserts)} new coverage rows "
            f"(total matrix: {len(pairs)})"
        )
    if priority_updates:
        # Top-100 leaderboard shifts → row tiers shift. Bulk-update
        # so a Wave 1→0 or vice-versa transition doesn't lag.
        CHUNK = 5000
        for i in range(0, len(priority_updates), CHUNK):
            db.bulk_update_mappings(
                BraveQueryCoverage, priority_updates[i:i + CHUNK]
            )
            db.commit()
        logger.info(
            f"vertical_geo: updated priority on {len(priority_updates)} rows"
        )

    # Pre-load existing LLMSource URLs once for dedupe in-memory —
    # same trick as seed_brave_from_zero_results.
    existing_urls = {
        u for (u,) in db.query(LLMSource.url).all() if u
    }

    # Pick the next batch — three-key ordering:
    #   1. priority DESC (Wave 1 first, then Wave 2, then long-tail)
    #   2. NULL fired_at first (never-fired before re-fires) via the
    #      CASE WHEN trick
    #   3. fired_at ASC (oldest re-fire next)
    # Cap at queries_per_run. The composite index on (priority,
    # fired_at) makes this a cheap scan.
    # The ``conference_country`` kind was retired 2026-05-18 (legacy
    # rows stay in the table but are filtered out here so they never
    # fire). See app/extractors/vertical_taxonomy.py for the rationale.
    due = db.execute(text(
        "SELECT id, kind, vertical, geo_type, geo_name FROM brave_query_coverage "
        "WHERE kind != 'conference_country' "
        "ORDER BY priority DESC, "
        "         CASE WHEN fired_at IS NULL THEN 0 ELSE 1 END ASC, "
        "         fired_at ASC "
        "LIMIT :n"
    ), {"n": queries_per_run}).fetchall()

    for row in due:
        cov_id, kind, vertical, geo_type, geo_name = row
        query = render_query(kind, vertical, geo_name)
        try:
            hits = brave_search(query, n=hits_per_query)
        except Exception as e:
            logger.warning(f"vertical_geo: brave_search({query!r}) failed: {e}")
            hits = []
        stats["queries_fired"] += 1
        stats["brave_hits"] += len(hits)

        new_sources_for_pair = 0
        for h in hits:
            if not h.url:
                continue
            if h.url in existing_urls:
                stats["skipped_existing"] += 1
                continue
            if _is_reserved_discovery_url(h.url):
                stats["skipped_reserved"] += 1
                continue
            existing_urls.add(h.url)
            note = (
                f"[vertical-geo seed {datetime.utcnow().date()}] "
                f"{kind}: {vertical} × {geo_name} via {query!r} — "
                f"{(h.title or '')[:120]}"
            )
            # city_name on LLMSource gets the geo_name if it's a
            # city, else left null (countries don't fit the column
            # semantics — the country field handles that).
            city_name = geo_name if geo_type == "city" else None
            country = geo_name if geo_type == "country" else None
            db.add(LLMSource(
                url=h.url,
                city_name=city_name,
                country=country,
                state="trial",
                runs_total=0,
                events_seen_total=0,
                events_saved_total=0,
                notes=note,
            ))
            new_sources_for_pair += 1
            stats["new_sources"] += 1

        # Stamp coverage row even when hits=0 — that signals "we did
        # try this combo, just got nothing useful". Sorts the row to
        # the back of the queue regardless of outcome.
        db.execute(text(
            "UPDATE brave_query_coverage "
            "SET fired_at = :now, hits = :h, new_sources = :ns "
            "WHERE id = :id"
        ), {
            "now": datetime.utcnow(),
            "h": len(hits),
            "ns": new_sources_for_pair,
            "id": cov_id,
        })
        db.commit()
        # Update the parent log incrementally so the LLM Pipeline tab
        # shows progress through the matrix in real time.
        log.detail = (
            f"vertical-geo {stats['queries_fired']}/{len(due)}: "
            f"hits={stats['brave_hits']} new_sources={stats['new_sources']}"
        )
        db.commit()

    logger.info(
        f"vertical_geo: fired={stats['queries_fired']} "
        f"hits={stats['brave_hits']} new_sources={stats['new_sources']} "
        f"skipped_reserved={stats['skipped_reserved']} "
        f"skipped_existing={stats['skipped_existing']}"
    )
    return stats


async def llm_discover_sources_job(
    candidates_per_city: int = 15,
    min_event_count_to_register: int = 3,
    max_cities_per_run: int = 10,
    vertical_geo_queries_per_run: int = 500,
):
    """Cadence B of Route 1 — find new candidate event sources per city.

    For each priority city we haven't recently scanned: ask Gemini's
    grounded search for candidate event-listing URLs, probe each, and
    register winners as new LLMSource rows in state='trial' so the
    recurring extraction job picks them up on its next cycle.

    Two registration paths (a candidate passes if EITHER fires):

      1. JSON-LD path — count_events(html) >= min_event_count_to_register
         (free, exact). The original cadence-B contract.
      2. Visible-content heuristic — looks_like_event_listing(html, url)
         passes. Picks up sites that publish events as visible HTML
         (JS-rendered tourism boards, Wordpress calendars) without
         schema.org markup. The LLM extractor handles actual extraction
         on first cycle; drift detection blocks any speculative
         registrations that consistently extract 0 events.

    Cost: 1 Gemini grounded call per city scanned (~$0.005 each on flash).
    With max_cities_per_run=10 and a daily cron, we make ~300 calls/month
    at ~$1.50/month — a comfortable trade for 10× faster source-inventory
    growth vs the original weekly cadence.

    Hallucination guards:
      • Gemini gets the existing-DB exclusion list in the prompt as a
        hard-negative constraint (saves candidate slots).
      • Probe step rejects URLs that don't fetch.
      • Visible-content heuristic requires real date strings on the page.
      • Drift detection prunes after the fact.
    """
    import gc
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
            from app.extractors.discovery import (
                discover_via_gemini,
                DiscoveryError,
                looks_like_event_listing,
            )
            from app.extractors.discovery_search import discover_via_search_pipeline
            from app.extractors.llm_extractor import _fetch_html
            from app.services.collectors._jsonld import iter_events, detect_pagination

            # Pick cities longest-untouched first (or never-touched).
            # We track that via a synthetic "discovery cursor" stored as
            # the most-recent created_at on any LLMSource row scoped to
            # that (city_name, country) tuple. New cities (no rows yet)
            # come first — they need the most help.
            #
            # Bias overlay: any city in DISCOVERY_BIAS_CITIES sorts ahead
            # of the rest, regardless of its LRU position. Within each
            # tier (biased / not-biased) the LRU rule still applies so
            # we don't keep hitting the same biased city run after run.
            # Cap of max_cities_per_run unchanged — biased cities take
            # the first slots, non-biased fill the remainder.
            city_pool = list(PRIORITY_CITIES)
            city_pool.sort(key=lambda cc: (
                0 if cc in DISCOVERY_BIAS_CITIES else 1,
                _city_last_discovered_at(db, *cc) or "",
            ))
            cities_to_scan = city_pool[:max_cities_per_run]
            biased_in_run = sum(1 for cc in cities_to_scan if cc in DISCOVERY_BIAS_CITIES)
            logger.info(
                f"llm_discover_sources: city pool of {len(city_pool)}, "
                f"this run scans {len(cities_to_scan)} "
                f"({biased_in_run} biased + {len(cities_to_scan) - biased_in_run} LRU)"
            )

            # Build the negative-constraint inputs for the discovery call
            # once, before the city loop. URLs already in the registry get
            # added to the exclusion list for any state — even blocked
            # sources are ones we don't want re-suggested.
            existing_urls = [u for (u,) in db.query(LLMSource.url).all() if u]
            excluded_domains = sorted(_RESERVED_DISCOVERY_DOMAINS)

            # Same-domain gate: build a {domain → set(states)} map so we
            # can skip fetching new URLs whose base domain already has
            # rows in the registry but none verified (recurring /
            # graduated). Without this, multi-city aggregator domains
            # (timeout.com, ifema.es, yesmilano.it, etc.) accumulate
            # 5–10+ trial rows in parallel before any single URL gets
            # extracted enough times to prove the domain is scrapable —
            # wasting Cadence A budget on duplicate-domain probes. The
            # set is mutated in-loop so newly-registered domains
            # immediately gate their siblings within the same run.
            domain_states: dict[str, set[str]] = {}
            for u, st in db.query(LLMSource.url, LLMSource.state).all():
                d = _registered_domain(u or "")
                if d:
                    domain_states.setdefault(d, set()).add(st or "")

            # Discovery method selection. ``search`` (default when
            # BRAVE_API_KEY is set) uses the hybrid Brave Search +
            # LLM-classifier pipeline — real indexed URLs, no hallucinations.
            # ``gemini`` uses the original grounded-search prompt that asks
            # Gemini to generate URLs from scratch.
            #
            # Resolution order:
            #   1. DISCOVERY_METHOD env var if set ('search' | 'gemini')
            #   2. 'search' if BRAVE_API_KEY is configured
            #   3. 'gemini' otherwise
            method = (os.environ.get("DISCOVERY_METHOD") or "").strip().lower()
            if method not in ("search", "gemini"):
                method = "search" if os.environ.get("BRAVE_API_KEY") else "gemini"
            logger.info(f"llm_discover_sources: discovery method = {method}")

            stats = {
                "scanned": 0,
                "registered": 0,
                "registered_jsonld": 0,
                "registered_visible": 0,
                "skipped_existing": 0,
                "skipped_reserved": 0,
                "skipped_same_domain": 0,
                "no_events": 0,
                "fetch_errors": 0,
                "method": method,
            }

            for city_name, country in cities_to_scan:
                try:
                    if method == "search":
                        candidates = await asyncio.to_thread(
                            discover_via_search_pipeline,
                            city_name,
                            candidates_per_city,
                            excluded_domains=excluded_domains,
                            excluded_urls=existing_urls,
                        )
                    else:
                        candidates = await asyncio.to_thread(
                            discover_via_gemini,
                            city_name,
                            candidates_per_city,
                            excluded_domains=excluded_domains,
                            excluded_urls=existing_urls,
                        )
                except DiscoveryError as e:
                    log.notes = f"discovery unavailable: {e}"
                    log.status = "failed"
                    logger.warning(f"llm_discover_sources: {e}")
                    return
                except Exception as e:
                    logger.warning(f"discover_{method}({city_name!r}) error: {e}")
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

                    # Same-domain gate (see domain_states block above).
                    # Skip URLs whose base domain is already represented
                    # by trial/blocked rows but has no verified
                    # (recurring/graduated) row yet. The first URL from
                    # a brand-new domain still gets through; only
                    # *additional* URLs from already-pending domains
                    # are blocked, until at least one URL on that
                    # domain proves itself.
                    domain = _registered_domain(url)
                    if domain:
                        states = domain_states.get(domain)
                        if states and not (states & _DOMAIN_VERIFIED_STATES):
                            stats["skipped_same_domain"] += 1
                            logger.info(
                                f"discovery: skipping {url} — "
                                f"domain {domain!r} has unverified "
                                f"siblings (states={sorted(states)})"
                            )
                            continue

                    # Probe — fetch is the only network call this loop
                    # makes (no LLM yet). Free fetch + cheap parsing.
                    raw_html = await asyncio.to_thread(_fetch_html, url)
                    if not raw_html:
                        stats["fetch_errors"] += 1
                        continue

                    # Path A: JSON-LD (exact, gold-standard signal)
                    ld_events = list(iter_events(raw_html, future_only=True))
                    jsonld_pass = len(ld_events) >= min_event_count_to_register

                    # Path B: visible-content heuristic (catches JS-rendered
                    # calendars and sites without schema.org markup)
                    visible_pass = False
                    visible_reason = ""
                    if not jsonld_pass:
                        visible_pass, visible_reason = looks_like_event_listing(
                            raw_html, url
                        )

                    if not (jsonld_pass or visible_pass):
                        stats["no_events"] += 1
                        continue

                    pag = detect_pagination(raw_html, base_url=url)
                    # NB: keep this variable name distinct from the outer
                    # `method` (the discovery method). Reusing the name
                    # here previously clobbered the outer scope and made
                    # subsequent cities silently fall through to the
                    # Gemini-grounded path.
                    register_via = (
                        f"jsonld ({len(ld_events)} events)" if jsonld_pass
                        else f"visible ({visible_reason})"
                    )
                    if jsonld_pass:
                        stats["registered_jsonld"] += 1
                    else:
                        stats["registered_visible"] += 1

                    note = (
                        f"[discovered {datetime.utcnow().date()}] "
                        f"{cand.get('source_type', '?')} / "
                        f"{cand.get('language', '?')} — "
                        f"{cand.get('why_relevant', '')[:160]} "
                        f"[via {register_via}]"
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
                    # Add the freshly registered URL to the in-memory
                    # exclusion list so subsequent cities in this run
                    # don't re-suggest it (rare but possible — Gemini
                    # often surfaces global aggregators across cities).
                    existing_urls.append(url)
                    # Mark this domain as pending in the gate map so
                    # any further URLs from the same domain in this
                    # run get blocked (until a future cycle promotes
                    # the source to recurring/graduated).
                    if domain:
                        domain_states.setdefault(domain, set()).add("trial")
                    logger.info(
                        f"discovered: {url} via {register_via} "
                        f"→ LLMSource state=trial for {city_name}"
                    )

                # Free per-city memory before the next city's batch.
                db.expire_all()
                gc.collect()

            # Phase 2 — vertical × geo matrix rotation. Hits the
            # operator-curated taxonomy in app/extractors/vertical_taxonomy.py
            # at the configured query budget. Wrapped in its own try
            # so a Brave outage in phase 2 doesn't undo phase 1's
            # successful registrations.
            phase2_stats = {}
            try:
                phase2_stats = _run_vertical_geo_brave_phase(
                    db, log,
                    queries_per_run=vertical_geo_queries_per_run,
                )
            except Exception as e:
                logger.exception(f"vertical_geo phase failed: {e}")

            log.status = "success" if log.status != "failed" else log.status
            log.events_found = stats["registered"] + phase2_stats.get("new_sources", 0)
            log.events_saved = log.events_found
            log.notes = (
                f"cities={stats['scanned']} registered={stats['registered']} "
                f"(jsonld={stats['registered_jsonld']}, "
                f"visible={stats['registered_visible']}) "
                f"skipped_reserved={stats['skipped_reserved']} "
                f"skipped_existing={stats['skipped_existing']} "
                f"skipped_same_domain={stats['skipped_same_domain']} "
                f"no_events={stats['no_events']} fetch_errors={stats['fetch_errors']}"
                + (f" | vertical_geo: fired={phase2_stats.get('queries_fired', 0)} "
                   f"hits={phase2_stats.get('brave_hits', 0)} "
                   f"new={phase2_stats.get('new_sources', 0)}"
                   if phase2_stats else "")
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


async def classify_new_artists_job(
    max_per_run: int = 800,
    retry_budget: int = 3,
):
    """Auto-classify newly-arrived event artists via the Brave-augmented
    Gemini classifier (Lever C).

    Cron-friendly wrapper around scripts.improve_genre_via_brave.run().
    Runs scoped to all countries — the pool query naturally narrows to
    upcoming-event artists missing a real classification, which after
    the Spotify-tag bridge (Lever A) is mostly LLM-extracted local
    acts (Hebrew, Arabic, etc.).

    Slot in the schedule: 30 minutes after Cadence A so any new artists
    introduced by tonight's LLM extractor get classified before
    tomorrow's search resolves Genre filters. Boot offset +270 (B at
    +210, A at +240, classify at +270) per app/main.py wiring.

    Cost gate: ``max_per_run`` caps the artists processed per fire,
    bounding nightly Brave + Gemini spend. Steady-state cost is
    dominated by net-new arrivals + the shrinking UNKNOWN-retry tail
    that the retry_budget gate eventually exhausts.

    Memory gate: heavy-job lock and synchronous run() means we don't
    overlap with collect_events / enrich_youtube / llm_extract.
    """
    if _heavy_job_lock.locked():
        logger.info("classify_new_artists: another heavy job is running — skipping")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="classify_new_artists", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)

        try:
            # Late import — avoids loading google-genai + Brave SDK at
            # scheduler-init time. The script function is sync, so we
            # offload to a thread.
            from scripts.improve_genre_via_brave import run as brave_classify

            stats = await asyncio.to_thread(
                brave_classify,
                country=None,
                max=max_per_run,
                retry_budget=retry_budget,
                dry_run=False,
                no_fetch=False,
            )

            log.status = "success"
            log.events_found = stats.targeted
            log.events_saved = stats.classified
            log.notes = (
                f"targeted={stats.targeted} brave_calls={stats.brave_calls} "
                f"brave_empty={stats.brave_empty} cached_hits={stats.cached_hits} "
                f"classified={stats.classified} "
                f"skipped_no_match={stats.skipped_no_match} "
                f"by_confidence={stats.by_confidence}"
            )
            logger.info(f"classify_new_artists: {log.notes}")
        except Exception as e:
            log.status = "failed"
            log.notes = str(e)
            logger.error(f"classify_new_artists error: {e}")
            db.rollback()
        finally:
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()


async def enrich_youtube_via_brave_job(max_per_run: int = 500):
    """Brave-search fallback for artist YouTube channel URLs.

    Companion to enrich_youtube_job (which uses the YouTube Data API).
    Runs daily *after* the YouTube API job has had its 4-hour cycles
    to claim what it can; whatever artists are still missing a
    channel after that get a Brave-search second pass. Hit rate
    observed at ~40% on the long-tail population (the top tier of
    well-known artists hits ~70% but those mostly clear via the
    YouTube API path first).

    Cost gate: ``max_per_run`` caps the Brave queries per fire.
    Each query is ~$0.005, so --limit=500 = ~$2.50/night. Cache at
    scripts/_brave_youtube_cache.jsonl makes the per-artist cost
    one-shot — re-runs after partial caps don't re-spend.

    Heavy-job lock as elsewhere — we don't want to overlap
    collect_events / enrich_youtube / llm_extract.
    """
    if _heavy_job_lock.locked():
        logger.info("enrich_youtube_via_brave: another heavy job is running — skipping")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="enrich_youtube_via_brave", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)

        try:
            from scripts.enrich_youtube_via_brave import run as run_brave_yt

            result = await asyncio.to_thread(
                run_brave_yt, apply=True, limit=max_per_run
            )
            stats = result.get("stats", {})

            log.status = "success"
            log.events_found = stats.get("targeted", 0)
            log.events_saved = stats.get("wrote_events", 0)
            log.notes = (
                f"targeted={stats.get('targeted', 0)} "
                f"channel_found={stats.get('channel_found', 0)} "
                f"no_match={stats.get('no_match', 0)} "
                f"brave_calls={stats.get('brave_calls', 0)} "
                f"brave_empty={stats.get('brave_empty', 0)} "
                f"cache_hits={stats.get('cache_hits', 0)} "
                f"wrote_events={stats.get('wrote_events', 0)}"
            )
            logger.info(f"enrich_youtube_via_brave: {log.notes}")
        except Exception as e:
            log.status = "failed"
            log.notes = str(e)
            logger.error(f"enrich_youtube_via_brave error: {e}")
            db.rollback()
        finally:
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()


async def recompute_popularity_job():
    """Weekly recompute of derived popularity scores + per-genre
    threshold table.

    Runs two scripts in sequence in-process (no subprocess shenanigans):
      1. scripts.recompute_popularity.run() — aggregate event signals
         + Brave footprint per artist, write Performer.derived_popularity.
      2. scripts.recompute_genre_thresholds.run() — partition the new
         scores by parent genre, write the 20/40/60/80 percentiles.

    Order matters: thresholds READ derived_popularity, so the popularity
    pass must complete first. The two are intentionally bundled into a
    single cron because thresholds without fresh scores would be stale
    by definition.

    Cadence: weekly. Popularity changes slowly (a single new tour
    rarely shifts an artist's percentile rank) and the recompute
    touches all 9.5k+ scored performers, so daily would be wasteful.
    Heavy-job lock serialises against collect_events / enrich_youtube /
    extract / discover so the wall-clock wait shouldn't matter in
    practice.
    """
    if _heavy_job_lock.locked():
        logger.info("recompute_popularity: another heavy job is running — skipping")
        return

    async with _heavy_job_lock:
        db = SessionLocal()
        log = ScanLog(job_name="recompute_popularity", status="running")
        db.add(log)
        db.commit()
        db.refresh(log)

        try:
            # Late imports — keep cold-start cheap, and avoid loading
            # statistics / dotenv at scheduler init.
            from scripts.recompute_popularity import run as run_popularity
            from scripts.recompute_genre_thresholds import run as run_thresholds

            pop_result = await asyncio.to_thread(run_popularity, apply=True)
            thr_result = await asyncio.to_thread(run_thresholds, apply=True)

            log.status = "success"
            log.events_found = pop_result.get("performers_scored", 0)
            log.events_saved = pop_result.get("performers_written", 0)
            log.notes = (
                f"performers_scored={pop_result.get('performers_scored', 0)} "
                f"distribution={pop_result.get('distribution')} "
                f"thresholds_kept={len(thr_result.get('kept', {}))} "
                f"thresholds_skipped={len(thr_result.get('skipped', {}))}"
            )
            logger.info(f"recompute_popularity: {log.notes}")
        except Exception as e:
            log.status = "failed"
            log.notes = str(e)
            logger.error(f"recompute_popularity error: {e}")
            db.rollback()
        finally:
            log.finished_at = datetime.utcnow()
            db.commit()
            db.close()


async def categorize_new_events_job(hours_back: int = 48):
    """Incremental, non-destructive event_type assignment for fresh rows.

    Why this exists
    ---------------
    Several ingestion paths write `Event` rows without populating the
    `event_event_types` m2m association:
      * `llm_extract_recurring_job` (Cadence A) — RawEvent → Event
        skips type resolution.
      * Manual one-off inserts (e.g. metopera_oneoff on 2026-05-11).
      * A few hand-coded collectors that don't run through
        `CollectorRegistry._resolve_event_type`.

    Those events render under `category=null` and are invisible to
    the `category=Music` filter — even when the artist (e.g. Sting)
    is in `performers` with `category=Music`.

    `scripts/categorize_events.py` already has the classifier logic
    (performer lookup + keyword fallback). This job calls its
    `run_incremental` entry point which:
      * Only touches events created in the last `hours_back` hours.
      * Only touches events with ZERO existing event_type assignments.
      * Never overwrites a non-empty assignment — making this safe to
        run hourly without thrashing manual overrides.

    Cadence: hourly, ~5 min after the top of the hour so collect_events
    (heavy lock) typically isn't running. No heavy_job_lock acquisition
    needed — this is read-heavy + a small write batch, completes in
    seconds against the ~daily-ingestion-rate window.
    """
    db = SessionLocal()
    log = ScanLog(job_name="categorize_new_events", status="running")
    db.add(log)
    db.commit()
    db.refresh(log)
    try:
        from scripts.categorize_events import run_incremental
        stats = await asyncio.to_thread(run_incremental, hours_back=hours_back, dry_run=False)
        log.status = "success"
        log.events_found = stats.get("scanned", 0)
        log.events_saved = stats.get("applied", 0)
        log.notes = (
            f"scanned={stats.get('scanned',0)} applied={stats.get('applied',0)} "
            f"perf={stats.get('performer_hit',0)} music_default={stats.get('music_default',0)} "
            f"keyword={stats.get('keyword_hit',0)} no_match={stats.get('no_match',0)} "
            f"hours_back={hours_back}"
        )
        logger.info(f"categorize_new_events_job: {log.notes}")
    except Exception as e:
        log.status = "failed"
        log.notes = str(e)
        logger.error(f"categorize_new_events_job error: {e}")
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
