"""
scripts/scan_bandsintown.py
===========================
Scan Bandsintown for upcoming events for every performer in our DB.

Unlike city-based scrapers, Bandsintown is queried per artist.
For each performer we:
  1. Call GET /artists/{name}/events
  2. Resolve (or create) the city in our DB from the event's venue.city / country
  3. Resolve (or create) the venue
  4. Save the event, deduped by source_id ("bandsintown:<event_id>")

Run:
    caffeinate -i python3 scripts/scan_bandsintown.py

Resume after interruption: already-saved events are skipped via dedup index.
Results logged to: /tmp/scan_bandsintown.log
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

# Make sure project root is on PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models import City, Venue, Event, EventType, Performer
from app.services.collectors.api.bandsintown import BandsintownClient
from app.services.collectors.base import default_end_time
from app.services.collectors.category_mapper import map_category
from app.config import settings

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_FILE = "/tmp/scan_bandsintown.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ── Rate limiting ─────────────────────────────────────────────────────────────
# Bandsintown asks for reasonable use — 1 req/sec is safe
DELAY_SECONDS = 1.1


def find_or_create_city(db, city_name: str, country: str, region: str | None,
                        lat: float | None, lon: float | None) -> City:
    """Look up city by name+country, create if missing."""
    city = db.query(City).filter_by(name=city_name, country=country).first()
    if city:
        return city

    city = City(
        name=city_name,
        country=country,
        state=region or None,
        latitude=lat,
        longitude=lon,
    )
    db.add(city)
    db.flush()
    logger.info(f"  Created new city: {city_name}, {country}")
    return city


def find_or_create_venue(db, venue_name: str | None, city: City,
                         lat: float | None, lon: float | None) -> Venue | None:
    if not venue_name:
        return None
    venue = db.query(Venue).filter_by(name=venue_name, city_id=city.id).first()
    if venue:
        return venue
    venue = Venue(
        name=venue_name,
        city_id=city.id,
        physical_city=city.name,
        physical_country=city.country,
        latitude=lat,
        longitude=lon,
    )
    db.add(venue)
    db.flush()
    return venue


def save_event(db, parsed: dict, performer: Performer) -> bool:
    """
    Insert event into DB. Returns True if newly inserted, False if skipped.
    """
    source_id = f"bandsintown:{parsed['source_id']}"

    # Dedup check
    if db.query(Event).filter_by(scrape_source="bandsintown", source_id=source_id).first():
        return False

    # Cross-source dedup: same venue + date + similar name
    if parsed["venue_name"]:
        from difflib import SequenceMatcher
        similar = db.query(Event).filter_by(
            start_date=parsed["start_date"],
            venue_name=parsed["venue_name"],
        ).all()
        if any(
            SequenceMatcher(None, parsed["name"].lower(), s.name.lower()).ratio() > 0.85
            for s in similar
        ):
            return False

    # Resolve city
    venue_city    = parsed.get("venue_city")
    venue_country = parsed.get("venue_country")
    if not venue_city or not venue_country:
        return False  # can't place event without location

    city = find_or_create_city(
        db,
        city_name=venue_city,
        country=venue_country,
        region=parsed.get("venue_region"),
        lat=parsed.get("venue_lat"),
        lon=parsed.get("venue_lon"),
    )

    venue = find_or_create_venue(
        db,
        venue_name=parsed.get("venue_name"),
        city=city,
        lat=parsed.get("venue_lat"),
        lon=parsed.get("venue_lon"),
    )

    # Compute end time
    end_date, end_time = default_end_time(
        parsed.get("start_time"), parsed["start_date"], None
    )

    event = Event(
        name=parsed["name"],
        artist_name=performer.name,
        artist_youtube_channel=None,   # will be enriched later
        start_date=parsed["start_date"],
        start_time=parsed.get("start_time"),
        end_date=end_date,
        end_time=end_time,
        purchase_link=parsed.get("purchase_link"),
        description=parsed.get("description"),
        is_online=False,
        venue_id=venue.id if venue else None,
        venue_name=parsed.get("venue_name"),
        scrape_source="bandsintown",
        source_id=source_id,
    )
    db.add(event)
    db.flush()

    # Assign event type: prefer venue-name signal over performer's stored type,
    # since performer data can be stale or over-specific (e.g. Lady Gaga ≠ Jazz)
    from app.services.collectors.registry import CollectorRegistry
    from app.services.collectors.base import RawEvent
    from datetime import date as _date
    _dummy_raw = RawEvent(
        name=parsed.get("name", ""),
        start_date=parsed.get("start_date") or _date.today(),
        venue_name=parsed.get("venue_name") or "",
        venue_city=parsed.get("venue_city") or "",
        source="bandsintown",
        source_id="",
    )
    _reg = CollectorRegistry()
    et = _reg._resolve_event_type(performer.category or "Music", _dummy_raw, db)
    if et is None and performer.event_type_name:
        # Fall back to performer's stored type if venue gave us nothing useful
        et = db.query(EventType).filter_by(name=performer.event_type_name).first()
    if et:
        event.event_types.append(et)

    return True


async def run():
    if not settings.BANDSINTOWN_APP_ID:
        logger.error(
            "BANDSINTOWN_APP_ID is not set!\n"
            "  1. Register free at https://bandsintown.com/for/developers\n"
            "  2. Add BANDSINTOWN_APP_ID=your_app_id to your .env file\n"
            "  3. Re-run this script"
        )
        sys.exit(1)

    db = SessionLocal()
    client = BandsintownClient()

    # Load all performers, ordered by name
    performers = db.query(Performer).order_by(Performer.name).all()
    total = len(performers)
    logger.info(f"Starting Bandsintown scan for {total} performers")

    new_events = 0
    no_results = 0
    errors     = 0

    for i, performer in enumerate(performers, 1):
        try:
            raw_events = await client.get_artist_events(performer.name)
            parsed_events = [
                client.parse_event(ev, performer.name)
                for ev in raw_events
            ]
            parsed_events = [p for p in parsed_events if p]

            artist_new = 0
            for parsed in parsed_events:
                try:
                    saved = save_event(db, parsed, performer)
                    if saved:
                        artist_new += 1
                except Exception as e:
                    logger.warning(f"  Save error for {performer.name!r}: {e}")
                    db.rollback()

            db.commit()

            if parsed_events:
                logger.info(
                    f"[{i}/{total}] {performer.name!r}  →  "
                    f"{len(parsed_events)} upcoming, {artist_new} new"
                )
                new_events += artist_new
            else:
                no_results += 1
                logger.debug(f"[{i}/{total}] {performer.name!r}  →  no upcoming events")

            if i % 100 == 0:
                logger.info(
                    f"── Progress: {i}/{total}  |  new_events={new_events}  "
                    f"no_results={no_results}  errors={errors} ──"
                )

        except Exception as e:
            errors += 1
            logger.error(f"[{i}/{total}] Error for {performer.name!r}: {e}")
            db.rollback()

        await asyncio.sleep(DELAY_SECONDS)

    db.close()
    logger.info(
        f"\n{'='*60}\n"
        f"Bandsintown scan complete.\n"
        f"  Performers scanned : {total}\n"
        f"  New events saved   : {new_events}\n"
        f"  Artists w/ 0 events: {no_results}\n"
        f"  Errors             : {errors}\n"
        f"{'='*60}"
    )


if __name__ == "__main__":
    asyncio.run(run())
