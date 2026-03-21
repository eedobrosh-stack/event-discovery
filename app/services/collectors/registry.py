from __future__ import annotations
import logging
from difflib import SequenceMatcher

from sqlalchemy.orm import Session
from sqlalchemy import or_

from app.models import Event, Venue, City, EventType, event_event_types
from app.services.collectors.base import BaseCollector, RawEvent
from app.services.youtube_lookup import lookup_youtube_video

logger = logging.getLogger(__name__)


class CollectorRegistry:
    def __init__(self):
        self._collectors: list[BaseCollector] = []

    def register(self, collector: BaseCollector):
        if collector.is_configured():
            self._collectors.append(collector)
            logger.info(f"Registered collector: {collector.source_name}")
        else:
            logger.warning(f"Collector {collector.source_name} not configured, skipping")

    async def collect_all(self, city: City, db: Session) -> dict:
        """Run all registered collectors for a city, save results, return stats."""
        stats = {}
        for collector in self._collectors:
            try:
                raw_events = await collector.collect(
                    city.name,
                    city.country,
                    lat=city.latitude,
                    lon=city.longitude,
                )
                saved = self._save_events(raw_events, city, db)
                stats[collector.source_name] = {"fetched": len(raw_events), "saved": saved}
                logger.info(f"{collector.source_name}: fetched={len(raw_events)}, saved={saved}")
            except Exception as e:
                logger.error(f"{collector.source_name} error: {e}")
                stats[collector.source_name] = {"error": str(e)}
        return stats

    def _save_events(self, raw_events: list[RawEvent], city: City, db: Session) -> int:
        saved = 0
        for raw in raw_events:
            try:
                # Dedup: check by source+source_id
                existing = db.query(Event).filter_by(
                    scrape_source=raw.source, source_id=raw.source_id
                ).first()
                if existing:
                    continue

                # Cross-source dedup: same venue + date + similar name
                if raw.venue_name:
                    similar = db.query(Event).filter_by(
                        start_date=raw.start_date, venue_name=raw.venue_name
                    ).all()
                    if any(
                        SequenceMatcher(None, raw.name.lower(), s.name.lower()).ratio() > 0.85
                        for s in similar
                    ):
                        continue

                # Find or create venue
                venue = self._find_or_create_venue(raw, city, db)

                # Create event
                event = Event(
                    name=raw.name,
                    artist_name=raw.artist_name,
                    start_date=raw.start_date,
                    start_time=raw.start_time,
                    end_date=raw.end_date,
                    end_time=raw.end_time,
                    purchase_link=raw.purchase_link,
                    price=raw.price,
                    price_currency=raw.price_currency,
                    description=raw.description,
                    image_url=raw.image_url,
                    is_online=raw.is_online,
                    venue_id=venue.id if venue else None,
                    venue_name=raw.venue_name or (venue.name if venue else None),
                    scrape_source=raw.source,
                    source_id=raw.source_id,
                )
                db.add(event)
                db.flush()

                # Assign categories
                if raw.raw_categories:
                    for cat_name in raw.raw_categories:
                        et = db.query(EventType).filter_by(category=cat_name).first()
                        if et and et not in event.event_types:
                            event.event_types.append(et)

                saved += 1
            except Exception as e:
                logger.error(f"Error saving event '{raw.name}': {e}")
                db.rollback()
                continue

        db.commit()
        return saved

    async def enrich_youtube(self, db: Session) -> int:
        """Fill in YouTube video URLs for events that have an artist but no YouTube link."""
        events = db.query(Event).filter(
            Event.artist_name.isnot(None),
            Event.artist_name != "",
            or_(
                Event.artist_youtube_channel.is_(None),
                Event.artist_youtube_channel == "",
            ),
        ).all()

        if not events:
            logger.info("No events need YouTube enrichment")
            return 0

        logger.info(f"Enriching {len(events)} events with YouTube links...")
        enriched = 0
        for i, event in enumerate(events):
            url = await lookup_youtube_video(event.artist_name)
            if url:
                event.artist_youtube_channel = url
                enriched += 1
            # Commit in batches of 20
            if (i + 1) % 20 == 0:
                db.commit()

        db.commit()
        logger.info(f"YouTube enrichment complete: {enriched}/{len(events)} artists found")
        return enriched

    def _find_or_create_venue(self, raw: RawEvent, city: City, db: Session) -> Venue | None:
        if not raw.venue_name:
            return None

        venue = db.query(Venue).filter_by(name=raw.venue_name, city_id=city.id).first()
        if venue:
            return venue

        venue = Venue(
            name=raw.venue_name,
            city_id=city.id,
            timezone=city.timezone,
            street_address=raw.venue_address,
            physical_city=raw.venue_city or city.name,
            physical_country=raw.venue_country or city.country,
            latitude=raw.venue_lat,
            longitude=raw.venue_lon,
        )
        db.add(venue)
        db.flush()
        return venue
