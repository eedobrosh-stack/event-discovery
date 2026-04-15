from __future__ import annotations
import logging
from difflib import SequenceMatcher

from sqlalchemy.orm import Session
from sqlalchemy import or_

from app.models import Event, Venue, City, EventType, Performer, event_event_types
from app.services.collectors.base import BaseCollector, RawEvent, default_end_time, infer_artist_from_name
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
                # Normalize: split "Artist/Event @ Venue Name" into name + venue
                if raw.name and " @ " in raw.name:
                    parts = raw.name.split(" @ ", 1)
                    raw.name = parts[0].strip()
                    if not raw.venue_name:
                        raw.venue_name = parts[1].strip()

                # Skip events with no date — DB has NOT NULL constraint on start_date
                if raw.start_date is None:
                    logger.debug(f"Skipping undated event '{raw.name}' from {raw.source}")
                    continue

                # Dedup: check by source+source_id
                existing = db.query(Event).filter_by(
                    scrape_source=raw.source, source_id=raw.source_id
                ).first()
                if existing:
                    updated = False
                    # Backfill price if missing
                    if existing.price is None and raw.price is not None:
                        existing.price = raw.price
                        existing.price_currency = raw.price_currency
                        updated = True
                    # Backfill start_time if missing
                    if existing.start_time is None and raw.start_time is not None:
                        existing.start_time = raw.start_time
                        updated = True
                    # Backfill end_time if missing
                    if existing.end_time is None and raw.end_time is not None:
                        existing.end_time = raw.end_time
                        existing.end_date = raw.end_date or existing.end_date
                        updated = True
                    # Compute end_time from start+2h if still missing
                    if existing.end_time is None and existing.start_time is not None:
                        existing.end_date, existing.end_time = default_end_time(
                            existing.start_time, existing.start_date, existing.end_date
                        )
                        updated = True
                    if updated:
                        db.commit()
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

                # Resolve end time: use scraped value, or default to start + 2h
                end_date = raw.end_date
                end_time = raw.end_time
                if end_time is None and raw.start_time is not None:
                    end_date, end_time = default_end_time(
                        raw.start_time, raw.start_date, raw.end_date
                    )

                # Infer artist from name if not explicitly provided
                artist_name = raw.artist_name or infer_artist_from_name(raw.name)

                # If still no artist, check if the event name IS a known performer
                matched_performer = None
                if not artist_name:
                    matched_performer = (
                        db.query(Performer)
                        .filter(Performer.name.ilike(raw.name.strip()))
                        .first()
                    )
                    if matched_performer:
                        artist_name = matched_performer.name

                # Create event
                event = Event(
                    name=raw.name,
                    artist_name=artist_name,
                    start_date=raw.start_date,
                    start_time=raw.start_time,
                    end_date=end_date,
                    end_time=end_time,
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

                # Assign event type using priority chain:
                # 1. Artist → Performer table (event_type_name > category)
                # 2. Venue default_event_type_id override
                # 3. Raw categories with keyword/fallback rules
                et = None

                # Priority 1: look up artist in Performer table
                # matched_performer is already set if event name IS a performer;
                # for events with an explicit artist_name, do a fresh lookup.
                performer_match = matched_performer
                if not performer_match and artist_name:
                    performer_match = (
                        db.query(Performer)
                        .filter(Performer.normalized_name == artist_name.strip().lower())
                        .first()
                    )
                if performer_match:
                    if performer_match.event_type_name:
                        et = db.query(EventType).filter_by(name=performer_match.event_type_name).first()
                    elif performer_match.category:
                        et = self._resolve_event_type(performer_match.category, raw, db)

                # Priority 2: venue-level override
                if not et and venue and venue.default_event_type_id:
                    et = db.query(EventType).filter_by(id=venue.default_event_type_id).first()

                # Priority 3: raw categories with keyword/fallback rules
                if not et and raw.raw_categories:
                    for cat_name in raw.raw_categories:
                        et = self._resolve_event_type(cat_name, raw, db)
                        if et:
                            break

                if et and et not in event.event_types:
                    event.event_types.append(et)

                saved += 1
            except Exception as e:
                logger.error(f"Error saving event '{raw.name}': {e}")
                db.rollback()
                continue

        db.commit()
        return saved

    # Venue-name keywords → preferred event type name
    _VENUE_TYPE_HINTS: list[tuple[str, str]] = [
        ("jazz",        "Jazz Concert"),
        ("rock",        "Rock Concert"),
        ("hip hop",     "Hip-Hop / Rap Concert"),
        ("hip-hop",     "Hip-Hop / Rap Concert"),
        ("comedy",      "Comedy Show"),
        ("opera",       "Fully Staged Opera"),
        ("symphony",    "Symphony Orchestral Performances"),
        ("orchestra",   "Symphony Orchestral Performances"),
        ("philharmon",  "Symphony Orchestral Performances"),
        ("electronic",  "Electronic / DJ Set"),
        ("techno",      "Electronic / DJ Set"),
        ("pop",         "Pop Concert"),
        ("latin",       "Latin Concert"),
        ("gospel",      "Gospel Concert"),
        ("country",     "Country Concert"),
        ("blues",       "R&B / Soul Concert"),
        ("soul",        "R&B / Soul Concert"),
        ("r&b",         "R&B / Soul Concert"),
        ("reggae",      "Reggae / Calypso Concert"),
    ]

    # Generic fallback type per broad category (avoids grabbing .first() randomly)
    _CATEGORY_FALLBACK: dict[str, str] = {
        "Music":    "Concert",
        "Art":      "Art Exhibition",
        "Comedy":   "Comedy Show",
        "Dance":    "Dance Performance",
        "Film":     "Film Screening",
        "Fitness":  "Marathons",
        "Sports":   "Sports Event",
        "Festival": "Festival",
        "Food & Drink": "Food & Drink Event",
        "Technology": "Tech Conference",
    }

    # Event-name keywords → specific sport type (checked before category fallback)
    _SPORT_NAME_HINTS: list[tuple[str, str]] = [
        ("baseball",  "Baseball Game"),
        (" mlb ",     "Baseball Game"),
        ("basketball","Basketball Game"),
        (" nba ",     "Basketball Game"),
        ("hockey",    "Hockey Game"),
        (" nhl ",     "Hockey Game"),
        ("soccer",    "Soccer Match"),
        (" mls ",     "Soccer Match"),
        (" nfl ",     "American Football Game"),
        ("tennis",    "Tennis Match"),
        (" atp ",     "Tennis Match"),
        (" wta ",     "Tennis Match"),
        ("golf",      "Golf Tournament"),
        (" pga ",     "Golf Tournament"),
        ("boxing",    "Boxing / MMA Event"),
        (" mma ",     "Boxing / MMA Event"),
        (" ufc ",     "Boxing / MMA Event"),
        ("wrestling", "Wrestling Event"),
        (" wwe ",     "Wrestling Event"),
        ("marathon",  "Marathons"),
        ("triathlon", "Marathons"),
        ("cycling",   "Cycling Races"),
        ("yoga",      "Yoga Retreats"),
        ("crossfit",  "CrossFit Competitions"),
    ]

    def _resolve_event_type(self, category: str, raw: "RawEvent", db: Session) -> "EventType | None":
        """
        Pick the most specific EventType for this category by checking venue
        name / type keywords, then falling back to the generic type for the
        category instead of taking a random .first().
        """
        # 1. Try to match venue name against keyword hints
        venue_text = " ".join(filter(None, [
            raw.venue_name or "",
            raw.venue_city or "",
        ])).lower()

        for keyword, preferred_type_name in self._VENUE_TYPE_HINTS:
            if keyword in venue_text:
                et = db.query(EventType).filter_by(name=preferred_type_name).first()
                if et:
                    return et

        # 2. "X vs Y" in name → always a sports game regardless of category
        event_text = f" {(raw.name or '').lower()} "
        if " vs " in event_text or " vs. " in event_text:
            et = db.query(EventType).filter_by(name="Sports Event").first()
            if et:
                return et

        # 3. For Sports/Fitness: match event name against specific sport keywords
        if category in ("Sports", "Fitness"):
            for keyword, preferred_type_name in self._SPORT_NAME_HINTS:
                if keyword in event_text:
                    et = db.query(EventType).filter_by(name=preferred_type_name).first()
                    if et:
                        return et

        # 4. Use a sensible generic fallback for the category
        fallback_name = self._CATEGORY_FALLBACK.get(category)
        if fallback_name:
            et = db.query(EventType).filter_by(name=fallback_name).first()
            if et:
                return et

        # 5. Last resort: any type in the category
        return db.query(EventType).filter_by(category=category).first()

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
            if raw.venue_website_url and not venue.website_url:
                venue.website_url = raw.venue_website_url
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
            website_url=raw.venue_website_url,
        )
        db.add(venue)
        db.flush()
        return venue
