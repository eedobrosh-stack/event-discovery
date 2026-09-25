from __future__ import annotations
import html
import logging
from datetime import date, timedelta
from difflib import SequenceMatcher

from sqlalchemy.orm import Session
from sqlalchemy import func, or_

from app.models import Event, Venue, City, EventType, Performer, event_event_types
from app.services.collectors.base import BaseCollector, RawEvent, default_end_time, infer_artist_from_name, CollectorAuthError
from app.services.youtube_lookup import lookup_youtube_video
from app.services.collectors.addon_filter import ticket_addon_reason
from app.services.dedup import find_null_venue_duplicate
from app.services.venue_aliases import ensure_venue_alias, find_venue_alias

# Reject events dated more than this far in the future. Real inventory
# tops out around 2-3 years (far-out conferences, the occasional tour);
# anything beyond is a date-parse error (LLM hallucinating a year like
# 3030/6960, tel_aviv_venues misreading a blog date as 2382) or a
# Ticketmaster placeholder/accounting SKU pinned at 2099-12-31. A single
# ceiling here in _save_events covers every source — all persistence
# funnels through this method. Added 2026-06-07 alongside the one-time
# cleanup of the ~154 rows that were already past this bound.
MAX_FUTURE_DAYS = 1095  # ~3 years

logger = logging.getLogger(__name__)


def _decode_entities(s, max_iters: int = 5):
    """Decode HTML entities in a text field. Safe on None/empty.
    Fixed-point: keeps decoding until stable, so double-encoded inputs
    (e.g. '&amp;amp;' → '&amp;' → '&') are fully resolved in one call.
    Capped at 5 iterations as a safety belt.

    Idempotent: the loop exits as soon as a pass changes nothing, so
    on already-clean text the cost is one extra equality check.

    Defensive against scrapers that pulled DOM attribute or JSON-LD
    values without decoding (image 5 / 2026-05-12 bug: event names
    like 'QUEENTA - ג&#039;יין בורדו' landed in DB with the apostrophe
    entity intact, then re-encoded by frontend esc() to display
    literally). Some sources double-encode (e.g. '&amp;amp;' in
    festival lineups) — the fixed-point loop handles those without
    needing a second backfill pass."""
    if not s:
        return s
    prev = s
    for _ in range(max_iters):
        nxt = html.unescape(prev)
        if nxt == prev:
            return nxt
        prev = nxt
    return prev


def venue_city_key(db, venue) -> int | None:
    """The canonical City id of a venue (alias City rows resolved)."""
    if venue is None or venue.city_id is None:
        return None
    c = db.get(City, venue.city_id)
    return (c.canonical_city_id or c.id) if c is not None else venue.city_id


def canonical_artist_spelling(db, artist_name):
    """Return the spelling we already use for this artist, if any.

    Lookup key = lower(trim(name)) — the same key artist_genre.normalized_name
    and performers.normalized_name are unique on (indexed, so this is one
    cheap lookup per event). Only case / whitespace variants are unified
    here; punctuation variants ("Six: The Musical" / "SIX the Musical") are
    handled by scripts/canonicalize_artist_names.py, which uses a broader
    key and is run as a backfill.
    """
    if not artist_name or not artist_name.strip():
        return artist_name
    from app.models.genre import ArtistGenre
    key = artist_name.strip().lower()
    cache = getattr(db, "_artist_spelling_cache", None)
    if cache is None:
        cache = {}
        try:
            db._artist_spelling_cache = cache
        except Exception:
            pass
    if key in cache:
        return cache[key] or artist_name
    known = None
    ag = db.query(ArtistGenre.artist_name).filter(ArtistGenre.normalized_name == key).first()
    if ag and ag[0] and ag[0].strip():
        known = ag[0].strip()
    else:
        perf = db.query(Performer.name).filter(Performer.normalized_name == key).first()
        if perf and perf[0] and perf[0].strip():
            known = perf[0].strip()
    cache[key] = known
    return known or artist_name.strip()


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
        import gc
        stats = {}
        for collector in self._collectors:
            # Init to None so the `finally` block can safely clean up even if
            # collector.collect(...) raises before raw_events is assigned.
            # (Prior version crashed with UnboundLocalError whenever any
            # collector threw — e.g. "Paris collect_events: cannot access
            # local variable 'raw_events' where it is not associated with
            # a value" on every HTTP timeout.)
            raw_events = None
            try:
                raw_events = await collector.collect(
                    city.name,
                    city.country,
                    lat=city.latitude,
                    lon=city.longitude,
                )
                fetched = len(raw_events)
                saved = self._save_events(raw_events, city, db)
                stats[collector.source_name] = {"fetched": fetched, "saved": saved}
                logger.info(f"{collector.source_name}: fetched={fetched}, saved={saved}")
            except CollectorAuthError as e:
                # Systemic key rejection — flag distinctly so collect_events
                # marks the run FAILED (not success/found=0). Other collectors
                # still run for this city.
                logger.error(f"{collector.source_name} AUTH error: {e}")
                stats[collector.source_name] = {"auth_error": str(e)}
            except Exception as e:
                logger.error(f"{collector.source_name} error: {e}")
                stats[collector.source_name] = {"error": str(e)}
            finally:
                # Release ORM identity map and raw event list after every collector
                # to prevent unbounded memory growth across 20+ collectors × 30 cities.
                raw_events = None
                db.expire_all()
                gc.collect()
        return stats

    _SAVE_BATCH = 50   # commit + expire every N events to keep identity map small

    def _save_events(
        self,
        raw_events: list[RawEvent],
        city: City,
        db: Session,
        llm_source_id: int | None = None,
    ) -> int:
        """Persist a batch of RawEvents under ``city``.

        ``llm_source_id`` is set when Cadence A's ``llm_extract_recurring_job``
        calls this — the FK on the resulting Event row closes the provenance
        chain LLMSource → Event → Performer so the Spotify funnel-attribution
        query can run. Route 2 collectors keep passing None (default).
        """
        saved = 0
        # Israeli artist_name hygiene (app/services/artist_names.py): show
        # titles are not artists. Cached per batch — a listing repeats the
        # same production dozens of times.
        is_israel = (city.country or "") == "Israel"
        artist_cache: dict = {}
        conflict_cache: dict = {}   # (date, city) → that day's candidate rows
        for i, raw in enumerate(raw_events):
            try:
                # Decode any HTML entities that slipped through the scraper
                # without unescaping. Applied as the first step so downstream
                # normalization (the " @ " splits, dedup matching, venue
                # resolution) runs on the human-readable text, not entity-
                # encoded gibberish.
                raw.name = _decode_entities(raw.name)
                raw.artist_name = _decode_entities(raw.artist_name)
                raw.venue_name = _decode_entities(raw.venue_name)
                if hasattr(raw, "description"):
                    raw.description = _decode_entities(raw.description)

                # Normalize: split "Event Title @ Venue Name" patterns
                # Case 1: @ in event name  → "Concert Title @ Venue" → split
                if raw.name and " @ " in raw.name:
                    parts = raw.name.split(" @ ", 1)
                    raw.name = parts[0].strip()
                    if not raw.venue_name:
                        raw.venue_name = parts[1].strip()
                # Case 2: @ in venue_name → bandsintown stores "Event Title @ Venue" in venue field
                if raw.venue_name and " @ " in raw.venue_name:
                    parts = raw.venue_name.split(" @ ", 1)
                    event_title = parts[0].strip()
                    raw.venue_name = parts[1].strip()
                    # Use the full event title as name if current name is just the artist
                    if event_title and (not raw.name or raw.name.lower() in event_title.lower()):
                        raw.name = event_title

                # Skip events with no date — DB has NOT NULL constraint on start_date
                if raw.start_date is None:
                    logger.debug(f"Skipping undated event '{raw.name}' from {raw.source}")
                    continue

                # Reject implausibly far-future dates — parse errors and
                # placeholder SKUs (see MAX_FUTURE_DAYS). Keeps the catalog's
                # "upcoming" pool honest now that we intentionally accept
                # events up to ~2 years out.
                if raw.start_date > date.today() + timedelta(days=MAX_FUTURE_DAYS):
                    logger.info(
                        f"Skipping far-future event '{(raw.name or '')[:60]}' "
                        f"@ {raw.start_date} from {raw.source} "
                        f"(> {MAX_FUTURE_DAYS}d ceiling)"
                    )
                    continue

                # Ticketmaster upsell SKUs ("Premium Seating", "Parking
                # permit", "Suite Reservation", "VIP M&G Add-On") arrive as
                # events. The collector's _transform already drops them;
                # this guard covers admin/scrape and any other TM entry
                # point so the rule lives in one predicate (addon_filter).
                if raw.source == "ticketmaster":
                    addon = ticket_addon_reason(raw.name)
                    if addon:
                        logger.info(
                            f"Skipping Ticketmaster add-on listing [{addon}] "
                            f"'{(raw.name or '')[:60]}' @ {raw.start_date}"
                        )
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
                    if is_israel and raw.artist_name:
                        # never back-fill a show title the cleanup removed
                        raw.artist_name = self._israeli_artist(db, raw.artist_name, artist_cache)
                    if not existing.artist_name and raw.artist_name:
                        # a recipe learned who performs/lectures after the
                        # row was first saved (katedra lecturer, 2026-09-21)
                        existing.artist_name = raw.artist_name
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
                    # Always sync sport fields + name format (catches events
                    # collected before the sports categorization fix)
                    if raw.sport and existing.sport != raw.sport:
                        existing.sport = raw.sport
                        existing.home_team = raw.home_team
                        existing.away_team = raw.away_team
                        updated = True
                    if raw.sport and existing.name != raw.name:
                        existing.name = raw.name
                        updated = True
                    # Sports schedules churn — playoffs get firmed up, neutral
                    # sites get assigned, tip-off times shift by 15-30 min as
                    # broadcast slots settle. Without authoritative overwrites,
                    # the stale early-season schedule sticks. Image 8 bug:
                    # 2026-05-13 game stored as 2026-05-11 22:00 because that
                    # was the original schedule; never re-synced. For sport
                    # rows, force-update start_date / start_time / end_* from
                    # the latest API row.
                    if raw.sport:
                        if raw.start_date and existing.start_date != raw.start_date:
                            existing.start_date = raw.start_date
                            updated = True
                        if raw.start_time and existing.start_time != raw.start_time:
                            existing.start_time = raw.start_time
                            updated = True
                        if raw.end_date and existing.end_date != raw.end_date:
                            existing.end_date = raw.end_date
                            updated = True
                        if raw.end_time and existing.end_time != raw.end_time:
                            existing.end_time = raw.end_time
                            updated = True
                        if raw.venue_name and existing.venue_name != raw.venue_name:
                            existing.venue_name = raw.venue_name
                            updated = True
                    # Backfill TV channels + YouTube highlights for sports
                    if raw.sport and raw.tv_channels and not existing.tv_channels:
                        existing.tv_channels = raw.tv_channels
                        updated = True
                    # Backfill tournament label for sport rows that pre-date the
                    # column. Overwrite when the collector now provides a value
                    # (collector configs occasionally rename labels, e.g. a
                    # rebranded league); never clear a value the row already has.
                    if raw.tournament and existing.tournament != raw.tournament:
                        existing.tournament = raw.tournament
                        updated = True
                    if (
                        raw.artist_youtube_channel
                        and not existing.artist_youtube_channel
                    ):
                        existing.artist_youtube_channel = raw.artist_youtube_channel
                        updated = True
                    if updated:
                        db.commit()
                    continue

                # Online / venue-less dedup: the LLM extractor and the
                # jsonld recipes see the same virtual event on many source
                # pages ("TECHSPO Philadelphia" x23, "Live 2026: Chicago
                # Marketing" x25 — 2026-09-18 QA report) and each page gets
                # its own source_id, so the (source, source_id) check above
                # never fires and the venue-keyed check below cannot run.
                # Same normalised title + same date + no venue on either
                # side (+ non-contradicting start_time) → duplicate. Narrow
                # by design: events *with* a venue are untouched, so two
                # legit same-named shows at different venues still both
                # save.
                if not raw.venue_name or raw.is_online:
                    dup = find_null_venue_duplicate(
                        db, raw.name, raw.start_date, raw.start_time
                    )
                    if dup is not None:
                        logger.debug(
                            f"Skipping venue-less duplicate of event {dup.id} "
                            f"'{(raw.name or '')[:60]}' @ {raw.start_date} from {raw.source}"
                        )
                        continue

                # Cross-source dedup: same venue + date + similar name.
                # Rule B (app/services/event_conflicts.py): another source's
                # copy is folded into the existing row (the earlier time
                # wins); the SAME source at another time is a second
                # performance (matinee) and is saved, not dropped.
                if raw.venue_name:
                    similar = db.query(Event).filter_by(
                        start_date=raw.start_date, venue_name=raw.venue_name
                    ).all()
                    match = next((
                        s for s in similar
                        if SequenceMatcher(None, raw.name.lower(), s.name.lower()).ratio() > 0.85
                        and not (s.scrape_source == raw.source and (s.start_time or "") != (raw.start_time or ""))
                    ), None)
                    if match is not None:
                        from app.services.event_conflicts import absorb_incoming
                        if not raw.sport and absorb_incoming(match, raw):
                            db.commit()
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
                if is_israel:
                    artist_name = self._israeli_artist(db, artist_name, artist_cache)

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

                # One spelling per artist: if we already know this artist
                # under another casing/punctuation ("RUSH" vs "Rush",
                # "Bill Bailey" vs "BILL BAILEY"), reuse the known spelling
                # so chips, genre rows and YouTube links don't split.
                artist_name = canonical_artist_spelling(db, artist_name)
                if is_israel and artist_name:
                    # the Performer fallback above can re-pick a show title
                    artist_name = self._israeli_artist(db, artist_name, artist_cache)

                # Same show from another source at another time / venue
                # (rule B, app/services/event_conflicts.py): enrich the
                # existing row — moving it to the earlier time — instead
                # of creating a second one. Also stops a row the 6-hourly
                # sweep folded from being re-created by the next scrape.
                if not raw.sport and venue is not None:
                    from app.services.event_conflicts import absorb_incoming, find_time_conflict
                    city_key = venue_city_key(db, venue)
                    other = find_time_conflict(
                        db, name=raw.name, artist=artist_name, start_date=raw.start_date,
                        start_time=raw.start_time, venue_id=venue.id, city_id=city_key,
                        scrape_source=raw.source, cache=conflict_cache)
                    if other is not None:
                        raw.artist_name = artist_name
                        if absorb_incoming(other, raw):
                            db.commit()
                        continue

                # Create event
                event = Event(
                    name=raw.name,
                    artist_name=artist_name,
                    artist_youtube_channel=raw.artist_youtube_channel,
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
                    llm_source_id=llm_source_id,
                    # Sports fields
                    sport=raw.sport,
                    home_team=raw.home_team,
                    away_team=raw.away_team,
                    tv_channels=raw.tv_channels or [],
                    tournament=raw.tournament,
                )
                db.add(event)
                db.flush()

                # Assign event type using priority chain:
                # 0. Sports shortcut — never go through Performer lookup for
                #    sports events (team names would be mis-matched as music artists)
                # 1. Artist → Performer table (event_type_name > category)
                # 2. Venue default_event_type_id override
                # 3. Raw categories with keyword/fallback rules
                et = None

                # Priority 0: sports events bypass Performer lookup entirely
                if raw.sport:
                    sport_cat = raw.sport  # e.g. "Basketball", "Football", "Formula 1"
                    et = self._resolve_event_type(sport_cat, raw, db)
                    if not et:
                        et = self._resolve_event_type("Sports", raw, db)

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

            # Commit and expire every SAVE_BATCH events — prevents the SQLAlchemy
            # identity map from accumulating hundreds of ORM objects in RAM.
            if (i + 1) % self._SAVE_BATCH == 0:
                db.commit()
                db.expire_all()

        db.commit()

        # ── Backfill venue_id for orphans in this city ───────────────────────
        # Events can be saved with venue_name (text) but no venue_id when
        # the venue record didn't exist yet at save time (e.g. RA, Bandsintown).
        # After the main loop, try to link them to now-existing venue records.
        try:
            orphans = (
                db.query(Event)
                .filter(
                    Event.venue_id.is_(None),
                    Event.venue_name.isnot(None),
                    Event.venue_name != "",
                )
                .limit(300)
                .all()
            )
            backfilled = 0
            for ev in orphans:
                venue = (
                    db.query(Venue)
                    .filter_by(name=ev.venue_name, city_id=city.id)
                    .first()
                )
                if venue:
                    ev.venue_id = venue.id
                    backfilled += 1
            if backfilled:
                db.commit()
                logger.info(f"Backfilled venue_id for {backfilled} orphaned events in {city.name}")
        except Exception as e:
            logger.warning(f"Venue backfill failed for {city.name}: {e}")
            db.rollback()

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

    # Generic fallback type per broad category
    _CATEGORY_FALLBACK: dict[str, str] = {
        "Music":        "Concert",
        "Art":          "Commercial Gallery Exhibitions",
        "Comedy":       "Open Mic Nights",
        "Dance":        "Modern Dance",
        "Film":         "Art House Cinema Screenings",
        "Fitness":      "Marathons",
        "Sports":       "Sports Event",
        "Festival":     "Multi-Genre Music Festivals",
        "Food & Drink": "Farmers Markets",
        "Technology":   "AI Tech Conferences",
        "Literature":   "Author Talks",
        "Charity":      "Formal Fundraising Galas",
        "Political":    "Campaign Events",
        "Religious":    "Church Services",
        "Career":       "Networking Expos",
        "Workshop":     "Photography Workshops",
        "Academic":     "Guest Speaker Events",
        "Automotive":   "Auto Expos",
        "Craft":        "Handmade Goods Markets",
        "Parade":       "Cultural Celebrations",
        "Theme Park":   "Seasonal Theme Park Celebrations",
        "Outdoor":      "Hiking Meetups",
        "Gaming":       "Board Game Conventions",
        "Pet":          "Pet Adoption Events",
        "Home & Garden": "Home Improvement Expos",
    }

    # Event-name keywords → specific event type (applied across all categories)
    _SPORT_NAME_HINTS: list[tuple[str, str]] = [
        # Sports
        ("baseball",        "Baseball Game"),
        (" mlb ",           "Baseball Game"),
        ("basketball",      "Basketball Game"),
        (" nba ",           "Basketball Game"),
        ("hockey",          "Hockey Game"),
        (" nhl ",           "Hockey Game"),
        ("soccer",          "Soccer Match"),
        (" mls ",           "Soccer Match"),
        (" nfl ",           "American Football Game"),
        ("tennis",          "Tennis Match"),
        (" atp ",           "Tennis Match"),
        (" wta ",           "Tennis Match"),
        ("golf",            "Golf Tournament"),
        (" pga ",           "Golf Tournament"),
        ("boxing",          "Boxing / MMA Event"),
        (" mma ",           "Boxing / MMA Event"),
        (" ufc ",           "Boxing / MMA Event"),
        ("wrestling",       "Wrestling Event"),
        (" wwe ",           "Wrestling Event"),
        # Fitness
        ("marathon",        "Marathons"),
        ("triathlon",       "Marathons"),
        ("cycling",         "Cycling Races"),
        ("yoga",            "Yoga Retreats"),
        ("crossfit",        "CrossFit Competitions"),
        ("hiking",          "Hiking Meetups"),
        ("rock climbing",   "Rock Climbing Competitions"),
        # Music
        ("symphony",        "Symphony Orchestral Performances"),
        ("orchestra",       "Symphony Orchestral Performances"),
        ("philharmonic",    "Symphony Orchestral Performances"),
        ("concerto",        "Concerto Performances"),
        ("string quartet",  "String Quartets"),
        ("chamber music",   "String Quartets"),
        ("piano trio",      "Piano Trios"),
        ("opera",           "Fully Staged Opera"),
        ("baroque",         "Baroque Orchestras"),
        ("early music",     "Baroque Orchestras"),
        ("renaissance",     "Renaissance Vocal Ensembles"),
        ("recital",         "Solo Recitals"),
        ("jazz",            "Jazz Concert"),
        ("hip-hop",         "Hip-Hop / Rap Concert"),
        ("hip hop",         "Hip-Hop / Rap Concert"),
        (" rap ",           "Hip-Hop / Rap Concert"),
        (" trap ",          "Hip-Hop / Rap Concert"),
        ("rock concert",    "Rock Concert"),
        (" rock ",          "Rock Concert"),    # "Somos Rock", "punk rock", etc.
        (" metal ",         "Rock Concert"),    # heavy metal — "metal detecting" unlikely in event names
        (" punk ",          "Rock Concert"),
        (" grunge ",        "Rock Concert"),
        ("indie rock",      "Rock Concert"),
        ("indie band",      "Rock Concert"),
        (" pop concert",    "Pop Concert"),
        ("pop music",       "Pop Concert"),
        ("electronic",      "Electronic / DJ Set"),
        (" dj set",         "Electronic / DJ Set"),
        (" dj ",            "Electronic / DJ Set"),
        ("techno",          "Electronic / DJ Set"),
        (" edm ",           "Electronic / DJ Set"),
        ("house music",     "Electronic / DJ Set"),
        (" trance ",        "Electronic / DJ Set"),
        ("r&b",             "R&B / Soul Concert"),
        (" soul ",          "R&B / Soul Concert"),
        (" blues ",         "R&B / Soul Concert"),
        (" funk ",          "R&B / Soul Concert"),
        ("gospel",          "Gospel Concert"),
        ("country",         "Country Concert"),
        ("bluegrass",       "Country Concert"),
        ("folk music",      "Concert"),
        ("folk band",       "Concert"),
        ("latin",           "Latin Concert"),
        ("salsa",           "Latin Concert"),
        ("cumbia",          "Latin Concert"),
        (" bossa ",         "Latin Concert"),
        ("reggae",          "Reggae / Calypso Concert"),
        (" ska ",           "Reggae / Calypso Concert"),
        # Dance
        ("ballet",          "Classical Ballet"),
        ("flamenco",        "Flamenco"),
        ("bharatanatyam",   "Bharatanatyam"),
        ("irish step",      "Irish Step Dance"),
        ("african dance",   "African Dance"),
        ("contemporary dance", "Modern Dance"),
        # Comedy
        ("open mic",        "Open Mic Nights"),
        ("stand-up",        "Comedy Club Headliners"),
        ("stand up",        "Comedy Club Headliners"),
        ("improv",          "Short-Form Improv"),
        ("sketch comedy",   "Sketch Comedy Performances"),
        # Film
        ("film festival",   "International Film Festivals"),
        ("film screening",  "Art House Cinema Screenings"),
        ("premiere",        "Red Carpet Premieres"),
        ("retrospective",   "Director Retrospectives"),
        # Literature
        ("book launch",     "Book Launches"),
        ("author talk",     "Author Talks"),
        ("poetry slam",     "Poetry Slams"),
        ("book club",       "Book Clubs"),
        ("literary",        "Panel Discussions"),
        # Food & Drink
        ("wine tasting",    "Wine Tastings"),
        ("beer festival",   "Craft Beer Events"),
        ("beer tasting",    "Beer Samplings"),
        ("farmers market",  "Farmers Markets"),
        ("food festival",   "Chef Demonstrations"),
        ("brewery",         "Brewery Tours"),
        ("vineyard",        "Vineyard Tours"),
        # Art
        ("art fair",        "Art Fairs"),
        ("gallery opening", "Museum Public Opening Receptions"),
        ("mural",           "Murals"),
        ("broadway",        "Broadway Show"),
        (" play ",          "Play / Drama"),
        ("theatre",         "Play / Drama"),
        ("theater",         "Play / Drama"),
        # Technology
        ("hackathon",       "Startup Showcases"),
        ("startup",         "Startup Showcases"),
        (" ai ",            "AI Tech Conferences"),
        ("cybersecurity",   "Cybersecurity Conferences"),
        ("tech conference", "AI Tech Conferences"),
        ("consumer electronics", "Consumer Electronics Shows"),
        # Career
        ("job fair",        "Job Fairs"),
        ("career fair",     "Job Fairs"),
        ("networking",      "Networking Expos"),
        # Charity
        ("gala",            "Formal Fundraising Galas"),
        ("fundrais",        "Fundraising Dinners"),
        ("benefit concert", "Benefit Concerts"),
        ("charity walk",    "Charity Walks"),
        ("charity run",     "Charity Runs"),
        ("auction",         "Charity Auctions"),
        # Political
        ("rally",           "Campaign Events"),
        ("protest",         "Protests"),
        (" march ",         "Marches"),
        ("town hall",       "Town Hall Meetings"),
        # Gaming
        ("esport",          "eSports Tournaments"),
        ("board game",      "Board Game Conventions"),
        ("video game",      "Video Game Expos"),
        (" rpg ",           "Tabletop RPG Events"),
        # Automotive
        ("car show",        "Auto Expos"),
        ("auto expo",       "Auto Expos"),
        ("vintage car",     "Vintage Car Exhibitions"),
        # Parade
        ("pride parade",    "Pride Parades"),
        (" parade",         "Cultural Celebrations"),
        # Craft
        ("craft fair",      "Handmade Goods Markets"),
        ("makers market",   "Artisan Events"),
        ("artisan market",  "Artisan Events"),
        # Pet
        ("dog show",        "Dog Shows"),
        ("cat show",        "Cat Shows"),
        ("pet adoption",    "Pet Adoption Events"),
        # Outdoor
        ("camping",         "Camping Rallies"),
        ("survival",        "Survival Workshops"),
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
        # Include a short description snippet for keyword matching (avoid false positives on long text)
        desc_snippet = (raw.description or "")[:400].lower()
        event_text = f" {(raw.name or '').lower()} {desc_snippet} "
        if " vs " in event_text or " vs. " in event_text:
            et = db.query(EventType).filter_by(name="Sports Event").first()
            if et:
                return et

        # 3. Match event name keywords across all categories
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

    @staticmethod
    def _israeli_artist(db: Session, name: str | None, cache: dict) -> str | None:
        """``clean_israeli_artist`` with the DB lookups it needs, cached."""
        if not name or not name.strip():
            return name
        key = name.strip().lower()
        if key in cache:
            return cache[key]
        from app.models.genre import ArtistGenre
        from app.services.artist_names import clean_israeli_artist

        def known(n: str) -> bool:
            p = db.query(Performer.mb_id, Performer.spotify_id).filter(Performer.normalized_name == n).first()
            return bool(p and (p[0] or p[1]))

        def sub_genre(n: str):
            g = db.query(ArtistGenre.primary_genre).filter(ArtistGenre.normalized_name == n).first()
            return g[0] if g else None

        cache[key] = clean_israeli_artist(name, known_performer=known, sub_genre=sub_genre)
        return cache[key]

    @staticmethod
    def _venue_home_city(raw: RawEvent, city: City, db: Session) -> City:
        """The City a venue belongs to, not the city the collector ran for.

        Israeli recipes and collectors are scoped to one city (mostly Tel
        Aviv) but list venues all over the country; filing every one of
        them under the collector's city put ~340 venues in the wrong City
        (Kfar Etzion, Kiryat Shmona, kibbutzim under Tel Aviv). When the
        raw venue_city canonicalises to another Israeli city, use that
        City row, or "Israel - Other" for a place without one.
        scripts/rehome_israel_venues.py fixed the rows already filed.
        """
        if city.country != "Israel" or not raw.venue_city:
            return city
        from app.services.il_places import PLACES, canon_place
        place = canon_place(raw.venue_city, city.name)
        if place.lower() == (city.name or "").lower() or place.startswith(("Outside Israel", "Online")):
            return city
        target = (db.query(City)
                  .filter(City.country == "Israel", func.lower(City.name) == place.lower())
                  .first())
        if target is None and place in set(PLACES.values()):
            target = (db.query(City)
                      .filter(City.country == "Israel", City.name == "Israel - Other").first())
        if target is None:
            return city
        if target.canonical_city_id:
            target = db.get(City, target.canonical_city_id) or target
        return target

    def _find_or_create_venue(self, raw: RawEvent, city: City, db: Session) -> Venue | None:
        if not raw.venue_name:
            return None
        city = self._venue_home_city(raw, city, db)

        venue = db.query(Venue).filter_by(name=raw.venue_name, city_id=city.id).first()
        if not venue:
            venue = find_venue_alias(db, city.id, raw.venue_name)
        if venue:
            ensure_venue_alias(db, venue, raw.venue_name)
            if raw.venue_website_url and not venue.website_url:
                venue.website_url = raw.venue_website_url
            if raw.venue_timezone and not venue.timezone:
                venue.timezone = raw.venue_timezone
            return venue

        venue = Venue(
            name=raw.venue_name,
            city_id=city.id,
            timezone=raw.venue_timezone or city.timezone,
            street_address=raw.venue_address,
            physical_city=raw.venue_city or city.name,
            physical_country=raw.venue_country or city.country,
            latitude=raw.venue_lat,
            longitude=raw.venue_lon,
            website_url=raw.venue_website_url,
        )
        db.add(venue)
        db.flush()
        ensure_venue_alias(db, venue, raw.venue_name)
        return venue
