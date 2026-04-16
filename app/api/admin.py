import csv
import io
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, BackgroundTasks, Depends, File, UploadFile
from fastapi.responses import Response
from sqlalchemy.orm import Session
from sqlalchemy import func, text
import asyncio
import httpx

from app.database import get_db, engine
from app.models import Event, Venue, City, EventType, event_event_types, PendingVenue, Performer, ScanLog
from app.scheduler.jobs import registry, collect_venue_websites
from app.services.dedup import dedup_events
from app.services.collectors.scrapers.venue_websites import scrape_venue_website
from app.services.collectors.scrapers.goshow import parse_goshow_venue_page
from app.services.collectors.scrapers.smarticket import parse_smarticket_venue_url
from app.seed.cities import CITIES
from app.seed.event_types import EVENT_TYPES
from app.config import settings

router = APIRouter(prefix="/api/admin", tags=["admin"])


@router.post("/seed")
def seed_database(db: Session = Depends(get_db)):
    """Seed cities and event types if not already present."""
    cities_added = 0
    for c in CITIES:
        exists = db.query(City).filter(City.name == c["name"], City.country == c["country"]).first()
        if not exists:
            db.add(City(**c))
            cities_added += 1
    db.commit()

    types_added = 0
    for t in EVENT_TYPES:
        exists = db.query(EventType).filter(EventType.name == t["name"]).first()
        if not exists:
            db.add(EventType(**t))
            types_added += 1
    db.commit()

    return {"cities_added": cities_added, "event_types_added": types_added}


@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    return {
        "total_events": db.query(func.count(Event.id)).scalar(),
        "total_venues": db.query(func.count(Venue.id)).scalar(),
        "total_cities": db.query(func.count(City.id)).scalar(),
        "total_event_types": db.query(func.count(EventType.id)).scalar(),
        "events_by_source": dict(
            db.query(Event.scrape_source, func.count(Event.id))
            .group_by(Event.scrape_source)
            .all()
        ),
    }


async def _run_scrape(city_ids: Optional[List[int]]):
    """Background scrape job — opens its own DB session so it outlives the request."""
    from app.database import SessionLocal
    db = SessionLocal()
    try:
        if city_ids:
            cities = db.query(City).filter(City.id.in_(city_ids)).all()
        else:
            cities = db.query(City).filter(City.name == "New York").all()
            if not cities:
                cities = db.query(City).limit(1).all()
        for city in cities:
            stats = await registry.collect_all(city, db)
            import logging
            logging.getLogger(__name__).info(f"Scrape done for {city.name}: {stats}")
    finally:
        db.close()


@router.post("/scrape")
async def trigger_scrape(
    background_tasks: BackgroundTasks,
    sources: Optional[List[str]] = None,
    city_ids: Optional[List[int]] = None,
):
    """Kick off a scrape in the background and return immediately."""
    background_tasks.add_task(_run_scrape, city_ids)
    label = f"city_ids={city_ids}" if city_ids else "New York (default)"
    return {"message": f"Scrape started in background for {label}"}


@router.post("/run-venuepilot")
async def scrape_venuepilot(db: Session = Depends(get_db)):
    """Run only the VenuePilot collector synchronously and return stats immediately."""
    from app.services.collectors.scrapers.venuepilot import VenuePilotCollector
    collector = VenuePilotCollector()
    all_stats = {}
    for city_name in ["San Francisco", "Berkeley"]:
        city = db.query(City).filter(City.name == city_name).first()
        if not city:
            continue
        raw_events = await collector.collect(city_name)
        saved = registry._save_events(raw_events, city, db)
        all_stats[city_name] = {"fetched": len(raw_events), "saved": saved}
    return {"venuepilot": all_stats}


@router.post("/enrich-youtube")
async def enrich_youtube(db: Session = Depends(get_db)):
    """Enrich all events that have an artist but no YouTube link."""
    enriched = await registry.enrich_youtube(db)
    return {"message": f"YouTube enrichment complete", "enriched": enriched}


@router.post("/propagate-youtube")
def propagate_youtube(db: Session = Depends(get_db)):
    """
    For every artist that already has a YouTube link on at least one event,
    copy that link to their other events that are missing it.
    """
    from sqlalchemy import func as sa_func

    # Find the canonical YouTube URL per artist (pick the most common one)
    artist_youtube = (
        db.query(
            func.lower(Event.artist_name).label("artist_key"),
            Event.artist_youtube_channel,
            func.count(Event.id).label("cnt"),
        )
        .filter(
            Event.artist_name.isnot(None),
            Event.artist_youtube_channel.isnot(None),
            Event.artist_youtube_channel != "",
        )
        .group_by(func.lower(Event.artist_name), Event.artist_youtube_channel)
        .all()
    )

    # Build dict: lowercase artist name → most-used YouTube URL
    best: dict = {}
    counts: dict = {}
    for artist_key, yt_url, cnt in artist_youtube:
        if artist_key not in counts or cnt > counts[artist_key]:
            best[artist_key] = yt_url
            counts[artist_key] = cnt

    if not best:
        return {"message": "No YouTube links found to propagate", "updated": 0}

    updated = 0
    for artist_key, yt_url in best.items():
        result = (
            db.query(Event)
            .filter(
                func.lower(Event.artist_name) == artist_key,
                Event.artist_youtube_channel.is_(None)
                | (Event.artist_youtube_channel == ""),
            )
            .update(
                {Event.artist_youtube_channel: yt_url},
                synchronize_session=False,
            )
        )
        updated += result

    db.commit()
    return {
        "message": "YouTube propagation complete",
        "artists_with_links": len(best),
        "events_updated": updated,
    }


async def _fetch_venue_url_from_event(client, source_id, api_key):
    try:
        resp = await client.get(
            f"https://app.ticketmaster.com/discovery/v2/events/{source_id}.json",
            params={"apikey": api_key}, timeout=10,
        )
        if resp.status_code == 200:
            venues = resp.json().get("_embedded", {}).get("venues", [])
            if venues:
                return venues[0].get("url")
    except Exception:
        pass
    return None


async def _search_venue_url(client, name, city, country, api_key):
    try:
        params = {"apikey": api_key, "keyword": name, "size": 5}
        if country:
            params["countryCode"] = country[:2].upper()
        resp = await client.get(
            "https://app.ticketmaster.com/discovery/v2/venues.json",
            params=params, timeout=10,
        )
        if resp.status_code == 200:
            for v in resp.json().get("_embedded", {}).get("venues", []):
                v_name = v.get("name", "").lower()
                v_city = (v.get("city") or {}).get("name", "").lower()
                if v_name == name.lower() and (not city or v_city == city.lower()):
                    return v.get("url")
                if name.lower().startswith(v_name[:15]) and (not city or v_city == city.lower()):
                    return v.get("url")
    except Exception:
        pass
    return None


@router.post("/scrape-venue-url")
async def scrape_venue_url(
    venue_url: str,
    venue_name: Optional[str] = None,
    city_name: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Scrape events from a specific venue URL and save them to the DB.
    Every submission is logged to pending_venues; failed ones stay as 'failed'
    so the background agent can pick them up and build a custom parser.
    """
    import httpx
    from bs4 import BeautifulSoup
    from datetime import datetime

    # Log this submission immediately as "pending"
    pending = PendingVenue(
        url=venue_url,
        venue_name=venue_name,
        city_name=city_name,
        status="pending",
    )
    db.add(pending)
    db.commit()
    db.refresh(pending)

    # Known platforms supply their own venue name — skip HTML pre-fetch for them
    _known_platform = any(p in venue_url for p in ("goshow.co.il", "smarticket.co.il"))

    # Auto-detect venue name from page title for generic URLs
    if not venue_name and not _known_platform:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(venue_url, follow_redirects=True,
                    headers={"User-Agent": "Supercaly/1.0"})
                soup = BeautifulSoup(resp.text, "lxml")
                og_site = soup.find("meta", property="og:site_name")
                title_tag = soup.find("title")
                venue_name = (
                    (og_site["content"] if og_site and og_site.get("content") else None)
                    or (title_tag.string.split(" - ")[0].split(" | ")[0].strip() if title_tag and title_tag.string else None)
                    or venue_url
                )
        except Exception:
            venue_name = venue_url

    # Resolve city
    city = None
    if city_name:
        city = db.query(City).filter(City.name.ilike(f"%{city_name}%")).first()
    if not city:
        city = db.query(City).filter(City.name == "Tel Aviv").first()
    if not city:
        city = db.query(City).first()

    venue_city = city.name if city else ""
    venue_country = city.country if city else ""

    # Run scraper — use platform-specific parsers when URL is recognised
    if "techconf.directory" in venue_url:
        from app.services.collectors.scrapers.techconf_directory import scrape_techconf_directory
        from app.scheduler.jobs import collect_techconf_job
        # Run the full directory job and report results
        await collect_techconf_job()
        # Return a summary (job handles its own DB writes)
        return {"venue_name": "TechConf.Directory", "events_found": -1, "events_saved": -1,
                "message": "TechConf.Directory scan triggered — check Scan Logs for results."}

    async with httpx.AsyncClient() as client:
        if "goshow.co.il" in venue_url:
            raw_events = await parse_goshow_venue_page(
                venue_url, client, venue_name, venue_city, venue_country
            )
        elif "smarticket.co.il" in venue_url:
            raw_events = await parse_smarticket_venue_url(
                venue_url, client, venue_name, venue_city, venue_country
            )
        else:
            sem = asyncio.Semaphore(1)
            raw_events = await scrape_venue_website(
                client, sem, venue_name, venue_city, venue_country, venue_url
            )

    # Pick up auto-detected venue name from first event if not provided
    if raw_events and not venue_name:
        venue_name = raw_events[0].venue_name

    # Update the pending record with the outcome
    if not raw_events:
        pending.status = "failed"
        pending.events_found = 0
        pending.events_saved = 0
        pending.agent_notes = "Generic scraper found 0 events — needs custom parser."
        pending.handled_at = datetime.utcnow()
        db.commit()
        return {"venue_name": venue_name, "events_found": 0, "events_saved": 0,
                "message": "No events found — flagged for the background agent to handle."}

    saved = registry._save_events(raw_events, city, db) if city else 0
    pending.status = "success" if saved > 0 else "partial"
    pending.venue_name = venue_name
    pending.events_found = len(raw_events)
    pending.events_saved = saved
    pending.handled_at = datetime.utcnow()
    db.commit()

    return {
        "venue_name": venue_name,
        "city": venue_city,
        "events_found": len(raw_events),
        "events_saved": saved,
        "message": f"Found {len(raw_events)} events, saved {saved} new ones.",
    }


@router.get("/pending-venues")
def list_pending_venues(status: Optional[str] = "failed", db: Session = Depends(get_db)):
    """Return venue submissions that need attention. Default: status=failed."""
    q = db.query(PendingVenue)
    if status:
        q = q.filter(PendingVenue.status == status)
    venues = q.order_by(PendingVenue.created_at.desc()).limit(50).all()
    return [
        {
            "id": v.id, "url": v.url, "venue_name": v.venue_name,
            "city_name": v.city_name, "status": v.status,
            "events_found": v.events_found, "events_saved": v.events_saved,
            "agent_notes": v.agent_notes, "created_at": str(v.created_at),
        }
        for v in venues
    ]


@router.post("/pending-venues/{venue_id}/resolve")
def resolve_pending_venue(venue_id: int, notes: str = "", db: Session = Depends(get_db)):
    """Mark a pending venue as resolved (called by the agent after handling it)."""
    from datetime import datetime
    v = db.query(PendingVenue).filter(PendingVenue.id == venue_id).first()
    if not v:
        from fastapi import HTTPException
        raise HTTPException(404, "Pending venue not found")
    v.status = "resolved"
    v.agent_notes = notes
    v.handled_at = datetime.utcnow()
    db.commit()
    return {"ok": True}


@router.post("/dedup")
def run_dedup(db: Session = Depends(get_db)):
    """Remove duplicate events across sources. Safe to run anytime."""
    result = dedup_events(db)
    return result


@router.post("/scrape-venue-websites")
async def scrape_venue_websites():
    """Manually trigger venue website scraping for all venues with a known URL."""
    await collect_venue_websites()
    return {"message": "Venue website scrape complete"}


@router.post("/enrich-venues-tm")
async def enrich_venues_tm():
    """Enrich venue website_url from Ticketmaster API. Runs Phase 1 (TM events)
    and Phase 2 (name search). Safe to call daily — skips already-enriched venues."""
    api_key = settings.TICKETMASTER_KEY
    if not api_key:
        return {"error": "TICKETMASTER_KEY not configured"}

    # Quick quota check
    async with httpx.AsyncClient() as client:
        probe = await client.get(
            "https://app.ticketmaster.com/discovery/v2/venues.json",
            params={"apikey": api_key, "keyword": "test", "size": 1}, timeout=10,
        )
        if probe.status_code == 429 or "QuotaViolation" in probe.text:
            return {"error": "Ticketmaster quota exhausted — try again after midnight UTC"}

    sem = asyncio.Semaphore(5)
    updated_tm = 0
    updated_search = 0

    with engine.connect() as conn:
        # Phase 1: TM-sourced venues
        rows1 = conn.execute(text("""
            SELECT v.id, MIN(e.source_id) as event_source_id
            FROM venues v
            JOIN events e ON e.venue_id = v.id
            WHERE e.scrape_source = 'ticketmaster'
              AND e.start_date >= date('now')
              AND (v.website_url IS NULL OR v.website_url = '')
              AND e.source_id IS NOT NULL
            GROUP BY v.id
            LIMIT 2000
        """)).fetchall()

        async def enrich_tm(client, vid, sid):
            async with sem:
                url = await _fetch_venue_url_from_event(client, sid, api_key)
                await asyncio.sleep(0.2)
                return vid, url

        async with httpx.AsyncClient() as client:
            tasks = [enrich_tm(client, vid, sid) for vid, sid in rows1]
            results1 = await asyncio.gather(*tasks)

        for venue_id, url in results1:
            if url:
                conn.execute(text("UPDATE venues SET website_url = :url WHERE id = :id"),
                             {"url": url, "id": venue_id})
                updated_tm += 1
        conn.commit()

        # Phase 2: non-TM venues (capped to stay within daily quota)
        rows2 = conn.execute(text("""
            SELECT DISTINCT v.id, v.name, v.physical_city, v.physical_country
            FROM venues v
            JOIN events e ON e.venue_id = v.id
            WHERE e.start_date >= date('now')
              AND (v.website_url IS NULL OR v.website_url = '')
              AND v.name IS NOT NULL
            LIMIT 2500
        """)).fetchall()

        async def enrich_search(client, vid, name, city, country):
            async with sem:
                url = await _search_venue_url(client, name, city, country, api_key)
                await asyncio.sleep(0.2)
                return vid, url

        async with httpx.AsyncClient() as client:
            tasks2 = [enrich_search(client, vid, name, city, country)
                      for vid, name, city, country in rows2]
            results2 = await asyncio.gather(*tasks2)

        for venue_id, url in results2:
            if url:
                conn.execute(text("UPDATE venues SET website_url = :url WHERE id = :id"),
                             {"url": url, "id": venue_id})
                updated_search += 1
        conn.commit()

    return {
        "message": "TM venue enrichment complete",
        "tm_venues_updated": updated_tm,
        "search_venues_updated": updated_search,
        "total": updated_tm + updated_search,
    }


# ── Admin Dashboard: CSV Exports ─────────────────────────────────────────────

def _csv_response(rows, headers, filename):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(headers)
    w.writerows(rows)
    return Response(
        content=buf.getvalue().encode("utf-8-sig"),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.get("/export/venues")
def export_venues(db: Session = Depends(get_db)):
    rows = db.query(
        Venue.id, Venue.name, Venue.physical_city, Venue.physical_country,
        Venue.website_url, Venue.street_address, Venue.venue_type,
        Venue.latitude, Venue.longitude, Venue.created_at,
    ).all()
    return _csv_response(rows,
        ["id", "name", "city", "country", "website_url", "address", "type",
         "latitude", "longitude", "created_at"],
        "venues.csv")


@router.get("/export/performers")
def export_performers(db: Session = Depends(get_db)):
    rows = db.query(
        Performer.id, Performer.name, Performer.category,
        Performer.event_type_name, Performer.genres,
        Performer.source, Performer.confidence, Performer.looked_up_at,
    ).all()
    return _csv_response(rows,
        ["id", "name", "category", "event_type", "genres",
         "source", "confidence", "looked_up_at"],
        "performers.csv")


@router.get("/export/cities")
def export_cities(db: Session = Depends(get_db)):
    rows = db.query(
        City.id, City.name, City.country, City.state,
        City.timezone, City.latitude, City.longitude,
    ).all()
    return _csv_response(rows,
        ["id", "name", "country", "state", "timezone", "latitude", "longitude"],
        "cities.csv")


@router.get("/export/events")
def export_events_admin(
    source: Optional[str] = None,
    db: Session = Depends(get_db),
):
    q = db.query(
        Event.id, Event.name, Event.artist_name, Event.start_date,
        Event.start_time, Event.venue_name, Event.scrape_source,
        Event.price, Event.price_currency, Event.purchase_link,
        Event.artist_youtube_channel, Event.created_at,
    )
    if source:
        q = q.filter(Event.scrape_source == source)
    rows = q.order_by(Event.start_date).all()
    return _csv_response(rows,
        ["id", "name", "artist", "date", "time", "venue", "source",
         "price", "currency", "link", "youtube", "created_at"],
        "events.csv")


# ── Admin Dashboard: Scan Logs ────────────────────────────────────────────────

@router.get("/scan-logs")
def get_scan_logs(limit: int = 100, job: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(ScanLog).order_by(ScanLog.started_at.desc())
    if job:
        q = q.filter(ScanLog.job_name == job)
    logs = q.limit(limit).all()
    return [
        {
            "id": l.id, "job_name": l.job_name, "detail": l.detail,
            "started_at": str(l.started_at), "finished_at": str(l.finished_at),
            "events_found": l.events_found, "events_saved": l.events_saved,
            "status": l.status, "notes": l.notes,
        }
        for l in logs
    ]


# ── Event Type Repair ─────────────────────────────────────────────────────────

@router.post("/fix-venue-event-types")
def fix_venue_event_types(payload: dict, db: Session = Depends(get_db)):
    """
    Replace all event-type associations for every event at a given venue.
    Body: { "venue_id": int, "event_type_name": str }
    Clears existing associations and assigns the single correct event type.
    """
    venue_id = int(payload["venue_id"])
    type_name = payload["event_type_name"].strip()

    venue = db.query(Venue).filter(Venue.id == venue_id).first()
    if not venue:
        return {"error": f"Venue {venue_id} not found"}

    event_type = db.query(EventType).filter(
        EventType.name.ilike(type_name)
    ).first()
    if not event_type:
        return {"error": f"EventType '{type_name}' not found"}

    event_ids = [e.id for e in db.query(Event.id).filter(Event.venue_id == venue_id).all()]
    if not event_ids:
        return {"message": "No events found for this venue", "updated": 0}

    # Remove all existing type associations for these events
    db.execute(
        event_event_types.delete().where(
            event_event_types.c.event_id.in_(event_ids)
        )
    )
    # Insert the correct association for each event
    db.execute(
        event_event_types.insert(),
        [{"event_id": eid, "event_type_id": event_type.id} for eid in event_ids],
    )

    # Persist as the venue's default so future scrapes use the same type
    venue.default_event_type_id = event_type.id

    db.commit()

    return {
        "venue": venue.name,
        "event_type_assigned": event_type.name,
        "events_updated": len(event_ids),
    }


# ── Admin Dashboard: Bulk Uploads (staged, no immediate scraping) ─────────────

@router.post("/upload/venues")
async def upload_venues(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    CSV columns: url, venue_name (opt), city (opt)
    Each row is staged as a PendingVenue with status='queued'.
    """
    content = (await file.read()).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    added = 0
    skipped = 0
    for row in reader:
        url = (row.get("url") or "").strip()
        if not url:
            skipped += 1
            continue
        existing = db.query(PendingVenue).filter(PendingVenue.url == url).first()
        if existing:
            skipped += 1
            continue
        db.add(PendingVenue(
            url=url,
            venue_name=(row.get("venue_name") or "").strip() or None,
            city_name=(row.get("city") or "").strip() or None,
            status="queued",
        ))
        added += 1
    db.commit()
    return {"queued": added, "skipped_duplicates": skipped}


@router.post("/upload/artists")
async def upload_artists(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    CSV column: artist_name
    Adds new Performer records with confidence=0 for later enrichment.
    """
    content = (await file.read()).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    added = 0
    skipped = 0
    for row in reader:
        name = (row.get("artist_name") or "").strip()
        if not name:
            skipped += 1
            continue
        norm = name.lower().strip()
        existing = db.query(Performer).filter(Performer.normalized_name == norm).first()
        if existing:
            skipped += 1
            continue
        db.add(Performer(name=name, normalized_name=norm, source="manual", confidence=0.0))
        added += 1
    db.commit()
    return {"added": added, "skipped_duplicates": skipped}


@router.post("/upload/cities")
async def upload_cities(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    CSV columns: name, country, state (opt), timezone (opt)
    Directly seeds City records.
    """
    content = (await file.read()).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    added = 0
    skipped = 0
    for row in reader:
        name = (row.get("name") or "").strip()
        country = (row.get("country") or "").strip()
        if not name or not country:
            skipped += 1
            continue
        existing = db.query(City).filter(
            City.name == name, City.country == country
        ).first()
        if existing:
            skipped += 1
            continue
        db.add(City(
            name=name,
            country=country,
            state=(row.get("state") or "").strip() or None,
            timezone=(row.get("timezone") or "").strip() or None,
        ))
        added += 1
    db.commit()
    return {"added": added, "skipped_duplicates": skipped}


# ── City Deduplication ────────────────────────────────────────────────────────

@router.get("/city-duplicates")
def get_city_duplicates(db: Session = Depends(get_db)):
    """
    Return pairs of cities (same country) where one name is a substring of the
    other — the most common pattern for duplicates (e.g. 'Tel Aviv' / 'Tel Aviv-Yafo',
    'New York' / 'East New York').
    Includes venue count per city so you can tell which record to keep.
    """
    venue_counts = dict(
        db.query(Venue.city_id, func.count(Venue.id))
        .group_by(Venue.city_id)
        .all()
    )

    cities = db.query(City).order_by(City.country, City.name).all()

    # Build pairs where one name is contained in the other (same country)
    seen = set()
    pairs = []
    for i, c1 in enumerate(cities):
        for c2 in cities[i + 1:]:
            if c1.country != c2.country:
                continue
            n1, n2 = c1.name.lower(), c2.name.lower()
            if n1 in n2 or n2 in n1:
                key = (min(c1.id, c2.id), max(c1.id, c2.id))
                if key in seen:
                    continue
                seen.add(key)
                pairs.append({
                    "city_a": {"id": c1.id, "name": c1.name, "country": c1.country,
                               "venues": venue_counts.get(c1.id, 0)},
                    "city_b": {"id": c2.id, "name": c2.name, "country": c2.country,
                               "venues": venue_counts.get(c2.id, 0)},
                })

    return pairs


@router.post("/merge-cities")
def merge_cities(payload: dict, db: Session = Depends(get_db)):
    """
    Merge one or more cities into a single canonical city.
    All venues pointing at merge_ids get reassigned to keep_id, then the
    duplicate city records are deleted and the cities cache is invalidated.

    Body: { "keep_id": int, "merge_ids": [int, ...] }
    """
    keep_id = int(payload["keep_id"])
    merge_ids = [int(x) for x in payload.get("merge_ids", [])]

    if not merge_ids:
        return {"error": "merge_ids must be a non-empty list"}
    if keep_id in merge_ids:
        return {"error": "keep_id cannot also appear in merge_ids"}

    keep_city = db.query(City).filter(City.id == keep_id).first()
    if not keep_city:
        return {"error": f"City {keep_id} not found"}

    venues_updated = 0
    merged_names = []
    for mid in merge_ids:
        mc = db.query(City).filter(City.id == mid).first()
        if not mc:
            continue
        merged_names.append(mc.name)
        count = (
            db.query(Venue)
            .filter(Venue.city_id == mid)
            .update({"city_id": keep_id}, synchronize_session=False)
        )
        venues_updated += count
        db.query(City).filter(City.id == mid).delete(synchronize_session=False)

    db.commit()

    # Bust the cities in-memory cache so the updated list is served immediately
    from app.api.cities import warm_cities_cache
    warm_cities_cache()

    return {
        "kept": {"id": keep_id, "name": keep_city.name, "country": keep_city.country},
        "merged": merged_names,
        "venues_reassigned": venues_updated,
    }


@router.post("/upload/events")
async def upload_events(file: UploadFile = File(...), db: Session = Depends(get_db)):
    """
    CSV columns: name, artist_name (opt), start_date (YYYY-MM-DD),
                 start_time (opt, HH:MM), venue_name (opt), city (opt),
                 price (opt), purchase_link (opt)
    Directly imports Event records.
    """
    from datetime import date as date_type
    content = (await file.read()).decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    added = 0
    skipped = 0
    for row in reader:
        name = (row.get("name") or "").strip()
        start_date_str = (row.get("start_date") or "").strip()
        if not name or not start_date_str:
            skipped += 1
            continue
        try:
            start_date = date_type.fromisoformat(start_date_str)
        except ValueError:
            skipped += 1
            continue
        price_str = (row.get("price") or "").strip()
        price = float(price_str) if price_str else None
        db.add(Event(
            name=name,
            artist_name=(row.get("artist_name") or "").strip() or None,
            start_date=start_date,
            start_time=(row.get("start_time") or "").strip() or None,
            venue_name=(row.get("venue_name") or "").strip() or None,
            price=price,
            purchase_link=(row.get("purchase_link") or "").strip() or None,
            scrape_source="manual_upload",
        ))
        added += 1
    db.commit()
    return {"added": added, "skipped": skipped}
