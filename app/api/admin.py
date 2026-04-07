from typing import Optional, List
from fastapi import APIRouter, BackgroundTasks, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, text
import asyncio
import httpx

from app.database import get_db, engine
from app.models import Event, Venue, City, EventType, PendingVenue
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
