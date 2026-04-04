from typing import Optional, List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, text
import asyncio
import httpx

from app.database import get_db, engine
from app.models import Event, Venue, City, EventType
from app.scheduler.jobs import registry, collect_venue_websites
from app.services.dedup import dedup_events
from app.services.collectors.scrapers.venue_websites import scrape_venue_website
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


@router.post("/scrape")
async def trigger_scrape(
    sources: Optional[List[str]] = None,
    city_ids: Optional[List[int]] = None,
    db: Session = Depends(get_db),
):
    # Resolve cities — default to NYC (id=1) if none specified
    if city_ids:
        cities = db.query(City).filter(City.id.in_(city_ids)).all()
    else:
        cities = db.query(City).filter(City.name == "New York").all()
        if not cities:
            cities = db.query(City).limit(1).all()

    all_stats = {}
    for city in cities:
        stats = await registry.collect_all(city, db)
        all_stats[city.name] = stats

    # YouTube enrichment
    youtube_count = await registry.enrich_youtube(db)

    return {
        "message": "Scrape complete",
        "collection_stats": all_stats,
        "youtube_enriched": youtube_count,
    }


@router.post("/enrich-youtube")
async def enrich_youtube(db: Session = Depends(get_db)):
    """Enrich all events that have an artist but no YouTube link."""
    enriched = await registry.enrich_youtube(db)
    return {"message": f"YouTube enrichment complete", "enriched": enriched}


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
    """Scrape events from a specific venue URL and save them to the DB."""
    import httpx
    from bs4 import BeautifulSoup

    # Auto-detect venue name from page title if not provided
    if not venue_name:
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

    # Run scraper
    sem = asyncio.Semaphore(1)
    async with httpx.AsyncClient() as client:
        raw_events = await scrape_venue_website(
            client, sem, venue_name, venue_city, venue_country, venue_url
        )

    if not raw_events:
        return {"venue_name": venue_name, "events_found": 0, "events_saved": 0,
                "message": "No events found. The site may require JavaScript or use an unsupported format."}

    saved = registry._save_events(raw_events, city, db) if city else 0

    return {
        "venue_name": venue_name,
        "city": venue_city,
        "events_found": len(raw_events),
        "events_saved": saved,
        "message": f"Found {len(raw_events)} events, saved {saved} new ones.",
    }


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
