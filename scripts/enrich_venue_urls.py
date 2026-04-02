"""
Enriches venue website_url by:
1. For TM-sourced events: fetches event detail from Ticketmaster API to get venue URL
2. For non-TM venues: searches Ticketmaster venue endpoint by name+city
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from sqlalchemy import text
from app.database import engine
from app.config import settings

TM_KEY = settings.TICKETMASTER_KEY
CONCURRENCY = 5  # TM rate limit


async def fetch_venue_url_from_event(client, source_id):
    """Fetch TM event detail and extract venue URL."""
    try:
        resp = await client.get(
            f"https://app.ticketmaster.com/discovery/v2/events/{source_id}.json",
            params={"apikey": TM_KEY},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        venues = data.get("_embedded", {}).get("venues", [])
        if venues:
            return venues[0].get("url")
    except Exception:
        pass
    return None


async def search_venue_url(client, name, city, country):
    """Search TM venue endpoint by name and city."""
    try:
        params = {"apikey": TM_KEY, "keyword": name, "size": 5}
        if country:
            params["countryCode"] = country[:2].upper()
        resp = await client.get(
            "https://app.ticketmaster.com/discovery/v2/venues.json",
            params=params,
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        venues = data.get("_embedded", {}).get("venues", [])
        for v in venues:
            v_name = v.get("name", "").lower()
            v_city = (v.get("city") or {}).get("name", "").lower()
            if v_name == name.lower() and (not city or v_city == city.lower()):
                return v.get("url")
            # Looser match: starts with
            if name.lower().startswith(v_name[:15]) and (not city or v_city == city.lower()):
                return v.get("url")
    except Exception:
        pass
    return None


async def enrich_tm_venues(sem, client, venue_id, source_id):
    async with sem:
        url = await fetch_venue_url_from_event(client, source_id)
        await asyncio.sleep(0.2)  # ~5 req/s
        return venue_id, url


async def enrich_search_venues(sem, client, venue_id, name, city, country):
    async with sem:
        url = await search_venue_url(client, name, city, country)
        await asyncio.sleep(0.2)
        return venue_id, url


async def main():
    with engine.connect() as conn:
        # --- Phase 1: TM-sourced venues ---
        print("Fetching TM venues with upcoming events...")
        rows = conn.execute(text("""
            SELECT v.id, MIN(e.source_id) as event_source_id
            FROM venues v
            JOIN events e ON e.venue_id = v.id
            WHERE e.scrape_source = 'ticketmaster'
              AND e.start_date >= date('now')
              AND (v.website_url IS NULL OR v.website_url = '')
              AND e.source_id IS NOT NULL
            GROUP BY v.id
        """)).fetchall()
        print(f"  → {len(rows)} TM venues to enrich")

        sem = asyncio.Semaphore(CONCURRENCY)
        async with httpx.AsyncClient() as client:
            tasks = [enrich_tm_venues(sem, client, vid, sid) for vid, sid in rows]
            results = []
            batch = 100
            for i in range(0, len(tasks), batch):
                chunk = await asyncio.gather(*tasks[i:i+batch])
                results.extend(chunk)
                updated = sum(1 for _, u in results if u)
                print(f"  Processed {min(i+batch, len(tasks))}/{len(tasks)} — {updated} URLs found", end="\r")

        print()
        updated_tm = 0
        for venue_id, url in results:
            if url:
                conn.execute(text(
                    "UPDATE venues SET website_url = :url WHERE id = :id"
                ), {"url": url, "id": venue_id})
                updated_tm += 1
        conn.commit()
        print(f"Phase 1 done: updated {updated_tm} TM venues")

        # --- Phase 2: Non-TM venues with upcoming events ---
        print("\nFetching non-TM venues with upcoming events...")
        rows2 = conn.execute(text("""
            SELECT DISTINCT v.id, v.name, v.physical_city, v.physical_country
            FROM venues v
            JOIN events e ON e.venue_id = v.id
            WHERE e.start_date >= date('now')
              AND (v.website_url IS NULL OR v.website_url = '')
              AND v.name IS NOT NULL
            LIMIT 5000
        """)).fetchall()
        print(f"  → {len(rows2)} non-TM venues to search")

        tasks2 = [enrich_search_venues(sem, client, vid, name, city, country)
                  for vid, name, city, country in rows2]
        results2 = []
        async with httpx.AsyncClient() as client:
            for i in range(0, len(tasks2), batch):
                chunk = await asyncio.gather(*tasks2[i:i+batch])
                results2.extend(chunk)
                updated = sum(1 for _, u in results2 if u)
                print(f"  Processed {min(i+batch, len(tasks2))}/{len(tasks2)} — {updated} URLs found", end="\r")

        print()
        updated_other = 0
        for venue_id, url in results2:
            if url:
                conn.execute(text(
                    "UPDATE venues SET website_url = :url WHERE id = :id"
                ), {"url": url, "id": venue_id})
                updated_other += 1
        conn.commit()
        print(f"Phase 2 done: updated {updated_other} non-TM venues")
        print(f"\nTotal: {updated_tm + updated_other} venues enriched")


if __name__ == "__main__":
    asyncio.run(main())
