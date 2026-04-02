"""
Enriches venue website_url using OpenStreetMap Nominatim.
Nominatim policy: max 1 request/second, must set User-Agent.
"""
import asyncio
import sys
import os
import urllib.parse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from sqlalchemy import text
from app.database import engine

HEADERS = {"User-Agent": "Supercaly/1.0 (event discovery app)"}
DELAY = 1.1  # Nominatim policy: max 1 req/s


async def lookup_venue(client, name, city, country):
    query = name
    if city:
        query += f" {city}"
    if country and len(country) <= 3:
        query += f" {country}"

    try:
        resp = await client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": query, "format": "json", "limit": 1, "extratags": 1},
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code == 200:
            results = resp.json()
            if results:
                extratags = results[0].get("extratags") or {}
                return extratags.get("website") or extratags.get("url") or extratags.get("contact:website")
    except Exception:
        pass
    return None


async def main():
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT v.id, v.name, v.physical_city, v.physical_country,
                   COUNT(e.id) as event_count
            FROM venues v
            JOIN events e ON e.venue_id = v.id
            WHERE e.start_date >= date('now')
              AND (v.website_url IS NULL OR v.website_url = '')
              AND v.name IS NOT NULL
            GROUP BY v.id
            ORDER BY event_count DESC
        """)).fetchall()

        # rows columns: id, name, city, country, event_count
        rows = [(r[0], r[1], r[2], r[3]) for r in rows]
        total = len(rows)
        print(f"Enriching {total} venues via OSM Nominatim (1 req/s)...")
        print(f"Estimated time: ~{total // 60} min {total % 60} sec\n")

        updated = 0
        save_every = 100

        async with httpx.AsyncClient() as client:
            for i, (venue_id, name, city, country) in enumerate(rows):
                url = await lookup_venue(client, name, city, country)
                await asyncio.sleep(DELAY)

                if url:
                    conn.execute(text(
                        "UPDATE venues SET website_url = :url WHERE id = :id"
                    ), {"url": url, "id": venue_id})
                    updated += 1

                if (i + 1) % save_every == 0:
                    conn.commit()
                    pct = (i + 1) * 100 // total
                    print(f"  {i+1}/{total} ({pct}%) — {updated} URLs found", flush=True)

        conn.commit()
        print(f"\nDone: updated {updated}/{total} venues ({updated*100//total}% hit rate)")


if __name__ == "__main__":
    asyncio.run(main())
