"""
Backfills venue website_url for existing RA venues by querying RA GraphQL
using the event source_ids already in the DB.
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from sqlalchemy import text
from app.database import engine

RA_GRAPHQL = "https://ra.co/graphql"
CONCURRENCY = 3

QUERY = """
query GET_EVENT($id: ID!) {
  event(id: $id) {
    venue { id name contentUrl }
  }
}
"""


async def fetch_venue_url(client, sem, source_id):
    async with sem:
        try:
            resp = await client.post(
                RA_GRAPHQL,
                json={"query": QUERY, "variables": {"id": int(source_id)}},
                headers={"Content-Type": "application/json", "Referer": "https://ra.co"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                venue = (data.get("data") or {}).get("event", {}).get("venue") or {}
                content_url = venue.get("contentUrl")
                if content_url:
                    return f"https://ra.co{content_url}"
        except Exception:
            pass
        await asyncio.sleep(0.4)
        return None


async def main():
    with engine.connect() as conn:
        # Get one source_id per venue for RA events with upcoming dates
        rows = conn.execute(text("""
            SELECT v.id, MIN(e.source_id) as source_id
            FROM venues v
            JOIN events e ON e.venue_id = v.id
            WHERE e.scrape_source = 'resident_advisor'
              AND e.start_date >= date('now')
              AND (v.website_url IS NULL OR v.website_url = '')
              AND e.source_id IS NOT NULL
            GROUP BY v.id
        """)).fetchall()
        print(f"Backfilling {len(rows)} RA venues...")

        sem = asyncio.Semaphore(CONCURRENCY)
        updated = 0
        batch = 50

        async with httpx.AsyncClient() as client:
            for i in range(0, len(rows), batch):
                chunk = rows[i:i+batch]
                tasks = [fetch_venue_url(client, sem, sid) for _, sid in chunk]
                results = await asyncio.gather(*tasks)
                for (venue_id, _), url in zip(chunk, results):
                    if url:
                        conn.execute(text(
                            "UPDATE venues SET website_url = :url WHERE id = :id"
                        ), {"url": url, "id": venue_id})
                        updated += 1
                conn.commit()
                print(f"  Processed {min(i+batch, len(rows))}/{len(rows)} — {updated} URLs found", end="\r", flush=True)

        print(f"\nDone: updated {updated} RA venues")


if __name__ == "__main__":
    asyncio.run(main())
