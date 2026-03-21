"""Manual trigger for running all collectors."""
import sys
import os
import asyncio
import argparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models import City
from app.scheduler.jobs import registry, collect_all_events


async def run(city_name, enrich_youtube):
    if city_name:
        db = SessionLocal()
        try:
            city = db.query(City).filter(City.name.ilike(f"%{city_name}%")).first()
            if not city:
                print(f"City '{city_name}' not found")
                return
            print(f"Collecting events for {city.name}...")
            stats = await registry.collect_all(city, db)
            print(f"Collection stats: {stats}")

            if enrich_youtube:
                print("Running YouTube enrichment...")
                count = await registry.enrich_youtube(db)
                print(f"YouTube enrichment: {count} artists linked")
        finally:
            db.close()
    else:
        await collect_all_events()
        if enrich_youtube:
            db = SessionLocal()
            try:
                print("Running YouTube enrichment...")
                count = await registry.enrich_youtube(db)
                print(f"YouTube enrichment: {count} artists linked")
            finally:
                db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run event collection")
    parser.add_argument("--city", type=str, help="Collect for a specific city (e.g. 'New York')")
    parser.add_argument("--enrich-youtube", action="store_true", default=True,
                        help="Run YouTube enrichment after collection (default: True)")
    parser.add_argument("--no-youtube", action="store_true",
                        help="Skip YouTube enrichment")
    args = parser.parse_args()

    enrich = args.enrich_youtube and not args.no_youtube
    asyncio.run(run(args.city, enrich))
