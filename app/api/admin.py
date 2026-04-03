from typing import Optional, List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models import Event, Venue, City, EventType
from app.scheduler.jobs import registry
from app.seed.cities import CITIES
from app.seed.event_types import EVENT_TYPES

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
