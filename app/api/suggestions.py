from typing import List
from fastapi import APIRouter, Depends, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Event, EventType, Performer, Venue

router = APIRouter(prefix="/api/suggestions", tags=["suggestions"])


@router.get("")
def get_suggestions(
    q: str = Query(..., min_length=1),
    limit: int = Query(12, le=30),
    db: Session = Depends(get_db),
):
    """
    Returns autocomplete suggestions mixing:
      - Event categories  (badge: "Category")
      - Event types       (badge: "Type")
      - Performer names   (badge: "Artist")
      - Venues            (badge: "Venue")
    Only shows artists and venues that have at least one event in the DB.
    """
    q_like = f"%{q}%"
    PER_TYPE = 3

    # 1. Categories (distinct values)
    cats = (
        db.query(EventType.category)
        .filter(EventType.category.ilike(q_like))
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    categories = [{"kind": "category", "value": cat, "label": cat, "badge": "Category"} for (cat,) in cats]

    # 2. Event types
    types = (
        db.query(EventType.name, EventType.category)
        .filter(EventType.name.ilike(q_like))
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    event_types = [{"kind": "event_type", "value": name, "label": name, "badge": "Type"} for name, _ in types]

    # 3. Performers / Artists
    # Primary: Performer records that have at least one matching event
    performers = (
        db.query(Performer.name)
        .filter(
            Performer.name.ilike(q_like),
            db.query(Event.id)
              .filter(func.lower(Event.artist_name) == func.lower(Performer.name))
              .exists(),
        )
        .limit(PER_TYPE)
        .all()
    )
    performer_names_lower = {name.lower() for (name,) in performers}

    # Fallback: artist_names from events directly (catches artists with no Performer record)
    event_artists = (
        db.query(Event.artist_name)
        .filter(
            Event.artist_name.isnot(None),
            Event.artist_name.ilike(q_like),
        )
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    # Merge, avoiding duplicates already covered by Performer records
    for (name,) in event_artists:
        if name and name.lower() not in performer_names_lower:
            performers.append((name,))
            performer_names_lower.add(name.lower())

    artists = [
        {"kind": "performer", "value": name, "label": name, "badge": "Artist"}
        for (name,) in performers[:PER_TYPE]
    ]

    # 4. Venues — only those with at least one event
    venues = (
        db.query(Venue.name, Venue.physical_city)
        .filter(
            Venue.name.ilike(q_like),
            db.query(Event.id)
              .filter(Event.venue_id == Venue.id)
              .exists(),
        )
        .distinct()
        .limit(PER_TYPE)
        .all()
    )
    venue_results = [
        {"kind": "venue", "value": name, "label": f"{name} — {city}" if city else name, "badge": "Venue"}
        for name, city in venues
    ]

    # Interleave so all types get representation before the cap
    results = categories + event_types + artists + venue_results
    return results[:limit]
