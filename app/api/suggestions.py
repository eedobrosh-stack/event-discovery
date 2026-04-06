from typing import List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import EventType, Performer, Venue

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
    performers = (
        db.query(Performer.name, Performer.event_type_name)
        .filter(Performer.name.ilike(q_like))
        .limit(PER_TYPE)
        .all()
    )
    artists = [{"kind": "performer", "value": name, "label": name, "badge": "Artist"} for name, _ in performers]

    # 4. Venues
    venues = (
        db.query(Venue.name, Venue.physical_city)
        .filter(Venue.name.ilike(q_like))
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
