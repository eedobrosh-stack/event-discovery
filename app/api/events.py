from typing import Optional, List
from datetime import date
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session, joinedload, selectinload

from app.database import get_db
from app.models import Event, EventType, Venue, event_event_types
from app.schemas.event import EventOut

router = APIRouter(prefix="/api/events", tags=["events"])


@router.get("", response_model=List[EventOut])
def list_events(
    categories: Optional[str] = Query(None, description="Comma-separated category names"),
    city_ids: Optional[str] = Query(None, description="Comma-separated city IDs"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None,
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    query = db.query(Event).options(
        joinedload(Event.venue),
        selectinload(Event.event_types),
    )

    if categories:
        cat_list = [c.strip() for c in categories.split(",")]
        type_ids = (
            db.query(EventType.id)
            .filter(EventType.category.in_(cat_list))
            .subquery()
        )
        query = query.join(event_event_types).filter(
            event_event_types.c.event_type_id.in_(
                db.query(type_ids.c.id)
            )
        )

    if city_ids:
        ids = [int(x.strip()) for x in city_ids.split(",")]
        query = query.join(Venue, Event.venue_id == Venue.id).filter(
            Venue.city_id.in_(ids)
        )

    if start_date:
        query = query.filter(Event.start_date >= start_date)
    if end_date:
        query = query.filter(Event.start_date <= end_date)
    if search:
        query = query.filter(Event.name.ilike(f"%{search}%"))

    events = (
        query.order_by(Event.start_date, Event.start_time)
        .offset(offset)
        .limit(limit)
        .all()
    )

    results = []
    for e in events:
        out = EventOut.model_validate(e)
        out.categories = [et.category for et in e.event_types if et.category]
        results.append(out)
    return results
