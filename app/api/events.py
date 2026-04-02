from typing import Optional, List
from datetime import date, datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session, joinedload, selectinload

from app.database import get_db
from app.models import Event, EventType, Venue, Performer, event_event_types
from app.schemas.event import EventOut

router = APIRouter(prefix="/api/events", tags=["events"])


@router.get("", response_model=List[EventOut])
def list_events(
    categories: Optional[str] = Query(None, description="Comma-separated category names (legacy)"),
    type_search: Optional[str] = Query(None, description="Comma-separated terms; OR-searches event type name, category, and artist name"),
    city_ids: Optional[str] = Query(None, description="Comma-separated city IDs"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None,
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    from sqlalchemy import or_

    query = db.query(Event).options(
        joinedload(Event.venue),
        selectinload(Event.event_types),
    )

    # Legacy: exact category filter
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

    # Broad OR search: type name ILIKE OR category ILIKE OR artist_name ILIKE
    # Multiple terms are ANDed (each term must match at least one dimension)
    if type_search:
        from sqlalchemy import or_, and_, select
        terms = [t.strip() for t in type_search.split(",") if t.strip()]
        for term in terms:
            like = f"%{term}%"
            # Subquery: event IDs whose event_type name or category matches
            type_matched_event_ids = (
                select(event_event_types.c.event_id)
                .join(EventType, EventType.id == event_event_types.c.event_type_id)
                .where(or_(
                    EventType.name.ilike(like),
                    EventType.category.ilike(like),
                ))
                .scalar_subquery()
            )
            # Event matches if: type/category matches OR artist_name matches OR event name matches
            query = query.filter(or_(
                Event.id.in_(type_matched_event_ids),
                Event.artist_name.ilike(like),
                Event.name.ilike(like),
            ))

    if city_ids:
        ids = [int(x.strip()) for x in city_ids.split(",")]
        query = query.join(Venue, Event.venue_id == Venue.id).filter(
            Venue.city_id.in_(ids)
        )

    # Default: never show past events
    query = query.filter(Event.start_date >= date.today())

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
        types = e.event_types or []
        out.categories = list(dict.fromkeys(et.category for et in types if et.category))
        out.event_types = [et.name for et in types if et.name]
        if e.venue and e.venue.timezone:
            out.venue_timezone = e.venue.timezone
        if e.venue and e.venue.website_url:
            out.venue_website_url = e.venue.website_url
        results.append(out)
    return results
