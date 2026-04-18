from typing import Optional, List
from datetime import date, datetime
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session, joinedload, selectinload

from app.database import get_db
from app.models import Event, EventType, Venue, Performer, event_event_types
from app.schemas.event import EventOut

router = APIRouter(prefix="/api/events", tags=["events"])


def _build_filter_query(db: Session, query, categories, type_search, city_ids, start_date, end_date, search, country=None):
    """Shared filter logic used by both list and count endpoints."""
    from sqlalchemy import or_, select
    from app.models import City

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

    if type_search:
        terms = [t.strip() for t in type_search.split(",") if t.strip()]
        for term in terms:
            like = f"%{term}%"
            type_matched_event_ids = (
                select(event_event_types.c.event_id)
                .join(EventType, EventType.id == event_event_types.c.event_type_id)
                .where(or_(EventType.name.ilike(like), EventType.category.ilike(like)))
                .scalar_subquery()
            )
            venue_matched_event_ids = (
                select(Event.id)
                .join(Venue, Event.venue_id == Venue.id)
                .where(Venue.name.ilike(like))
                .scalar_subquery()
            )
            query = query.filter(or_(
                Event.id.in_(type_matched_event_ids),
                Event.artist_name.ilike(like),
                Event.name.ilike(like),
                Event.id.in_(venue_matched_event_ids),
            ))

    if city_ids:
        ids = [int(x.strip()) for x in city_ids.split(",") if x.strip().isdigit()]
        query = query.join(Venue, Event.venue_id == Venue.id).filter(
            Venue.city_id.in_(ids)
        )
    elif country:
        # Country filter — join through venue→city and match country name
        query = (
            query
            .join(Venue, Event.venue_id == Venue.id)
            .join(City, Venue.city_id == City.id)
            .filter(City.country.ilike(country))
        )

    query = query.filter(Event.start_date >= date.today())
    if start_date:
        query = query.filter(Event.start_date >= start_date)
    if end_date:
        query = query.filter(Event.start_date <= end_date)
    if search:
        # If the search term is a sports league prefix (e.g. "NBA", "La Liga"),
        # use a strict prefix match "NBA - %" so we never match substrings like
        # "Birn**bau**m" or "R**NB**A Sundays".
        league_prefix = f"{search} -%"
        is_league = (
            db.query(Event.id)
            .filter(Event.sport.isnot(None), Event.name.ilike(league_prefix))
            .limit(1)
            .scalar()
        )
        if is_league:
            query = query.filter(Event.name.ilike(league_prefix))
        else:
            query = query.filter(Event.name.ilike(f"%{search}%"))

    return query


@router.get("/count")
def count_events(
    categories: Optional[str] = Query(None),
    type_search: Optional[str] = Query(None),
    city_ids: Optional[str] = Query(None),
    country: Optional[str] = Query(None, description="Filter by country name"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None,
    db: Session = Depends(get_db),
):
    from sqlalchemy import func
    query = _build_filter_query(
        db, db.query(func.count(Event.id.distinct())),
        categories, type_search, city_ids, start_date, end_date, search, country
    )
    return {"total": query.scalar() or 0}


@router.get("", response_model=List[EventOut])
def list_events(
    categories: Optional[str] = Query(None, description="Comma-separated category names (legacy)"),
    type_search: Optional[str] = Query(None, description="Comma-separated terms; OR-searches event type name, category, and artist name"),
    city_ids: Optional[str] = Query(None, description="Comma-separated city IDs"),
    country: Optional[str] = Query(None, description="Filter by country name"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    search: Optional[str] = None,
    limit: int = Query(50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    base_query = db.query(Event).options(
        joinedload(Event.venue),
        selectinload(Event.event_types),
    )
    query = _build_filter_query(
        db, base_query, categories, type_search, city_ids, start_date, end_date, search, country
    )

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
        if e.venue and e.venue.physical_city:
            out.venue_city = e.venue.physical_city
        if e.venue and e.venue.physical_country:
            out.venue_country = e.venue.physical_country
        results.append(out)
    return results
