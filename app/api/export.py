import csv
import io
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import or_, select
from sqlalchemy.orm import Session, joinedload, selectinload

from app.database import get_db
from app.models import Event, EventType, Venue, Performer, event_event_types
from app.schemas.event import ExportRequest
from app.services.export.ics_generator import generate_ics, generate_subscription_ics
from app.services.export.google_sheets import export_to_sheets
from app.api.auth import get_credentials, SESSION_COOKIE

router = APIRouter(prefix="/api/export", tags=["export"])


def _get_filtered_events(req: ExportRequest, db: Session) -> List[Event]:
    query = db.query(Event).options(
        joinedload(Event.venue),
        selectinload(Event.event_types),
    )

    if req.type_search:
        terms = [t.strip() for t in req.type_search.split(",") if t.strip()]
        for term in terms:
            like = f"%{term}%"
            type_matched_event_ids = (
                select(event_event_types.c.event_id)
                .join(EventType, EventType.id == event_event_types.c.event_type_id)
                .where(or_(EventType.name.ilike(like), EventType.category.ilike(like)))
                .scalar_subquery()
            )
            query = query.filter(or_(
                Event.id.in_(type_matched_event_ids),
                Event.artist_name.ilike(like),
                Event.name.ilike(like),
            ))

    if req.categories:
        type_ids = (
            db.query(EventType.id)
            .filter(EventType.category.in_(req.categories))
            .subquery()
        )
        query = query.join(event_event_types).filter(
            event_event_types.c.event_type_id.in_(
                db.query(type_ids.c.id)
            )
        )

    if req.city_ids:
        query = query.join(Venue, Event.venue_id == Venue.id).filter(
            Venue.city_id.in_(req.city_ids)
        )

    # Default: never show past events
    query = query.filter(Event.start_date >= date.today())

    if req.start_date:
        query = query.filter(Event.start_date >= req.start_date)
    if req.end_date:
        query = query.filter(Event.start_date <= req.end_date)

    return query.order_by(Event.start_date, Event.start_time).all()


def _get_filtered_events_from_params(
    db: Session,
    type_search: Optional[str] = None,
    city_ids: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> List[Event]:
    """Shared filter logic for GET-based subscription endpoint."""
    query = db.query(Event).options(
        joinedload(Event.venue),
        selectinload(Event.event_types),
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
            query = query.filter(or_(
                Event.id.in_(type_matched_event_ids),
                Event.artist_name.ilike(like),
                Event.name.ilike(like),
            ))

    if city_ids:
        ids = [int(x.strip()) for x in city_ids.split(",") if x.strip()]
        query = query.join(Venue, Event.venue_id == Venue.id).filter(
            Venue.city_id.in_(ids)
        )

    query = query.filter(Event.start_date >= date.today())
    if start_date:
        query = query.filter(Event.start_date >= start_date)
    if end_date:
        query = query.filter(Event.start_date <= end_date)

    return query.order_by(Event.start_date, Event.start_time).all()


def _subscription_label(type_search: Optional[str], city_ids: Optional[str], db: Session) -> str:
    """Build a human-readable calendar name from filter params."""
    parts = []
    if type_search:
        terms = [t.strip().title() for t in type_search.split(",") if t.strip()]
        parts.append(", ".join(terms))
    if city_ids:
        ids = [int(x.strip()) for x in city_ids.split(",") if x.strip()]
        cities = db.query(Venue.physical_city).filter(
            Venue.city_id.in_(ids)
        ).distinct().limit(3).all()
        city_names = [c[0] for c in cities if c[0]]
        if city_names:
            parts.append(", ".join(city_names))
    return " · ".join(parts) + " – Supercaly" if parts else "Supercaly Events"


@router.get("/subscribe")
def subscribe_calendar(
    type_search: Optional[str] = Query(None),
    city_ids: Optional[str] = Query(None),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
):
    """Live calendar feed for subscriptions. Returns ICS with auto-refresh headers."""
    events = _get_filtered_events_from_params(db, type_search, city_ids, start_date, end_date)
    name = _subscription_label(type_search, city_ids, db)
    ics_bytes = generate_subscription_ics(events, name=name)
    return Response(
        content=ics_bytes,
        media_type="text/calendar",
        headers={"Content-Disposition": f'inline; filename="supercaly.ics"'},
    )


@router.post("/ics")
def export_ics(req: ExportRequest, db: Session = Depends(get_db)):
    events = _get_filtered_events(req, db)
    ics_bytes = generate_ics(events)
    return Response(
        content=ics_bytes,
        media_type="text/calendar",
        headers={"Content-Disposition": "attachment; filename=events.ics"},
    )


@router.post("/csv")
def export_csv(req: ExportRequest, db: Session = Depends(get_db)):
    events = _get_filtered_events(req, db)

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "Event", "Artist", "Date", "Start Time", "End Time",
        "Venue", "City", "Country", "Price", "Currency",
        "Category", "Type", "Link", "YouTube",
    ])
    for e in events:
        venue = e.venue
        writer.writerow([
            e.name,
            e.artist_name or "",
            str(e.start_date) if e.start_date else "",
            e.start_time or "",
            e.end_time or "",
            e.venue_name or "",
            (venue.physical_city if venue else "") or "",
            (venue.physical_country if venue else "") or "",
            e.price if e.price is not None else "",
            e.price_currency or "",
            ", ".join(t.category for t in e.event_types) if e.event_types else "",
            ", ".join(t.name for t in e.event_types) if e.event_types else "",
            e.purchase_link or "",
            e.artist_youtube_channel or "",
        ])

    content = buf.getvalue()
    return Response(
        content=content.encode("utf-8-sig"),  # utf-8-sig adds BOM for Excel compatibility
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=supercaly_events.csv"},
    )


@router.post("/sheets")
def export_sheets(req: ExportRequest, request: Request, db: Session = Depends(get_db)):
    import traceback

    session_token = request.cookies.get(SESSION_COOKIE)
    print(f"[sheets] session_token from cookie: {session_token!r}")
    creds = get_credentials(session_token)
    print(f"[sheets] credentials found: {creds is not None}")

    if not creds:
        return {
            "needs_auth": True,
            "auth_url": "/api/auth/google",
            "message": "Please authorize Google Sheets access first.",
        }

    try:
        events = _get_filtered_events(req, db)
        print(f"[sheets] exporting {len(events)} events...")
        spreadsheet_url = export_to_sheets(events, title="Event Export", credentials=creds)
        return {
            "spreadsheet_url": spreadsheet_url,
            "event_count": len(events),
        }
    except Exception as e:
        traceback.print_exc()
        return {"message": f"Export failed: {str(e)}"}
