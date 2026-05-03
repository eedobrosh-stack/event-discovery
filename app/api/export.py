import csv
import io
from typing import List, Optional
from datetime import date
from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import Response, StreamingResponse
from sqlalchemy import or_, func, select
from sqlalchemy.orm import Session, joinedload, selectinload

from app.database import get_db
from app.models import Event, EventType, Venue, Performer, City, event_event_types
from app.schemas.event import ExportRequest
from app.api._search_filters import resolve_genre_artist_names
from app.services.export.ics_generator import generate_ics, generate_subscription_ics
from app.services.export.google_sheets import export_to_sheets
from app.api.auth import get_credentials, SESSION_COOKIE

router = APIRouter(prefix="/api/export", tags=["export"])


def _get_filtered_events(req: ExportRequest, db: Session) -> List[Event]:
    query = db.query(Event).options(
        joinedload(Event.venue).joinedload(Venue.city),
        selectinload(Event.event_types),
    )

    # Strict artist filter (set when the user picked an Artist autocomplete
    # suggestion). Exact case-insensitive match on artist_name only.
    if req.artist_exact:
        names = [n.strip() for n in req.artist_exact.split(",") if n.strip()]
        if names:
            lowered = [n.lower() for n in names]
            query = query.filter(
                Event.artist_name.isnot(None),
                func.lower(Event.artist_name).in_(lowered),
            )

    # Genre filter — same parent → sub-genres → artists expansion as /api/events.
    artist_norms = resolve_genre_artist_names(db, req.genres)
    if artist_norms is not None:
        if artist_norms:
            query = query.filter(
                Event.artist_name.isnot(None),
                func.lower(Event.artist_name).in_(artist_norms),
            )
        else:
            query = query.filter(False)

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
        # Use a subquery instead of a direct JOIN to avoid conflicting with
        # the joinedload(Event.venue) eager-load join already on this query.
        city_matched_event_ids = (
            select(Event.id)
            .join(Venue, Event.venue_id == Venue.id)
            .where(Venue.city_id.in_(req.city_ids))
            .scalar_subquery()
        )
        query = query.filter(Event.id.in_(city_matched_event_ids))

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
    artist_exact: Optional[str] = None,
    genres: Optional[str] = None,
) -> List[Event]:
    """Shared filter logic for GET-based subscription endpoint."""
    query = db.query(Event).options(
        joinedload(Event.venue).joinedload(Venue.city),
        selectinload(Event.event_types),
    )

    if artist_exact:
        names = [n.strip() for n in artist_exact.split(",") if n.strip()]
        if names:
            lowered = [n.lower() for n in names]
            query = query.filter(
                Event.artist_name.isnot(None),
                func.lower(Event.artist_name).in_(lowered),
            )

    artist_norms = resolve_genre_artist_names(db, genres)
    if artist_norms is not None:
        if artist_norms:
            query = query.filter(
                Event.artist_name.isnot(None),
                func.lower(Event.artist_name).in_(artist_norms),
            )
        else:
            query = query.filter(False)

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
        ids = [int(x.strip()) for x in city_ids.split(",") if x.strip()]
        city_matched_event_ids = (
            select(Event.id)
            .join(Venue, Event.venue_id == Venue.id)
            .where(Venue.city_id.in_(ids))
            .scalar_subquery()
        )
        query = query.filter(Event.id.in_(city_matched_event_ids))

    query = query.filter(Event.start_date >= date.today())
    if start_date:
        query = query.filter(Event.start_date >= start_date)
    if end_date:
        query = query.filter(Event.start_date <= end_date)

    return query.order_by(Event.start_date, Event.start_time).all()


def _subscription_label(type_search: Optional[str], city_ids: Optional[str], db: Session, artist_exact: Optional[str] = None, genres: Optional[str] = None) -> str:
    """Build a human-readable calendar name from filter params."""
    parts = []
    if artist_exact:
        # Artist names already canonical-cased — don't .title() (e.g. "AC/DC")
        names = [n.strip() for n in artist_exact.split(",") if n.strip()]
        if names:
            parts.append(", ".join(names))
    if genres:
        # Parent genres are canonical-cased in the taxonomy already.
        gs = [g.strip() for g in genres.split(",") if g.strip()]
        if gs:
            parts.append(", ".join(gs))
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
    artist_exact: Optional[str] = Query(None),
    genres: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Live calendar feed for subscriptions. Returns ICS with auto-refresh headers."""
    events = _get_filtered_events_from_params(
        db, type_search, city_ids, start_date, end_date,
        artist_exact=artist_exact, genres=genres,
    )
    name = _subscription_label(type_search, city_ids, db, artist_exact=artist_exact, genres=genres)
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

    # Bulk-fetch artist genres for all events being exported. One IN-query;
    # mirrors the per-page approach used in /api/events.
    from app.models.genre import ArtistGenre
    artist_lowered = {e.artist_name.lower() for e in events if e.artist_name}
    artist_genre_map: dict[str, str] = {}
    if artist_lowered:
        rows = (
            db.query(ArtistGenre.normalized_name, ArtistGenre.primary_genre)
            .filter(ArtistGenre.normalized_name.in_(artist_lowered))
            .all()
        )
        artist_genre_map = {n: g for (n, g) in rows if g and g != "UNKNOWN"}

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "Event", "Artist", "Date", "Start Time", "End Time",
        "Venue", "City", "Country", "Price", "Currency",
        "Category", "Format", "Genre", "Link", "YouTube",
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
            (artist_genre_map.get(e.artist_name.lower()) if e.artist_name else "") or "",
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
