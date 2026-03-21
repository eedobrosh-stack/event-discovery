from typing import List
from fastapi import APIRouter, Depends, Request
from fastapi.responses import Response
from sqlalchemy.orm import Session, joinedload, selectinload

from app.database import get_db
from app.models import Event, EventType, Venue, event_event_types
from app.schemas.event import ExportRequest
from app.services.export.ics_generator import generate_ics
from app.services.export.google_sheets import export_to_sheets
from app.api.auth import get_credentials, SESSION_COOKIE

router = APIRouter(prefix="/api/export", tags=["export"])


def _get_filtered_events(req: ExportRequest, db: Session) -> List[Event]:
    query = db.query(Event).options(
        joinedload(Event.venue),
        selectinload(Event.event_types),
    )

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

    if req.start_date:
        query = query.filter(Event.start_date >= req.start_date)
    if req.end_date:
        query = query.filter(Event.start_date <= req.end_date)

    return query.order_by(Event.start_date, Event.start_time).all()


@router.post("/ics")
def export_ics(req: ExportRequest, db: Session = Depends(get_db)):
    events = _get_filtered_events(req, db)
    ics_bytes = generate_ics(events)
    return Response(
        content=ics_bytes,
        media_type="text/calendar",
        headers={"Content-Disposition": "attachment; filename=events.ics"},
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
