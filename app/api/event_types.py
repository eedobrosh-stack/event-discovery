from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import distinct

from app.database import get_db
from app.models import EventType
from app.schemas.event_type import EventTypeOut

router = APIRouter(prefix="/api/event-types", tags=["event_types"])


@router.get("", response_model=List[EventTypeOut])
def list_event_types(db: Session = Depends(get_db)):
    return db.query(EventType).order_by(EventType.category, EventType.name).all()


@router.get("/categories", response_model=List[str])
def list_categories(db: Session = Depends(get_db)):
    rows = db.query(distinct(EventType.category)).order_by(EventType.category).all()
    return [r[0] for r in rows if r[0]]
