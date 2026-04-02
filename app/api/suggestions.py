from typing import List
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import EventType, Performer

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
    results = []

    # 1. Categories (distinct values)
    cats = (
        db.query(EventType.category)
        .filter(EventType.category.ilike(q_like))
        .distinct()
        .limit(4)
        .all()
    )
    for (cat,) in cats:
        results.append({"kind": "category", "value": cat, "label": cat, "badge": "Category"})

    # 2. Event types
    types = (
        db.query(EventType.name, EventType.category)
        .filter(EventType.name.ilike(q_like))
        .distinct()
        .limit(5)
        .all()
    )
    for name, cat in types:
        results.append({"kind": "event_type", "value": name, "label": name, "badge": "Type"})

    # 3. Performers / Artists
    performers = (
        db.query(Performer.name, Performer.event_type_name)
        .filter(Performer.name.ilike(q_like))
        .limit(6)
        .all()
    )
    for name, type_name in performers:
        results.append({"kind": "performer", "value": name, "label": name, "badge": "Artist"})

    return results[:limit]
