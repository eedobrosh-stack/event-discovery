from typing import List, Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Venue
from app.schemas.venue import VenueOut

router = APIRouter(prefix="/api/venues", tags=["venues"])


@router.get("", response_model=List[VenueOut])
def list_venues(
    city_id: Optional[int] = Query(None),
    db: Session = Depends(get_db),
):
    q = db.query(Venue)
    if city_id:
        q = q.filter(Venue.city_id == city_id)
    return q.order_by(Venue.name).all()
