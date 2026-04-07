from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import City, Event, Venue
from app.schemas.city import CityOut

router = APIRouter(prefix="/api/cities", tags=["cities"])


@router.get("", response_model=List[CityOut])
def list_cities(db: Session = Depends(get_db)):
    """Only return cities that have at least one venue with at least one event."""
    # One non-correlated subquery: collect all city_ids that have events,
    # then filter cities by that set — avoids per-row correlated EXISTS.
    active_city_ids = (
        db.query(Venue.city_id)
        .join(Event, Event.venue_id == Venue.id)
        .filter(Venue.city_id.isnot(None))
        .distinct()
        .scalar_subquery()
    )
    return (
        db.query(City)
        .filter(City.id.in_(active_city_ids))
        .order_by(City.name)
        .all()
    )
