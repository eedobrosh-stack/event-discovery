from typing import List
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import City
from app.schemas.city import CityOut

router = APIRouter(prefix="/api/cities", tags=["cities"])


@router.get("", response_model=List[CityOut])
def list_cities(db: Session = Depends(get_db)):
    return db.query(City).order_by(City.name).all()
