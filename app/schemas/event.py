from typing import Optional, List
from datetime import date, datetime
from pydantic import BaseModel


class EventOut(BaseModel):
    id: int
    name: str
    artist_name: Optional[str] = None
    artist_youtube_channel: Optional[str] = None
    start_date: date
    start_time: Optional[str] = None
    end_date: Optional[date] = None
    end_time: Optional[str] = None
    purchase_link: Optional[str] = None
    price: Optional[float] = None
    price_currency: Optional[str] = None
    venue_id: Optional[int] = None
    venue_name: Optional[str] = None
    venue_city: Optional[str] = None
    venue_country: Optional[str] = None
    venue_website_url: Optional[str] = None
    venue_timezone: Optional[str] = None
    image_url: Optional[str] = None
    is_online: bool = False
    scrape_source: Optional[str] = None
    source_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    categories: List[str] = []
    event_types: List[str] = []

    model_config = {"from_attributes": True, "arbitrary_types_allowed": True}

    @classmethod
    def model_validate(cls, obj, **kwargs):
        # Prevent Pydantic from touching the ORM relationship named 'event_types'
        data = {
            c.key: getattr(obj, c.key)
            for c in obj.__table__.columns
        } if hasattr(obj, "__table__") else obj
        return super().model_validate(data, **kwargs)


class ExportRequest(BaseModel):
    type_search: Optional[str] = None
    categories: List[str] = []
    city_ids: List[int] = []
    start_date: Optional[date] = None
    end_date: Optional[date] = None
