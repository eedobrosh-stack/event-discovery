from typing import Optional
from datetime import datetime
from pydantic import BaseModel


class VenueOut(BaseModel):
    id: int
    name: str
    city_id: int
    timezone: Optional[str] = None
    website_url: Optional[str] = None
    street_address: Optional[str] = None
    physical_city: Optional[str] = None
    physical_country: Optional[str] = None
    venue_type: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    phone: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}
