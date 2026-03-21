from typing import Optional
from pydantic import BaseModel


class CityOut(BaseModel):
    id: int
    name: str
    country: str
    state: Optional[str] = None
    timezone: Optional[str] = None

    model_config = {"from_attributes": True}
