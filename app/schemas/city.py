from typing import List, Optional
from pydantic import BaseModel


class CityOut(BaseModel):
    id: int
    name: str
    country: str
    state: Optional[str] = None
    timezone: Optional[str] = None
    # Other spellings the location autocomplete also matches on
    # ("תל אביב", "Tel Aviv-Yafo" → Tel Aviv). Never displayed.
    aliases: List[str] = []

    model_config = {"from_attributes": True}
