from typing import Optional
from pydantic import BaseModel


class EventTypeOut(BaseModel):
    id: int
    name: str
    category: Optional[str] = None

    model_config = {"from_attributes": True}
