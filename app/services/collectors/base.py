from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date


@dataclass
class RawEvent:
    """Normalized event data from any source."""
    name: str
    start_date: date
    start_time: str | None = None
    end_date: date | None = None
    end_time: str | None = None
    artist_name: str | None = None
    description: str | None = None
    price: float | None = None
    price_currency: str = "USD"
    purchase_link: str | None = None
    image_url: str | None = None
    is_online: bool = False
    venue_name: str | None = None
    venue_address: str | None = None
    venue_city: str | None = None
    venue_country: str | None = None
    venue_lat: float | None = None
    venue_lon: float | None = None
    source: str = ""
    source_id: str = ""
    raw_categories: list[str] = field(default_factory=list)


class BaseCollector(ABC):
    """All data sources implement this interface."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Unique identifier: 'eventbrite', 'ticketmaster', etc."""

    @abstractmethod
    async def collect(self, city_name: str, country_code: str, **kwargs) -> list[RawEvent]:
        """Fetch events for a given city."""

    def is_configured(self) -> bool:
        """Return True if this collector has the required API keys."""
        return True
