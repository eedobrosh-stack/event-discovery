from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta


def safe_time(dt: datetime | None) -> str | None:
    """Return HH:MM only if the time is genuinely known (not midnight 00:00)."""
    if dt is None:
        return None
    t = dt.strftime("%H:%M")
    return None if t == "00:00" else t


def default_end_time(start_time: str | None, start_date: date | None,
                     end_date: date | None) -> tuple[date | None, str | None]:
    """If end time is unknown but start time is known, default end = start + 2h."""
    if start_time is None:
        return end_date, None
    try:
        h, m = map(int, start_time.split(":"))
        base = datetime(
            start_date.year, start_date.month, start_date.day, h, m
        )
        end_dt = base + timedelta(hours=2)
        return end_dt.date(), end_dt.strftime("%H:%M")
    except Exception:
        return end_date, None


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
    venue_website_url: str | None = None
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
