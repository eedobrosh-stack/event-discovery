from __future__ import annotations
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

# Suffixes that indicate the preceding words are the artist/bandleader name
_ENSEMBLE_SUFFIXES = re.compile(
    r"""^(.+?)\s+
    (Trio|Quartet|Quintet|Sextet|Septet|Octet|Nonet|
     Duo|Band|Big\s+Band|Group|Ensemble|Orchestra|
     Project|Experience|Connection|Collective|
     Jazz\s+Quartet|Jazz\s+Trio|String\s+Quartet)
    (\s|$)""",
    re.IGNORECASE | re.VERBOSE,
)
# Suffixes that explicitly mean the artist is NOT performing
_NO_ARTIST_SUFFIXES = re.compile(
    r"\b(Tribute|Memorial|Festival|Night|Evening|Showcase|Session|Workshop)\b",
    re.IGNORECASE,
)


def infer_artist_from_name(name: str) -> str | None:
    """
    Try to extract an artist/bandleader name from an event title.
    e.g. "Yonatan Riklis Trio" → "Yonatan Riklis"
         "Chick Corea Tribute" → None  (tribute, not the real artist)
    """
    if not name:
        return None
    # Bail if the name signals a tribute / festival / generic event
    if _NO_ARTIST_SUFFIXES.search(name):
        return None
    m = _ENSEMBLE_SUFFIXES.match(name.strip())
    if m:
        candidate = m.group(1).strip()
        # Sanity-check: must look like a name (2+ chars, not all caps acronym)
        if len(candidate) >= 2:
            return candidate
    return None


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
