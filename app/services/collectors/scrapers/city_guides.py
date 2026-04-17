"""
Generic WordPress / Events-Calendar city-guide scraper.

Each entry in CITY_GUIDES defines one city's event listing site.
All supported sites use The Events Calendar WordPress plugin, which
outputs schema.org JSON-LD Event blocks and paginates via /page/N/.

URL pattern: {base_url}           → page 1
             {base_url}page/{N}/  → pages 2-N

Adding a new city:
  1. Verify the site outputs JSON-LD events (not JS-rendered).
  2. Add an entry to CITY_GUIDES: city name → CityGuideConfig.
  3. Deploy — no other changes needed.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent, safe_time

logger = logging.getLogger(__name__)


@dataclass
class CityGuideConfig:
    base_url: str    # trailing slash required
    max_pages: int = 5
    source_tag: str = ""   # e.g. "choosechicago" — used in source_id prefix


# City name → config
# All sites verified to return schema.org JSON-LD Event blocks server-side.
CITY_GUIDES: dict[str, CityGuideConfig] = {
    "Chicago": CityGuideConfig(
        base_url="https://www.choosechicago.com/events/",
        max_pages=5,
        source_tag="choosechicago",
    ),
    "Toronto": CityGuideConfig(
        base_url="https://nowtoronto.com/events/",
        max_pages=3,
        source_tag="nowtoronto",
    ),
    # Add more as verified:
    # "Seattle": CityGuideConfig(base_url="https://visitseattle.org/events/", source_tag="visitseattle"),
    # "New Orleans": CityGuideConfig(base_url="https://www.neworleans.com/events/", source_tag="neworleans"),
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_ONLINE_MODE = "https://schema.org/OnlineEventAttendanceMode"


def _parse_event(ev: dict, source_tag: str) -> RawEvent | None:
    if ev.get("eventStatus") == "https://schema.org/EventCancelled":
        return None
    if ev.get("eventAttendanceMode") == _ONLINE_MODE:
        return None

    start_str = ev.get("startDate", "")
    if not start_str:
        return None
    try:
        start_dt = datetime.fromisoformat(start_str)
    except ValueError:
        return None

    if start_dt.date() < date.today():
        return None

    end_dt = None
    end_str = ev.get("endDate", "")
    if end_str:
        try:
            end_dt = datetime.fromisoformat(end_str)
        except ValueError:
            pass

    location = ev.get("location") or {}
    address  = location.get("address") or {}
    geo      = location.get("geo") or {}

    venue_name    = location.get("name")
    venue_city    = address.get("addressLocality")
    venue_country = address.get("addressCountry")
    venue_address = address.get("streetAddress")
    venue_lat     = float(geo["latitude"])  if geo.get("latitude")  else None
    venue_lon     = float(geo["longitude"]) if geo.get("longitude") else None

    performers = ev.get("performer") or []
    if isinstance(performers, dict):
        performers = [performers]
    artist_name = performers[0].get("name") if performers else None

    offers = ev.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price    = None
    currency = "USD"
    low = offers.get("lowPrice") or offers.get("price")
    if low is not None:
        try:
            price = float(str(low).replace("$", "").replace(",", ""))
        except (TypeError, ValueError):
            pass
    currency = offers.get("priceCurrency", currency) or currency

    url = ev.get("url", "")
    # source_id: tag + last path segment of event URL
    slug = url.rstrip("/").split("/")[-1] if url else ""
    source_id = f"{source_tag}:{slug}" if slug else None

    return RawEvent(
        name=ev.get("name") or "Untitled Event",
        start_date=start_dt.date(),
        start_time=safe_time(start_dt),
        end_date=end_dt.date() if end_dt else None,
        end_time=safe_time(end_dt) if end_dt else None,
        artist_name=artist_name,
        price=price,
        price_currency=currency,
        purchase_link=url or None,
        image_url=ev.get("image"),
        description=ev.get("description"),
        venue_name=venue_name,
        venue_address=venue_address,
        venue_city=venue_city,
        venue_country=venue_country,
        venue_lat=venue_lat,
        venue_lon=venue_lon,
        source="city_guide",
        source_id=source_id,
        raw_categories=[],
    )


class CityGuideCollector(BaseCollector):
    """Collects events from WordPress/Events-Calendar city guide sites."""

    @property
    def source_name(self) -> str:
        return "city_guide"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        config = CITY_GUIDES.get(city_name)
        if not config:
            return []

        events: list[RawEvent] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            for page in range(1, config.max_pages + 1):
                if page == 1:
                    url = config.base_url
                else:
                    url = f"{config.base_url}page/{page}/"

                try:
                    resp = await client.get(url, headers=_HEADERS)
                except Exception as exc:
                    logger.warning(f"CityGuide: request error for {city_name} p{page}: {exc}")
                    break

                if resp.status_code != 200:
                    logger.warning(f"CityGuide: HTTP {resp.status_code} for {city_name} p{page}")
                    break

                soup = BeautifulSoup(resp.text, "lxml")
                blocks = soup.find_all("script", type="application/ld+json")

                page_count = 0
                for block in blocks:
                    try:
                        data = json.loads(block.string or "")
                    except (json.JSONDecodeError, TypeError):
                        continue
                    items = data if isinstance(data, list) else [data]
                    for ev in items:
                        if ev.get("@type") not in ("Event", "MusicEvent", "EventSeries"):
                            continue
                        raw = _parse_event(ev, config.source_tag)
                        if raw and raw.source_id and raw.source_id not in seen_ids:
                            seen_ids.add(raw.source_id)
                            events.append(raw)
                            page_count += 1

                logger.info(f"CityGuide ({config.source_tag}): {city_name} p{page} → {page_count} events")
                if page_count == 0:
                    break  # no events on this page — stop paginating

        return events
