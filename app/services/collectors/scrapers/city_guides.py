"""
Generic city-guide scraper for sites that emit schema.org JSON-LD events.

Each entry in CITY_GUIDES maps a city name to one or more CityGuideConfig
objects.  Most supported sites use The Events Calendar WordPress plugin and
paginate via /page/N/.  Sites without pagination use max_pages=1.

URL pattern: {base_url}           → page 1
             {base_url}page/{N}/  → pages 2-N  (WordPress sites only)

Adding a new city:
  1. Verify the site outputs JSON-LD events server-side (not JS-rendered).
  2. Add a CityGuideConfig (or list of them) to CITY_GUIDES.
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


# City name → one or more configs
# All sites verified to return schema.org JSON-LD Event blocks server-side.
# Sites marked max_pages=1 serve all events on a single page (no /page/N/ path).
CITY_GUIDES: dict[str, list[CityGuideConfig]] = {
    "Chicago": [CityGuideConfig(
        base_url="https://www.choosechicago.com/events/",
        max_pages=10,
        source_tag="choosechicago",
    )],
    "Toronto": [CityGuideConfig(
        base_url="https://nowtoronto.com/events/",
        max_pages=3,
        source_tag="nowtoronto",
    )],
    "Melbourne": [
        CityGuideConfig(
            base_url="https://concreteplayground.com/melbourne/events/",
            max_pages=1,
            source_tag="concreteplayground_mel",
        ),
        CityGuideConfig(
            base_url="https://www.whatsoninmelbourne.com/events/",
            max_pages=3,
            source_tag="whatsoninmelbourne",
        ),
    ],
    "Sydney": [CityGuideConfig(
        base_url="https://concreteplayground.com/sydney/events/",
        max_pages=1,
        source_tag="concreteplayground_syd",
    )],
    "Brisbane": [CityGuideConfig(
        base_url="https://concreteplayground.com/brisbane/events/",
        max_pages=1,
        source_tag="concreteplayground_bne",
    )],
    # Add more as verified:
    # "Seattle": [CityGuideConfig(base_url="https://visitseattle.org/events/", source_tag="visitseattle")],
    # "New Orleans": [CityGuideConfig(base_url="https://www.neworleans.com/events/", source_tag="neworleans")],
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
        configs = CITY_GUIDES.get(city_name)
        if not configs:
            return []

        events: list[RawEvent] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            for config in configs:
                await self._collect_config(client, city_name, config, events, seen_ids)

        return events

    async def _collect_config(
        self,
        client: httpx.AsyncClient,
        city_name: str,
        config: CityGuideConfig,
        events: list[RawEvent],
        seen_ids: set[str],
    ) -> None:
        for page in range(1, config.max_pages + 1):
            url = config.base_url if page == 1 else f"{config.base_url}page/{page}/"

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
