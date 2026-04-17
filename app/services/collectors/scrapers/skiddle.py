"""
Skiddle event scraper — parses the single JSON-LD block (76 events) from
city listing pages.  UK-heavy but covers other European cities too.
No API key required.

URL pattern: https://www.skiddle.com/whats-on/{City}/
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent, safe_time

logger = logging.getLogger(__name__)

# City → Skiddle city slug (verified to return events)
CITY_SLUGS: dict[str, str] = {
    "London":       "London",
    "Manchester":   "Manchester",
    "Birmingham":   "Birmingham",
    "Glasgow":      "Glasgow",
    "Edinburgh":    "Edinburgh",
    "Bristol":      "Bristol",
    "Leeds":        "Leeds",
    "Liverpool":    "Liverpool",
    "Dublin":       "Dublin",
    "Belfast":      "Belfast",
    "Amsterdam":    "Amsterdam",
    "Berlin":       "Berlin",
    "Barcelona":    "Barcelona",
    "Madrid":       "Madrid",
    "Paris":        "Paris",
    "Ibiza":        "Ibiza",
}

BASE_URL = "https://www.skiddle.com"
MAX_PAGES = 3   # 76 events × 3 = ~228 per city

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}


def _parse_event(ev: dict) -> RawEvent | None:
    if ev.get("eventStatus") == "https://schema.org/EventCancelled":
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
    currency = "GBP"
    low = offers.get("lowPrice") or offers.get("price")
    if low is not None:
        try:
            price = float(str(low).replace("£", "").replace(",", ""))
        except (TypeError, ValueError):
            pass
    currency = offers.get("priceCurrency", currency) or currency

    url = ev.get("url", "")
    source_id = url.rstrip("/").split("/")[-1] if url else None

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
        source="skiddle",
        source_id=source_id,
        raw_categories=[],
    )


class SkiddleCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "skiddle"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        slug = CITY_SLUGS.get(city_name)
        if not slug:
            return []

        events: list[RawEvent] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            for page in range(1, MAX_PAGES + 1):
                url = f"{BASE_URL}/whats-on/{slug}/"
                params = {"page": page} if page > 1 else {}
                try:
                    resp = await client.get(url, headers=_HEADERS, params=params)
                except Exception as exc:
                    logger.warning(f"Skiddle: request error for {city_name} p{page}: {exc}")
                    break

                if resp.status_code != 200:
                    logger.warning(f"Skiddle: HTTP {resp.status_code} for {city_name} p{page}")
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
                        if ev.get("@type") not in ("Event", "MusicEvent"):
                            continue
                        raw = _parse_event(ev)
                        if raw and raw.source_id and raw.source_id not in seen_ids:
                            seen_ids.add(raw.source_id)
                            events.append(raw)
                            page_count += 1

                logger.info(f"Skiddle: {city_name} p{page} → {page_count} events")
                if page_count < 20:
                    break  # last page

        return events
