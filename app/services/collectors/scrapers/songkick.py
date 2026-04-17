"""
Songkick concert scraper — parses JSON-LD (schema.org MusicEvent) from
metro-area calendar pages.  No API key required.

Each metro-area page returns 51 events.  We paginate up to MAX_PAGES.
Metro area IDs verified by confirming city in the first event's location.

To find IDs for new cities: visit songkick.com, search for the city, and
copy the number from the metro-areas URL  e.g. /metro-areas/24426-uk-london
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent, safe_time

logger = logging.getLogger(__name__)

# metro-area slug → (city_name_in_db, slug)
# Only include IDs verified to return events for the correct city.
# Verified by checking addressLocality of the first returned event.
METRO_SLUGS: dict[str, str] = {
    "New York":      "7644-us-new-york-nyc",
    "Los Angeles":   "17835-us-los-angeles",
    "Chicago":       "9426-us-chicago",
    "London":        "24426-uk-london",
    "San Francisco": "26330-us-sf-bay-area",
    "Tel Aviv":      "33176-israel-tel-aviv-jaffa",
    "Paris":         "28909-france-paris",
    # IDs below could not be discovered from an Israeli IP (Songkick geo-locks
    # its search results). Add verified slugs here as they become known.
    # "Berlin":    "????-de-berlin",
    # "Amsterdam": "????-nl-amsterdam",
    # "Sydney":    "????-au-sydney",
    # "Toronto":   "????-ca-toronto",
}

BASE_URL = "https://www.songkick.com"
MAX_PAGES = 4   # 51 events × 4 pages = ~200 events per city

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _parse_event(ev: dict) -> RawEvent | None:
    """Parse a schema.org MusicEvent / Event dict → RawEvent."""
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
    if end_str and "T" in end_str:
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
            price = float(low)
        except (TypeError, ValueError):
            pass
    currency = offers.get("priceCurrency", currency) or currency

    event_type = ev.get("@type", "Event")
    raw_cats = ["Music"] if event_type == "MusicEvent" else []

    name = ev.get("name") or "Untitled Event"

    return RawEvent(
        name=name,
        start_date=start_dt.date(),
        start_time=safe_time(start_dt),
        end_date=end_dt.date() if end_dt else None,
        end_time=safe_time(end_dt) if end_dt else None,
        artist_name=artist_name,
        price=price,
        price_currency=currency,
        purchase_link=ev.get("url"),
        image_url=ev.get("image"),
        description=ev.get("description"),
        venue_name=venue_name,
        venue_address=venue_address,
        venue_city=venue_city,
        venue_country=venue_country,
        venue_lat=venue_lat,
        venue_lon=venue_lon,
        source="songkick",
        source_id=ev.get("url", "").rstrip("/").split("/")[-1].split("?")[0] or None,
        raw_categories=raw_cats,
    )


class SongkickCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "songkick"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        slug = METRO_SLUGS.get(city_name)
        if not slug:
            return []

        events: list[RawEvent] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            for page in range(1, MAX_PAGES + 1):
                url = f"{BASE_URL}/metro-areas/{slug}/calendar"
                params = {"page": page} if page > 1 else {}
                try:
                    resp = await client.get(url, headers=_HEADERS, params=params)
                except Exception as exc:
                    logger.warning(f"Songkick: request error for {city_name} p{page}: {exc}")
                    break

                if resp.status_code != 200:
                    logger.warning(f"Songkick: HTTP {resp.status_code} for {city_name} p{page}")
                    break

                soup = BeautifulSoup(resp.text, "lxml")
                blocks = soup.find_all("script", type="application/ld+json")
                if not blocks:
                    break

                page_count = 0
                for block in blocks:
                    try:
                        data = json.loads(block.string or "")
                    except (json.JSONDecodeError, TypeError):
                        continue
                    items = data if isinstance(data, list) else [data]
                    for ev in items:
                        if ev.get("@type") not in ("MusicEvent", "Event"):
                            continue
                        raw = _parse_event(ev)
                        if raw and raw.source_id and raw.source_id not in seen_ids:
                            seen_ids.add(raw.source_id)
                            events.append(raw)
                            page_count += 1

                logger.info(f"Songkick: {city_name} p{page} → {page_count} events")
                if page_count == 0:
                    break

                # Check if there's a next page
                next_link = soup.find("a", rel="next")
                if not next_link:
                    break

        return events
