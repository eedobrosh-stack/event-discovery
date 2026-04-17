"""
Allevents.in event scraper — parses the JSON-LD Event array (48 events)
embedded in city listing pages.  No API key required.

URL pattern: https://allevents.in/{city-slug}/

City slugs are generally lower-cased, hyphenated versions of city names.
Verified to return events for all listed cities.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent, safe_time

logger = logging.getLogger(__name__)

# City name → allevents.in URL slug
CITY_SLUGS: dict[str, str] = {
    # North America
    "New York":      "new-york",
    "Los Angeles":   "los-angeles",
    "Chicago":       "chicago",
    "San Francisco": "san-francisco",
    "Miami":         "miami",
    "Austin":        "austin",
    "Seattle":       "seattle",
    "Boston":        "boston",
    "Nashville":     "nashville",
    "Denver":        "denver",
    "Atlanta":       "atlanta",
    "Philadelphia":  "philadelphia",
    "Portland":      "portland",
    "Las Vegas":     "las-vegas",
    "Houston":       "houston",
    "Dallas":        "dallas",
    "Toronto":       "toronto",
    "Vancouver":     "vancouver",
    "Montreal":      "montreal",
    # Europe
    "London":        "london",
    "Berlin":        "berlin",
    "Paris":         "paris",
    "Amsterdam":     "amsterdam",
    "Barcelona":     "barcelona",
    "Madrid":        "madrid",
    "Dublin":        "dublin",
    "Vienna":        "vienna",
    "Prague":        "prague",
    "Budapest":      "budapest",
    "Lisbon":        "lisbon",
    "Rome":          "rome",
    "Milan":         "milan",
    "Brussels":      "brussels",
    "Edinburgh":     "edinburgh",
    "Manchester":    "manchester",
    # Middle East
    "Tel Aviv":      "tel-aviv",
    "Jerusalem":     "jerusalem",
    "Dubai":         "dubai",
    # Asia-Pacific
    "Tokyo":         "tokyo",
    "Seoul":         "seoul",
    "Singapore":     "singapore",
    "Sydney":        "sydney",
    "Melbourne":     "melbourne",
    "Bangkok":       "bangkok",
}

BASE_URL = "https://allevents.in"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_ONLINE_MODE = "https://schema.org/OnlineEventAttendanceMode"


def _parse_event(ev: dict) -> RawEvent | None:
    if ev.get("eventStatus") == "https://schema.org/EventCancelled":
        return None

    # Skip pure online events — we only want in-person
    if ev.get("eventAttendanceMode") == _ONLINE_MODE:
        return None

    start_str = ev.get("startDate", "")
    if not start_str:
        return None
    try:
        # startDate may be "YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS"
        if "T" in start_str:
            start_dt: datetime | None = datetime.fromisoformat(start_str)
        else:
            start_dt = datetime.combine(date.fromisoformat(start_str), datetime.min.time())
            start_dt = start_dt.replace(hour=0, minute=0, second=0)
    except ValueError:
        return None

    if start_dt.date() < date.today():
        return None

    end_dt = None
    end_str = ev.get("endDate", "")
    if end_str:
        try:
            if "T" in end_str:
                end_dt = datetime.fromisoformat(end_str)
            else:
                end_dt = datetime.combine(date.fromisoformat(end_str), datetime.min.time())
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

    # Allevents JSON-LD has no `performer` key; extract from organizer as fallback
    artist_name = None
    organizers = ev.get("organizer") or []
    if isinstance(organizers, dict):
        organizers = [organizers]
    # Only use organizer as artist if it looks like a real act, not a venue/promoter
    # (We leave this None — the dedup/enrichment pipeline will match via event name)

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
    # source_id = numeric ID at the end of the URL
    source_id = url.rstrip("/").split("/")[-1] if url else None

    # start_time: only set if the startDate had a time component
    has_time = "T" in (ev.get("startDate") or "")
    start_time = safe_time(start_dt) if has_time else None
    end_time   = safe_time(end_dt)   if (end_dt and "T" in end_str) else None

    return RawEvent(
        name=ev.get("name") or "Untitled Event",
        start_date=start_dt.date(),
        start_time=start_time,
        end_date=end_dt.date() if end_dt else None,
        end_time=end_time,
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
        source="allevents",
        source_id=source_id,
        raw_categories=[],
    )


class AlleventsCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "allevents"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        slug = CITY_SLUGS.get(city_name)
        if not slug:
            return []

        events: list[RawEvent] = []
        seen_ids: set[str] = set()

        url = f"{BASE_URL}/{slug}/"
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(url, headers=_HEADERS)
        except Exception as exc:
            logger.warning(f"Allevents: request error for {city_name}: {exc}")
            return []

        if resp.status_code != 200:
            logger.warning(f"Allevents: HTTP {resp.status_code} for {city_name}")
            return []

        soup = BeautifulSoup(resp.text, "lxml")
        blocks = soup.find_all("script", type="application/ld+json")

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

        logger.info(f"Allevents: {city_name} → {len(events)} events")
        return events
