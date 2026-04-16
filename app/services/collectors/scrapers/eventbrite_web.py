"""
Eventbrite web scraper — replaces the defunct v3 search API.

Scrapes the Eventbrite city browse page and extracts events from the
window.__SERVER_DATA__ JSON-LD payload embedded in the HTML.
Returns up to _MAX_PAGES * ~40 events per city run.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime

import httpx

from app.services.collectors.base import BaseCollector, RawEvent, safe_time
from app.services.collectors.category_mapper import map_category

logger = logging.getLogger(__name__)

_MAX_PAGES = 3   # ~120 events per city run
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# city name → Eventbrite URL slug (state/country--city)
CITY_SLUGS: dict[str, str] = {
    # North America
    "New York":        "ny--new-york",
    "Los Angeles":     "ca--los-angeles",
    "Chicago":         "il--chicago",
    "San Francisco":   "ca--san-francisco",
    "Berkeley":        "ca--berkeley",
    "Miami":           "fl--miami",
    "Austin":          "tx--austin",
    "Seattle":         "wa--seattle",
    "Boston":          "ma--boston",
    "Denver":          "co--denver",
    "Atlanta":         "ga--atlanta",
    "Portland":        "or--portland",
    "Nashville":       "tn--nashville",
    "Las Vegas":       "nv--las-vegas",
    "Toronto":         "ontario--toronto",
    "Vancouver":       "british-columbia--vancouver",
    "Montreal":        "quebec--montreal",
    # Europe
    "London":          "united-kingdom--london",
    "Berlin":          "germany--berlin",
    "Paris":           "france--paris",
    "Amsterdam":       "netherlands--amsterdam",
    "Barcelona":       "spain--barcelona",
    "Madrid":          "spain--madrid",
    "Lisbon":          "portugal--lisbon",
    "Vienna":          "austria--vienna",
    "Prague":          "czech-republic--prague",
    "Budapest":        "hungary--budapest",
    "Dublin":          "ireland--dublin",
    "Zurich":          "switzerland--zurich",
    # Middle East
    "Tel Aviv":        "israel--tel-aviv",
    "Jerusalem":       "israel--jerusalem",
    # Asia-Pacific
    "Sydney":          "new-south-wales--sydney",
    "Melbourne":       "victoria--melbourne",
    "Tokyo":           "japan--tokyo",
    "Seoul":           "south-korea--seoul",
    "Singapore":       "singapore--singapore",
}


def _extract_server_data(html: str) -> dict:
    marker = "__SERVER_DATA__ = "
    idx = html.find(marker)
    if idx == -1:
        return {}
    try:
        decoder = json.JSONDecoder()
        data, _ = decoder.raw_decode(html[idx + len(marker):])
        return data
    except Exception:
        return {}


def _parse_event(item: dict) -> RawEvent | None:
    ev = item.get("item", item)   # jsonld wraps in {position, item}

    start_str = ev.get("startDate", "")
    if not start_str:
        return None
    try:
        # startDate can be "YYYY-MM-DD" or full ISO datetime
        if "T" in start_str:
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            start_d = start_dt.date()
            start_t = safe_time(start_dt)
        else:
            start_d = date.fromisoformat(start_str[:10])
            start_t = None
    except ValueError:
        return None

    if start_d < date.today():
        return None

    end_str = ev.get("endDate", "")
    end_d = end_t = None
    if end_str:
        try:
            if "T" in end_str:
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                end_d = end_dt.date()
                end_t = safe_time(end_dt)
            else:
                end_d = date.fromisoformat(end_str[:10])
        except ValueError:
            pass

    location = ev.get("location") or {}
    address = location.get("address") or {}
    geo = location.get("geo") or {}

    venue_name = location.get("name")
    venue_city = address.get("addressLocality")
    venue_country = address.get("addressCountry")
    venue_address = address.get("streetAddress")
    venue_lat = float(geo["latitude"]) if geo.get("latitude") else None
    venue_lon = float(geo["longitude"]) if geo.get("longitude") else None

    # Eventbrite JSON-LD has no explicit category — infer from description keywords
    raw_cats: list[str] = []
    desc = (ev.get("description") or "").lower()
    for kw, cat in [("music", "Music"), ("concert", "Music"), ("comedy", "Comedy"),
                    ("art", "Art"), ("dance", "Dance"), ("film", "Film"),
                    ("food", "Food & Drink"), ("tech", "Technology"),
                    ("sport", "Sports"), ("festival", "Festival")]:
        if kw in desc:
            raw_cats.append(cat)
            break

    return RawEvent(
        name=ev.get("name") or ev.get("description", "")[:80] or "Untitled Event",
        start_date=start_d,
        start_time=start_t,
        end_date=end_d,
        end_time=end_t,
        description=ev.get("description"),
        purchase_link=ev.get("url"),
        image_url=ev.get("image"),
        is_online=("online" in (ev.get("eventAttendanceMode") or "").lower()),
        venue_name=venue_name,
        venue_address=venue_address,
        venue_city=venue_city,
        venue_country=venue_country,
        venue_lat=venue_lat,
        venue_lon=venue_lon,
        source="eventbrite",
        source_id=ev.get("url", "").rstrip("/").split("/")[-1] or None,
        raw_categories=raw_cats,
    )


class EventbriteWebScraper(BaseCollector):

    @property
    def source_name(self) -> str:
        return "eventbrite"

    def is_configured(self) -> bool:
        return True   # no API key needed

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        slug = CITY_SLUGS.get(city_name)
        if not slug:
            return []

        events: list[RawEvent] = []
        seen_ids: set[str] = set()

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            for page in range(1, _MAX_PAGES + 1):
                url = f"https://www.eventbrite.com/d/{slug}/events/"
                params = {"page": page} if page > 1 else {}
                try:
                    resp = await client.get(url, headers=_HEADERS, params=params)
                    if resp.status_code != 200:
                        logger.warning(f"EventbriteWeb: {resp.status_code} for {city_name} p{page}")
                        break
                except Exception as e:
                    logger.warning(f"EventbriteWeb: request error for {city_name}: {e}")
                    break

                data = _extract_server_data(resp.text)
                jsonld = data.get("jsonld", [])
                if not jsonld:
                    break

                items = jsonld[0].get("itemListElement", [])
                if not items:
                    break

                page_count = 0
                for item in items:
                    raw = _parse_event(item)
                    if raw and raw.source_id not in seen_ids:
                        seen_ids.add(raw.source_id or "")
                        events.append(raw)
                        page_count += 1

                logger.info(f"EventbriteWeb: {city_name} p{page} → {page_count} events")

                # If fewer than 20 results, we've hit the last page
                if page_count < 20:
                    break

        return events
