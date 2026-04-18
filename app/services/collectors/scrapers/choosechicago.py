"""
Choose Chicago events scraper — The Events Calendar (TEC) REST API.

Endpoint:
  GET https://www.choosechicago.com/wp-json/tribe/events/v1/events
    ?per_page=100&page=N&status=publish&start_date=YYYY-MM-DD

Returns up to ~5 500 events; we filter to upcoming only via start_date and
cap at MAX_PAGES pages (~500 events) per run to stay polite.
Pagination is driven by the `next_rest_url` field in each response.

Each event has rich venue, cost, image, and category data — much richer than
the 24-event JSON-LD baked into the HTML.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

BASE_URL = "https://www.choosechicago.com/wp-json/tribe/events/v1/events"
PER_PAGE = 100
MAX_PAGES = 5          # 500 events max per run — polite ceiling
TIMEOUT   = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _parse_price(cost: str, cost_details: dict) -> tuple[Optional[float], str]:
    """Extract (price, currency) from the TEC cost fields."""
    currency = (cost_details.get("currency_code") or "USD").strip()

    # Prefer numeric value from cost_details
    values = cost_details.get("values") or []
    if values:
        try:
            return float(values[0]), currency
        except (TypeError, ValueError):
            pass

    # Fall back to parsing the cost string ("$15", "Free", "$10 - $30")
    if not cost or cost.strip().lower() in ("free", ""):
        return 0.0, currency
    nums = re.findall(r"[\d]+(?:\.\d+)?", cost.replace(",", ""))
    if nums:
        try:
            return float(nums[0]), currency
        except ValueError:
            pass
    return None, currency


def _parse_event(ev: dict) -> Optional[RawEvent]:
    """Convert one TEC API event dict into a RawEvent."""
    today = date.today()

    # Dates — API returns local Chicago time as "YYYY-MM-DD HH:MM:SS"
    start_str = ev.get("start_date") or ev.get("start_date_details", {})
    end_str   = ev.get("end_date")   or ""
    if isinstance(start_str, dict):
        # start_date_details breakdown — shouldn't happen but guard it
        start_str = (f"{start_str.get('year','')}-{start_str.get('month','')}-"
                     f"{start_str.get('day','')} {start_str.get('hour','00')}:"
                     f"{start_str.get('minutes','00')}:00")
    try:
        start_dt = datetime.strptime(str(start_str)[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None
    if start_dt.date() < today:
        return None

    try:
        end_dt: Optional[datetime] = datetime.strptime(str(end_str)[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        end_dt = None

    # Skip all-day events with no useful time (they have 00:00:00 start)
    start_time = None if ev.get("all_day") else (
        start_dt.strftime("%H:%M") if start_dt.strftime("%H:%M") != "00:00" else None
    )
    end_time = (
        end_dt.strftime("%H:%M")
        if end_dt and not ev.get("all_day") and end_dt.strftime("%H:%M") != "00:00"
        else None
    )

    # Venue
    venue_block = ev.get("venue") or {}
    venue_name    = venue_block.get("venue") or None        # TEC calls it "venue" not "name"
    venue_address = venue_block.get("address") or None
    venue_city    = venue_block.get("city") or "Chicago"
    venue_country = "United States"
    venue_lat     = venue_block.get("geo_lat") or None
    venue_lon     = venue_block.get("geo_lng") or None
    venue_url     = venue_block.get("url") or None

    # Price
    cost_details = ev.get("cost_details") or {}
    price, currency = _parse_price(ev.get("cost") or "", cost_details)

    # Image
    image_block = ev.get("image")
    image_url   = image_block.get("url") if isinstance(image_block, dict) else None

    # Categories
    cats = [c["name"] for c in (ev.get("categories") or []) if c.get("name")]

    # Purchase / info link
    purchase_link = ev.get("website") or ev.get("url") or None

    # Online event
    is_online = bool(ev.get("is_virtual")) or bool(ev.get("virtual_url"))

    # Source ID: stable WP post ID
    post_id   = ev.get("id") or ""
    source_id = f"choosechicago:{post_id}"

    # Description: strip HTML tags
    raw_desc = ev.get("description") or ""
    description = re.sub(r"<[^>]+>", " ", raw_desc).strip() or None

    return RawEvent(
        name=ev.get("title") or "Untitled Event",
        start_date=start_dt.date(),
        start_time=start_time,
        end_date=end_dt.date() if end_dt else None,
        end_time=end_time,
        price=price,
        price_currency=currency,
        purchase_link=purchase_link,
        image_url=image_url,
        description=description,
        is_online=is_online,
        venue_name=venue_name,
        venue_address=venue_address,
        venue_city=venue_city,
        venue_country=venue_country,
        venue_lat=float(venue_lat) if venue_lat else None,
        venue_lon=float(venue_lon) if venue_lon else None,
        venue_website_url=venue_url,
        source="choosechicago",
        source_id=source_id,
        raw_categories=cats,
    )


class ChooseChicagoCollector(BaseCollector):
    """Scrapes upcoming Chicago events from choosechicago.com TEC REST API."""

    @property
    def source_name(self) -> str:
        return "choosechicago"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if city_name != "Chicago":
            return []

        results: list[RawEvent] = []
        url: Optional[str] = (
            f"{BASE_URL}?per_page={PER_PAGE}&status=publish"
            f"&start_date={date.today().isoformat()}"
        )
        page = 0

        async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS,
                                     follow_redirects=True) as client:
            while url and page < MAX_PAGES:
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        logger.warning(f"ChooseChicago: HTTP {resp.status_code} on page {page+1}")
                        break
                    body = resp.json()
                except Exception as e:
                    logger.warning(f"ChooseChicago: fetch error page {page+1} — {e}")
                    break

                for ev in body.get("events") or []:
                    try:
                        raw = _parse_event(ev)
                        if raw:
                            results.append(raw)
                    except Exception as e:
                        logger.debug(f"ChooseChicago: skipping event {ev.get('id')} — {e}")

                url  = body.get("next_rest_url") or None
                page += 1

        logger.info(f"ChooseChicago: {len(results)} upcoming events ({page} pages fetched)")
        return results
