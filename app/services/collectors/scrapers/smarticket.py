"""
Smarticket.co.il scraper.

Many Israeli venues self-host on the smarticket.co.il platform, each with their
own subdomain (e.g. shablul.smarticket.co.il). All expose a clean JSON API at
  GET /api/shows
which returns shows with nested event dates, times, and pricing.

Add a new venue by appending to VENUES below.
"""
from __future__ import annotations

import html
import logging
from datetime import date

import httpx

from app.services.collectors.base import BaseCollector, RawEvent, default_end_time

logger = logging.getLogger(__name__)

# (subdomain, venue_name, city, country, category)
VENUES = [
    ("shablul",  "Shablul Jazz Club", "Tel Aviv", "IL", "Music"),
]

BASE_URL = "https://{subdomain}.smarticket.co.il"
IMAGE_BASE = "https://static.smarticket.co.il/uploaded_files/"


def _clean(text: str | None) -> str:
    """Decode HTML entities and strip whitespace."""
    return html.unescape(text or "").strip()


class SmartticketCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "smarticket"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        events: list[RawEvent] = []
        today = date.today()

        async with httpx.AsyncClient(timeout=15) as client:
            for subdomain, venue_name, venue_city, venue_country, category in VENUES:
                # Filter by city
                if city_name.lower() not in venue_city.lower():
                    continue

                base = BASE_URL.format(subdomain=subdomain)
                try:
                    resp = await client.get(f"{base}/api/shows")
                    resp.raise_for_status()
                    shows = resp.json()
                except Exception as e:
                    logger.warning(f"Smarticket fetch failed for {subdomain}: {e}")
                    continue

                for show in shows:
                    # Prefer English title; fall back to Hebrew
                    title = _clean(show.get("title_en") or show.get("title") or "")
                    if not title:
                        continue

                    image_file = show.get("image_en") or show.get("image") or ""
                    image_url = f"{IMAGE_BASE}{image_file}" if image_file else None

                    for ev in show.get("events", []):
                        if not ev.get("visibility") or not ev.get("availability"):
                            continue

                        show_date_str = ev.get("show_date", "")
                        if not show_date_str:
                            continue
                        try:
                            start_date = date.fromisoformat(show_date_str)
                        except ValueError:
                            continue
                        if start_date < today:
                            continue

                        start_time = ev.get("show_time") or ev.get("time_label") or None
                        end_time = ev.get("end_time") or None
                        end_date, end_time = default_end_time(start_time, start_date, None) \
                            if not end_time else (start_date, end_time)

                        # Price: minimum across pricelist
                        prices = [
                            p["price"] for p in ev.get("pricelist", [])
                            if isinstance(p.get("price"), (int, float))
                        ]
                        price = float(min(prices)) if prices else None

                        purchase_link = f"{base}{ev.get('permalink', '')}"
                        source_id = f"smarticket-{subdomain}-{ev.get('id')}"

                        events.append(RawEvent(
                            name=title,
                            start_date=start_date,
                            start_time=start_time,
                            end_date=end_date,
                            end_time=end_time,
                            price=price,
                            price_currency="ILS",
                            purchase_link=purchase_link,
                            image_url=image_url,
                            venue_name=venue_name,
                            venue_city=venue_city,
                            venue_country=venue_country,
                            venue_website_url=base + "/",
                            source=self.source_name,
                            source_id=source_id,
                            raw_categories=[category],
                        ))

        return events
