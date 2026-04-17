"""
Barby Tel Aviv collector — iconic Israeli music venue (direct REST API).
Calls the Barby internal API which returns upcoming shows as JSON.
No API key needed.
"""
from __future__ import annotations
import httpx
import re
import logging
from datetime import date, datetime

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time

logger = logging.getLogger(__name__)

SHOWS_API = "https://barby.co.il/api/shows/find"
IMAGE_BASE = "https://images.barby.co.il/Logos/"
SHOW_BASE = "https://barby.co.il/show/"


class BarbyCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "barby"

    def is_configured(self) -> bool:
        return True  # No API key required

    async def collect(self, city_name: str, country_code: str = "IL", **kwargs) -> list[RawEvent]:
        # Barby is in Tel Aviv only
        if city_name not in ("Tel Aviv", "Tel Aviv-Yafo"):
            return []

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://www.barby.co.il",
            "Referer": "https://www.barby.co.il/",
            "Accept": "application/json",
        }

        async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
            try:
                resp = await client.get(SHOWS_API)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning(f"Barby: API call failed: {e}")
                return []

        raw_shows = data.get("returnShow", {}).get("show", [])
        today = date.today()
        events = []

        for show in raw_shows:
            raw = self._transform(show, today)
            if raw:
                events.append(raw)

        logger.info(f"Barby: found {len(events)} events")
        return events

    def _transform(self, show: dict, today: date) -> RawEvent | None:
        show_name = show.get("showName", "").strip()
        if not show_name or len(show_name) < 2:
            return None

        # Skip non-event entries (e.g. customer service announcements)
        if any(kw in show_name for kw in ["מייל שירות", "שירות לקוחות", "תקנון"]):
            return None

        # Parse date: DD/MM/YYYY
        date_str = show.get("showDate", "")
        if not date_str:
            return None
        try:
            parts = date_str.split("/")
            start_date = date(int(parts[2]), int(parts[1]), int(parts[0]))
        except (ValueError, IndexError):
            return None

        if start_date < today:
            return None

        # Parse time: HH:MM — keep as string for RawEvent
        time_str = show.get("showTime", "")
        start_time = None
        if time_str and ":" in time_str:
            parts = time_str.strip().split(":")
            if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                start_time = f"{int(parts[0]):02d}:{int(parts[1]):02d}"

        end_date_val, end_time = default_end_time(start_time, start_date, None)

        # Price
        price = None
        price_str = show.get("showPrice")
        if price_str:
            try:
                price = float(price_str)
                if price == 0:
                    price = None
            except (ValueError, TypeError):
                pass

        # Image
        show_image = show.get("showImage", "")
        image_url = f"{IMAGE_BASE}{show_image}" if show_image else None

        # Purchase link
        show_id = show.get("showId", "")
        purchase_link = f"{SHOW_BASE}{show_id}" if show_id else "https://www.barby.co.il"

        # Guest names for artist — fall back to show name when not set
        artist_name = show.get("showGuestsNames", "").strip() or show_name

        return RawEvent(
            name=show_name,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date_val,
            end_time=end_time,
            artist_name=artist_name,
            price=price,
            price_currency="ILS",
            image_url=image_url,
            purchase_link=purchase_link,
            venue_name="Barby",
            venue_address="המרד 52, תל אביב",
            venue_city="Tel Aviv",
            venue_country="Israel",
            source="barby",
            source_id=f"barby_{show_id}",
            raw_categories=["Music"],
        )
