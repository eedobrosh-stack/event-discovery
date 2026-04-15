"""
Luma (lu.ma) event scraper.

Fetches events from Luma city pages via __NEXT_DATA__ JSON embedded in the page.
Covers conferences, meetups, tech events, workshops, and networking events.
No API key required.
"""
from __future__ import annotations

import hashlib
import logging
import re
import json
from datetime import datetime, timezone
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time

logger = logging.getLogger(__name__)

# Map our city names → Luma city slugs
CITY_SLUGS: dict[str, str] = {
    "New York":      "nyc",
    "London":        "london",
    "Los Angeles":   "la",
    "San Francisco": "sf",
    "Chicago":       "chicago",
    "Miami":         "miami",
    "Austin":        "austin",
    "Boston":        "boston",
    "Seattle":       "seattle",
    "Toronto":       "toronto",
    "Berlin":        "berlin",
    "Paris":         "paris",
    "Amsterdam":     "amsterdam",
    "Singapore":     "singapore",
    "Tokyo":         "tokyo",
    "Tel Aviv":      "tel-aviv",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _sid(key: str) -> str:
    return hashlib.md5(f"luma:{key}".encode()).hexdigest()[:16]


def _parse_luma_dt(iso_str: str) -> Optional[datetime]:
    """Parse Luma ISO datetime string (always UTC)."""
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


class LumaCollector(BaseCollector):
    """Scrapes Luma city pages for conferences, meetups, and tech events."""

    @property
    def source_name(self) -> str:
        return "luma"

    def is_configured(self) -> bool:
        return True  # No API key needed

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        slug = CITY_SLUGS.get(city_name)
        if not slug:
            return []

        url = f"https://lu.ma/{slug}"
        try:
            async with httpx.AsyncClient(
                timeout=20,
                follow_redirects=True,
                headers=HEADERS,
            ) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                html = resp.text
        except Exception as e:
            logger.debug(f"Luma fetch failed for {city_name}: {type(e).__name__}: {e}")
            return []

        # Extract __NEXT_DATA__
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if not m:
            logger.debug(f"Luma: no __NEXT_DATA__ for {city_name}")
            return []

        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            return []

        try:
            page_data = data["props"]["pageProps"]["initialData"]["data"]
        except (KeyError, TypeError):
            return []

        # Combine events + featured_events, deduplicate by api_id
        raw_entries = page_data.get("events", []) + page_data.get("featured_events", [])
        seen_ids: set[str] = set()
        events: list[RawEvent] = []

        for entry in raw_entries:
            ev = entry.get("event") or {}
            api_id = ev.get("api_id") or entry.get("api_id", "")
            if not api_id or api_id in seen_ids:
                continue
            seen_ids.add(api_id)

            raw = self._transform(ev, entry, city_name)
            if raw:
                events.append(raw)

        logger.info(f"Luma {city_name}: {len(events)} events")
        return events

    def _transform(self, ev: dict, entry: dict, city_name: str) -> Optional[RawEvent]:
        try:
            name = ev.get("name", "").strip()
            if not name:
                return None

            # Skip online-only events
            if ev.get("location_type") == "online":
                return None

            start_str = ev.get("start_at") or entry.get("start_at")
            if not start_str:
                return None

            start_dt = _parse_luma_dt(start_str)
            if not start_dt:
                return None

            from datetime import date
            if start_dt.date() < date.today():
                return None

            end_dt = _parse_luma_dt(ev.get("end_at", ""))

            # Venue info
            geo = ev.get("geo_address_info") or {}
            venue_name = geo.get("full_address") or geo.get("place_name") or ""
            venue_city = geo.get("city") or city_name

            # Calendar = organizer / venue context
            calendar = entry.get("calendar") or {}
            if not venue_name:
                venue_name = calendar.get("name", "")

            # Purchase / event link
            slug = ev.get("url", "")
            purchase_link = f"https://lu.ma/{slug}" if slug else None

            # Cover image
            image_url = ev.get("cover_url") or ""
            if not image_url:
                cover = entry.get("cover_image") or {}
                image_url = cover.get("url") or cover.get("original_url") or None

            # Determine category from event name / calendar name
            combined_text = f"{name} {calendar.get('name', '')}".lower()
            raw_cats = _infer_categories(combined_text)

            end_date = end_dt.date() if end_dt else None
            end_time = safe_time(end_dt) if end_dt else None
            if end_time is None:
                end_date, end_time = default_end_time(
                    safe_time(start_dt), start_dt.date(), None
                )

            return RawEvent(
                source="luma",
                source_id=_sid(ev.get("api_id", name)),
                name=name,
                start_date=start_dt.date(),
                start_time=safe_time(start_dt),
                end_date=end_date,
                end_time=end_time,
                venue_name=venue_name or None,
                venue_city=venue_city,
                venue_country=None,
                purchase_link=purchase_link,
                image_url=image_url or None,
                price=None,
                price_currency="USD",
                raw_categories=raw_cats,
            )
        except Exception:
            return None


def _infer_categories(text: str) -> list[str]:
    """Infer broad categories from event name/calendar text."""
    if any(w in text for w in ["conference", "summit", "congress", "symposium", "expo"]):
        return ["Technology"]
    if any(w in text for w in ["hackathon", "hack", "buildathon"]):
        return ["Technology"]
    if any(w in text for w in ["meetup", "networking", "happy hour", "mixer"]):
        return ["Networking"]
    if any(w in text for w in ["workshop", "bootcamp", "training", "course", "class"]):
        return ["Education"]
    if any(w in text for w in ["music", "concert", "jazz", "dj", "live"]):
        return ["Music"]
    if any(w in text for w in ["art", "gallery", "exhibit"]):
        return ["Art"]
    return ["Technology"]  # Luma skews heavily tech
