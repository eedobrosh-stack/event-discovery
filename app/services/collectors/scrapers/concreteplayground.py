"""
Concrete Playground event scraper — pulls curated lifestyle/culture events
from the per-city /events landing pages.

Australia/NZ-focused editorial outlet.  Sydney, Melbourne, and Brisbane each
yield ~10–25 events per page, all in clean schema.org Event JSON-LD blocks.
No pagination, no API key.

URL pattern: https://concreteplayground.com/{slug}/events
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent, safe_time

logger = logging.getLogger(__name__)

# City → CP slug.  Auckland exists but consistently empty + not a priority city.
CITY_SLUGS: dict[str, str] = {
    "Sydney":    "sydney",
    "Melbourne": "melbourne",
    "Brisbane":  "brisbane",
}

# CP slug → ISO country code (every CP slug we ship is AU)
CITY_COUNTRY: dict[str, str] = {
    "sydney":    "AU",
    "melbourne": "AU",
    "brisbane":  "AU",
}

BASE_URL = "https://concreteplayground.com"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-AU,en;q=0.9",
}

# CP sometimes returns a date as "YYYY-MM-DD HH:MM:SS" without a timezone.
_NAIVE_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$")


def _parse_dt(raw: str) -> datetime | None:
    if not raw:
        return None
    s = raw.strip()
    # Accept both ISO-8601 with offset ("2026-05-02T10:00:00+10:00") and
    # naive forms ("2026-05-02 23:59:59").
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        if _NAIVE_DT_RE.match(s):
            try:
                return datetime.strptime(s.replace("T", " "), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    return None


def _flatten_image(image) -> str | None:
    """CP nests images as list[list[str]] — pick the first scalar URL."""
    if not image:
        return None
    if isinstance(image, str):
        return image
    if isinstance(image, list):
        for item in image:
            scalar = _flatten_image(item)
            if scalar:
                return scalar
    return None


def _parse_event(ev: dict, city_slug: str) -> RawEvent | None:
    if ev.get("eventStatus") == "https://schema.org/EventCancelled":
        return None

    name = ev.get("name")
    start_dt = _parse_dt(ev.get("startDate", ""))
    if not name or not start_dt:
        return None
    if start_dt.date() < date.today():
        return None

    end_dt = _parse_dt(ev.get("endDate", ""))

    location = ev.get("location") or {}
    if not isinstance(location, dict):
        location = {}
    address_raw = location.get("address") or {}
    if isinstance(address_raw, str):
        # Some posts inline the address as a plain string.
        venue_address = address_raw
        address = {}
    else:
        address = address_raw if isinstance(address_raw, dict) else {}
        # CP uses address.name as the human-readable street; streetAddress is rarely set.
        venue_address = address.get("streetAddress") or address.get("name")
    venue_name = location.get("name")

    geo = location.get("geo") or {}
    if not isinstance(geo, dict):
        geo = {}
    venue_lat = float(geo["latitude"]) if geo.get("latitude") else None
    venue_lon = float(geo["longitude"]) if geo.get("longitude") else None

    # @id is the canonical CP article URL; ev.url is the *external* organizer's
    # site.  Prefer the external link for purchase, fall back to the CP page.
    cp_id = ev.get("@id") or ""
    external_url = ev.get("url")
    purchase_link = external_url or cp_id or None

    source_id = cp_id.rstrip("/").split("/")[-1] if cp_id else None

    description = ev.get("description") or None
    if not description:
        article = ev.get("articleBody")
        if isinstance(article, str):
            description = article[:500]

    # CP cards never expose pricing in JSON-LD.
    return RawEvent(
        name=name,
        start_date=start_dt.date(),
        start_time=safe_time(start_dt),
        end_date=end_dt.date() if end_dt else None,
        end_time=safe_time(end_dt) if end_dt else None,
        artist_name=None,
        price=None,
        price_currency="AUD",
        purchase_link=purchase_link,
        image_url=_flatten_image(ev.get("image")),
        description=description,
        venue_name=venue_name,
        venue_address=venue_address,
        venue_city=None,  # CP doesn't tag city per-event; collector knows from slug
        venue_country=CITY_COUNTRY.get(city_slug),
        venue_lat=venue_lat,
        venue_lon=venue_lon,
        source="concreteplayground",
        source_id=source_id,
        raw_categories=[],
    )


def parse_listing(html: str, city_slug: str) -> list[RawEvent]:
    soup = BeautifulSoup(html, "lxml")
    seen: set[str] = set()
    out: list[RawEvent] = []
    for block in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(block.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("@type") not in ("Event", "MusicEvent"):
                continue
            parsed = _parse_event(item, city_slug)
            if not parsed or not parsed.source_id:
                continue
            if parsed.source_id in seen:
                continue
            seen.add(parsed.source_id)
            out.append(parsed)
    return out


class ConcretePlaygroundCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "concreteplayground"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(
        self, city_name: str, country_code: str = "AU", **kwargs
    ) -> list[RawEvent]:
        slug = CITY_SLUGS.get(city_name)
        if not slug:
            return []

        url = f"{BASE_URL}/{slug}/events"
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(url, headers=_HEADERS)
        except Exception as exc:
            logger.warning(f"ConcretePlayground: request error for {city_name}: {exc}")
            return []

        if resp.status_code != 200:
            logger.warning(f"ConcretePlayground: HTTP {resp.status_code} for {city_name}")
            return []

        events = parse_listing(resp.text, slug)
        logger.info(f"ConcretePlayground: {city_name} → {len(events)} events")
        return events
