"""
Venue website scraper: extracts events directly from each venue's own website.

Tries three tiers in order per venue:
  1. JSON-LD / Schema.org Event markup (structured, reliable)
  2. iCal feed linked from the page
  3. HTML heuristics (common event listing patterns)
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

from app.services.collectors.base import RawEvent

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Supercaly/1.0 (+https://event-discovery.onrender.com; event aggregator bot)"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}
TIMEOUT = 15
CONCURRENCY = 8   # parallel venue fetches
DELAY = 0.3       # seconds between requests per worker


# ---------------------------------------------------------------------------
# Tier 1 — JSON-LD / Schema.org
# ---------------------------------------------------------------------------

_EVENT_TYPES = {
    "Event", "MusicEvent", "TheaterEvent", "DanceEvent",
    "ComedyEvent", "SportsEvent", "EducationEvent", "SocialEvent",
}


def _extract_json_ld_events(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    events = []
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else data.get("@graph", [data])
        for item in items:
            if isinstance(item, dict) and item.get("@type") in _EVENT_TYPES:
                events.append(item)
    return events


def _str(val) -> str:
    return str(val).strip() if val else ""


def _parse_json_ld_event(item: dict, venue_name: str, venue_city: str,
                          venue_country: str, venue_url: str) -> Optional[RawEvent]:
    name = _str(item.get("name"))
    if not name:
        return None

    start_raw = _str(item.get("startDate"))
    if not start_raw:
        return None
    try:
        start_dt = dateutil_parser.parse(start_raw)
        start_date_ = start_dt.date()
        start_time_ = start_dt.strftime("%H:%M") if (start_dt.hour or start_dt.minute) else None
    except Exception:
        return None

    if start_date_ < date.today():
        return None

    end_date_, end_time_ = None, None
    end_raw = _str(item.get("endDate"))
    if end_raw:
        try:
            end_dt = dateutil_parser.parse(end_raw)
            end_date_ = end_dt.date()
            end_time_ = end_dt.strftime("%H:%M") if (end_dt.hour or end_dt.minute) else None
        except Exception:
            pass

    # Offers → price
    price, price_currency = None, "USD"
    offers = item.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        try:
            price = float(offers.get("price") or 0) or None
        except Exception:
            pass
        price_currency = _str(offers.get("priceCurrency")) or "USD"

    # URL
    url = _str(item.get("url")) or (_str(offers.get("url")) if isinstance(offers, dict) else "") or venue_url

    # Image
    image = item.get("image") or ""
    if isinstance(image, list):
        image = image[0] if image else ""
    if isinstance(image, dict):
        image = image.get("url", "")
    image = _str(image)

    # Performer
    performer = item.get("performer") or {}
    if isinstance(performer, list):
        performer = performer[0] if performer else {}
    artist = _str(performer.get("name")) if isinstance(performer, dict) else ""

    source_id = url or f"{venue_url}|{name}|{start_date_}"

    return RawEvent(
        name=name,
        start_date=start_date_,
        start_time=start_time_,
        end_date=end_date_,
        end_time=end_time_,
        artist_name=artist or None,
        price=price,
        price_currency=price_currency,
        purchase_link=url or venue_url,
        image_url=image or None,
        venue_name=venue_name,
        venue_city=venue_city,
        venue_country=venue_country,
        venue_website_url=venue_url,
        source="venue_web",
        source_id=source_id,
    )


# ---------------------------------------------------------------------------
# Tier 2 — iCal feed
# ---------------------------------------------------------------------------

def _find_ical_url(html: str, base_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "lxml")
    link = soup.find("link", type="text/calendar")
    if link and link.get("href"):
        return urljoin(base_url, link["href"])
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".ics") or "ical" in href.lower():
            return urljoin(base_url, href)
    return None


def _parse_ical_bytes(ical_bytes: bytes, venue_name: str, venue_city: str,
                       venue_country: str, venue_url: str) -> list[RawEvent]:
    try:
        from icalendar import Calendar
        cal = Calendar.from_ical(ical_bytes)
        events, today = [], date.today()
        for component in cal.walk():
            if component.name != "VEVENT":
                continue
            name = _str(component.get("SUMMARY"))
            if not name:
                continue
            dtstart = component.get("DTSTART")
            if not dtstart:
                continue
            dt = dtstart.dt
            if isinstance(dt, datetime):
                start_date_ = dt.date()
                start_time_ = dt.strftime("%H:%M") if (dt.hour or dt.minute) else None
            else:
                start_date_ = dt
                start_time_ = None
            if start_date_ < today:
                continue
            url = _str(component.get("URL")) or venue_url
            uid = _str(component.get("UID")) or f"{venue_url}|{name}|{start_date_}"
            events.append(RawEvent(
                name=name,
                start_date=start_date_,
                start_time=start_time_,
                purchase_link=url,
                venue_name=venue_name,
                venue_city=venue_city,
                venue_country=venue_country,
                venue_website_url=venue_url,
                source="venue_web",
                source_id=uid,
            ))
        return events
    except Exception as e:
        logger.debug(f"iCal parse error for {venue_url}: {e}")
        return []


# ---------------------------------------------------------------------------
# Tier 3 — HTML heuristics
# ---------------------------------------------------------------------------

_DATE_RE = re.compile(
    r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
    r'\s+\d{1,2}(?:,?\s+\d{4})?'
    r'|\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}',
    re.IGNORECASE,
)

_CONTAINER_SELECTORS = (
    ".event, .show, .concert, .performance, .gig, "
    "[class*='event-item'], [class*='show-item'], [class*='event-card'], "
    "[class*='event-listing'], [class*='event-row'], [class*='gig-item']"
)


def _extract_heuristic_events(html: str, venue_name: str, venue_city: str,
                               venue_country: str, venue_url: str) -> list[RawEvent]:
    soup = BeautifulSoup(html, "lxml")
    today = date.today()
    events, seen = [], set()
    this_year = datetime.now().year

    for container in soup.select(_CONTAINER_SELECTORS)[:60]:
        title_el = (
            container.find(["h1", "h2", "h3", "h4"]) or
            container.find(class_=re.compile(r"title|name|heading", re.I))
        )
        name = title_el.get_text(strip=True) if title_el else container.get_text(" ", strip=True)[:80]
        if not name or len(name) < 3:
            continue

        text = container.get_text(" ")
        date_match = _DATE_RE.search(text)
        if not date_match:
            continue
        try:
            start_date_ = dateutil_parser.parse(
                date_match.group(),
                default=datetime(this_year, 1, 1),
            ).date()
            if start_date_ < today:
                continue
        except Exception:
            continue

        key = (name[:40], str(start_date_))
        if key in seen:
            continue
        seen.add(key)

        link_el = container.find("a", href=True)
        url = urljoin(venue_url, link_el["href"]) if link_el else venue_url

        events.append(RawEvent(
            name=name,
            start_date=start_date_,
            purchase_link=url,
            venue_name=venue_name,
            venue_city=venue_city,
            venue_country=venue_country,
            venue_website_url=venue_url,
            source="venue_web",
            source_id=f"{venue_url}|{name[:40]}|{start_date_}",
        ))

    return events


# ---------------------------------------------------------------------------
# Main per-venue entry point
# ---------------------------------------------------------------------------

async def scrape_venue_website(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    venue_name: str,
    venue_city: str,
    venue_country: str,
    venue_url: str,
) -> list[RawEvent]:
    async with sem:
        try:
            resp = await client.get(
                venue_url, timeout=TIMEOUT, headers=HEADERS, follow_redirects=True
            )
            if resp.status_code != 200:
                return []
            html = resp.text
        except Exception as e:
            logger.debug(f"Fetch failed {venue_url}: {e}")
            return []
        finally:
            await asyncio.sleep(DELAY)

    # Tier 1: JSON-LD
    ld_items = _extract_json_ld_events(html)
    if ld_items:
        events = [
            e for item in ld_items
            if (e := _parse_json_ld_event(item, venue_name, venue_city, venue_country, venue_url))
        ]
        if events:
            return events

    # Tier 2: iCal
    ical_url = _find_ical_url(html, venue_url)
    if ical_url:
        try:
            async with sem:
                ical_resp = await client.get(
                    ical_url, timeout=TIMEOUT, headers=HEADERS, follow_redirects=True
                )
                await asyncio.sleep(DELAY)
            if ical_resp.status_code == 200:
                events = _parse_ical_bytes(
                    ical_resp.content, venue_name, venue_city, venue_country, venue_url
                )
                if events:
                    return events
        except Exception:
            pass

    # Tier 3: HTML heuristics
    return _extract_heuristic_events(html, venue_name, venue_city, venue_country, venue_url)
