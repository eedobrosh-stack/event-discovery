"""Platform-first venue event fetching.

Each "platform" is a ticketing/calendar system that powers many venues.
Instead of hardcoding venue configs in scraper files, venues are stored in the
platform_venues DB table and detected from a URL.

Adding a new venue = paste URL → detect → confirm → save → events auto-scrape daily.

Supported platforms:  venuepilot (full), dice (stub), eventbrite (stub), resident_advisor (stub).
To add a platform: implement _fetch_<platform> and add its domain to KNOWN_PLATFORM_DOMAINS.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from typing import Optional

import httpx

from app.services.collectors.base import RawEvent

logger = logging.getLogger(__name__)

# Maps a domain substring → platform key
KNOWN_PLATFORM_DOMAINS: dict[str, str] = {
    "venuepilot.co": "venuepilot",
    "dice.fm": "dice",
    "eventbrite.com": "eventbrite",
    "ra.co": "resident_advisor",
}

PLATFORM_LABELS: dict[str, str] = {
    "venuepilot": "VenuePilot",
    "dice": "DICE",
    "eventbrite": "Eventbrite",
    "resident_advisor": "Resident Advisor",
}


# ---------------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------------

async def detect_platform(url: str) -> dict | None:
    """
    Identify which ticketing platform powers a given venue URL.

    Returns a dict with keys:
        platform      - platform key  (e.g. "venuepilot")
        platform_id   - venue-level ID on that platform (str | None)
        name          - guessed venue name (str | None)
        confidence    - "high" | "medium" | "low"

    Returns None if the platform cannot be determined.
    """
    url_lower = url.lower()

    # 1. Direct domain match (no HTTP needed)
    for domain, platform in KNOWN_PLATFORM_DOMAINS.items():
        if domain in url_lower:
            return {
                "platform": platform,
                "platform_id": _extract_id_from_url(url, platform),
                "name": None,
                "confidence": "high",
            }

    # 2. Fetch the page and look for embedded platform widgets
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Supercaly/1.0"})
        html = resp.text
    except Exception as e:
        logger.warning(f"detect_platform: could not fetch {url}: {e}")
        return None

    # VenuePilot widget patterns
    vp_id_match = (
        re.search(r'data-account-id=["\']?(\d+)', html)
        or re.search(r'accountId["\s]*:["\s]*(\d+)', html)
        or re.search(r'account_ids?["\s]*:["\s]*\[?\s*(\d+)', html, re.IGNORECASE)
    )
    if vp_id_match or "venuepilot.co" in html:
        return {
            "platform": "venuepilot",
            "platform_id": vp_id_match.group(1) if vp_id_match else None,
            "name": _og_name(html),
            "confidence": "high" if vp_id_match else "medium",
        }

    # DICE widget
    dice_id_match = re.search(r'dice\.fm/venue/([a-z0-9-]+)', html, re.IGNORECASE)
    if dice_id_match or "dice.fm" in html:
        return {
            "platform": "dice",
            "platform_id": dice_id_match.group(1) if dice_id_match else None,
            "name": _og_name(html),
            "confidence": "high" if dice_id_match else "medium",
        }

    return None


def _extract_id_from_url(url: str, platform: str) -> Optional[str]:
    """Extract the platform-specific venue ID from a platform-native URL."""
    if platform == "venuepilot":
        m = re.search(r'/venues?/(\d+)', url)
        return m.group(1) if m else None
    if platform == "dice":
        m = re.search(r'/venue/([a-z0-9-]+)', url, re.IGNORECASE)
        return m.group(1) if m else None
    if platform == "resident_advisor":
        m = re.search(r'/clubs?/(\d+)', url)
        return m.group(1) if m else None
    return None


def _og_name(html: str) -> Optional[str]:
    """Try og:site_name, then og:title, then <title>."""
    for pat in [
        r'property=["\']og:site_name["\'][^>]*content=["\']([^"\']+)',
        r'content=["\']([^"\']+)["\'][^>]*property=["\']og:site_name["\']',
    ]:
        m = re.search(pat, html)
        if m:
            return m.group(1).strip()
    m = re.search(r'<title[^>]*>([^<]+)</title>', html)
    if m:
        t = m.group(1).strip()
        return t.split(" - ")[0].split(" | ")[0].strip()
    return None


# ---------------------------------------------------------------------------
# Event fetching — one handler per platform
# ---------------------------------------------------------------------------

async def fetch_platform_venue_events(
    pv,
    city_name: str,
    city_country: str,
) -> list[RawEvent]:
    """
    Fetch raw events for a PlatformVenue record.

    Args:
        pv            - PlatformVenue ORM instance
        city_name     - name of the linked City (used for venue_city on RawEvent)
        city_country  - country code of the linked City
    """
    if pv.platform == "venuepilot":
        return await _fetch_venuepilot(pv, city_name, city_country)
    if pv.platform == "dice":
        return await _fetch_dice(pv, city_name, city_country)
    logger.warning(f"No event-fetch handler for platform '{pv.platform}' (venue={pv.name})")
    return []


async def _fetch_venuepilot(pv, city_name: str, city_country: str) -> list[RawEvent]:
    """Fetch events via the VenuePilot public GraphQL API."""
    from app.services.collectors.scrapers.venuepilot import (
        VENUEPILOT_GRAPHQL, _QUERY, _fmt_time, _min_price, _tags_to_category,
    )

    if not pv.platform_id:
        logger.warning(f"VenuePilot venue '{pv.name}' has no platform_id — skipping")
        return []

    today = date.today()
    query = _QUERY % (pv.platform_id, today.isoformat())

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                VENUEPILOT_GRAPHQL,
                json={"query": query},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            resp.raise_for_status()
    except Exception as e:
        logger.error(f"VenuePilot fetch error for '{pv.name}': {e}")
        return []

    items = resp.json().get("data", {}).get("publicEvents", [])
    raw_events: list[RawEvent] = []

    for ev in items:
        try:
            date_str = ev.get("date")
            if not date_str:
                continue
            event_date = date.fromisoformat(date_str)
            if event_date < today:
                continue

            start_time = _fmt_time(ev.get("startTime"))
            end_time = _fmt_time(ev.get("endTime"))
            price = _min_price(ev.get("tickets") or [])
            tags = ev.get("tags") or []

            raw_events.append(RawEvent(
                name=ev["name"],
                start_date=event_date,
                start_time=start_time,
                end_date=event_date if end_time else None,
                end_time=end_time,
                venue_name=pv.name,
                venue_city=city_name,
                venue_country=city_country,
                venue_address=pv.address,
                venue_website_url=pv.website_url,
                purchase_link=ev.get("ticketsUrl"),
                price=price,
                price_currency="USD" if price else None,
                raw_categories=[_tags_to_category(tags)],
                source="venuepilot",
                source_id=str(ev["id"]),
            ))
        except Exception as e:
            logger.warning(f"VenuePilot: skipping event {ev.get('id')} for '{pv.name}': {e}")

    return raw_events


async def _fetch_dice(pv, city_name: str, city_country: str) -> list[RawEvent]:
    """DICE venue-specific event fetch — not yet implemented."""
    logger.info(f"DICE venue-specific fetch not yet implemented for '{pv.name}'")
    return []
