"""
GoShow (goshow.co.il) venue-page scraper.

GoShow is an Israeli ticketing platform where each venue has a page at:
  https://www.goshow.co.il/pages/place/{venue_id}

The page is server-rendered HTML — no JS required. Events are structured
inside .resultData blocks with Hebrew date strings.

Usage:
  - Called directly from the admin scrape-venue-url endpoint when a GoShow URL is detected.
  - Also usable as a standalone async function: parse_goshow_venue_page(url, client)
"""
from __future__ import annotations

import logging
import re
from datetime import date

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import RawEvent, default_end_time

logger = logging.getLogger(__name__)

_DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")
_TIME_RE = re.compile(r"בשעה\s+(\d{1,2}):(\d{2})")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Supercaly/1.0)",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
}


def _parse_date_time(text: str):
    """Extract (date, 'HH:MM') from a Hebrew GoShow date string."""
    dm = _DATE_RE.search(text)
    if not dm:
        return None, None
    try:
        start_date = date(int(dm.group(3)), int(dm.group(2)), int(dm.group(1)))
    except ValueError:
        return None, None
    tm = _TIME_RE.search(text)
    start_time = f"{int(tm.group(1)):02d}:{tm.group(2)}" if tm else None
    return start_date, start_time


async def parse_goshow_venue_page(
    url: str,
    client: httpx.AsyncClient,
    venue_name: str = "",
    venue_city: str = "Tel Aviv",
    venue_country: str = "IL",
) -> list[RawEvent]:
    """Fetch a GoShow venue page and return RawEvent list."""
    today = date.today()
    try:
        resp = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"GoShow fetch failed for {url}: {e}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    # Derive venue name from page if not supplied —
    # prefer the venue span over the generic og:title / page title
    if not venue_name:
        first_span = soup.select_one(".closeShowTitleNew")
        og_site = soup.find("meta", property="og:site_name")
        venue_name = (
            (first_span.get_text(strip=True) if first_span else None)
            or (og_site["content"] if og_site and og_site.get("content") else None)
            or url
        )

    events: list[RawEvent] = []
    for block in soup.select(".resultData"):
        title_a = block.select_one(".resultLiText a")
        if not title_a:
            continue
        title = title_a.get_text(strip=True)
        if not title:
            continue

        date_span = block.select_one(".closeShowDateNew")
        if not date_span:
            continue
        start_date, start_time = _parse_date_time(date_span.get_text(strip=True))
        if not start_date or start_date < today:
            continue

        # Ticket link — prefer the Go button href, fall back to title link
        go_btn = block.select_one("a.resultGoBtn")
        purchase_link = (go_btn["href"] if go_btn and go_btn.get("href") else None) or title_a.get("href")

        # Venue name per show (may differ from page venue if GoShow aggregates)
        show_venue_span = block.select_one(".closeShowTitleNew")
        show_venue = show_venue_span.get_text(strip=True) if show_venue_span else venue_name

        end_date, end_time = default_end_time(start_time, start_date, None)

        source_id = f"goshow-{url.rstrip('/').split('/')[-1]}-{purchase_link}"

        events.append(RawEvent(
            name=title,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            artist_name=title,
            purchase_link=purchase_link,
            venue_name=show_venue,
            venue_city=venue_city,
            venue_country=venue_country,
            venue_website_url=url,
            source="goshow",
            source_id=source_id,
            raw_categories=["Music"],
        ))

    logger.info(f"GoShow: parsed {len(events)} upcoming events from {url}")
    return events
