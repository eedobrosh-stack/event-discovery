"""
Scraper for Heichal HaTarbut (היכל התרבות) Tel Aviv — hatarbut.co.il

Strategy:
  1. Fetch /events/ listing page → collect all /events/event/* links (upcoming events)
  2. Fetch each event page concurrently
  3. Parse Pojo Events fields: date (תאריך), time (תחילת המופע), title (h1)
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent, default_end_time

logger = logging.getLogger(__name__)

BASE_URL = "https://www.hatarbut.co.il"
LISTING_URL = f"{BASE_URL}/events/"
VENUE_NAME = "Heichal HaTarbut"
VENUE_CITY = "Tel Aviv"
VENUE_COUNTRY = "IL"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
}
TIMEOUT = 15
CONCURRENCY = 8

# DD.MM.YY or DD.MM.YYYY
_DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})")
_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})")


def _parse_hebrew_date(text: str) -> Optional[date]:
    """Parse Israeli date format DD.MM.YY(YY) → date object."""
    m = _DATE_RE.search(text)
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if year < 100:
        year += 2000
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _parse_time(text: str) -> Optional[str]:
    m = _TIME_RE.search(text)
    return f"{int(m.group(1)):02d}:{m.group(2)}" if m else None


def _parse_event_page(html: str, url: str) -> Optional[RawEvent]:
    soup = BeautifulSoup(html, "lxml")

    # Title: h1 or <title> (strip site suffix)
    h1 = soup.find("h1")
    name = h1.get_text(" ", strip=True) if h1 else ""
    if not name:
        title_tag = soup.find("title")
        name = (title_tag.string or "").split(" - ")[0].strip() if title_tag else ""
    if not name:
        return None

    # Image: og:image
    og_image = soup.find("meta", property="og:image")
    image_url = og_image["content"] if og_image and og_image.get("content") else None

    # Pojo Events fields — look for label text then sibling/child value
    start_date_: Optional[date] = None
    start_time_: Optional[str] = None

    for el in soup.find_all(class_=re.compile(r"pojo|tribe|event-meta", re.I)):
        text = el.get_text(" ", strip=True)

        # Date field: "תאריך: DD.MM.YY"
        if "תאריך" in text and not start_date_:
            start_date_ = _parse_hebrew_date(text)

        # Time field: "תחילת המופע: HH:MM"
        if "תחילת" in text and not start_time_:
            start_time_ = _parse_time(text)

    # Fallback: search full page text for date
    if not start_date_:
        page_text = soup.get_text(" ")
        start_date_ = _parse_hebrew_date(page_text)

    if not start_date_ or start_date_ < date.today():
        return None

    end_date_, end_time_ = default_end_time(start_time_, start_date_, None)

    # Purchase link: look for a ticket/buy button
    purchase_link = url  # default to event page itself
    for a in soup.find_all("a", href=True):
        href = a["href"]
        link_text = a.get_text(strip=True).lower()
        if any(kw in link_text for kw in ["רכישת כרטיסים", "לרכישה", "כרטיסים", "buy", "ticket"]):
            purchase_link = href if href.startswith("http") else urljoin(BASE_URL, href)
            break

    return RawEvent(
        name=name,
        start_date=start_date_,
        start_time=start_time_,
        end_date=end_date_,
        end_time=end_time_,
        purchase_link=purchase_link,
        image_url=image_url,
        venue_name=VENUE_NAME,
        venue_city=VENUE_CITY,
        venue_country=VENUE_COUNTRY,
        venue_website_url=BASE_URL + "/",
        source="hatarbut",
        source_id=url,
        raw_categories=["Music"],
    )


async def _fetch_event(
    client: httpx.AsyncClient, sem: asyncio.Semaphore, url: str
) -> Optional[RawEvent]:
    async with sem:
        try:
            resp = await client.get(url, timeout=TIMEOUT, headers=HEADERS, follow_redirects=True)
            if resp.status_code != 200:
                return None
            return _parse_event_page(resp.text, url)
        except Exception as e:
            logger.debug(f"hatarbut: failed to fetch {url}: {e}")
            return None


class HatarbutCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "hatarbut"

    def is_configured(self) -> bool:
        return True

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if city_name.lower() not in ("tel aviv", "tel aviv-yafo"):
            return []

        async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
            # Step 1: get listing page → extract upcoming event URLs
            try:
                resp = await client.get(LISTING_URL, follow_redirects=True)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"hatarbut: failed to fetch listing: {e}")
                return []

            soup = BeautifulSoup(resp.text, "lxml")
            event_urls = list({
                a["href"] for a in soup.find_all("a", href=True)
                if "/events/event/" in a["href"]
                and a["href"] != f"{BASE_URL}/events/event/"
            })
            logger.info(f"hatarbut: found {len(event_urls)} event pages to fetch")

            # Step 2: fetch each event page concurrently
            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [_fetch_event(client, sem, url) for url in event_urls]
            results = await asyncio.gather(*tasks)

        events = [r for r in results if r is not None]
        logger.info(f"hatarbut: parsed {len(events)} upcoming events")
        return events
