"""
Israel Sites Collector — scrapes multiple Israeli event websites.

Covered sources:
  - secrettelaviv.com  (HTML table, English, Tel Aviv)
  - levontin7.com      (WordPress fat-event-item, Tel Aviv)
  - habama.co.il       (ASP.NET event links, all Israel)
  - yellowsubmarine.org.il (WordPress, Jerusalem / Tel Aviv)
  - tmisrael.co.il     (Ticketmaster Israel, all Israel)
  - haifahaifa.co.il   (Haifa events portal)
  - il.funzing.com     (experience/workshop events, all Israel)
  - timeout.co.il      (editorial aggregator, Tel Aviv)
  - ligdol.co.il       (family/kids events, all Israel)

Blocked / JS-only (need Playwright, deferred):
  - eventim.co.il, mouse.co.il, goout.co.il, mega-tickets.co.il,
    bravo.co.il, kidguide.co.il, beit-haamudim.com, bargiora.co.il,
    jerusalem.muni.il, kfar-saba.muni.il
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from datetime import date, datetime
from typing import Optional
from urllib.parse import urljoin, unquote

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time
from app.services.collectors.scrapers.base_scraper import HEBREW_MONTHS

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}

CITY_MAP_HEB = {
    "תל אביב": "Tel Aviv", "תל אביב-יפו": "Tel Aviv", "תל-אביב": "Tel Aviv",
    "ירושלים": "Jerusalem", "חיפה": "Haifa", "כפר סבא": "Kfar Saba",
    "ראשון לציון": "Rishon LeZion", "פתח תקווה": "Petah Tikva",
    "נתניה": "Netanya", "באר שבע": "Beersheba", "רמת גן": "Ramat Gan",
    "הרצליה": "Herzliya", "רעננה": "Ra'anana",
}


def _sid(source: str, key: str) -> str:
    return hashlib.md5(f"{source}:{key}".encode()).hexdigest()[:16]


def _parse_ddmmyyyy(text: str) -> Optional[date]:
    """Parse DD/MM/YYYY or DD.MM.YYYY → date. Returns None if past."""
    m = re.search(r"(\d{1,2})[/.](\d{1,2})[/.](\d{4})", text)
    if not m:
        return None
    try:
        d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        return d if d >= date.today() else None
    except ValueError:
        return None


def _parse_ddmm(text: str, default_year: int = None) -> Optional[date]:
    """Parse DD.MM (no year) → date."""
    m = re.search(r"(\d{1,2})\.(\d{2})(?:[^\d]|$)", text)
    if not m:
        return None
    year = default_year or date.today().year
    try:
        d = date(year, int(m.group(2)), int(m.group(1)))
        if d < date.today():
            d = date(year + 1, int(m.group(2)), int(m.group(1)))
        return d
    except ValueError:
        return None


def _parse_hhmm(text: str) -> Optional[str]:
    m = re.search(r"(\d{1,2}):(\d{2})", text)
    if m:
        h, mn = int(m.group(1)), int(m.group(2))
        if 0 <= h <= 23:
            return f"{h:02d}:{mn:02d}"
    return None


def _parse_hebrew_date_str(text: str) -> Optional[date]:
    """Parse dates like '14 מרץ 2026' or '14 מרץ'."""
    for heb, month_num in HEBREW_MONTHS.items():
        m = re.search(rf"(\d{{1,2}})\s+{heb}\s*(\d{{4}})?", text)
        if m:
            day = int(m.group(1))
            year = int(m.group(2)) if m.group(2) else date.today().year
            try:
                d = date(year, month_num, day)
                return d if d >= date.today() else None
            except ValueError:
                pass
    return None


# ---------------------------------------------------------------------------
# SecretTelAviv
# ---------------------------------------------------------------------------

SECRET_TLV_PAGES = [
    # Main listing + pagination
    "https://www.secrettelaviv.com/tickets",
    "https://www.secrettelaviv.com/tickets?pno=2",
    "https://www.secrettelaviv.com/tickets?pno=3",
    "https://www.secrettelaviv.com/tickets?pno=4",
    "https://www.secrettelaviv.com/tickets?pno=5",
    # Category pages
    "https://www.secrettelaviv.com/tickets/categories/live-music",
    "https://www.secrettelaviv.com/tickets/categories/culture-highlights",
    "https://www.secrettelaviv.com/tickets/categories/parties",
    "https://www.secrettelaviv.com/tickets/categories/meetups",
    "https://www.secrettelaviv.com/tickets/categories/music-festivals",
    "https://www.secrettelaviv.com/tickets/categories/exhibitions",
    "https://www.secrettelaviv.com/tickets/categories/food-events",
    "https://www.secrettelaviv.com/tickets/categories/hi-tech-events",
    "https://www.secrettelaviv.com/tickets/categories/environmental",
    "https://www.secrettelaviv.com/tickets/categories/holidays",
    "https://www.secrettelaviv.com/tickets/categories/pride",
    "https://www.secrettelaviv.com/tickets/categories/religious",
    "https://www.secrettelaviv.com/tickets/categories/shopping",
]


def _parse_secrettelaviv_page(html: str) -> list[dict]:
    """Parse a single SecretTelAviv events table page. Returns list of raw dicts."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr")[1:]  # skip header
    results = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        datetime_text = cells[1].get_text(" ", strip=True)
        start_date = _parse_ddmmyyyy(datetime_text)
        if not start_date:
            continue
        start_time = _parse_hhmm(datetime_text) or "20:00"

        # cells[2]: event link text is "EventName @ VenueName"
        name_el = cells[2].find("a") or cells[2]
        name_venue = name_el.get_text(strip=True) if name_el else ""
        name_venue = re.split(r"\n|(?<=[.!?])\s", name_venue)[0].strip()

        if " @ " in name_venue:
            parts = name_venue.split(" @ ", 1)
            event_name = parts[0].strip()
            venue_raw = parts[1].strip()
            venue_name = re.split(r"[,.]|(?<=\w{3})\s{2,}", venue_raw)[0].strip()
        else:
            event_name = name_venue
            venue_name = "Tel Aviv"

        if not event_name or len(event_name) < 3:
            continue

        link_el = row.find("a")
        url = link_el.get("href", "") if link_el else ""
        if url and not url.startswith("http"):
            url = urljoin("https://www.secrettelaviv.com", url)

        results.append({
            "name": event_name,
            "venue": venue_name,
            "start_date": start_date,
            "start_time": start_time,
            "url": url,
        })
    return results


async def scrape_secrettelaviv(client: httpx.AsyncClient) -> list[RawEvent]:
    """
    Scrapes all SecretTelAviv event category pages:
      /tickets/ (all), /live-music, /culture-highlights, /parties, /meetups
    Deduplicates across pages by (date, event_name).
    """
    seen: set = set()
    events: list[RawEvent] = []

    for url in SECRET_TLV_PAGES:
        try:
            resp = await client.get(url, timeout=20)
            if resp.status_code != 200:
                continue
        except Exception as e:
            logger.debug(f"SecretTelAviv {url}: {e}")
            continue

        for item in _parse_secrettelaviv_page(resp.text):
            key = f"{item['start_date']}:{item['name'].lower()}"
            if key in seen:
                continue
            seen.add(key)

            end_date, end_time = default_end_time(item["start_time"], item["start_date"], None)
            events.append(RawEvent(
                source="secrettelaviv",
                source_id=_sid("secrettelaviv", key),
                name=item["name"],
                start_date=item["start_date"],
                start_time=item["start_time"],
                end_date=end_date,
                end_time=end_time,
                venue_name=item["venue"],
                venue_city="Tel Aviv",
                venue_country="Israel",
                purchase_link=item["url"] or "https://www.secrettelaviv.com/tickets/",
                image_url=None,
                price=None,
                price_currency="ILS",
            ))

    logger.info(f"SecretTelAviv: {len(events)} events across {len(SECRET_TLV_PAGES)} pages")
    return events


# ---------------------------------------------------------------------------
# Levontin 7
# ---------------------------------------------------------------------------

async def scrape_levontin7(client: httpx.AsyncClient) -> list[RawEvent]:
    """
    levontin7.com — WordPress with fat-event-item divs.
    data-time="20:00 - 21:30, 30/03/2026"
    data-event-id="92455"
    """
    try:
        resp = await client.get("https://www.levontin7.com", timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Levontin7: fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    items = soup.find_all(class_=re.compile(r"fat-event-item"))
    seen_ids: set = set()
    events = []

    for item in items:
        event_id = item.get("data-event-id", "")
        if not event_id or event_id in seen_ids:
            continue
        if "No upcoming" in item.get_text():
            continue
        seen_ids.add(event_id)

        time_data = item.get("data-time", "")  # "20:00 - 21:30, 30/03/2026"

        start_date = _parse_ddmmyyyy(time_data)
        if not start_date:
            continue

        start_time = _parse_hhmm(time_data) or "20:00"

        # Extract end time: second HH:MM in time_data
        all_times = re.findall(r"\d{1,2}:\d{2}", time_data)
        end_time_raw = all_times[1] if len(all_times) >= 2 else None

        title_el = (item.find(class_=re.compile(r"event-title|title|name", re.I))
                    or item.find(["h2", "h3", "h4", "h5"]))
        title = title_el.get_text(strip=True) if title_el else ""
        # Fallback: first non-empty text node
        if not title:
            for el in item.descendants:
                t = el.get_text(strip=True) if hasattr(el, "get_text") else str(el).strip()
                if t and len(t) > 3 and not re.match(r"^\d", t):
                    title = t
                    break
        if not title:
            continue
        # Clean "…" truncation
        title = title.replace("…", "").strip().rstrip("/")

        link_el = item.find("a")
        url = link_el.get("href", "") if link_el else "https://www.levontin7.com"

        if end_time_raw:
            end_date, end_time = default_end_time(start_time, start_date, None)
            end_time = end_time_raw
        else:
            end_date, end_time = default_end_time(start_time, start_date, None)

        img_el = item.find("img")
        image_url = img_el.get("src") if img_el else None

        events.append(RawEvent(
            source="levontin7",
            source_id=event_id,
            name=title,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            venue_name="Levontin 7",
            venue_address="7 Levontin St, Tel Aviv",
            venue_city="Tel Aviv",
            venue_country="Israel",
            purchase_link=url,
            image_url=image_url,
            price=None,
            price_currency="ILS",
        ))

    logger.info(f"Levontin7: {len(events)} events")
    return events


# ---------------------------------------------------------------------------
# Habama (Israeli theatre aggregator)
# ---------------------------------------------------------------------------

async def scrape_habama(client: httpx.AsyncClient, city_name: str) -> list[RawEvent]:
    """
    habama.co.il — ASP.NET portal listing Israeli theatre shows.
    The homepage lists ongoing productions (city, venue, name, time) but NO dates.
    Event detail pages return HTTP 400. With Option-A date filtering this yields 0 events.
    Kept as a stub — may become useful if habama.co.il adds dates to their listing.
    """
    try:
        resp = await client.get("https://www.habama.co.il", timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Habama: fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    event_links = soup.find_all("a", href=re.compile(r"Event\.aspx\?.*EventId=\d+"))
    seen_ids: set = set()
    events = []

    for link in event_links:
        href = link.get("href", "")
        m = re.search(r"EventId=(\d+)", href)
        if not m:
            continue
        event_id = m.group(1)
        if event_id in seen_ids:
            continue
        seen_ids.add(event_id)

        name = link.get_text(strip=True)
        if not name or len(name) < 3:
            # Try parent TD for name
            parent = link.find_parent("td")
            name = parent.get_text(strip=True) if parent else ""
        if not name or len(name) < 3:
            continue

        # Try to extract venue / city from surrounding context
        row = link.find_parent("tr")
        cells = row.find_all("td") if row else []
        venue_name = ""
        city_text = ""
        for cell in cells:
            text = cell.get_text(strip=True)
            if "תיאטרון" in text or "אולם" in text or "בית" in text:
                venue_name = text
            # Hebrew city names
            for heb, eng in CITY_MAP_HEB.items():
                if heb in text:
                    city_text = eng
                    break

        # Filter by city if requested
        if city_name not in ("", "Israel") and city_text and city_text != city_name:
            continue

        full_url = urljoin("https://www.habama.co.il", href)

        # Try to extract a date from the row cells (format: DD/MM/YYYY or DD.MM.YYYY)
        start_date = None
        for cell in cells:
            start_date = _parse_ddmmyyyy(cell.get_text(" ", strip=True))
            if start_date:
                break

        # Option A: skip events with no parseable date to avoid NOT NULL DB errors
        if not start_date:
            continue

        end_date, end_time = default_end_time("20:00", start_date, None)

        events.append(RawEvent(
            source="habama",
            source_id=event_id,
            name=name,
            start_date=start_date,
            start_time="20:00",
            end_date=end_date,
            end_time=end_time,
            venue_name=venue_name or "Habima Theatre",
            venue_city=city_text or city_name or "Tel Aviv",
            venue_country="Israel",
            purchase_link=full_url,
            image_url=None,
            price=None,
            price_currency="ILS",
        ))

    logger.info(f"Habama: {len(events)} events for {city_name}")
    return events


# ---------------------------------------------------------------------------
# Yellow Submarine
# ---------------------------------------------------------------------------

async def scrape_yellowsubmarine(client: httpx.AsyncClient) -> list[RawEvent]:
    """
    yellowsubmarine.org.il — WordPress. Events listed on /event/ page.
    """
    try:
        resp = await client.get("https://www.yellowsubmarine.org.il/event/", timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"YellowSub: fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    # Tribe Events plugin uses <article class="tribe_events_cat-...">
    articles = soup.find_all("article", class_=re.compile(r"tribe|event", re.I))
    events = []

    for article in articles:
        title_el = article.find(class_=re.compile(r"tribe-event-title|entry-title", re.I)) \
                   or article.find(["h2", "h3"])
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title or len(title) < 3:
            continue

        link_el = title_el.find("a") or article.find("a")
        url = link_el.get("href", "https://www.yellowsubmarine.org.il") if link_el else ""

        # Date: tribe_events_start_datetime or .tribe-event-schedule-details
        date_el = article.find(class_=re.compile(r"tribe-event-date|tribe-event-schedule|event-date", re.I)) \
                  or article.find("time")
        start_date = None
        start_time = "20:00"
        if date_el:
            dt_text = date_el.get("datetime", "") or date_el.get_text(strip=True)
            # ISO datetime
            iso_m = re.search(r"(\d{4}-\d{2}-\d{2})", dt_text)
            if iso_m:
                try:
                    d = date.fromisoformat(iso_m.group(1))
                    start_date = d if d >= date.today() else None
                except ValueError:
                    pass
            if not start_date:
                start_date = _parse_ddmmyyyy(dt_text) or _parse_hebrew_date_str(dt_text)
            t = _parse_hhmm(dt_text)
            if t:
                start_time = t

        end_date, end_time = default_end_time(start_time, start_date, None)

        img_el = article.find("img")
        image_url = img_el.get("src") if img_el else None

        events.append(RawEvent(
            source="yellowsubmarine",
            source_id=_sid("yellowsubmarine", title + str(start_date)),
            name=title,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            venue_name="Yellow Submarine",
            venue_address="13 HaRechev St, Jerusalem",
            venue_city="Jerusalem",
            venue_country="Israel",
            purchase_link=url,
            image_url=image_url,
            price=None,
            price_currency="ILS",
        ))

    logger.info(f"YellowSub: {len(events)} events")
    return events


# ---------------------------------------------------------------------------
# TMIsrael (Ticketmaster Israel)
# ---------------------------------------------------------------------------

TM_IL_API = "https://www.ticketmaster.co.il/wbtxapi/api/v1/bxcached/event/getAllTopEvent/iw"
TM_IL_IMG = "https://www.ticketmaster.co.il/tmol-media/media/"
TM_IL_EVENT_BASE = "https://www.tmisrael.co.il/event/"

CITY_MAP_TM = {
    "תל אביב": "Tel Aviv", "תל אביב - יפו": "Tel Aviv", "תל-אביב": "Tel Aviv",
    "ירושלים": "Jerusalem", "חיפה": "Haifa", "באר שבע": "Beersheba",
    "קיסריה": "Caesarea", "קרית מוצקין": "Kiryat Motzkin",
    "אילת": "Eilat", "נתניה": "Netanya", "רמת גן": "Ramat Gan",
}


async def scrape_tmisrael(client: httpx.AsyncClient, city_name: str) -> list[RawEvent]:
    """
    Ticketmaster Israel internal JSON API.
    GET /wbtxapi/api/v1/bxcached/event/getAllTopEvent/iw
    Returns featured/spotlight events with full date + venue data.
    """
    try:
        resp = await client.get(
            TM_IL_API,
            timeout=20,
            headers={
                "Referer": "https://www.tmisrael.co.il/",
                "Accept": "application/json",
            },
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning(f"TMIsrael API: {e}")
        return []

    raw_items = data.get("data", [])
    events: list[RawEvent] = []
    today = date.today()

    for item in raw_items:
        event_id = item.get("btxEventId") or str(item.get("btxEventGroupId", ""))
        if not event_id:
            continue

        name = (item.get("eventName") or item.get("eventGroupName") or "").strip()
        if not name or len(name) < 2:
            continue

        # Timestamp in ms → date
        ts = item.get("firstPerformanceDate")
        start_date = None
        if ts:
            try:
                d = date.fromtimestamp(ts / 1000)
                start_date = d if d >= today else None
            except (OSError, OverflowError):
                pass
        if not start_date:
            continue

        # City mapping
        raw_city = (item.get("venueCity") or "").strip()
        venue_city = CITY_MAP_TM.get(raw_city, raw_city or "Israel")

        venue_name = (item.get("venueName") or "").strip()
        image_file = item.get("smallImage", "")
        image_url = f"{TM_IL_IMG}{image_file}" if image_file else None

        custom_url = item.get("customUrl", "")
        if custom_url and custom_url.startswith("http"):
            ticket_url = custom_url
        elif item.get("btxEventId"):
            ticket_url = f"{TM_IL_EVENT_BASE}{item['btxEventId']}/ALL/iw"
        else:
            ticket_url = "https://www.tmisrael.co.il"

        end_date, end_time = default_end_time("20:00", start_date, None)

        events.append(RawEvent(
            source="tmisrael",
            source_id=event_id,
            name=name,
            start_date=start_date,
            start_time="20:00",
            end_date=end_date,
            end_time=end_time,
            venue_name=venue_name,
            venue_city=venue_city,
            venue_country="Israel",
            purchase_link=ticket_url,
            image_url=image_url,
            price=None,
            price_currency="ILS",
        ))

    logger.info(f"TMIsrael: {len(events)} events from JSON API")
    return events


# ---------------------------------------------------------------------------
# Funzing Israel
# ---------------------------------------------------------------------------

async def scrape_funzing(client: httpx.AsyncClient) -> list[RawEvent]:
    """il.funzing.com — experiences/workshops platform."""
    try:
        resp = await client.get("https://il.funzing.com", timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Funzing: fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    # Cards usually have class containing 'card' or 'product' or 'experience'
    cards = soup.find_all(class_=re.compile(r"card|product|experience|event", re.I))
    events = []
    seen: set = set()

    for card in cards:
        title_el = card.find(["h2", "h3", "h4"]) or card.find(class_=re.compile(r"title|name", re.I))
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title or len(title) < 4 or title in seen:
            continue
        # Skip nav/UI items
        if any(w in title.lower() for w in ["menu", "search", "cart", "login", "sign"]):
            continue
        seen.add(title)

        link_el = card.find("a")
        url = link_el.get("href", "") if link_el else ""
        if url and not url.startswith("http"):
            url = urljoin("https://il.funzing.com", url)

        # Extract date
        date_el = card.find(class_=re.compile(r"date|when|time", re.I)) or card.find("time")
        start_date = None
        start_time = "19:00"
        if date_el:
            dt_text = date_el.get_text(strip=True)
            start_date = _parse_ddmmyyyy(dt_text) or _parse_hebrew_date_str(dt_text)
            t = _parse_hhmm(dt_text)
            if t:
                start_time = t

        # Price
        price_el = card.find(class_=re.compile(r"price|cost", re.I))
        price = None
        if price_el:
            pm = re.search(r"[\d,]+", price_el.get_text(strip=True).replace(",", ""))
            price = float(pm.group(0)) if pm else None

        img_el = card.find("img")
        image_url = img_el.get("src") if img_el else None

        end_date, end_time = default_end_time(start_time, start_date, None)

        events.append(RawEvent(
            source="funzing",
            source_id=_sid("funzing", title),
            name=title,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            venue_name="",
            venue_city="Tel Aviv",
            venue_country="Israel",
            purchase_link=url or "https://il.funzing.com",
            image_url=image_url,
            price=price,
            price_currency="ILS",
        ))

    logger.info(f"Funzing: {len(events)} events")
    return events


# ---------------------------------------------------------------------------
# Timeout Tel Aviv
# ---------------------------------------------------------------------------

async def scrape_timeout(client: httpx.AsyncClient) -> list[RawEvent]:
    """
    timeout.co.il — editorial events listing.
    Tries /events/ category pages.
    """
    urls_to_try = [
        "https://www.timeout.co.il/tel-aviv/music",
        "https://www.timeout.co.il/tel-aviv/nightlife",
        "https://www.timeout.co.il/tel-aviv/theatre",
        "https://www.timeout.co.il/tel-aviv/art",
    ]
    events = []
    seen: set = set()

    for url in urls_to_try:
        try:
            resp = await client.get(url, timeout=20)
            if resp.status_code != 200:
                continue
        except Exception:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        # Timeout uses article tags or listing cards
        articles = soup.find_all(["article", "div"], class_=re.compile(r"listing|card|event|item", re.I))

        for article in articles:
            title_el = article.find(["h2", "h3", "h4"]) or article.find(class_=re.compile(r"title", re.I))
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if not title or len(title) < 4 or title in seen:
                continue
            seen.add(title)

            link_el = title_el.find("a") or article.find("a")
            art_url = link_el.get("href", url) if link_el else url
            if art_url and not art_url.startswith("http"):
                art_url = urljoin("https://www.timeout.co.il", art_url)

            date_el = article.find(class_=re.compile(r"date|when|time", re.I)) or article.find("time")
            start_date = None
            start_time = "20:00"
            if date_el:
                dt_text = date_el.get("datetime", "") or date_el.get_text(strip=True)
                start_date = _parse_ddmmyyyy(dt_text) or _parse_hebrew_date_str(dt_text)
                t = _parse_hhmm(dt_text)
                if t:
                    start_time = t

            end_date, end_time = default_end_time(start_time, start_date, None)
            img_el = article.find("img")
            image_url = img_el.get("src") if img_el else None

            events.append(RawEvent(
                source="timeout_il",
                source_id=_sid("timeout_il", title),
                name=title,
                start_date=start_date,
                start_time=start_time,
                end_date=end_date,
                end_time=end_time,
                venue_name="",
                venue_city="Tel Aviv",
                venue_country="Israel",
                purchase_link=art_url,
                image_url=image_url,
                price=None,
                price_currency="ILS",
            ))

    logger.info(f"Timeout IL: {len(events)} events")
    return events


# ---------------------------------------------------------------------------
# LiGdol (family/kids events)
# ---------------------------------------------------------------------------

async def scrape_ligdol(client: httpx.AsyncClient) -> list[RawEvent]:
    """ligdol.co.il — family & children events across Israel."""
    try:
        resp = await client.get("https://www.ligdol.co.il", timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"LiGdol: fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.find_all(["article", "div"], class_=re.compile(r"event|card|item|post", re.I))
    events = []
    seen: set = set()

    for card in cards:
        title_el = card.find(["h2", "h3", "h4"]) or card.find(class_=re.compile(r"title", re.I))
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title or len(title) < 4 or title in seen:
            continue
        seen.add(title)

        link_el = card.find("a")
        url = link_el.get("href", "") if link_el else ""
        if url and not url.startswith("http"):
            url = urljoin("https://www.ligdol.co.il", url)

        date_el = card.find(class_=re.compile(r"date|time|when", re.I)) or card.find("time")
        start_date = None
        start_time = "10:00"  # family events tend to be daytime
        if date_el:
            dt_text = date_el.get_text(strip=True)
            start_date = _parse_ddmmyyyy(dt_text) or _parse_hebrew_date_str(dt_text)
            t = _parse_hhmm(dt_text)
            if t:
                start_time = t

        # Try to detect city
        city_text = "Tel Aviv"
        for heb, eng in CITY_MAP_HEB.items():
            if heb in card.get_text():
                city_text = eng
                break

        end_date, end_time = default_end_time(start_time, start_date, None)
        img_el = card.find("img")
        image_url = img_el.get("src") if img_el else None

        events.append(RawEvent(
            source="ligdol",
            source_id=_sid("ligdol", title),
            name=title,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            venue_name="",
            venue_city=city_text,
            venue_country="Israel",
            purchase_link=url or "https://www.ligdol.co.il",
            image_url=image_url,
            price=None,
            price_currency="ILS",
        ))

    logger.info(f"LiGdol: {len(events)} events")
    return events


# ---------------------------------------------------------------------------
# HaifaHaifa
# ---------------------------------------------------------------------------

async def scrape_haifahaifa(client: httpx.AsyncClient) -> list[RawEvent]:
    """haifahaifa.co.il — Haifa events portal."""
    try:
        resp = await client.get("https://haifahaifa.co.il", timeout=20)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"HaifaHaifa: fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    # WordPress-style posts
    articles = soup.find_all(["article", "div"], class_=re.compile(r"event|post|entry", re.I))
    events = []
    seen: set = set()

    for article in articles:
        title_el = article.find(["h2", "h3"]) or article.find(class_=re.compile(r"title|entry-title", re.I))
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title or len(title) < 4 or title in seen:
            continue
        # Skip navigation/ui elements
        if any(w in title.lower() for w in ["home", "about", "contact", "menu"]):
            continue
        seen.add(title)

        link_el = title_el.find("a") or article.find("a")
        url = link_el.get("href", "") if link_el else ""
        if url and not url.startswith("http"):
            url = urljoin("https://haifahaifa.co.il", url)

        date_el = article.find(class_=re.compile(r"date|time|when", re.I)) or article.find("time")
        start_date = None
        start_time = "20:00"
        if date_el:
            dt_text = date_el.get("datetime", "") or date_el.get_text(strip=True)
            start_date = _parse_ddmmyyyy(dt_text) or _parse_hebrew_date_str(dt_text)
            t = _parse_hhmm(dt_text)
            if t:
                start_time = t

        end_date, end_time = default_end_time(start_time, start_date, None)
        img_el = article.find("img")
        image_url = img_el.get("src") if img_el else None

        events.append(RawEvent(
            source="haifahaifa",
            source_id=_sid("haifahaifa", title),
            name=title,
            start_date=start_date,
            start_time=start_time,
            end_date=end_date,
            end_time=end_time,
            venue_name="",
            venue_city="Haifa",
            venue_country="Israel",
            purchase_link=url or "https://haifahaifa.co.il",
            image_url=image_url,
            price=None,
            price_currency="ILS",
        ))

    logger.info(f"HaifaHaifa: {len(events)} events")
    return events


# ---------------------------------------------------------------------------
# Main Collector class
# ---------------------------------------------------------------------------

ISRAEL_CITIES = {
    "Tel Aviv", "Tel Aviv-Yafo", "Jerusalem", "Haifa", "Kfar Saba",
    "Rishon LeZion", "Petah Tikva", "Netanya", "Beersheba",
}


class IsraelSitesCollector(BaseCollector):
    """Aggregates events from multiple Israeli websites."""

    @property
    def source_name(self) -> str:
        return "israel_sites"

    def is_configured(self) -> bool:
        return True  # No API key required

    async def collect(self, city_name: str, country_code: str = "IL", **kwargs) -> list[RawEvent]:
        # Only run for Israeli cities
        if country_code not in ("IL", "Israel") and city_name not in ISRAEL_CITIES:
            return []

        async with httpx.AsyncClient(
            timeout=25,
            follow_redirects=True,
            headers=HEADERS,
        ) as client:
            tasks = [
                scrape_secrettelaviv(client),
                scrape_levontin7(client),
                scrape_habama(client, city_name),
                scrape_yellowsubmarine(client),
                scrape_tmisrael(client, city_name),
                scrape_funzing(client),
                scrape_ligdol(client),
                scrape_haifahaifa(client),
                scrape_timeout(client),
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events: list[RawEvent] = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Israel sites sub-scraper error: {result}")
            elif result:
                all_events.extend(result)

        # Filter by city if specific city requested
        if city_name and city_name not in ("Israel", ""):
            filtered = [
                e for e in all_events
                if not e.venue_city
                or e.venue_city == city_name
                or (city_name in ("Tel Aviv", "Tel Aviv-Yafo") and e.venue_city in ("Tel Aviv", "Tel Aviv-Yafo"))
            ]
        else:
            filtered = all_events

        logger.info(f"IsraelSites: {len(filtered)} total events for {city_name}")
        return filtered
