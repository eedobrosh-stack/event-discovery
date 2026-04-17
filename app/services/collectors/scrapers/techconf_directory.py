"""
Scraper for https://techconf.directory/conferences

Fetches upcoming tech conferences from the global directory, returns them
as a flat list of dicts.  The scheduler job (collect_techconf_job in jobs.py)
handles city/venue resolution and DB persistence.
"""
from __future__ import annotations

import logging
import re
from datetime import date

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as _du

logger = logging.getLogger(__name__)

CONFERENCES_URL = "https://techconf.directory/conferences"
BASE_URL = "https://techconf.directory"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _strip_ordinal(s: str) -> str:
    """'August 1st, 2026' → 'August 1, 2026'"""
    return re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", s)


def _parse_date(s: str) -> date | None:
    s = _strip_ordinal(s.strip())
    try:
        return _du.parse(s, dayfirst=False).date()
    except Exception:
        return None


async def scrape_techconf_directory() -> list[dict]:
    """
    Returns a list of upcoming conference dicts, each with:
        name, start_date, end_date (or None), city, country, url, is_online
    Conferences whose start_date has already passed are excluded.
    """
    today = date.today()
    results: list[dict] = []

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        try:
            resp = await client.get(CONFERENCES_URL, headers=_HEADERS)
        except Exception as exc:
            logger.warning(f"TechConfDirectory: fetch error — {exc}")
            return []

    if resp.status_code != 200:
        logger.warning(f"TechConfDirectory: HTTP {resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, "lxml")

    for li in soup.find_all("li", recursive=True):
        h2 = li.find("h2")
        if not h2:
            continue
        a = h2.find("a")
        if not a:
            continue

        name = a.get_text(strip=True)
        if not name:
            continue

        href = a.get("href", "")
        url = (BASE_URL + href) if href.startswith("/") else href

        # Real structure: <ul><li><p>dates</p><p>location</p>[<p>format</p>]</li></ul>
        # One inner <li> holds multiple <p> tags — NOT separate <li> per field.
        meta_ul = li.find("ul")
        inner_li = meta_ul.find("li") if meta_ul else None
        paras = inner_li.find_all("p") if inner_li else []

        # ── Dates ──────────────────────────────────────────────────────────
        date_text = paras[0].get_text(strip=True) if paras else ""
        start_date = end_date = None
        if " to " in date_text:
            parts = date_text.split(" to ", 1)
            start_date = _parse_date(parts[0])
            end_date   = _parse_date(parts[1])
        else:
            start_date = _parse_date(date_text)

        if not start_date or start_date < today:
            continue

        # ── Location ───────────────────────────────────────────────────────
        location_text = paras[1].get_text(strip=True) if len(paras) > 1 else ""
        is_online = location_text.lower() in ("online", "virtual", "remote", "")
        city = country = ""
        if not is_online:
            loc_parts = [p.strip() for p in location_text.split(",")]
            if len(loc_parts) >= 2:
                city    = loc_parts[0]
                country = loc_parts[-1]
            elif loc_parts:
                city = loc_parts[0]

        results.append(
            {
                "name":       name,
                "start_date": start_date,
                "end_date":   end_date,
                "city":       city,
                "country":    country,
                "url":        url,
                "is_online":  is_online,
            }
        )

    logger.info(f"TechConfDirectory: scraped {len(results)} upcoming conferences")
    return results
