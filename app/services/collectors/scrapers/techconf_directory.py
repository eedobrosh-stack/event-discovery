"""
Scraper for https://techconf.directory

Strategy: the site's `/conferences` HTML listing is a **curated subset** —
it surfaces only 144 of the ~254 conferences that actually exist on the
site (as verified against `sitemap.xml`). Previously we hit that listing
and silently missed 43% of the catalog, including conferences like
Stir Trek, Stanford WebCamp detail pages, and many regional AFUP/WordCamp
/DrupalCamp/SymfonyDay events.

New approach:
    1. Fetch `sitemap.xml` → extract every `/conferences/<slug>` URL.
    2. GET each detail page (with a polite 150ms delay between requests).
    3. Parse name, date(s), and location from the detail page DOM.
    4. Skip anything whose start_date is already in the past.

Coverage: 144 → ~254 conferences per run. Runtime impact: ~40s total
(254 × 150ms delay + ~80ms fetch), still well under the 30-min job window.

The scheduler job (collect_techconf_job in jobs.py) is unchanged — this
module just returns a longer list of dicts with the same shape.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import date
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as _du

logger = logging.getLogger(__name__)

BASE_URL = "https://techconf.directory"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
CONFERENCE_URL_PREFIX = f"{BASE_URL}/conferences/"

# Politeness: delay between detail-page fetches (seconds)
_DETAIL_DELAY_SEC = 0.15
# Hard ceiling on concurrent requests — sitemap-based scrape is sequential
# by default (single client, polite delay) but keep an explicit upper bound.
_MAX_CONCURRENCY = 1

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Matches "May 1st, 2026" or "May 1st, 2026 to May 2nd, 2026"
_DATE_RE = re.compile(
    r"^[A-Z][a-z]+\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4}"
    r"(\s+to\s+[A-Z][a-z]+\s+\d{1,2}(?:st|nd|rd|th)?,\s+\d{4})?$"
)

# Single-word location strings that indicate an online-only event
_ONLINE_TOKENS = {"online", "virtual", "remote"}

# Footer paragraphs we must not mistake for a location line
_FOOTER_PREFIXES = ("Site created by", "Built with")


def _strip_ordinal(s: str) -> str:
    """'August 1st, 2026' → 'August 1, 2026'"""
    return re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", s)


def _parse_date(s: str) -> date | None:
    s = _strip_ordinal(s.strip())
    try:
        return _du.parse(s, dayfirst=False).date()
    except Exception:
        return None


def _parse_date_range(date_text: str) -> tuple[date | None, date | None]:
    """'May 1st, 2026'  → (May 1, None)
       'May 1st, 2026 to May 2nd, 2026' → (May 1, May 2)"""
    if " to " in date_text:
        a, b = date_text.split(" to ", 1)
        return _parse_date(a), _parse_date(b)
    return _parse_date(date_text), None


def _parse_location(loc_text: str) -> tuple[str, str, bool]:
    """
    Returns (city, country, is_online) from a location paragraph.

    Observed formats on techconf.directory detail pages:
        'Columbus, United States •  In Person'    → ('Columbus',  'United States', False)
        'Paris, France •  In Person'              → ('Paris',     'France',        False)
        'Palo Alto, United States •  Hybrid'      → ('Palo Alto', 'United States', False)
        'Virtual'                                 → ('',          '',              True)
        'Online' / 'Remote'                       → ('',          '',              True)
    """
    if not loc_text:
        return "", "", True
    # Strip the ' • In Person / Online / Hybrid' tail
    head = loc_text.split("•", 1)[0].strip()
    if head.lower() in _ONLINE_TOKENS:
        return "", "", True
    parts = [p.strip() for p in head.split(",")]
    # The format line ("Online"/"Virtual"/"In Person"/"Hybrid") sometimes
    # appears as the whole text with no bullet — then head is that word and
    # we've already handled it. Hybrid/In Person with a city → in-person.
    if len(parts) >= 2:
        return parts[0], parts[-1], False
    if len(parts) == 1 and parts[0]:
        return parts[0], "", False
    return "", "", True


def _extract_from_detail(html: str, conf_url: str) -> dict | None:
    """Parse one conference detail page → dict, or None if unparseable."""
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    if not h1:
        return None
    name = h1.get_text(strip=True)
    if not name:
        return None

    # Walk <p> tags in document order. First date-matching <p> is the date;
    # the *next* non-empty, non-footer <p> after it is the location.
    all_p = soup.find_all("p")
    date_text = loc_text = None
    for i, p in enumerate(all_p):
        t = p.get_text(" ", strip=True)
        if not t:
            continue
        if _DATE_RE.match(t):
            date_text = t
            for q in all_p[i + 1:]:
                qt = q.get_text(" ", strip=True)
                if not qt:
                    continue
                if qt.startswith(_FOOTER_PREFIXES):
                    continue
                loc_text = qt
                break
            break

    if not date_text:
        return None

    start_date, end_date = _parse_date_range(date_text)
    if not start_date:
        return None

    city, country, is_online = _parse_location(loc_text or "")

    return {
        "name":       name,
        "start_date": start_date,
        "end_date":   end_date,
        "city":       city,
        "country":    country,
        "url":        conf_url,
        "is_online":  is_online,
    }


async def _fetch_sitemap_slugs(client: httpx.AsyncClient) -> list[str]:
    """Return all conference URLs found in the site's sitemap.xml."""
    try:
        resp = await client.get(SITEMAP_URL, headers=_HEADERS)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"TechConfDirectory: sitemap fetch failed — {exc}")
        return []

    try:
        root = ET.fromstring(resp.text)
    except ET.ParseError as exc:
        logger.warning(f"TechConfDirectory: sitemap XML parse error — {exc}")
        return []

    # Namespace for standard sitemap protocol
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = [loc.text for loc in root.findall(".//sm:loc", ns) if loc.text]

    conf_urls = [
        u for u in urls
        if u.startswith(CONFERENCE_URL_PREFIX)
        # Skip the index page itself (`/conferences`) — only detail pages
        and len(u) > len(CONFERENCE_URL_PREFIX)
    ]
    logger.info(
        f"TechConfDirectory: sitemap lists {len(conf_urls)} conference detail pages"
    )
    return conf_urls


async def _fetch_detail(client: httpx.AsyncClient, url: str) -> dict | None:
    """Fetch and parse one conference detail page."""
    try:
        resp = await client.get(url, headers=_HEADERS)
        if resp.status_code != 200:
            logger.debug(f"TechConfDirectory: {url} → HTTP {resp.status_code}")
            return None
        return _extract_from_detail(resp.text, url)
    except Exception as exc:
        logger.debug(f"TechConfDirectory: {url} fetch error — {exc}")
        return None


async def scrape_techconf_directory() -> list[dict]:
    """
    Returns a list of upcoming conference dicts, each with:
        name, start_date, end_date (or None), city, country, url, is_online
    Conferences whose start_date has already passed are excluded.
    """
    today = date.today()
    results: list[dict] = []
    skipped_past = 0
    skipped_unparseable = 0

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        slugs = await _fetch_sitemap_slugs(client)
        if not slugs:
            return []

        # Sequential with polite delay; sitemap-based scrape is a once-daily
        # job so we don't need parallelism — and we don't want to hammer a
        # small community-run site.
        for url in slugs:
            detail = await _fetch_detail(client, url)
            if detail is None:
                skipped_unparseable += 1
            elif detail["start_date"] < today:
                skipped_past += 1
            else:
                results.append(detail)
            await asyncio.sleep(_DETAIL_DELAY_SEC)

    logger.info(
        f"TechConfDirectory: scraped {len(results)} upcoming "
        f"(skipped {skipped_past} past, {skipped_unparseable} unparseable) "
        f"from {len(slugs)} sitemap entries"
    )
    return results
