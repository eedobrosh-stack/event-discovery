"""
Scraper for Mevalim — mevalim.co.il

Mevalim is an Israeli event-aggregator (not a single venue), so each show it
lists happens at a real venue across Israel. The generic single-page venue
scraper only catches the ~12 events embedded in the homepage JSON-LD and
collapses them all under a fake "Mevalim" venue — that's what we're replacing.

Strategy:
  1. Read both Yoast sitemap files (page-sitemap.xml, page-sitemap2.xml).
     Every user-facing page under /{category}/{slug}/ carries JSON-LD Events,
     including the show pages AND the per-city archive pages. Crawling the
     union and de-duping by offer URL is simpler (and safer against future
     slug changes) than trying to distinguish the two up front.
  2. For each URL, parse every JSON-LD `*Event` item (Event, MusicEvent,
     TheaterEvent, ComedyEvent, ChildrensEvent, DanceEvent, SocialEvent,
     SportsEvent, EducationEvent).
  3. Dedupe by the offer URL (`tickets.mevalim.co.il/event/{id}`) — that's
     the canonical ID and it's stable across mirrors of the same show on
     multiple archive pages.
  4. Resolve each event's real city from the JSON-LD `location.name`
     (Hebrew venue name) via a Hebrew-city dictionary. The caller creates
     the City record if it doesn't exist yet (technology-conf style).

Notes on politeness:
  - ~1,500 URLs total. With CONCURRENCY=4 and 0.3 s per-worker sleep, a
    full run takes ~2 min and stays well within Cloudflare rate limits.
  - We follow the site's robots.txt — the disallowed paths (/wp-admin,
    /get-tickets, /mevalim_event_name, /mevalim_hall, …) never appear in
    the sitemap, so by pulling URLs from the sitemap we stay compliant.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date
from typing import Optional
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

from app.services.collectors.base import RawEvent, default_end_time

logger = logging.getLogger(__name__)

BASE_URL = "https://www.mevalim.co.il"
SITEMAP_URLS = [
    f"{BASE_URL}/page-sitemap.xml",
    f"{BASE_URL}/page-sitemap2.xml",
]
SOURCE_NAME = "mevalim"
VENUE_COUNTRY = "Israel"

HEADERS = {
    "User-Agent": "Supercaly/1.0 (+https://event-discovery.onrender.com; event aggregator bot)",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
TIMEOUT = 15
CONCURRENCY = 4   # polite — Cloudflare in front of the site
DELAY = 0.3       # seconds between requests per worker

# JSON-LD types that carry event data. The site uses MusicEvent / TheaterEvent /
# ComedyEvent / ChildrensEvent depending on the category; "Event" is the generic
# fallback for pages that don't type-specialise.
_EVENT_TYPES = {
    "Event", "MusicEvent", "TheaterEvent", "ComedyEvent", "ChildrensEvent",
    "DanceEvent", "SocialEvent", "SportsEvent", "EducationEvent",
}

# Only category prefixes we want to crawl. Everything else in the sitemap
# (e.g. /about, /contact) has no JSON-LD Events so we'd just waste requests.
#
# /shows/ is the canonical post-2025 location for music acts (Mevalim moved
# the bulk of its catalogue out of /concerts/, which now holds <20 URLs). It
# also serves as a 301 target for legacy /concerts/<slug>/ paths. Skipping
# /shows/ silently drops ~400 sitemap URLs covering most Israeli touring
# musicians (טונה, חיים משה, יהודה פוליקר, …). /musicals/ is small but its
# pages do carry Event JSON-LD, so we include it for parity.
#
# /lectures/, /festivals/, /exhibitions/, /dance/ pages were sampled and do
# NOT carry Event JSON-LD (only WebPage/Org metadata), so they stay excluded.
_CATEGORY_PREFIXES = (
    "/stand-up/", "/concerts/", "/kids-shows/", "/theater/",
    "/shows/", "/musicals/",
)

# Category slug → event-type hint used for the `raw_categories` field so the
# registry's Priority-3 classifier can slot these into the right bucket.
_CATEGORY_HINTS = {
    "/concerts/":   ["Music", "Concert"],
    "/stand-up/":   ["Comedy", "Stand-up"],
    "/kids-shows/": ["Family", "Children"],
    "/theater/":    ["Theater"],
    # /shows/ is the catch-all music slug; tag generically so the classifier
    # still routes these to the Music bucket (the JSON-LD @type=MusicEvent
    # remains the authoritative signal downstream).
    "/shows/":      ["Music", "Concert"],
    "/musicals/":   ["Theater", "Musical"],
}

# Hebrew → English canonical city names. Ordered longest-first for substring
# matching (so "רמת גן" doesn't lose to "גן" etc.). Covers the Israeli cities
# that host >99 % of mevalim events; unknown cities fall back to the venue's
# name alone (the venue still gets created, just unattached to a known city).
_HEBREW_CITIES: list[tuple[str, str]] = sorted(
    [
        # Metro Tel Aviv
        ("תל אביב יפו", "Tel Aviv"),
        ("תל אביב-יפו", "Tel Aviv"),
        ("תל אביב",    "Tel Aviv"),
        ("תל-אביב",    "Tel Aviv"),
        ("רמת גן",     "Ramat Gan"),
        ("גבעתיים",    "Givatayim"),
        ("בני ברק",    "Bnei Brak"),
        ("חולון",      "Holon"),
        ("בת ים",      "Bat Yam"),
        ("רמת השרון",  "Ramat HaSharon"),
        # Jerusalem + surrounds
        ("ירושלים",    "Jerusalem"),
        ("מעלה אדומים", "Maale Adumim"),
        ("בית שמש",    "Beit Shemesh"),
        # Sharon / central
        ("הרצליה",     "Herzliya"),
        ("רעננה",      "Raanana"),
        ("כפר סבא",    "Kfar Saba"),
        ("כפר-סבא",    "Kfar Saba"),
        ("הוד השרון",  "Hod HaSharon"),
        ("נתניה",      "Netanya"),
        ("פתח תקווה",  "Petah Tikva"),
        ("פתח-תקווה",  "Petah Tikva"),
        ("ראש העין",   "Rosh HaAyin"),
        ("ראש-העין",   "Rosh HaAyin"),
        ("דרום השרון", "Drom HaSharon"),
        ("שוהם",       "Shoham"),
        ("יהוד",       "Yehud"),
        ("קדימה צורן", "Kadima-Tzoran"),
        ("גני תקווה",  "Gani Tikva"),
        # Shfela
        ("ראשון לציון", "Rishon LeZion"),
        ("רחובות",     "Rehovot"),
        ("נס ציונה",   "Ness Ziona"),
        ("יבנה",       "Yavne"),
        ("מודיעין",    "Modiin"),
        ("לוד",        "Lod"),
        ("רמלה",       "Ramla"),
        ("אריאל",      "Ariel"),
        ("אשדוד",      "Ashdod"),
        ("אשקלון",     "Ashkelon"),
        ("קרית גת",    "Kiryat Gat"),
        ("קריית גת",   "Kiryat Gat"),
        # South
        ("באר שבע",    "Beersheba"),
        ("באר-שבע",    "Beersheba"),
        ("אילת",       "Eilat"),
        # North / Haifa bay
        ("חיפה",       "Haifa"),
        ("קרית אונו",  "Kiryat Ono"),
        ("קריית אונו", "Kiryat Ono"),
        ("קרית מוצקין", "Kiryat Motzkin"),
        ("קריית מוצקין", "Kiryat Motzkin"),
        ("קרית ביאליק", "Kiryat Bialik"),
        ("קריית ביאליק", "Kiryat Bialik"),
        ("קרית ים",    "Kiryat Yam"),
        ("קריית ים",   "Kiryat Yam"),
        ("קרית חיים",  "Kiryat Haim"),
        ("קריית חיים", "Kiryat Haim"),
        ("נהריה",      "Nahariya"),
        ("עכו",        "Acre"),
        ("כרמיאל",     "Karmiel"),
        ("מעלות תרשיחא", "Maalot-Tarshiha"),
        ("נצרת",       "Nazareth"),
        ("טבריה",      "Tiberias"),
        ("צפת",        "Safed"),
        ("קרית שמונה", "Kiryat Shmona"),
        ("קריית שמונה", "Kiryat Shmona"),
        # North + Jezreel valley
        ("עפולה",      "Afula"),
        ("יקנעם",      "Yokneam"),
        ("חדרה",       "Hadera"),
        ("פרדס חנה",   "Pardes Hanna"),
        ("זכרון יעקב", "Zichron Yaakov"),
        ("בית שאן",    "Beit She'an"),
        # Kibbutzim / smaller localities seen in Mevalim data
        ("גן שמואל",   "Gan Shmuel"),
        ("כפר תבור",   "Kfar Tavor"),
        ("כפר בלום",   "Kfar Blum"),
        ("כפר ויתקין", "Kfar Vitkin"),
        ("גבעת ברנר", "Givat Brenner"),
        ("יפעת",       "Yifat"),
        ("יגור",       "Yagur"),
        ("עמק המעיינות", "Emek HaMaayanot"),
        ("כנרת",       "Kinneret"),
        ("עשרת",       "Aseret"),
    ],
    key=lambda kv: -len(kv[0]),
)


# ---------------------------------------------------------------------------
# Sitemap + URL filtering
# ---------------------------------------------------------------------------

_SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


async def _fetch_sitemap_urls(client: httpx.AsyncClient) -> list[str]:
    urls: list[str] = []
    for sm_url in SITEMAP_URLS:
        try:
            resp = await client.get(sm_url, timeout=TIMEOUT, headers=HEADERS)
            if resp.status_code != 200:
                logger.warning(f"mevalim: sitemap {sm_url} returned {resp.status_code}")
                continue
            root = ET.fromstring(resp.content)
            for loc in root.findall(".//sm:url/sm:loc", _SITEMAP_NS):
                if loc.text:
                    urls.append(loc.text.strip())
        except Exception as e:
            logger.warning(f"mevalim: failed to load sitemap {sm_url}: {e}")

    # Keep only URLs under one of the known category prefixes. Everything else
    # (homepage, static pages) either has no events or is covered elsewhere.
    filtered: list[str] = []
    dropped: list[str] = []
    for u in urls:
        if any(prefix in u for prefix in _CATEGORY_PREFIXES):
            filtered.append(u)
        else:
            dropped.append(u)

    # Diagnose silent drops: when Mevalim restructures (e.g. /concerts/ → /shows/
    # in 2025), the dropped bucket suddenly fills up with a brand-new prefix
    # we never added to _CATEGORY_PREFIXES. Logging the top dropped prefixes
    # makes that visible immediately instead of waiting for "an artist isn't
    # showing up" to surface weeks later.
    top_dropped = ""
    if dropped:
        from collections import Counter
        prefix_counts: Counter[str] = Counter()
        for u in dropped:
            # First non-empty path segment, e.g. "/lectures/some-slug/" → "lectures"
            try:
                parts = urlparse(u).path.strip("/").split("/", 1)
                prefix_counts[parts[0] if parts and parts[0] else "(root)"] += 1
            except Exception:
                continue
        top = prefix_counts.most_common(5)
        top_dropped = " dropped_top=" + ",".join(f"{p}:{n}" for p, n in top)

    logger.info(
        f"mevalim: sitemap URLs total={len(urls)} crawl_candidates={len(filtered)} "
        f"dropped={len(dropped)}{top_dropped}"
    )
    return filtered


# ---------------------------------------------------------------------------
# JSON-LD parsing
# ---------------------------------------------------------------------------

def _extract_json_ld_events(html: str) -> list[dict]:
    """Pull every @type=*Event item out of a page's JSON-LD blocks."""
    soup = BeautifulSoup(html, "lxml")
    events: list[dict] = []
    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        items = data if isinstance(data, list) else data.get("@graph", [data])
        for item in items:
            if isinstance(item, dict) and item.get("@type") in _EVENT_TYPES:
                events.append(item)
    return events


_H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.DOTALL | re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")


def _extract_page_h1(html: str) -> str:
    """First <h1> text on the page, stripped of HTML tags.

    Used to recover full performer names that Mevalim's kids-shows JSON-LD
    truncates to the URL slug — e.g. JSON-LD `name` is just "מיקי" while
    the H1 says "מיקי מוכתר - הופעות 2026". Stand-up pages don't have this
    issue (their JSON-LD already carries the full name), so the override
    in `_parse_event` only fires when the H1 strictly extends the JSON-LD
    name.
    """
    m = _H1_RE.search(html)
    if not m:
        return ""
    return _TAG_RE.sub("", m.group(1)).strip()


def _strip_suffix(name: str) -> str:
    """Drop a trailing ' - <suffix>' segment from a Mevalim title/H1.

    Mevalim H1s follow the pattern '<Performer> - <descriptor>' (e.g.
    'מיקי מוכתר - הופעות 2026', 'שחר חסון - הופעות 2026'). Returning
    just the first segment gives us the canonical performer name.
    """
    if " - " in name:
        return name.split(" - ", 1)[0].strip()
    return name.strip()


def _str(val) -> str:
    return str(val).strip() if val else ""


def _resolve_city_from_venue(venue_name: str) -> Optional[str]:
    """Scan the Hebrew venue name for a known city substring.
    Returns the canonical English city name, or None if no match."""
    if not venue_name:
        return None
    for he, en in _HEBREW_CITIES:
        if he in venue_name:
            return en
    return None


def _categories_from_url(url: str) -> list[str]:
    for prefix, hints in _CATEGORY_HINTS.items():
        if prefix in url:
            return list(hints)
    return []


def _parse_event(item: dict, page_url: str, page_h1: str = "") -> Optional[RawEvent]:
    name = _str(item.get("name"))
    if not name:
        return None

    # Recover full performer name when Mevalim's JSON-LD carries only the URL
    # slug (kids-shows: name="מיקי" while H1 says "מיקי מוכתר - הופעות 2026").
    # Only override when the H1 stem strictly EXTENDS the JSON-LD name — that
    # way we never replace a correct JSON-LD name (Shahar Hasson's stand-up
    # page already has the full "שחר חסון" in JSON-LD; H1 stem is identical
    # so this branch is a no-op for it).
    if page_h1:
        h1_stem = _strip_suffix(page_h1)
        if h1_stem and h1_stem != name and len(h1_stem) > len(name) and name in h1_stem:
            name = h1_stem

    start_raw = _str(item.get("startDate"))
    if not start_raw:
        return None
    try:
        start_dt = dateutil_parser.parse(start_raw)
        start_date_ = start_dt.date()
        start_time_ = (
            start_dt.strftime("%H:%M")
            if (start_dt.hour or start_dt.minute) else None
        )
    except Exception:
        return None

    if start_date_ < date.today():
        return None

    # Venue — required. Skip events without a location (can't attribute them).
    location = item.get("location") or {}
    if isinstance(location, list):
        location = location[0] if location else {}
    venue_name = _str(location.get("name")) if isinstance(location, dict) else ""
    if not venue_name:
        return None

    venue_city = _resolve_city_from_venue(venue_name) or ""
    venue_address = (
        _str(location.get("address")) if isinstance(location, dict) else ""
    )

    # Offers → price + canonical purchase URL (also used as source_id for dedup).
    # IMPORTANT: Archive pages (/stand-up/, /concerts/ …) emit one Event per
    # performer with `offers.url` pointing back to that performer's page on
    # mevalim.co.il — i.e. a DIFFERENT show's next date but the same offer URL
    # as all their other shows. The per-show detail pages emit events with
    # `offers.url` pointing to the real ticket provider (smarticket, zappa,
    # comedybar…). To avoid dedup collisions AND save only the real show-level
    # data, we skip events whose offer URL is on mevalim.co.il itself.
    price: Optional[float] = None
    price_currency = "ILS"
    offer_url = ""
    offers = item.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    if isinstance(offers, dict):
        p = offers.get("price")
        if p is not None:
            try:
                price = float(p)
            except (TypeError, ValueError):
                pass
        price_currency = _str(offers.get("priceCurrency")) or "ILS"
        offer_url = _str(offers.get("url"))

    # Reject archive-page shadow events whose offer URL points back at the
    # main mevalim.co.il site (one performer's archive entry circles to that
    # performer's detail page on the same site, with the same offer URL as
    # all of their other shows — useless for dedup and not the real ticket).
    #
    # CRITICAL: only reject the MAIN-SITE host. The `tickets.mevalim.co.il`
    # subdomain is Mevalim's own ticketing platform — kids-shows use it
    # exclusively (every Mickey Mukhtar show has tickets.mevalim.co.il/event/),
    # and stand-up pages emit it alongside external smarticket URLs. A loose
    # `"mevalim.co.il" in offer_url` check incorrectly drops the tickets
    # subdomain too, making every kids-show invisible.
    offer_host = urlparse(offer_url).netloc.lower() if offer_url else ""
    if not offer_url or offer_host in ("mevalim.co.il", "www.mevalim.co.il"):
        return None

    purchase_link = offer_url
    end_date_, end_time_ = default_end_time(start_time_, start_date_, None)

    return RawEvent(
        name=name,
        start_date=start_date_,
        start_time=start_time_,
        end_date=end_date_,
        end_time=end_time_,
        price=price,
        price_currency=price_currency,
        purchase_link=purchase_link,
        image_url=_str(item.get("image")) or None,
        venue_name=venue_name,
        venue_address=venue_address or None,
        venue_city=venue_city or None,
        venue_country=VENUE_COUNTRY,
        source=SOURCE_NAME,
        # Offer URL is stable and already unique per (show, date, venue),
        # so it's the perfect dedup key across overlapping archive pages.
        source_id=purchase_link,
        raw_categories=_categories_from_url(page_url),
    )


# ---------------------------------------------------------------------------
# Page fetch
# ---------------------------------------------------------------------------

async def _fetch_page_events(
    client: httpx.AsyncClient, sem: asyncio.Semaphore, url: str
) -> list[RawEvent]:
    async with sem:
        try:
            resp = await client.get(
                url, timeout=TIMEOUT, headers=HEADERS, follow_redirects=True
            )
            if resp.status_code != 200:
                return []
            html = resp.text
        except Exception as e:
            logger.debug(f"mevalim: fetch failed {url}: {e}")
            return []
        finally:
            await asyncio.sleep(DELAY)

    page_h1 = _extract_page_h1(html)
    events: list[RawEvent] = []
    for item in _extract_json_ld_events(html):
        parsed = _parse_event(item, url, page_h1)
        if parsed is not None:
            events.append(parsed)
    return events


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def scrape_mevalim() -> list[RawEvent]:
    """Fetch every show + archive page from the Mevalim sitemap and return
    a deduplicated list of upcoming RawEvents across all IL cities."""
    async with httpx.AsyncClient(timeout=TIMEOUT, headers=HEADERS) as client:
        urls = await _fetch_sitemap_urls(client)
        if not urls:
            return []

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [_fetch_page_events(client, sem, url) for url in urls]
        per_page = await asyncio.gather(*tasks)

    # Flatten + dedup by (source_id = offer URL). A single show appears on
    # both the /{category}/{slug}/ page and one or more /{category}/{city}/
    # archives, so we collect the same event many times across pages.
    seen: set[str] = set()
    deduped: list[RawEvent] = []
    for batch in per_page:
        for ev in batch:
            if ev.source_id in seen:
                continue
            seen.add(ev.source_id)
            deduped.append(ev)

    with_city = sum(1 for e in deduped if e.venue_city)
    logger.info(
        f"mevalim: parsed events total={len(deduped)} with_city={with_city} "
        f"unresolved_city={len(deduped) - with_city}"
    )
    return deduped
