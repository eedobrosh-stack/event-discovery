"""
Xceed event scraper — pulls events from xceed.me city listing pages.

Xceed is a multi-country nightlife platform (heavy Spain coverage + IT/PT/BE).
Each city listing page server-side-renders three event-discovery surfaces:

  1. JSON-LD <script> block with 3 schema.org Event objects (rich offers/price)
  2. Next.js React Query hydration data (`self.__next_f.push([1,"…"])`) with
     up to 24 events including full venue coordinates, lineup, and music genres
  3. Anchor tags to event detail pages (used as a sanity / discovery signal)

Strategy: parse both (1) and (2) and merge by legacyId, preferring hydration
for venue details and JSON-LD for price/offers.

URL pattern: https://xceed.me/en/{slug}/events
No API key required.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timezone

import httpx
from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent, safe_time

logger = logging.getLogger(__name__)

# City → xceed slug.  Sourced from /en homepage city grid (filtered).
CITY_SLUGS: dict[str, str] = {
    # Spain (primary market)
    "Madrid":      "madrid",
    "Barcelona":   "barcelona",
    "Valencia":    "valencia",
    "Sevilla":     "sevilla",
    "Seville":     "sevilla",
    "Bilbao":      "bilbao",
    "Malaga":      "malaga",
    "Málaga":      "malaga",
    "Marbella":    "marbella",
    "Ibiza":       "ibiza",
    "Palma de Mallorca": "palma-de-mallorca",
    "Mallorca":    "palma-de-mallorca",
    "Tenerife":    "tenerife",
    "Las Palmas":  "las-palmas-de-gran-canaria",
    "Lanzarote":   "lanzarote",
    "Alicante":    "alicante",
    "Benidorm":    "benidorm",
    "Murcia":      "murcia",
    "San Sebastian": "san-sebastian",
    "San Sebastián": "san-sebastian",
    "Tarragona":   "tarragona",
    "Vigo":        "vigo",
    "Girona":      "girona",
    "Segovia":     "segovia",
    "Tarifa":      "tarifa",
    "Torremolinos": "torremolinos",
    "Maspalomas":  "maspalomas",
    "Arrecife":    "arrecife",
    "Huelva":      "huelva",
    # Italy
    "Rome":        "roma",
    "Roma":        "roma",
    "Turin":       "torino",
    "Torino":      "torino",
    # Portugal
    "Porto":       "porto",
    # Belgium
    "Brussels":    "brussels",
}

BASE_URL = "https://xceed.me"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
}

# ─── Hydration extraction ──────────────────────────────────────────────────
_NEXT_F_RE = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', re.DOTALL)
_DATA_PAGES_RE = re.compile(r'"data":\{"pages":\[\[')


def _decode_next_payload(html: str) -> str:
    """Concatenate all __next_f.push payloads and unescape JSON-string."""
    chunks = _NEXT_F_RE.findall(html)
    if not chunks:
        return ""
    joined = "".join(chunks)
    try:
        return json.loads(f'"{joined}"')
    except (json.JSONDecodeError, ValueError):
        return ""


def _walk_balanced_array(text: str, start: int) -> str | None:
    """Return the substring of a balanced [...] starting at text[start] == '['."""
    if start >= len(text) or text[start] != "[":
        return None
    depth = 0
    in_str = False
    esc = False
    for i, ch in enumerate(text[start:], start):
        if esc:
            esc = False
            continue
        if ch == "\\":
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _extract_hydration_events(html: str) -> list[dict]:
    """Pull the popular-events query payload out of Next.js hydration chunks."""
    decoded = _decode_next_payload(html)
    if not decoded:
        return []
    idx = decoded.find('"popular-events"')
    if idx < 0:
        return []
    m = _DATA_PAGES_RE.search(decoded, idx)
    if not m:
        return []
    # m.end() is one past the second '['; back up one so we walk the INNER
    # bracket — that array holds the flat list of event dicts for page 0.
    start = m.end() - 1
    arr_text = _walk_balanced_array(decoded, start)
    if not arr_text:
        return []
    try:
        events = json.loads(arr_text)
    except json.JSONDecodeError:
        return []
    return [e for e in events if isinstance(e, dict)]


def _parse_hydration_event(ev: dict, city_slug: str) -> RawEvent | None:
    name = ev.get("name")
    starting = ev.get("startingTime")
    if not name or not starting:
        return None
    try:
        start_dt = datetime.fromtimestamp(int(starting), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
    if start_dt.date() < date.today():
        return None

    end_dt = None
    ending = ev.get("endingTime")
    if ending:
        try:
            end_dt = datetime.fromtimestamp(int(ending), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            end_dt = None

    venue = ev.get("venue") or {}
    coords = venue.get("coordinates") or {}
    city_obj = venue.get("city") or {}
    country_obj = city_obj.get("country") or {}

    lineup = ev.get("lineup") or []
    artist_name = None
    if lineup:
        first = lineup[0]
        if isinstance(first, dict) and not first.get("isGeneric", False):
            artist_name = first.get("name")

    genres = []
    for g in ev.get("musicGenres") or []:
        if isinstance(g, dict) and g.get("name"):
            genres.append(g["name"])

    legacy_id = ev.get("legacyId")
    slug = ev.get("slug")
    purchase_link = (
        f"{BASE_URL}/{city_slug}/event/{slug}/{legacy_id}"
        if slug and legacy_id else None
    )

    cover = ev.get("coverUrl")

    return RawEvent(
        name=name,
        start_date=start_dt.date(),
        start_time=safe_time(start_dt),
        end_date=end_dt.date() if end_dt else None,
        end_time=safe_time(end_dt) if end_dt else None,
        artist_name=artist_name,
        price=None,
        price_currency=city_obj.get("currency") or "EUR",
        purchase_link=purchase_link,
        image_url=cover,
        description=None,
        venue_name=venue.get("name"),
        venue_address=venue.get("address"),
        venue_city=city_obj.get("name"),
        venue_country=country_obj.get("isoCode") or country_obj.get("name"),
        venue_lat=coords.get("latitude"),
        venue_lon=coords.get("longitude"),
        venue_timezone=city_obj.get("timezone"),
        source="xceed",
        source_id=str(legacy_id) if legacy_id else None,
        raw_categories=genres,
    )


# ─── JSON-LD extraction (richer pricing) ───────────────────────────────────
def _parse_jsonld_event(ev: dict) -> RawEvent | None:
    if ev.get("eventStatus") == "https://schema.org/EventCancelled":
        return None
    start_str = ev.get("startDate", "")
    if not start_str:
        return None
    try:
        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
    except ValueError:
        return None
    if start_dt.date() < date.today():
        return None

    end_dt = None
    end_str = ev.get("endDate", "")
    if end_str:
        try:
            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        except ValueError:
            pass

    location = ev.get("location") or {}
    address = location.get("address") or {}
    geo = location.get("geo") or {}

    performers = ev.get("performer") or []
    if isinstance(performers, dict):
        performers = [performers]
    artist_name = None
    if performers:
        first = performers[0]
        if isinstance(first, dict):
            artist_name = first.get("name")

    offers_raw = ev.get("offers") or []
    if isinstance(offers_raw, dict):
        offers_raw = [offers_raw]
    cheapest = None
    currency = "EUR"
    for o in offers_raw:
        if not isinstance(o, dict):
            continue
        if o.get("availability") == "https://schema.org/SoldOut":
            continue
        raw_price = o.get("lowPrice") or o.get("price")
        try:
            p = float(raw_price)
        except (TypeError, ValueError):
            continue
        if cheapest is None or p < cheapest:
            cheapest = p
            currency = o.get("priceCurrency", currency) or currency

    image = ev.get("image")
    if isinstance(image, list) and image:
        image = image[0]

    url = ev.get("url", "")
    source_id = url.rstrip("/").split("/")[-1] if url else None

    return RawEvent(
        name=ev.get("name") or "Untitled Event",
        start_date=start_dt.date(),
        start_time=safe_time(start_dt),
        end_date=end_dt.date() if end_dt else None,
        end_time=safe_time(end_dt) if end_dt else None,
        artist_name=artist_name,
        price=cheapest,
        price_currency=currency,
        purchase_link=url or None,
        image_url=image,
        description=ev.get("description"),
        venue_name=location.get("name"),
        venue_address=address.get("streetAddress"),
        venue_city=address.get("addressLocality"),
        venue_country=address.get("addressCountry"),
        venue_lat=float(geo["latitude"]) if geo.get("latitude") else None,
        venue_lon=float(geo["longitude"]) if geo.get("longitude") else None,
        source="xceed",
        source_id=source_id,
        raw_categories=[],
    )


def _extract_jsonld_events(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    out: list[dict] = []
    for block in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(block.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if isinstance(item, dict) and item.get("@type") in ("Event", "MusicEvent"):
                out.append(item)
    return out


def _merge_price(raw: RawEvent, jsonld_by_legacy: dict[str, RawEvent]) -> RawEvent:
    """If JSON-LD knows a price for this legacyId, copy it onto the hydration row."""
    if not raw.source_id:
        return raw
    jl = jsonld_by_legacy.get(raw.source_id)
    if jl and jl.price is not None and raw.price is None:
        raw.price = jl.price
        raw.price_currency = jl.price_currency
        if not raw.description and jl.description:
            raw.description = jl.description
    return raw


def parse_listing(html: str, city_slug: str) -> list[RawEvent]:
    """Parse a single listing-page HTML into deduped RawEvents."""
    hydration_evs = _extract_hydration_events(html)
    jsonld_evs = _extract_jsonld_events(html)

    # Index JSON-LD parsed rows by legacyId (last URL segment) for price merge.
    jsonld_parsed: dict[str, RawEvent] = {}
    for ev in jsonld_evs:
        parsed = _parse_jsonld_event(ev)
        if parsed and parsed.source_id:
            jsonld_parsed[parsed.source_id] = parsed

    seen: set[str] = set()
    out: list[RawEvent] = []

    for hev in hydration_evs:
        parsed = _parse_hydration_event(hev, city_slug)
        if not parsed or not parsed.source_id:
            continue
        if parsed.source_id in seen:
            continue
        seen.add(parsed.source_id)
        out.append(_merge_price(parsed, jsonld_parsed))

    # Add JSON-LD-only events that weren't covered by hydration.
    for sid, parsed in jsonld_parsed.items():
        if sid in seen:
            continue
        seen.add(sid)
        out.append(parsed)

    return out


class XceedCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "xceed"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(
        self, city_name: str, country_code: str = "ES", **kwargs
    ) -> list[RawEvent]:
        slug = CITY_SLUGS.get(city_name)
        if not slug:
            return []

        url = f"{BASE_URL}/en/{slug}/events"
        try:
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(url, headers=_HEADERS)
        except Exception as exc:
            logger.warning(f"Xceed: request error for {city_name}: {exc}")
            return []

        if resp.status_code != 200:
            logger.warning(f"Xceed: HTTP {resp.status_code} for {city_name}")
            return []

        events = parse_listing(resp.text, slug)
        logger.info(f"Xceed: {city_name} → {len(events)} events")
        return events
