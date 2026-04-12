"""
OpenStreetMap utilities used by scheduled jobs:
  - Nominatim: look up a venue's website URL by name + city
  - Overpass: discover venue nodes/ways near a city centre
  - web_search_venue_url: DuckDuckGo → Serper.dev fallback for website lookup
"""
from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import quote_plus

import httpx

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "Supercaly/1.0 (+https://supercaly.app; event discovery bot)",
    "Accept": "application/json",
}
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URL  = "https://overpass-api.de/api/interpreter"
DDG_URL       = "https://api.duckduckgo.com/"
SERPER_URL    = "https://google.serper.dev/search"

# ─── OSM tags we consider "event venues" ────────────────────────────────────
# amenity=* tags
AMENITY_TAGS = [
    # Music / nightlife
    "music_venue", "nightclub", "concert_hall", "events_venue", "comedy_club",
    # Performing arts
    "theatre", "arts_centre",
    # Community / civic
    "community_centre", "social_centre", "public_hall",
    # Conferences / exhibitions
    "conference_centre", "exhibition_centre",
    # Other cultural / entertainment
    "casino", "cinema",
    # Hospitality (hotels that regularly host events / conferences)
    "bar",
]
# leisure=* tags
LEISURE_TAGS = [
    "music_venue",
    "stadium",
]
# tourism=* tags
TOURISM_TAGS = [
    "hotel",
    "conference_centre",
]
# building=* tags (fallback for venues only tagged by building type)
BUILDING_TAGS = [
    "concert_hall",
    "stadium",
]

DISCOVERY_RADIUS_M = 12_000   # 12 km around city centre

# Domains that are never useful as a venue "official website"
_JUNK_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "wikipedia.org", "wikidata.org", "yelp.com", "tripadvisor.com",
    "google.com", "maps.google.com", "foursquare.com",
    "booking.com", "expedia.com",
}


def _is_useful_url(url: str | None) -> bool:
    if not url:
        return False
    try:
        from urllib.parse import urlparse
        host = urlparse(url).netloc.lower().lstrip("www.")
        return not any(host.endswith(j) for j in _JUNK_DOMAINS)
    except Exception:
        return False


# ─── Nominatim ──────────────────────────────────────────────────────────────

async def nominatim_venue_url(
    client: httpx.AsyncClient, name: str, city: str, country: str
) -> Optional[str]:
    """
    Look up a single venue on OSM Nominatim and return its website URL
    (from extratags), or None if not found / URL is a junk domain.
    Rate limit: caller must sleep ≥ 1.1 s between calls.
    """
    query = name
    if city:
        query += f" {city}"
    if country and len(country) <= 3:
        query += f" {country}"

    try:
        resp = await client.get(
            NOMINATIM_URL,
            params={"q": query, "format": "json", "limit": 1, "extratags": 1},
            headers=_HEADERS,
            timeout=12,
        )
        if resp.status_code == 200:
            hits = resp.json()
            if hits:
                tags = hits[0].get("extratags") or {}
                url = (
                    tags.get("website")
                    or tags.get("url")
                    or tags.get("contact:website")
                )
                if _is_useful_url(url):
                    return url
    except Exception as e:
        logger.debug(f"Nominatim error for {name!r}: {e}")
    return None


# ─── DuckDuckGo Instant Answer (free, no key) ────────────────────────────────

async def _ddg_venue_url(
    client: httpx.AsyncClient, name: str, city: str
) -> Optional[str]:
    """
    Query the DuckDuckGo Instant Answer API for a venue URL.
    Reliable for well-known venues; returns None for obscure ones.
    """
    q = f"{name} {city} official website"
    try:
        resp = await client.get(
            DDG_URL,
            params={"q": q, "format": "json", "no_redirect": "1", "no_html": "1"},
            headers=_HEADERS,
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        # AbstractURL is the canonical URL DDG associates with the entity
        url = data.get("AbstractURL") or ""
        if _is_useful_url(url):
            return url
        # Fall through to Results list
        for r in data.get("Results", []):
            url = r.get("FirstURL", "")
            if _is_useful_url(url):
                return url
    except Exception as e:
        logger.debug(f"DDG error for {name!r}: {e}")
    return None


# ─── Serper.dev (paid, Google results) ───────────────────────────────────────

async def _serper_venue_url(
    client: httpx.AsyncClient, name: str, city: str, api_key: str
) -> Optional[str]:
    """
    Search Google via Serper.dev and return the first organic result URL
    that looks like an official venue website.
    """
    q = f"{name} {city} official site"
    try:
        resp = await client.post(
            SERPER_URL,
            json={"q": q, "num": 5},
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json",
            },
            timeout=12,
        )
        if resp.status_code != 200:
            logger.warning(f"Serper {resp.status_code} for {name!r}")
            return None
        for hit in resp.json().get("organic", []):
            url = hit.get("link", "")
            if _is_useful_url(url):
                return url
    except Exception as e:
        logger.debug(f"Serper error for {name!r}: {e}")
    return None


# ─── Public helper: full fallback chain ─────────────────────────────────────

async def find_venue_url(
    client: httpx.AsyncClient,
    name: str,
    city: str,
    country: str,
    serper_api_key: str = "",
) -> Optional[str]:
    """
    Try to find a venue's official website using a cascading strategy:
      1. OSM Nominatim  (free, OSM-crowd-sourced)
      2. DuckDuckGo Instant Answer  (free, less reliable)
      3. Serper.dev Google search   (paid, most reliable — only if key provided)

    Returns the first non-junk URL found, or None.
    Nominatim rate-limiting is the caller's responsibility (sleep ≥ 1.1 s).
    """
    url = await nominatim_venue_url(client, name, city, country)
    if url:
        logger.debug(f"find_venue_url [{name!r}] → Nominatim hit: {url}")
        return url

    url = await _ddg_venue_url(client, name, city)
    if url:
        logger.debug(f"find_venue_url [{name!r}] → DDG hit: {url}")
        return url

    if serper_api_key:
        url = await _serper_venue_url(client, name, city, serper_api_key)
        if url:
            logger.debug(f"find_venue_url [{name!r}] → Serper hit: {url}")
            return url

    logger.debug(f"find_venue_url [{name!r}] → no URL found")
    return None


# ─── Overpass venue discovery ────────────────────────────────────────────────

async def overpass_discover_venues(
    client: httpx.AsyncClient, lat: float, lon: float, city_name: str
) -> list[dict]:
    """
    Query Overpass API for venue nodes/ways within DISCOVERY_RADIUS_M of
    (lat, lon).  Returns a list of dicts with keys:
        name, lat, lon, website, address, venue_type
    """
    amenity_filter = "|".join(AMENITY_TAGS)
    leisure_filter = "|".join(LEISURE_TAGS)
    tourism_filter = "|".join(TOURISM_TAGS)
    building_filter = "|".join(BUILDING_TAGS)
    r = DISCOVERY_RADIUS_M

    query = f"""
[out:json][timeout:40];
(
  node["amenity"~"^({amenity_filter})$"]["name"](around:{r},{lat},{lon});
  way["amenity"~"^({amenity_filter})$"]["name"](around:{r},{lat},{lon});
  node["leisure"~"^({leisure_filter})$"]["name"](around:{r},{lat},{lon});
  way["leisure"~"^({leisure_filter})$"]["name"](around:{r},{lat},{lon});
  node["tourism"~"^({tourism_filter})$"]["name"](around:{r},{lat},{lon});
  way["tourism"~"^({tourism_filter})$"]["name"](around:{r},{lat},{lon});
  node["building"~"^({building_filter})$"]["name"](around:{r},{lat},{lon});
  way["building"~"^({building_filter})$"]["name"](around:{r},{lat},{lon});
);
out center body;
"""
    try:
        resp = await client.post(
            OVERPASS_URL,
            content=f"data={quote_plus(query)}",
            headers={**_HEADERS, "Content-Type": "application/x-www-form-urlencoded"},
            timeout=50,
        )
        if resp.status_code != 200:
            logger.warning(f"Overpass {resp.status_code} for {city_name}")
            return []

        venues = []
        for el in resp.json().get("elements", []):
            tags = el.get("tags", {})
            name = tags.get("name", "").strip()
            if not name:
                continue
            # nodes have lat/lon directly; ways expose a center object
            v_lat = el.get("lat") or (el.get("center") or {}).get("lat")
            v_lon = el.get("lon") or (el.get("center") or {}).get("lon")
            website = (
                tags.get("website")
                or tags.get("contact:website")
                or tags.get("url")
            )
            venues.append({
                "name": name,
                "lat": v_lat,
                "lon": v_lon,
                "website": website if _is_useful_url(website) else None,
                "address": " ".join(filter(None, [
                    tags.get("addr:housenumber", ""),
                    tags.get("addr:street", ""),
                ])).strip() or None,
                "venue_type": (
                    tags.get("amenity")
                    or tags.get("leisure")
                    or tags.get("tourism")
                    or tags.get("building")
                ),
            })
        logger.info(f"Overpass: {len(venues)} venues found near {city_name}")
        return venues

    except Exception as e:
        logger.warning(f"Overpass error for {city_name}: {e}")
        return []
