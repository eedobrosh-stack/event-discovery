"""Server-side IP → city/country geo lookup.

Replaces the client-side ipapi.co fetch that v2.html used to do
directly. Going through our backend buys us three things:

  - Ad-blockers can't kill it. uBlock/EasyPrivacy block ipapi.co
    (and a bunch of other consumer IP-lookup endpoints) in the
    browser because the same endpoints are widely used for ad
    fingerprinting; a same-origin /api/geo call sails through.
  - We can switch providers without touching the frontend.
  - Per-IP caching pins each visitor at one upstream call per
    day regardless of how many times they reload.

Upstream is a cascade: **ipapi.co primary → ipwho.is fallback**.
The classic homepage hits ipapi.co directly from the browser and
returns the most accurate city for IL users (reported delta:
ipapi → Tel Aviv (correct), ipwho.is → Beer Sheva (~100km off).
Different providers, different MaxMind / IP2Location-vintage DBs).
So we want ipapi's accuracy when we can have it.

The catch: ipapi.co's free tier is 1k/day per CLIENT IP. From a
single server that's one bucket shared across all of Supercaly,
which is why the naive route was burning out fast. With the
24h per-visitor-IP cache here, we only spend one ipapi call per
unique visitor per day — comfortably under the limit until we hit
~1k unique visits/day. Past that, ipapi starts 429-ing and the
fallback kicks in: ipwho.is (10k/month, HTTPS, no auth, less
accurate but better than "Couldn't detect"). Visitors past the
budget get degraded data, not broken geo.

The response shape we hand back mirrors the ipapi.co schema the
frontend was already consuming (city, country_name, latitude,
longitude). ipwho.is uses `country` (full name) instead of
`country_name`; the per-provider parser normalizes to a single
schema so the caller doesn't need to know which DB it came from.
"""
from __future__ import annotations

import ipaddress
import time
from typing import Optional

import httpx
from fastapi import APIRouter, Request

router = APIRouter(prefix="/api/geo", tags=["geo"])

# In-memory cache. Same client IP usually hits us multiple times in
# a session (page reloads, tab navigation), and even across days
# users mostly stay in the same city — a 24h TTL keeps the upstream
# call count tiny without serving stale-by-a-month data.
_TTL_SECONDS = 24 * 60 * 60
_CACHE_MAX = 5000  # plenty of headroom; visitor IP count won't hit this
_cache: dict[str, tuple[dict, float]] = {}

# Empty-but-valid responses we hand back when there's no lookup to
# do. Mirrors the ipapi.co shape so the frontend's null-checks
# (geo.city, geo.country_name) work without special-casing.
_EMPTY_GEO: dict = {}


def _client_ip(request: Request) -> Optional[str]:
    """Pull the originating client IP. Render fronts uvicorn with a
    proxy that sets X-Forwarded-For; the FIRST entry is the actual
    client (subsequent entries are intermediaries we don't care
    about). Falls back to request.client.host for local dev.
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    client = request.client
    return client.host if client else None


def _is_lookup_eligible(ip: str) -> bool:
    """Filter out IPs that ipapi will refuse anyway (loopback,
    private RFC1918, link-local) — saves a round-trip on local dev
    and shields us from burning upstream quota on garbage."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return not (
        addr.is_loopback
        or addr.is_private
        or addr.is_link_local
        or addr.is_unspecified
        or addr.is_multicast
        or addr.is_reserved
    )


def _cache_get(ip: str) -> Optional[dict]:
    entry = _cache.get(ip)
    if entry is None:
        return None
    payload, ts = entry
    if (time.time() - ts) > _TTL_SECONDS:
        _cache.pop(ip, None)
        return None
    return payload


def _cache_put(ip: str, payload: dict) -> None:
    # Naive LRU-ish trim: when full, drop the oldest entry by
    # timestamp. Simple and good enough — the cache turns over
    # slowly and we only pay this cost on cold-cache writes.
    if len(_cache) >= _CACHE_MAX:
        oldest_ip = min(_cache, key=lambda k: _cache[k][1])
        _cache.pop(oldest_ip, None)
    _cache[ip] = (payload, time.time())


async def _lookup_ipapi(ip: str) -> dict:
    """Primary provider — ipapi.co. Reported as the most accurate
    geo-DB for our user base (matches what the classic homepage
    has shown all along, since home.js fetches ipapi.co directly).
    Returns {} on any failure including 429 rate limits so the
    cascade can fall through to the secondary."""
    url = f"https://ipapi.co/{ip}/json/"
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            r = await client.get(url, headers={"User-Agent": "supercaly/1.0"})
        if r.status_code != 200:
            return _EMPTY_GEO
        data = r.json()
    except Exception:
        return _EMPTY_GEO
    # ipapi.co failure shape: {"error": true, "reason": "...", ...}.
    if not isinstance(data, dict) or data.get("error"):
        return _EMPTY_GEO
    return {
        "city":         data.get("city") or "",
        "country_name": data.get("country_name") or "",
        "country_code": data.get("country_code") or "",
        "region":       data.get("region") or "",
        "latitude":     data.get("latitude"),
        "longitude":    data.get("longitude"),
        "timezone":     data.get("timezone") or "",
        "_provider":    "ipapi",
    }


async def _lookup_ipwho(ip: str) -> dict:
    """Fallback provider — ipwho.is. Less accurate DB (sometimes
    off by 100km — Tel Aviv → Beer Sheva for our IL users) but
    much higher free-tier budget (10k/month, no auth) so it can
    pick up the slack when ipapi.co's per-IP daily cap exhausts.
    Normalizes ipwho.is's `country` (full name) → `country_name`
    so the merged response matches the ipapi.co schema."""
    url = f"https://ipwho.is/{ip}"
    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            r = await client.get(url, headers={"User-Agent": "supercaly/1.0"})
        if r.status_code != 200:
            return _EMPTY_GEO
        data = r.json()
    except Exception:
        return _EMPTY_GEO
    if not isinstance(data, dict) or not data.get("success"):
        return _EMPTY_GEO
    return {
        "city":         data.get("city") or "",
        "country_name": data.get("country") or "",
        "country_code": data.get("country_code") or "",
        "region":       data.get("region") or "",
        "latitude":     data.get("latitude"),
        "longitude":    data.get("longitude"),
        "timezone":     (data.get("timezone") or {}).get("id") or "",
        "_provider":    "ipwho",
    }


async def _lookup(ip: str) -> dict:
    """Provider cascade. ipapi.co → ipwho.is → {}.

    Returns the first non-empty response. ipapi.co goes first
    because it's measurably more accurate for our user base; the
    fallback only kicks in once ipapi.co rate-limits us, so
    most visits get the better DB. Worst case (both providers
    fail / rate-limit), returns {} and the frontend renders
    "Couldn't detect your location" cleanly.

    A "good" ipapi response means city is non-empty — empty-city
    responses still indicate the provider gave us something but
    we wouldn't render anything useful, so let the secondary
    have a crack.
    """
    primary = await _lookup_ipapi(ip)
    if primary and primary.get("city"):
        return primary
    return await _lookup_ipwho(ip)


@router.get("")
async def geo(request: Request) -> dict:
    """Resolve the caller's IP to a city/country bundle.

    Always returns 200 with a JSON object. On any failure (private
    IP, upstream timeout, rate limit, parse error) the response is
    just {} — the frontend treats that as "no geo available" and
    falls through to its existing "Couldn't detect your location"
    branch.
    """
    ip = _client_ip(request)
    if not ip or not _is_lookup_eligible(ip):
        return _EMPTY_GEO

    cached = _cache_get(ip)
    if cached is not None:
        return cached

    payload = await _lookup(ip)
    # Cache even empty responses — if ipapi rate-limited us we
    # don't want to hammer them on every reload of the same
    # page. The TTL is long but a 24h backoff on failures is fine
    # for a low-stakes UX hint like geo prefilling.
    _cache_put(ip, payload)
    return payload
