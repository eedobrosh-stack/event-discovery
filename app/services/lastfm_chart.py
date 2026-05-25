"""Last.fm chart fetcher — replaces Spotify's gated browse endpoints.

Background — Spotify deprecated /v1/browse/featured-playlists,
/v1/browse/new-releases, /v1/browse/categories, and /v1/playlists/*/tracks
for Client Credentials apps in late 2024. The original spotify_scan_job
design relied on all of those; first prod scan returned 0/0/0/0 because
every endpoint we hit responded 403 Forbidden.

Pivot: source "anybody who's anybody" from Last.fm instead. Last.fm's
chart API is free, no OAuth, no per-app quota application. The data is
plays-weighted (Spotify scrobbles feed Last.fm in real-time so the
chart is effectively a popularity ranking across Spotify, Apple Music,
Tidal, etc.) — strictly stronger as a "real artists" signal than the
single-platform editorial picks we'd lost.

Endpoints we use:

    chart.gettopartists
        Global top-played artists this week. One call → up to 1000
        artists, each with name, MBID (when known), URL, listeners,
        playcount. No country axis.

    geo.gettopartists?country=<full name>
        Same shape, scoped to one country. Country parameter is the
        ENGLISH NAME, not an ISO code — Spotify-market-code rotation
        from the previous design doesn't translate; the COUNTRIES tuple
        below uses Last.fm-accepted names.

The Spotify-profile-verification gate from the spec was dropped during
the pivot — Last.fm's chart presence already filters to artists with
real listener counts, and most charted artists have Spotify profiles
anyway since Spotify listening feeds the underlying scrobbles. Adding a
per-candidate Spotify Search step would re-trigger the 19.5h penalty
box the old enrich_spotify_job kept hitting.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

LASTFM_API_ROOT = "https://ws.audioscrobbler.com/2.0/"

# Hard caps so a single bad response can't balloon the result set. 1000
# is Last.fm's documented max per page; we never paginate (the second
# page of a top-artists chart is uninteresting long-tail).
CHART_LIMIT = 1000
GEO_LIMIT = 500
# Inter-call sleep — Last.fm doesn't publish a rate limit but informal
# guidance is ≤5 req/s sustained. 200ms keeps us under that.
INTER_CALL_SLEEP = 0.2


async def _lastfm_get(
    http: httpx.AsyncClient,
    api_key: str,
    method: str,
    *,
    extra_params: Optional[dict] = None,
) -> Optional[dict]:
    """Single Last.fm GET with JSON decoding + soft-failure handling.

    Returns the parsed JSON body or None on any error. We treat Last.fm
    errors as soft because the scan is best-effort and a transient
    failure on one country shouldn't kill the whole run.
    """
    params = {
        "method": method,
        "api_key": api_key,
        "format": "json",
        **(extra_params or {}),
    }
    try:
        resp = await http.get(LASTFM_API_ROOT, params=params, timeout=15)
    except Exception as e:
        logger.warning(f"lastfm_get({method}): {type(e).__name__}: {e}")
        return None
    if resp.status_code != 200:
        logger.warning(
            f"lastfm_get({method}): HTTP {resp.status_code}: "
            f"{(resp.text or '')[:200]}"
        )
        return None
    try:
        body = resp.json()
    except Exception as e:
        logger.warning(f"lastfm_get({method}): JSON decode failed: {e}")
        return None
    # Last.fm returns {error: N, message: "..."} on application-level
    # errors (e.g. invalid api key, unknown country). HTTP 200 in those
    # cases — surface them as logged misses.
    if isinstance(body, dict) and "error" in body:
        logger.warning(
            f"lastfm_get({method}): app error {body.get('error')}: "
            f"{body.get('message')}"
        )
        return None
    return body


def _normalize_artist_id(name: str, mbid: Optional[str]) -> str:
    """Derive a stable identifier for an artist row.

    Preference: MusicBrainz ID (36 chars when present — UUID-ish, the
    most stable identifier music data has). When Last.fm doesn't carry
    an MBID for an artist (~30% of long-tail), fall back to a SHA1 of
    the lowercase trimmed name — 40 chars hex, deterministic, fits in
    the existing SpotifyArtist.id String(40) column without a schema
    change. Different name spellings will produce different SHA1s; we
    accept the dedup imperfection because performer-side matching is
    case-folded anyway.
    """
    if mbid and len(mbid) >= 8:
        return mbid
    import hashlib
    return hashlib.sha1((name or "").strip().lower().encode("utf-8")).hexdigest()


def _extract_artists(body: dict, *, source_tag: str) -> dict[str, dict]:
    """Parse a chart/geo response into our common {id: {name, ...}} shape.

    `source_tag` is mixed into the markets_surfaced_in set so the
    SpotifyArtist row carries provenance (`global` or `country:US` etc).
    """
    out: dict[str, dict] = {}
    raw = ((body or {}).get("topartists") or {}).get("artist") or []
    # Some responses return a single dict instead of a list when there
    # is exactly one artist. Normalize.
    if isinstance(raw, dict):
        raw = [raw]
    for a in raw:
        name = (a or {}).get("name")
        if not name:
            continue
        mbid = (a or {}).get("mbid") or None
        aid = _normalize_artist_id(name, mbid)
        url = (a or {}).get("url") or None
        out.setdefault(aid, {
            "name": name,
            "external_url": url,
            "markets": set(),
        })["markets"].add(source_tag)
    return out


async def fetch_global_top(
    http: httpx.AsyncClient,
    api_key: str,
    *,
    limit: int = CHART_LIMIT,
) -> dict[str, dict]:
    """Top-played artists globally this week."""
    body = await _lastfm_get(
        http,
        api_key,
        "chart.gettopartists",
        extra_params={"limit": str(limit)},
    )
    await asyncio.sleep(INTER_CALL_SLEEP)
    if not body:
        return {}
    return _extract_artists(body, source_tag="global")


async def fetch_country_top(
    http: httpx.AsyncClient,
    api_key: str,
    country: str,
    *,
    limit: int = GEO_LIMIT,
) -> dict[str, dict]:
    """Top-played artists in one country.

    `country` is the English name (Last.fm doesn't accept ISO codes here).
    """
    body = await _lastfm_get(
        http,
        api_key,
        "geo.gettopartists",
        extra_params={"country": country, "limit": str(limit)},
    )
    await asyncio.sleep(INTER_CALL_SLEEP)
    if not body:
        return {}
    return _extract_artists(body, source_tag=f"country:{country}")
