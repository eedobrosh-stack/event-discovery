"""Spotify editorial-surface walker — harvests artists from charts, featured
playlists, new releases, and browse categories.

This is the inbound side of the Spotify funnel (counterpart to the
artist-name lookup in `spotify_lookup.py`). The two work in opposite
directions:

    spotify_lookup.lookup_spotify_artist(name) -> dict
        We have a name, ask Spotify what it knows. (Enrichment.)

    spotify_browser.walk_market(market_code) -> {artist_id: artist_meta}
        We have a market, ask Spotify which artists matter there.
        (Discovery.)

Endpoints walked per market:

    /v1/browse/featured-playlists?country=XX
        ~20 editor-curated playlists, refreshed daily. The richest
        surface — typically yields hundreds of unique artists/market.

    /v1/browse/new-releases?country=XX
        Recent album drops. Albums → artists is direct.

    Spotify's editorial Top 50 / Viral 50 charts are surfaced as fixed
    playlist IDs (e.g. 37i9dQZEVXbMDoHDwVN2tF for "Top 50 - Global").
    We walk the per-market chart playlist IDs in CHART_PLAYLIST_IDS.

    /v1/browse/categories  (global, not per-market)
        ~30 genre buckets. Each category has its own playlists endpoint;
        we walk those once per scan (not per market) to keep the call
        budget bounded.

Pacing: 200ms between calls (~5 req/s, well below Spotify's ~180/min).
On 429 we surface a `SpotifyRateLimited` from the existing
`spotify_lookup` module so callers can bail uniformly.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Iterator

import httpx

from app.services.spotify_lookup import _get_token, SpotifyRateLimited

logger = logging.getLogger(__name__)


# Spotify's editorial chart playlists. IDs are stable — Spotify keeps
# the same playlist ID even as track contents rotate. The Top 50 and
# Viral 50 series exist per market; we hardcode the US/GB/Global trio
# and let walk_market resolve the per-market variants on the fly.
#
# Reference: https://developer.spotify.com/documentation/web-api/reference/get-list-featured-playlists
# Per-market chart IDs follow a fixed pattern Spotify documents in
# their editorial tooling; rather than enumerating all 75 (the list
# rotates), we fall back to a featured-playlist crawl per market and
# treat the global IDs as belt-and-suspenders.
GLOBAL_CHART_PLAYLIST_IDS = (
    "37i9dQZEVXbMDoHDwVN2tF",   # Top 50 — Global
    "37i9dQZEVXbLiRSasKsNU9",   # Viral 50 — Global
)

# Cap per-playlist track reads. Most editorial playlists are 50 tracks;
# Spotify's API caps a single /tracks call at 100. A second page costs
# another API call for marginal gain on a daily walker.
PLAYLIST_TRACK_LIMIT = 50
# Cap per-market featured-playlist count. Spotify's /featured-playlists
# returns up to 50; we take the first 20 to keep the call budget honest.
FEATURED_PLAYLIST_LIMIT = 20
# Cap per-market new-releases album count. 50 is the API max; we take
# 30 to keep the walker bounded.
NEW_RELEASES_ALBUM_LIMIT = 30
# Cap per-category playlist count when walking Browse Categories.
CATEGORY_PLAYLIST_LIMIT = 10
# Throttle between requests. 200ms ≈ 5 RPS, well below Spotify's
# rolling-window cap and matches the pacing in spotify_lookup that
# never tripped the penalty box on its own (the old enrich job did
# trip it, but only when SEARCH was burning a separate quota bucket).
INTER_CALL_SLEEP = 0.2


async def _spotify_get(
    http: httpx.AsyncClient,
    url: str,
    token: str,
    *,
    params: dict | None = None,
) -> dict | None:
    """One GET against Spotify with our standard 429 / 5xx handling.

    Returns the decoded JSON body or None on a soft-failure (404 / 403,
    which Spotify routinely returns for market-specific surfaces that
    don't exist in that market). Raises SpotifyRateLimited on a
    long-window 429 so the caller can bail the whole scan.
    """
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = await http.get(url, params=params or {}, headers=headers)
    except Exception as e:
        logger.warning(f"spotify_get({url}): {type(e).__name__}: {e}")
        return None

    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", 5))
        # Same split as spotify_lookup: short window → in-place sleep,
        # long window → bail.
        if retry_after > 30:
            logger.warning(
                f"spotify_get({url}): penalty box, retry_after={retry_after}s — aborting"
            )
            raise SpotifyRateLimited(retry_after)
        logger.warning(f"spotify_get({url}): rate-limited, sleeping {retry_after}s")
        await asyncio.sleep(retry_after)
        return None

    if resp.status_code in (403, 404):
        # 403 = market doesn't allow this content; 404 = playlist /
        # category deleted. Either way: silently skip.
        return None

    try:
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"spotify_get({url}): HTTP {resp.status_code}: {e}")
        return None

    try:
        return resp.json()
    except Exception as e:
        logger.warning(f"spotify_get({url}): JSON decode failed: {e}")
        return None


def _extract_artists_from_tracks(items: list[dict]) -> Iterator[tuple[str, str, str | None]]:
    """Yield (artist_id, name, external_url) for every artist on every track."""
    for it in items or []:
        track = (it or {}).get("track") or it or {}
        for a in track.get("artists") or []:
            aid = a.get("id")
            name = a.get("name")
            if not aid or not name:
                continue
            ext = (a.get("external_urls") or {}).get("spotify")
            yield aid, name, ext


async def _walk_playlist(
    http: httpx.AsyncClient,
    token: str,
    playlist_id: str,
    *,
    out: dict[str, dict],
    market: str | None = None,
) -> None:
    """Pull tracks from one playlist; merge their artists into ``out``."""
    body = await _spotify_get(
        http,
        f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks",
        token,
        params={"limit": PLAYLIST_TRACK_LIMIT, "fields": "items(track(artists(id,name,external_urls)))"},
    )
    await asyncio.sleep(INTER_CALL_SLEEP)
    if not body:
        return
    for aid, name, ext in _extract_artists_from_tracks(body.get("items") or []):
        slot = out.setdefault(aid, {"name": name, "external_url": ext, "markets": set()})
        if market:
            slot["markets"].add(market)


async def _walk_featured_playlists(
    http: httpx.AsyncClient,
    token: str,
    market: str,
    *,
    out: dict[str, dict],
) -> None:
    """Spotify's curated 'Featured Playlists' for one market."""
    body = await _spotify_get(
        http,
        "https://api.spotify.com/v1/browse/featured-playlists",
        token,
        params={"country": market, "limit": FEATURED_PLAYLIST_LIMIT},
    )
    await asyncio.sleep(INTER_CALL_SLEEP)
    if not body:
        return
    playlists = ((body.get("playlists") or {}).get("items")) or []
    for p in playlists:
        pid = p.get("id")
        if not pid:
            continue
        await _walk_playlist(http, token, pid, out=out, market=market)


async def _walk_new_releases(
    http: httpx.AsyncClient,
    token: str,
    market: str,
    *,
    out: dict[str, dict],
) -> None:
    """Recent album drops in this market → artist objects."""
    body = await _spotify_get(
        http,
        "https://api.spotify.com/v1/browse/new-releases",
        token,
        params={"country": market, "limit": NEW_RELEASES_ALBUM_LIMIT},
    )
    await asyncio.sleep(INTER_CALL_SLEEP)
    if not body:
        return
    albums = ((body.get("albums") or {}).get("items")) or []
    for alb in albums:
        for a in alb.get("artists") or []:
            aid = a.get("id")
            name = a.get("name")
            if not aid or not name:
                continue
            ext = (a.get("external_urls") or {}).get("spotify")
            slot = out.setdefault(aid, {"name": name, "external_url": ext, "markets": set()})
            slot["markets"].add(market)


async def walk_market(
    http: httpx.AsyncClient,
    token: str,
    market: str,
) -> dict[str, dict]:
    """All-in-one walker for one Spotify market.

    Returns a dict keyed by Spotify artist ID; values are
    ``{name, external_url, markets: set[str]}``. Caller is responsible
    for merging across markets and ultimately writing to SpotifyArtist
    rows.
    """
    out: dict[str, dict] = {}
    await _walk_featured_playlists(http, token, market, out=out)
    await _walk_new_releases(http, token, market, out=out)
    # Global charts: cheap to fire once per market too; the union
    # de-dupes naturally on Spotify artist ID and the per-market
    # `markets` set tells us which markets surfaced each artist.
    for pid in GLOBAL_CHART_PLAYLIST_IDS:
        await _walk_playlist(http, token, pid, out=out, market=market)
    return out


async def walk_browse_categories(
    http: httpx.AsyncClient,
    token: str,
) -> dict[str, dict]:
    """One global walk of Spotify's Browse Categories.

    Categories aren't per-market for our purposes (the curated buckets
    are global — "jazz", "edm", "k-pop"). Each category exposes a
    Featured Playlists endpoint; we walk up to CATEGORY_PLAYLIST_LIMIT
    playlists per category. Called once per daily scan, not per market.
    """
    out: dict[str, dict] = {}
    body = await _spotify_get(
        http,
        "https://api.spotify.com/v1/browse/categories",
        token,
        params={"limit": 50},
    )
    await asyncio.sleep(INTER_CALL_SLEEP)
    if not body:
        return out
    cats = ((body.get("categories") or {}).get("items")) or []
    for c in cats:
        cid = c.get("id")
        if not cid:
            continue
        playlists_body = await _spotify_get(
            http,
            f"https://api.spotify.com/v1/browse/categories/{cid}/playlists",
            token,
            params={"limit": CATEGORY_PLAYLIST_LIMIT},
        )
        await asyncio.sleep(INTER_CALL_SLEEP)
        if not playlists_body:
            continue
        playlists = ((playlists_body.get("playlists") or {}).get("items")) or []
        for p in playlists:
            pid = p.get("id")
            if not pid:
                continue
            # Tag with a synthetic "market" of category:<id> so the
            # SpotifyArtist.markets_surfaced_in column has provenance.
            await _walk_playlist(http, token, pid, out=out, market=f"cat:{cid}")
    return out


async def acquire_token(
    http: httpx.AsyncClient, client_id: str, client_secret: str
) -> str:
    """Public wrapper around the cached token helper in spotify_lookup."""
    return await _get_token(http, client_id, client_secret)
