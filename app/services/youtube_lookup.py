"""YouTube video lookup — find a sample video for an artist."""

from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import quote_plus

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

# In-memory cache: artist_name -> video_url (or None)
_cache: dict[str, str | None] = {}

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


async def lookup_youtube_video(artist_name: str) -> str | None:
    """Return a YouTube video URL for the given artist, or None."""
    if not artist_name or len(artist_name.strip()) < 3:
        return None

    key = artist_name.strip().lower()
    if key in _cache:
        return _cache[key]

    url = None
    try:
        if settings.YOUTUBE_API_KEY:
            url = await _lookup_via_api(artist_name.strip())
        if not url:
            url = await _lookup_via_scrape(artist_name.strip())
    except Exception as e:
        logger.warning(f"YouTube lookup failed for '{artist_name}': {e}")

    _cache[key] = url
    # Rate limit — 1 request per second
    await asyncio.sleep(1)
    return url


async def _lookup_via_api(artist_name: str) -> str | None:
    """Use YouTube Data API v3 to find a video."""
    params = {
        "part": "snippet",
        "q": artist_name,
        "type": "video",
        "maxResults": 1,
        "key": settings.YOUTUBE_API_KEY,
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            "https://www.googleapis.com/youtube/v3/search", params=params
        )
        resp.raise_for_status()
        data = resp.json()

    items = data.get("items", [])
    if items:
        video_id = items[0].get("id", {}).get("videoId")
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"
    return None


async def _lookup_via_scrape(artist_name: str) -> str | None:
    """Fallback: scrape YouTube search results page for the first video link."""
    query = quote_plus(f"{artist_name}")
    search_url = f"https://www.youtube.com/results?search_query={query}"

    async with httpx.AsyncClient(
        timeout=10,
        follow_redirects=True,
        headers={"User-Agent": _USER_AGENT},
    ) as client:
        resp = await client.get(search_url)
        resp.raise_for_status()
        html = resp.text

    # YouTube embeds video IDs in the initial page data
    match = re.search(r'"videoId"\s*:\s*"([a-zA-Z0-9_-]{11})"', html)
    if match:
        return f"https://www.youtube.com/watch?v={match.group(1)}"
    return None


def clear_cache() -> None:
    """Clear the in-memory YouTube cache."""
    _cache.clear()
