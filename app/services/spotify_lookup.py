"""
Spotify Web API — artist lookup for performer enrichment.

Uses the Client Credentials flow (no user login required).
Set SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET in .env.

What we get per artist:
  - genres[]        → event type / category assignment
  - images[0].url   → artist photo for event cards
  - popularity      → 0-100 ranking signal
  - external_urls.spotify → deep link
  - id              → stable Spotify artist ID

Spotify genre strings are lower-case, space-separated tags:
  "jazz", "bebop", "blues", "indie rock", "edm", "k-pop", etc.
We map them to our EventType taxonomy via GENRE_TO_EVENT_TYPE.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── Auth ─────────────────────────────────────────────────────────────────────

_token: Optional[str] = None
_token_expires_at: float = 0.0


async def _get_token(client: httpx.AsyncClient, client_id: str, client_secret: str) -> str:
    """Return a valid Client Credentials token, refreshing if expired."""
    global _token, _token_expires_at
    if _token and time.time() < _token_expires_at - 30:
        return _token
    resp = await client.post(
        "https://accounts.spotify.com/api/token",
        data={"grant_type": "client_credentials"},
        auth=(client_id, client_secret),
        timeout=10,
    )
    resp.raise_for_status()
    body = resp.json()
    _token = body["access_token"]
    _token_expires_at = time.time() + body.get("expires_in", 3600)
    return _token


# ── Genre → EventType mapping ─────────────────────────────────────────────────
# Spotify genres are very granular; we map sub-genres to our taxonomy.
# Order matters: first match wins, so put more specific entries first.

GENRE_TO_EVENT_TYPE: list[tuple[str, str]] = [
    # Electronic / Dance
    ("edm",              "Electronic / DJ Set"),
    ("electronic",       "Electronic / DJ Set"),
    ("techno",           "Electronic / DJ Set"),
    ("house",            "Electronic / DJ Set"),
    ("trance",           "Electronic / DJ Set"),
    ("drum and bass",    "Electronic / DJ Set"),
    ("dubstep",          "Electronic / DJ Set"),
    ("ambient",          "Electronic / DJ Set"),
    # Hip-Hop / Rap
    ("hip hop",          "Hip-Hop / Rap Concert"),
    ("rap",              "Hip-Hop / Rap Concert"),
    ("trap",             "Hip-Hop / Rap Concert"),
    ("grime",            "Hip-Hop / Rap Concert"),
    # R&B / Soul
    ("r&b",              "R&B / Soul Concert"),
    ("soul",             "R&B / Soul Concert"),
    ("neo soul",         "R&B / Soul Concert"),
    # Jazz
    ("jazz",             "Jazz Concert"),
    ("bebop",            "Jazz Concert"),
    ("swing",            "Jazz Concert"),
    ("blues",            "Blues Concert"),
    # Classical / Opera
    ("classical",        "Classical Concert"),
    ("opera",            "Opera"),
    ("orchestral",       "Classical Concert"),
    ("chamber music",    "Classical Concert"),
    ("choral",           "Classical Concert"),
    # Country
    ("country",          "Country Concert"),
    ("bluegrass",        "Country Concert"),
    ("americana",        "Country Concert"),
    # Folk / Acoustic
    ("folk",             "Folk / Acoustic Concert"),
    ("singer-songwriter","Folk / Acoustic Concert"),
    ("acoustic",         "Folk / Acoustic Concert"),
    # Reggae
    ("reggae",           "Reggae Concert"),
    ("dancehall",        "Reggae Concert"),
    # Metal / Punk
    ("metal",            "Rock Concert"),
    ("punk",             "Rock Concert"),
    ("hardcore",         "Rock Concert"),
    # Rock
    ("rock",             "Rock Concert"),
    ("indie",            "Rock Concert"),
    ("alternative",      "Rock Concert"),
    ("grunge",           "Rock Concert"),
    # Pop (broad — keep near bottom so specific genres win first)
    ("pop",              "Concert"),
    ("k-pop",            "Concert"),
    ("latin",            "Concert"),
    ("reggaeton",        "Concert"),
    # Comedy / Spoken Word
    ("comedy",           "Stand-Up Comedy"),
    ("spoken word",      "Spoken Word"),
    # Default
]


def genres_to_event_type(genres: list[str]) -> Optional[str]:
    """Return our EventType name for the first matching Spotify genre."""
    lowered = [g.lower() for g in genres]
    for keyword, event_type in GENRE_TO_EVENT_TYPE:
        if any(keyword in g for g in lowered):
            return event_type
    return None


def genres_to_category(genres: list[str]) -> str:
    """Return a broad category string from Spotify genres."""
    lowered = " ".join(genres).lower()
    if any(k in lowered for k in ("comedy", "spoken word")):
        return "Comedy"
    if any(k in lowered for k in ("classical", "opera", "orchestral", "choral")):
        return "Classical"
    return "Music"


# ── Main lookup ───────────────────────────────────────────────────────────────

_cache: dict[str, Optional[dict]] = {}


async def lookup_spotify_artist(
    artist_name: str,
    client_id: str,
    client_secret: str,
    http: Optional[httpx.AsyncClient] = None,
) -> Optional[dict]:
    """
    Search Spotify for `artist_name` and return enrichment data, or None.

    Returns:
        {
          "spotify_id": str,
          "spotify_url": str,
          "image_url": str | None,
          "popularity": int,          # 0-100
          "genres": list[str],
          "event_type_name": str | None,
          "category": str,
        }
    """
    if not artist_name or len(artist_name.strip()) < 2:
        return None

    name_key = artist_name.strip().lower()
    if name_key in _cache:
        return _cache[name_key]

    own_client = http is None
    if own_client:
        http = httpx.AsyncClient(timeout=10)

    try:
        token = await _get_token(http, client_id, client_secret)
        headers = {"Authorization": f"Bearer {token}"}

        resp = await http.get(
            "https://api.spotify.com/v1/search",
            params={"q": artist_name, "type": "artist", "limit": 3},
            headers=headers,
        )
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 5))
            logger.warning(f"Spotify rate-limited — sleeping {retry_after}s")
            await asyncio.sleep(retry_after)
            return None
        resp.raise_for_status()

        items = resp.json().get("artists", {}).get("items") or []
        if not items:
            _cache[name_key] = None
            return None

        # Pick the best match: exact name match preferred, else first result
        artist = next(
            (a for a in items if a["name"].lower() == name_key),
            items[0],
        )

        # Confidence check: reject if name similarity is too low
        sp_name = artist["name"].lower()
        if not (name_key in sp_name or sp_name in name_key):
            # Neither is a substring of the other — likely wrong artist
            _cache[name_key] = None
            return None

        genres: list[str] = artist.get("genres") or []
        images: list[dict] = artist.get("images") or []
        image_url = images[0]["url"] if images else None

        result = {
            "spotify_id":      artist["id"],
            "spotify_url":     artist.get("external_urls", {}).get("spotify"),
            "image_url":       image_url,
            "popularity":      artist.get("popularity", 0),
            "genres":          genres,
            "event_type_name": genres_to_event_type(genres),
            "category":        genres_to_category(genres) if genres else "Music",
        }
        _cache[name_key] = result
        return result

    except Exception as e:
        logger.warning(f"Spotify lookup error for {artist_name!r}: {e}")
        _cache[name_key] = None
        return None
    finally:
        if own_client:
            await http.aclose()
