"""
MusicBrainz-based performer lookup.

Resolves an artist name → (category, event_type_name, genres, mb_id, mb_type).

MusicBrainz is free, no API key required.
Rate limit: 1 request/second per the MusicBrainz ToS.
"""
import asyncio
import json
import logging
import re
import unicodedata
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

MB_BASE = "https://musicbrainz.org/ws/2"
MB_HEADERS = {
    "User-Agent": "Supercaly/1.0 (event-discovery; contact@supercaly.app)",
    "Accept": "application/json",
}
# Respect 1 req/sec MusicBrainz rate limit
_MB_DELAY = 1.1

# ─────────────────────────────────────────────────────────────────────────────
# Genre tag → our EventType name
# Priority: first match wins.  More specific tags should be listed first.
# ─────────────────────────────────────────────────────────────────────────────
TAG_TO_TYPE: list[tuple[list[str], str]] = [
    # Classical / Orchestral (must precede generic "rock", "pop" etc.)
    (["symphony orchestra", "philharmonic", "chamber orchestra",
      "orchestral music", "symphonic"],           "Symphony Orchestral Performances"),
    (["opera", "operetta"],                       "Fully Staged Opera"),
    (["baroque", "early music"],                  "Baroque Orchestras"),
    (["string quartet", "chamber music",
      "chamber ensemble"],                        "String Quartets"),
    (["choral", "choir", "a cappella"],            "Choral Performance"),

    # Jazz / Blues
    (["jazz", "bebop", "swing", "bossa nova",
      "smooth jazz", "fusion jazz", "blues"],     "Jazz Concert"),

    # Electronic
    (["techno", "house", "trance", "edm",
      "drum and bass", "dubstep", "electronic",
      "electronica", "ambient", "breakbeat",
      "rave", "psytrance", "industrial",
      "experimental electronic"],                 "Electronic / DJ Set"),

    # Hip-Hop
    (["hip-hop", "hip hop", "rap", "trap",
      "drill", "grime"],                          "Hip-Hop / Rap Concert"),

    # R&B / Soul
    (["r&b", "soul", "neo-soul", "funk",
      "motown", "rhythm and blues"],              "R&B / Soul Concert"),

    # Reggae
    (["reggae", "dancehall", "ska", "dub",
      "calypso"],                                 "Reggae / Calypso Concert"),

    # Latin
    (["latin", "salsa", "reggaeton", "cumbia",
      "bachata", "bossa nova", "samba",
      "latin pop"],                               "Latin Concert"),

    # Country / Folk / Americana
    (["country", "bluegrass", "americana",
      "folk rock", "folk"],                       "Country Concert"),

    # Gospel / Christian
    (["gospel", "christian music", "worship",
      "contemporary christian"],                  "Gospel Concert"),

    # Metal
    (["heavy metal", "death metal", "black metal",
      "thrash metal", "metal"],                   "Rock Concert"),

    # Rock / Alternative
    (["rock", "indie", "alternative", "punk",
      "grunge", "post-rock", "emo",
      "hard rock", "classic rock"],               "Rock Concert"),

    # Pop (broad – goes late so more specific tags win first)
    (["pop", "dance-pop", "synth-pop",
      "k-pop", "j-pop"],                          "Pop Concert"),

    # Comedy
    (["stand-up comedy", "comedy", "comedian",
      "satirical"],                               "Comedy Club Headliners"),

    # Theatre / Musical
    (["musical theatre", "musical theater",
      "broadway", "west end"],                    "Broadway Show"),
    (["theatre", "theater", "drama"],             "Play / Drama"),

    # Dance performance
    (["ballet"],                                  "Classical Ballet"),
    (["contemporary dance", "modern dance"],      "Modern Dance"),
    (["flamenco"],                                "Flamenco"),
]

# MusicBrainz artist *type* → event_type override (before looking at tags)
MB_TYPE_OVERRIDE: dict[str, tuple[str, str]] = {
    "Orchestra": ("Music", "Symphony Orchestral Performances"),
    "Choir":     ("Music", "Choral Performance"),
}

# Category lookup from type name
TYPE_TO_CATEGORY: dict[str, str] = {
    "Symphony Orchestral Performances": "Music",
    "Fully Staged Opera":               "Music",
    "Baroque Orchestras":               "Music",
    "String Quartets":                  "Music",
    "Choral Performance":               "Music",
    "Jazz Concert":                     "Music",
    "Electronic / DJ Set":              "Music",
    "Hip-Hop / Rap Concert":            "Music",
    "R&B / Soul Concert":               "Music",
    "Reggae / Calypso Concert":         "Music",
    "Latin Concert":                    "Music",
    "Country Concert":                  "Music",
    "Gospel Concert":                   "Music",
    "Rock Concert":                     "Music",
    "Pop Concert":                      "Music",
    "Comedy Club Headliners":           "Comedy",
    "Broadway Show":                    "Art",
    "Play / Drama":                     "Art",
    "Classical Ballet":                 "Dance",
    "Modern Dance":                     "Dance",
    "Flamenco":                         "Dance",
}


def normalize(name: str) -> str:
    """Lowercase, strip accents, remove non-alphanumeric except spaces."""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    name = re.sub(r"\s+", " ", name)
    return name


def tags_to_type(tags: list[str]) -> Optional[str]:
    """Map a list of MusicBrainz/Wikipedia tag names → our EventType name.
    Uses substring word matching so e.g. 'rock' matches 'progressive rock'."""
    tag_set = {t.lower() for t in tags}
    for keywords, event_type in TAG_TO_TYPE:
        for kw in keywords:
            for tag in tag_set:
                # Exact match OR keyword appears as a word within the tag
                if kw == tag or kw in tag.split() or tag in kw.split():
                    return event_type
    return None


async def lookup_musicbrainz(
    artist_name: str,
    http: httpx.AsyncClient,
) -> dict:
    """
    Query MusicBrainz for an artist.
    Returns dict with keys: mb_id, mb_type, genres, category, event_type_name, source, confidence
    """
    result = {
        "mb_id": None,
        "mb_type": None,
        "genres": [],
        "category": "Music",          # safe default when artist_name is set
        "event_type_name": "Concert",  # generic fallback
        "source": "fallback",
        "confidence": 0.3,
    }

    try:
        # Step 1 – Search
        search_resp = await http.get(
            f"{MB_BASE}/artist",
            params={"query": f'artist:"{artist_name}"', "limit": 3, "fmt": "json"},
            headers=MB_HEADERS,
            timeout=10,
        )
        await asyncio.sleep(_MB_DELAY)

        if search_resp.status_code != 200:
            logger.debug(f"MB search non-200 for '{artist_name}': {search_resp.status_code}")
            return result

        data = search_resp.json()
        artists = data.get("artists", [])
        if not artists:
            return result

        # Pick highest-score artist
        artist = artists[0]
        score = int(artist.get("score", 0))
        if score < 60:
            logger.debug(f"MB low confidence ({score}) for '{artist_name}'")
            return result

        mb_id = artist.get("id")
        mb_type = artist.get("type", "")
        result["mb_id"] = mb_id
        result["mb_type"] = mb_type
        result["source"] = "musicbrainz"
        result["confidence"] = round(score / 100, 2)

        # Step 2 – Fetch full artist with tags
        detail_resp = await http.get(
            f"{MB_BASE}/artist/{mb_id}",
            params={"inc": "tags", "fmt": "json"},
            headers=MB_HEADERS,
            timeout=10,
        )
        await asyncio.sleep(_MB_DELAY)

        if detail_resp.status_code == 200:
            detail = detail_resp.json()
            raw_tags = detail.get("tags", [])
            # Sort by vote count descending
            raw_tags.sort(key=lambda t: t.get("count", 0), reverse=True)
            genres = [t["name"].lower() for t in raw_tags[:20]]
            result["genres"] = genres

            # Tags take priority — they tell us the actual genre
            event_type = tags_to_type(genres)
            if event_type:
                result["event_type_name"] = event_type
                result["category"] = TYPE_TO_CATEGORY.get(event_type, "Music")
            elif mb_type in MB_TYPE_OVERRIDE:
                # No specific genre tags → fall back to MB artist type
                # (e.g. a symphony orchestra with no genre tags)
                result["category"], result["event_type_name"] = MB_TYPE_OVERRIDE[mb_type]
            else:
                # Has tags but none we recognise → generic Music
                result["event_type_name"] = "Concert"
                result["category"] = "Music"

        return result

    except httpx.TimeoutException:
        logger.warning(f"MB timeout for '{artist_name}'")
        return result
    except Exception as exc:
        logger.warning(f"MB error for '{artist_name}': {exc}")
        return result
