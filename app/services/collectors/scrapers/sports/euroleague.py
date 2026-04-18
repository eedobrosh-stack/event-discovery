"""
EuroLeague Basketball & EuroCup scraper — official API, no key required.

Endpoint:
  GET https://api-live.euroleague.net/v2/competitions/{comp}/seasons/{season}/games

comp:   E = EuroLeague  |  U = EuroCup
season: E{year} where year is the *start* year of the season
        (e.g. "E2025" = 2025-26 season, "U2025" = EuroCup 2025-26)

Each game record contains:
  local.club   → home team (name, code, crest URL)
  road.club    → away team
  utcDate      → UTC ISO-8601 tip-off time
  venue        → name + address (city parsed from address)
  played       → bool; we skip already-played games

City filtering: venue.address is parsed to extract the host city name and
compared to the city being collected. This correctly attributes home games to
their host city and neutral-venue games (Final Four) to whichever city hosts
the event.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

EUROLEAGUE_API = "https://api-live.euroleague.net/v2"
TIMEOUT = 20

# Known EuroLeague broadcast rights per country (2025-26 season)
_EL_TV: dict[str, list[dict]] = {
    "Spain":          [{"channel": "DAZN", "market": "national", "country": "ES", "type": "Streaming"}],
    "France":         [{"channel": "Canal+", "market": "national", "country": "FR", "type": "TV"},
                       {"channel": "Canal+ Sport", "market": "national", "country": "FR", "type": "TV"}],
    "Germany":        [{"channel": "DAZN", "market": "national", "country": "DE", "type": "Streaming"},
                       {"channel": "MagentaSport", "market": "national", "country": "DE", "type": "Streaming"}],
    "Italy":          [{"channel": "DAZN", "market": "national", "country": "IT", "type": "Streaming"},
                       {"channel": "Eurosport", "market": "national", "country": "IT", "type": "TV"}],
    "Turkey":         [{"channel": "beIN Sports", "market": "national", "country": "TR", "type": "TV"},
                       {"channel": "DAZN", "market": "national", "country": "TR", "type": "Streaming"}],
    "Greece":         [{"channel": "Cosmote Sport", "market": "national", "country": "GR", "type": "TV"}],
    "Israel":         [{"channel": "Sport 5", "market": "national", "country": "IL", "type": "TV"},
                       {"channel": "Sport 5+", "market": "national", "country": "IL", "type": "Streaming"}],
    "Lithuania":      [{"channel": "LNK", "market": "national", "country": "LT", "type": "TV"},
                       {"channel": "DAZN", "market": "national", "country": "LT", "type": "Streaming"}],
    "Serbia":         [{"channel": "Arena Sport", "market": "national", "country": "RS", "type": "TV"}],
    "Monaco":         [{"channel": "Canal+", "market": "national", "country": "MC", "type": "TV"}],
    "United States":  [{"channel": "ESPN+", "market": "national", "country": "US", "type": "Streaming"}],
    "United Kingdom": [{"channel": "DAZN", "market": "national", "country": "GB", "type": "Streaming"},
                       {"channel": "Eurosport", "market": "national", "country": "GB", "type": "TV"}],
    "Netherlands":    [{"channel": "DAZN", "market": "national", "country": "NL", "type": "Streaming"}],
}


def _season_codes(comp: str) -> list[str]:
    """Return the 1-2 season codes most likely to have upcoming fixtures."""
    today = date.today()
    year = today.year
    # Season starts in autumn: E2025 = Oct 2025 – May 2026
    # If month >= 7 (autumn), new season has started; also check previous for stragglers
    if today.month >= 7:
        return [f"{comp}{year}", f"{comp}{year - 1}"]
    else:
        return [f"{comp}{year - 1}", f"{comp}{year}"]


def _parse_city(address: str) -> str | None:
    """
    Extract city name from a venue address string.
    Handles formats like:
      "Aristides Maillol Av S/N, 08028 Barcelona"           → "Barcelona"
      "Yildirim … Sokak 2, 34354 Istanbul, Turkey"          → "Istanbul"
      "25 Avenue Marechal Juin, 01000 Bourg en Bresse, France" → "Bourg en Bresse"
      "37 Kifisias Avenue, 15123 Marousi"                   → "Marousi"

    Strategy: walk backwards through comma-separated parts; prefer a part that
    contains a digit (postal code prefix) since those reliably mark the city
    segment.  Skip pure-text trailing parts (country names like "Turkey",
    "France") that contain no digits.
    """
    if not address:
        return None
    parts = [p.strip() for p in address.split(",")]

    # First pass: find a part with a postal code → strip it → that's the city
    for part in reversed(parts):
        if any(c.isdigit() for c in part):
            city = re.sub(r"^\d[\d\s\-]+", "", part).strip()
            if city and len(city) > 1 and not city.isdigit():
                return city

    # Fallback: last non-trivial, non-digit-only part (handles "Street, City")
    for part in reversed(parts):
        city = part.strip()
        if city and len(city) > 1 and not city.isdigit():
            return city
    return None


def _parse_game(g: dict, city_name: str, country_code: str) -> Optional[RawEvent]:
    """Convert one API game dict into a RawEvent, or None if not in our city."""
    if g.get("played", True):
        return None

    # Venue + city filtering
    venue_block = g.get("venue") or {}
    venue_name = venue_block.get("name") or ""
    raw_address = venue_block.get("address") or ""
    venue_city = _parse_city(raw_address)

    # Keep only games hosted in the requested city
    if not venue_city:
        return None
    if venue_city.lower() != city_name.lower():
        return None

    # Date / time — use utcDate for consistency
    utc_str = g.get("utcDate") or g.get("date") or ""
    if not utc_str:
        return None
    try:
        utc_dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    if utc_dt.date() < date.today():
        return None

    # Teams
    home_club = (g.get("local") or {}).get("club") or {}
    away_club = (g.get("road") or {}).get("club") or {}
    home_name = home_club.get("name") or home_club.get("abbreviatedName") or ""
    away_name = away_club.get("name") or away_club.get("abbreviatedName") or ""
    if not home_name or not away_name:
        return None

    # Competition label
    season = g.get("season") or {}
    comp_code = season.get("competitionCode", "E")
    comp_label = "EuroLeague" if comp_code == "E" else "EuroCup"
    phase = (g.get("phaseType") or {}).get("name") or ""
    description = f"{comp_label} Basketball · {phase}" if phase else f"{comp_label} Basketball"

    # Home team logo
    image_url = (home_club.get("images") or {}).get("crest") or None

    # TV channels for this country
    tv_channels = _EL_TV.get(country_code, [])

    identifier = g.get("identifier") or g.get("id") or ""

    return RawEvent(
        name=f"{home_name} vs {away_name}",
        start_date=utc_dt.date(),
        start_time=utc_dt.strftime("%H:%M"),
        end_date=utc_dt.date(),
        end_time=None,
        artist_name=home_name,
        home_team=home_name,
        away_team=away_name,
        sport="Basketball",
        description=description,
        tv_channels=tv_channels,
        image_url=image_url,
        venue_name=venue_name or None,
        venue_city=venue_city,
        venue_country=country_code,
        purchase_link=None,
        source="euroleague",
        source_id=f"euroleague-{identifier}",
        raw_categories=["Sports", "Basketball"],
    )


async def _fetch_season(
    client: httpx.AsyncClient, comp: str, season_code: str
) -> list[dict]:
    """Fetch all games for one competition-season. Returns [] on any error."""
    url = f"{EUROLEAGUE_API}/competitions/{comp}/seasons/{season_code}/games"
    try:
        resp = await client.get(url, timeout=TIMEOUT)
        if resp.status_code == 200:
            return resp.json().get("data") or []
        if resp.status_code not in (404, 400):
            logger.warning(f"EuroLeague API {comp}/{season_code}: HTTP {resp.status_code}")
        return []
    except Exception as e:
        logger.warning(f"EuroLeague API {comp}/{season_code}: {e}")
        return []


class EuroLeagueCollector(BaseCollector):
    """
    Collects upcoming EuroLeague and EuroCup basketball fixtures
    from the official Euroleague Basketball API (free, no key).
    Filters to home games played in the requested city.
    """

    @property
    def source_name(self) -> str:
        return "euroleague"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if not city_name:
            return []

        headers = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

        all_games: list[dict] = []
        async with httpx.AsyncClient(timeout=TIMEOUT, headers=headers) as client:
            for comp in ("E", "U"):                          # EuroLeague + EuroCup
                for season_code in _season_codes(comp):
                    games = await _fetch_season(client, comp, season_code)
                    all_games.extend(games)

        results: list[RawEvent] = []
        for g in all_games:
            try:
                raw = _parse_game(g, city_name, country_code)
                if raw:
                    results.append(raw)
            except Exception as e:
                logger.debug(f"EuroLeague: skipping game {g.get('identifier')} — {e}")

        logger.info(
            f"EuroLeague/EuroCup: {len(results)} upcoming home games "
            f"in {city_name} ({country_code})"
        )
        return results
