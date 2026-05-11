"""Israeli Basketball Premier League (ליגת Winner / BSL) collector.

The Israeli BSL — known commercially as ליגת ווינר / "Winner League" —
is the top-tier domestic basketball competition. Closes the gap left
by ESPN (their /basketball/isr.1/scoreboard endpoint returns HTTP 400)
and complements EuroLeagueCollector, which only covers the Israeli
clubs' European competition games (Maccabi Tel Aviv, Hapoel Tel Aviv).

Source: the public JSON feed behind the official site
https://basket.co.il/ ("מנהלת ליגת העל בכדורסל"). Discovered via
Chrome MCP network trace on 2026-05-12.

  https://basket.co.il/pbp/json/games_all.json    — full-season games
  https://basket.co.il/pbp/json/config.json       — season metadata

Each game record:
  id, GN (game number), team1/team2 (numeric ids), team_name_eng_1/2
  (English), team_name_1/2 (Hebrew, sometimes HTML-entity encoded),
  game_date_txt (DD/MM/YYYY), game_time (HH:MM), score_team1/2
  (0/0 when unplayed), pbp_link (live stats URL), liveChannel.

The feed does NOT include venue/arena, so we hardcode team_id →
home-city for the 14 active BSL teams. Home team is always team1.

Granularity: one Event per game. start_time stored in venue-local
(Asia/Jerusalem) wall-clock — the iCal export will stamp it with the
venue's TZID.
"""
from __future__ import annotations

import html
import logging
from datetime import date, datetime
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

_GAMES_URL = "https://basket.co.il/pbp/json/games_all.json"
_CONFIG_URL = "https://basket.co.il/pbp/json/config.json"
_TIMEOUT = 30
_HEADERS = {"User-Agent": "Mozilla/5.0"}


# Numeric team_id → (home city, country). Sourced from the BSL 2025-26
# season home arenas. Update when teams relocate / new clubs promote.
_TEAM_HOME: dict[int, tuple[str, str]] = {
    1109: ("Tel Aviv",         "Israel"),  # Maccabi Tel Aviv
    1110: ("Tel Aviv",         "Israel"),  # Hapoel Tel Aviv
    1111: ("Ramat Gan",        "Israel"),  # Maccabi Ramat Gan
    1112: ("Jerusalem",        "Israel"),  # Hapoel Jerusalem
    1113: ("Holon",            "Israel"),  # Hapoel Holon
    1114: ("Kiryat Ata",       "Israel"),  # Ironi Kiryat Ata
    1116: ("Ness Ziona",       "Israel"),  # Ness Ziona
    1118: ("Herzliya",         "Israel"),  # Bnei Herzliya
    1119: ("Kfar Blum",        "Israel"),  # Hapoel Galil Elyon (Upper Galilee)
    1120: ("Be'er Sheva",      "Israel"),  # Hapoel Be'er Sheva / Dimona
    1122: ("Afula",            "Israel"),  # Hapoel HaEmek (Jezreel Valley)
    1123: ("Rishon LeZion",    "Israel"),  # Maccabi Rishon LeZion
    1124: ("Ra'anana",         "Israel"),  # Maccabi Ra'anana
    2109: ("Netanya",          "Israel"),  # Elitzur Netanya
}


def _parse_dmy(s: str) -> Optional[date]:
    """Parse the BSL feed's 'DD/MM/YYYY' game_date_txt format."""
    if not s or "/" not in s:
        return None
    try:
        d, m, y = s.split("/")
        return date(int(y), int(m), int(d))
    except (ValueError, AttributeError):
        return None


def _team_name_clean(s: str) -> str:
    """English names are clean ASCII. Hebrew names sometimes contain
    HTML entities like '&quot;'. Decode them before use."""
    if not s:
        return s
    return html.unescape(s).strip()


def _build_raw(game: dict, requested_city: str) -> Optional[RawEvent]:
    team1_id = game.get("team1")
    home = _TEAM_HOME.get(team1_id)
    if not home:
        return None  # unknown team — likely a guest from a cup tournament
    home_city, home_country = home
    if home_city.lower() != requested_city.lower():
        return None

    gd = _parse_dmy(game.get("game_date_txt", ""))
    if not gd:
        return None
    if gd < date.today():
        return None  # already played

    home_name = _team_name_clean(game.get("team_name_eng_1") or game.get("team_name_1") or "")
    away_name = _team_name_clean(game.get("team_name_eng_2") or game.get("team_name_2") or "")
    if not home_name or not away_name:
        return None

    game_time = game.get("game_time") or None
    if game_time and len(game_time) > 5:
        game_time = game_time[:5]

    game_id = game.get("id")
    external = game.get("ExternalID")

    pbp = game.get("pbp_link") or None

    return RawEvent(
        name=f"Winner League - {home_name} vs {away_name}",
        start_date=gd,
        start_time=game_time,
        end_date=gd,
        end_time=None,
        artist_name=None,
        home_team=home_name,
        away_team=away_name,
        sport="Basketball",
        description="Israeli Basketball Premier League (ליגת Winner)",
        venue_name=None,
        venue_city=home_city,
        venue_country=home_country,
        purchase_link=pbp,
        source="bsl_winner_league",
        source_id=f"bsl-{game_id}" if game_id else f"bsl-ext-{external}",
        raw_categories=["Sports", "Basketball"],
    )


class BSLWinnerLeagueCollector(BaseCollector):
    @property
    def source_name(self) -> str:
        return "bsl_winner_league"

    def is_configured(self) -> bool:
        return True

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if not city_name:
            return []

        # Skip non-Israel cities outright — the entire BSL plays in IL.
        # Save the network hop when the registry iterates priority cities
        # outside the country.
        is_il_city = any(
            v[0].lower() == city_name.lower() for v in _TEAM_HOME.values()
        )
        if not is_il_city:
            return []

        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS) as client:
                r = await client.get(_GAMES_URL)
                if r.status_code != 200:
                    logger.warning(f"bsl_winner_league: {_GAMES_URL} → {r.status_code}")
                    return []
                data = r.json()
        except Exception as e:
            logger.warning(f"bsl_winner_league: fetch failed: {type(e).__name__}: {e}")
            return []

        # Feed is a 1-element list wrapping a dict with key 'games'.
        if isinstance(data, list) and data:
            games = (data[0] or {}).get("games") or []
        elif isinstance(data, dict):
            games = data.get("games") or []
        else:
            games = []

        results: list[RawEvent] = []
        for g in games:
            try:
                raw = _build_raw(g, city_name)
                if raw:
                    results.append(raw)
            except Exception as e:
                logger.debug(f"bsl_winner_league: skip game {g.get('id')}: {e}")

        logger.info(f"BSL Winner League: {len(results)} games in {city_name}")
        return results
