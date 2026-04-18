"""
MLB StatsAPI scraper — official, free, no key required.

Endpoint:
  https://statsapi.mlb.com/api/v1/schedule
    ?sportId=1&startDate=YYYY-MM-DD&endDate=YYYY-MM-DD
    &hydrate=venue(location),broadcasts(all),team

Returns regional TV broadcasts (home/away market channels) which the ESPN
API doesn't provide for MLB. Use alongside espn.py; the dedup logic in
registry.py will deduplicate by source_id (which differs between the two).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

BASE_URL = "https://statsapi.mlb.com/api/v1/schedule"
WINDOW_DAYS = 45
TIMEOUT = 20


def _parse_game(game: dict) -> Optional[RawEvent]:
    """Convert one MLB StatsAPI game dict into a RawEvent."""
    today = date.today()

    # Date / time
    date_str = game.get("gameDate", "")
    if not date_str:
        return None
    try:
        utc_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None
    if utc_dt.date() < today:
        return None

    # Teams
    teams = game.get("teams") or {}
    home_team = ((teams.get("home") or {}).get("team") or {}).get("name", "")
    away_team = ((teams.get("away") or {}).get("team") or {}).get("name", "")
    if not home_team or not away_team:
        return None

    # Venue
    venue_block = game.get("venue") or {}
    venue_name  = venue_block.get("name", "")
    location    = venue_block.get("location") or {}
    venue_city  = location.get("city", "")

    # TV broadcasts — regional channels included
    tv_channels: list[dict] = []
    seen: set[str] = set()
    for b in game.get("broadcasts") or []:
        ch   = b.get("name", "")
        mkt  = b.get("homeAway", "national")   # "home", "away", or absent
        btype = b.get("type", "TV")
        if ch and ch not in seen:
            seen.add(ch)
            tv_channels.append({
                "channel": ch,
                "market": mkt,
                "country": "US",
                "type": btype,
            })

    game_pk = game.get("gamePk", "")
    source_id = f"mlb-{game_pk}"

    return RawEvent(
        name=f"{home_team} vs {away_team}",
        start_date=utc_dt.date(),
        start_time=utc_dt.strftime("%H:%M"),
        end_date=utc_dt.date(),
        end_time=None,
        artist_name=home_team,
        home_team=home_team,
        away_team=away_team,
        sport="Baseball",
        tv_channels=tv_channels,
        purchase_link=None,
        venue_name=venue_name or None,
        venue_city=venue_city or None,
        venue_country="US",
        source="mlb_statsapi",
        source_id=source_id,
        raw_categories=["Sports", "Baseball"],
    )


class MlbStatsApiCollector(BaseCollector):
    """Collects MLB fixtures from the official free StatsAPI."""

    @property
    def source_name(self) -> str:
        return "mlb_statsapi"

    def is_configured(self) -> bool:
        return True  # no API key

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if (country_code or "").upper() not in ("US", "CA"):
            return []

        start = date.today()
        end   = start + timedelta(days=WINDOW_DAYS)

        url = (
            f"{BASE_URL}?sportId=1"
            f"&startDate={start.isoformat()}"
            f"&endDate={end.isoformat()}"
            f"&hydrate=venue(location),broadcasts(all),team"
        )

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.warning(f"MLB StatsAPI: fetch error — {e}")
                return []

        events: list[RawEvent] = []
        for day in data.get("dates") or []:
            for game in day.get("games") or []:
                try:
                    raw = _parse_game(game)
                    if raw:
                        events.append(raw)
                except Exception as e:
                    logger.debug(f"MLB StatsAPI: skipping game — {e}")

        logger.info(f"MLB StatsAPI: {len(events)} upcoming games")
        return events
