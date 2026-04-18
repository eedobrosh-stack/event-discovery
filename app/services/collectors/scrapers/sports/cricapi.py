"""
CricAPI scraper — Australian cricket fixtures (BBL, Sheffield Shield, etc.)

Free API — 100 requests/day on the free tier.
Sign up at https://cricapi.com to get a key; set CRICAPI_KEY in your .env.

Endpoint: GET https://api.cricapi.com/v1/matches?apikey={key}&offset=0
Response: paginated list of upcoming matches with teams, venue, date, series info.

We filter by known Australian domestic competition names (BBL, WBBL, Sheffield Shield,
Marsh Cup) rather than relying on a country field, which is inconsistent in the API.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

import httpx

from app.config import settings
from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

CRICAPI_BASE = "https://api.cricapi.com/v1"
TIMEOUT = 15

# Australian domestic competition keywords (case-insensitive substring match)
AU_COMPETITIONS = [
    "big bash",
    "bbl",
    "wbbl",                         # Women's Big Bash
    "sheffield shield",
    "marsh cup",                    # domestic one-day cup
    "marsh one-day cup",
    "australia vs",                 # international home Tests/ODIs/T20s
    "vs australia",
]

# Israeli cricket is negligible — this collector is Australian-only
AU_TEAMS = {
    # BBL / WBBL
    "adelaide strikers",
    "brisbane heat",
    "hobart hurricanes",
    "melbourne renegades",
    "melbourne stars",
    "perth scorchers",
    "sydney sixers",
    "sydney thunder",
}

# Australian cricket broadcast rights 2024-25.
# Channel 7 holds free-to-air rights for Tests + selected BBL;
# Fox Sports / Kayo Sports carry all matches.
AU_CRICKET_TV: list[dict] = [
    {"channel": "Channel 7",    "market": "national", "country": "AU", "type": "TV"},
    {"channel": "Fox Cricket",  "market": "national", "country": "AU", "type": "TV"},
    {"channel": "Kayo Sports",  "market": "national", "country": "AU", "type": "Streaming"},
]


def _is_australian(match: dict) -> bool:
    """Return True if this match is an Australian domestic or home international."""
    series_name = (match.get("series") or match.get("series_name") or "").lower()
    if any(kw in series_name for kw in AU_COMPETITIONS):
        return True
    # Fallback: check team names
    teams = [t.lower() for t in (match.get("teams") or [])]
    if any(t in AU_TEAMS for t in teams):
        return True
    return False


def _parse_match(m: dict) -> Optional[RawEvent]:
    """Convert one CricAPI match dict into a RawEvent."""
    date_str = m.get("dateTimeGMT") or m.get("date") or ""
    if not date_str:
        return None
    try:
        if "T" in date_str:
            start_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            start_time = start_dt.strftime("%H:%M")
        else:
            start_dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
            start_time = None
    except (ValueError, AttributeError):
        return None

    if start_dt.date() < date.today():
        return None

    teams = m.get("teams") or []
    name = m.get("name") or (f"{teams[0]} vs {teams[1]}" if len(teams) >= 2 else "Cricket Match")
    series_name = m.get("series") or m.get("series_name") or ""
    venue_name = m.get("venue") or None
    match_id = m.get("id") or m.get("matchId") or ""

    # Determine match format label for categories
    match_type = (m.get("matchType") or "").lower()
    if match_type == "t20":
        sport_label = "Cricket (T20)"
    elif match_type == "test":
        sport_label = "Cricket (Test)"
    elif match_type in ("odi", "odi-w"):
        sport_label = "Cricket (ODI)"
    else:
        sport_label = "Cricket"

    home_team = teams[0] if teams else None
    away_team = teams[1] if len(teams) >= 2 else None

    return RawEvent(
        name=name,
        start_date=start_dt.date(),
        start_time=start_time,
        end_date=start_dt.date(),
        end_time=None,
        artist_name=None,
        sport=sport_label,
        home_team=home_team,
        away_team=away_team,
        tv_channels=AU_CRICKET_TV,
        venue_name=venue_name,
        venue_city=None,  # CricAPI doesn't return venue city reliably
        venue_country="Australia",
        description=series_name or None,
        purchase_link=None,
        image_url=None,
        source="cricapi",
        source_id=f"cricapi-{match_id}",
        raw_categories=["Sports", sport_label],
    )


class CricApiCollector(BaseCollector):
    """Collects Australian cricket fixtures from CricAPI (free 100 req/day)."""

    @property
    def source_name(self) -> str:
        return "cricapi"

    def is_configured(self) -> bool:
        return bool(settings.CRICAPI_KEY)

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        """Only runs for Australian cities."""
        if country_code != "Australia":
            return []
        if not settings.CRICAPI_KEY:
            return []

        all_matches: list[dict] = []
        offset = 0
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            while True:
                url = (
                    f"{CRICAPI_BASE}/matches"
                    f"?apikey={settings.CRICAPI_KEY}"
                    f"&offset={offset}"
                )
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        logger.warning(f"CricAPI: HTTP {resp.status_code}")
                        break
                    body = resp.json()
                    if body.get("status") != "success":
                        logger.warning(f"CricAPI: API error — {body.get('reason', 'unknown')}")
                        break
                    batch = body.get("data") or []
                    if not batch:
                        break
                    all_matches.extend(batch)
                    # CricAPI paginates at 25 per page; stop after 3 pages (75 matches)
                    # to stay well within the 100/day free limit
                    if len(all_matches) >= 75 or len(batch) < 25:
                        break
                    offset += 25
                except Exception as e:
                    logger.warning(f"CricAPI: fetch error — {e}")
                    break

        results: list[RawEvent] = []
        for m in all_matches:
            if not _is_australian(m):
                continue
            try:
                raw = _parse_match(m)
                if raw:
                    results.append(raw)
            except Exception as e:
                logger.debug(f"CricAPI: skipping match {m.get('id')} — {e}")

        logger.info(f"CricAPI: {len(results)} upcoming Australian cricket matches")
        return results
