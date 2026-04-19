"""
ESPN hidden JSON API scraper for sports fixtures.

Endpoint pattern:
  https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}/scoreboard
    ?limit=200&dates=YYYYMMDD-YYYYMMDD

Returns upcoming fixtures with venue name, city, country and TV broadcast
channel names (US-market) for most leagues. No API key required.

Each fixture becomes a RawEvent where:
  name        = "Home Team vs Away Team"
  artist_name = None (sports events are not music — avoids mis-categorization)
  home_team   = home team name
  away_team   = away team name
  sport       = league category slug
  tv_channels = JSON list of broadcast dicts
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent
from app.services.collectors.scrapers.sports.leagues import ESPN_LEAGUES, LeagueConfig, COUNTRY_NAME_TO_ISO2

logger = logging.getLogger(__name__)

ESPN_API = "https://site.api.espn.com/apis/site/v2/sports"
ESPN_WEB = "https://site.web.api.espn.com/apis/site/v2/sports"

# City → IANA timezone for common sports venue cities.
# Covers every NBA/NHL/NFL/MLB market and major international cities.
_CITY_TZ: dict[str, str] = {
    # US East
    "Atlanta": "America/New_York", "Boston": "America/New_York",
    "Brooklyn": "America/New_York", "Charlotte": "America/New_York",
    "Cleveland": "America/New_York", "Detroit": "America/Detroit",
    "Indianapolis": "America/Indiana/Indianapolis", "Miami": "America/New_York",
    "Milwaukee": "America/Chicago", "New York": "America/New_York",
    "Newark": "America/New_York", "Orlando": "America/New_York",
    "Philadelphia": "America/New_York", "Pittsburgh": "America/New_York",
    "Toronto": "America/Toronto", "Washington": "America/New_York",
    "Baltimore": "America/New_York", "Buffalo": "America/New_York",
    "Jacksonville": "America/New_York", "Nashville": "America/Chicago",
    "Tampa": "America/New_York", "Raleigh": "America/New_York",
    # US Central
    "Chicago": "America/Chicago", "Dallas": "America/Chicago",
    "Houston": "America/Chicago", "Kansas City": "America/Chicago",
    "Memphis": "America/Chicago", "Minneapolis": "America/Chicago",
    "New Orleans": "America/Chicago", "Oklahoma City": "America/Chicago",
    "San Antonio": "America/Chicago", "St. Louis": "America/Chicago",
    "Elmont": "America/New_York",
    # US Mountain
    "Denver": "America/Denver", "Salt Lake City": "America/Denver",
    "Phoenix": "America/Phoenix",
    # US Pacific
    "Golden State": "America/Los_Angeles", "Los Angeles": "America/Los_Angeles",
    "Portland": "America/Los_Angeles", "Sacramento": "America/Los_Angeles",
    "San Francisco": "America/Los_Angeles", "Seattle": "America/Los_Angeles",
    "Vancouver": "America/Vancouver", "Las Vegas": "America/Los_Angeles",
    # Canada
    "Calgary": "America/Edmonton", "Edmonton": "America/Edmonton",
    "Montreal": "America/Toronto", "Ottawa": "America/Toronto",
    "Winnipeg": "America/Winnipeg",
    # Europe
    "London": "Europe/London", "Manchester": "Europe/London",
    "Berlin": "Europe/Berlin", "Munich": "Europe/Berlin",
    "Paris": "Europe/Paris", "Amsterdam": "Europe/Amsterdam",
    "Madrid": "Europe/Madrid", "Barcelona": "Europe/Madrid",
    "Rome": "Europe/Rome", "Milan": "Europe/Rome",
    "Athens": "Europe/Athens", "Istanbul": "Europe/Istanbul",
    "Moscow": "Europe/Moscow",
    # Australia
    "Sydney": "Australia/Sydney", "Melbourne": "Australia/Melbourne",
    "Brisbane": "Australia/Brisbane", "Adelaide": "Australia/Adelaide",
    "Perth": "Australia/Perth",
    # Israel
    "Tel Aviv": "Asia/Jerusalem",
}


def _utc_to_local(utc_dt: datetime, city: str) -> tuple[datetime, str | None]:
    """Convert a UTC datetime to venue-local time using city name. Returns (local_dt, tz_str)."""
    tz_str = _CITY_TZ.get(city)
    if not tz_str:
        return utc_dt, None
    try:
        from zoneinfo import ZoneInfo
        local_dt = utc_dt.astimezone(ZoneInfo(tz_str))
        return local_dt, tz_str
    except Exception:
        return utc_dt, None

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Fetch a 45-day rolling window
WINDOW_DAYS = 45
TIMEOUT = 20
CONCURRENCY = 4   # be gentle — undocumented API


def _fmt_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def _parse_broadcasts(competition: dict, country: str) -> list[dict]:
    """Extract TV broadcast info from a competition block."""
    seen: set[str] = set()
    result: list[dict] = []

    # broadcasts[] → simple list of channel name strings
    for b in competition.get("broadcasts", []):
        for name in b.get("names", []):
            if name and name not in seen:
                seen.add(name)
                result.append({
                    "channel": name,
                    "market": "national",
                    "country": country,
                    "type": "TV",
                })

    # geoBroadcasts[] → richer: type (TV/STREAMING) + market
    for gb in competition.get("geoBroadcasts", []):
        ch = (gb.get("media") or {}).get("shortName", "")
        t  = (gb.get("type") or {}).get("shortName", "TV")
        mk = (gb.get("market") or {}).get("type", "National")
        if ch and ch not in seen:
            seen.add(ch)
            result.append({
                "channel": ch,
                "market": mk.lower(),
                "country": country,
                "type": t,
            })

    return result


def _parse_event(raw: dict, cfg: LeagueConfig) -> Optional[RawEvent]:
    """Convert one ESPN scoreboard event dict into a RawEvent."""
    competitions = raw.get("competitions") or []
    if not competitions:
        return None
    comp = competitions[0]

    # Teams
    competitors = comp.get("competitors") or []
    if len(competitors) < 2:
        return None
    home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
    away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])
    home_name = (home.get("team") or {}).get("displayName", "")
    away_name = (away.get("team") or {}).get("displayName", "")
    if not home_name or not away_name:
        return None

    # Date / time (ESPN returns UTC ISO-8601)
    date_str = raw.get("date", "")
    if not date_str:
        return None
    try:
        utc_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError:
        return None
    if utc_dt.date() < date.today():
        return None

    # Venue
    venue_block = comp.get("venue") or {}
    venue_name = venue_block.get("fullName") or venue_block.get("name") or ""
    addr = venue_block.get("address") or {}
    venue_city    = addr.get("city", "")
    venue_country = addr.get("country", cfg.country)

    # TV broadcasts
    tv_channels = _parse_broadcasts(comp, cfg.country)

    # Source ID: ESPN's stable event id
    source_id = f"espn-{cfg.league}-{raw.get('id', '')}"

    # Image: prefer team logo
    image_url = (home.get("team") or {}).get("logo") or None

    # Convert UTC → venue local time so displayed times match the arena clock
    local_dt, tz_str = _utc_to_local(utc_dt, venue_city)

    # Ticket link: Ticketmaster search for the matchup
    team_query = f"{home_name} {away_name}".replace(" ", "+")
    purchase_link = f"https://www.ticketmaster.com/search?q={team_query}"

    return RawEvent(
        name=f"{cfg.label} - {home_name} vs {away_name}",
        start_date=local_dt.date(),
        start_time=local_dt.strftime("%H:%M"),
        end_date=local_dt.date(),
        end_time=None,
        artist_name=None,
        home_team=home_name,
        away_team=away_name,
        sport=cfg.category,
        tv_channels=tv_channels,
        purchase_link=purchase_link,
        image_url=image_url,
        venue_name=venue_name or None,
        venue_city=venue_city or None,
        venue_country=venue_country or cfg.country,
        venue_timezone=tz_str,
        source="espn_sports",
        source_id=source_id,
        raw_categories=["Sports", cfg.category],
    )


async def _fetch_league(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    cfg: LeagueConfig,
    start: date,
    end: date,
) -> list[RawEvent]:
    base = ESPN_WEB if cfg.use_web_domain else ESPN_API
    url = (
        f"{base}/{cfg.sport}/{cfg.league}/scoreboard"
        f"?limit=200&dates={_fmt_date(start)}-{_fmt_date(end)}"
    )
    async with sem:
        try:
            resp = await client.get(url, headers=HEADERS, timeout=TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"ESPN {cfg.label}: HTTP {resp.status_code}")
                return []
            data = resp.json()
        except Exception as e:
            logger.warning(f"ESPN {cfg.label}: fetch error — {e}")
            return []

    events_raw = data.get("events") or []
    results: list[RawEvent] = []
    for ev in events_raw:
        try:
            raw_event = _parse_event(ev, cfg)
            if raw_event:
                results.append(raw_event)
        except Exception as e:
            logger.debug(f"ESPN {cfg.label}: skipping malformed event — {e}")

    logger.info(f"ESPN {cfg.label} ({cfg.country}): {len(results)} upcoming fixtures")
    return results


class EspnSportsCollector(BaseCollector):
    """Collects upcoming sports fixtures from the ESPN hidden scoreboard API."""

    @property
    def source_name(self) -> str:
        return "espn_sports"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        # country_code is actually city.country — a full name like "United Kingdom".
        # Map it to ISO-2 to match LeagueConfig.country.
        iso2 = COUNTRY_NAME_TO_ISO2.get(country_code, "")
        leagues = [lg for lg in ESPN_LEAGUES if lg.matches_country(iso2)]
        if not leagues:
            return []

        start = date.today()
        end   = start + timedelta(days=WINDOW_DAYS)

        sem = asyncio.Semaphore(CONCURRENCY)
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            tasks = [_fetch_league(client, sem, lg, start, end) for lg in leagues]
            results = await asyncio.gather(*tasks)

        all_events: list[RawEvent] = []
        for batch in results:
            all_events.extend(batch)

        logger.info(
            f"ESPN sports: {len(all_events)} total fixtures for "
            f"{iso2} / {country_code} ({len(leagues)} leagues)"
        )
        return all_events
