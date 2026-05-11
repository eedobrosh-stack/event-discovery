"""ATP & WTA tennis tournament collector.

Pulls upcoming professional tennis tournaments from ESPN's public
tennis scoreboard endpoints:

  https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard?dates=YYYYMMDD-YYYYMMDD
  https://site.api.espn.com/apis/site/v2/sports/tennis/wta/scoreboard?dates=YYYYMMDD-YYYYMMDD

Each tournament arrives as a single multi-day event with start_date /
endDate / venue.displayName ("City, Country"). We persist one Event row
per tournament (not per individual match) — that's the granularity our
calendar UI is designed for, matching how people plan around tennis
("Roland Garros May 24 – June 8 in Paris") rather than per-match.

Filtering pattern mirrors EuroLeagueCollector: each registered city
gets only the tournaments hosted there. Israel and other non-tour-stop
countries naturally return zero results.

No API key needed — same free ESPN endpoint family the rest of
sports/espn.py uses.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

_ATP_URL = "https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard"
_WTA_URL = "https://site.api.espn.com/apis/site/v2/sports/tennis/wta/scoreboard"

# Look-ahead window. ESPN serves the full season inside one query but
# returns a smaller payload for shorter ranges — 180 days covers the
# next half-year of tour stops while keeping the response under ~3 MB
# (the 90-day probe was ~2 MB).
_LOOKAHEAD_DAYS = 180

_TIMEOUT = 30
_HEADERS = {"Accept": "application/json", "User-Agent": "Mozilla/5.0"}

# ESPN's tennis API serves several major tournaments with venue.displayName
# blank. We backfill the obvious ones by tournament name — the list is
# bounded (Slams + the biggest US events ESPN consistently misses).
# Case-insensitive substring match on the tournament name.
_KNOWN_TOURNAMENT_CITY: list[tuple[str, str, str]] = [
    ("us open",                       "New York",      "United States"),
    ("cincinnati open",               "Cincinnati",    "United States"),
    ("western & southern open",       "Cincinnati",    "United States"),
    ("winston-salem open",            "Winston-Salem", "United States"),
    ("indian wells",                  "Indian Wells",  "United States"),
    ("bnp paribas open",              "Indian Wells",  "United States"),
    ("miami open",                    "Miami",         "United States"),
    ("atlanta open",                  "Atlanta",       "United States"),
    ("citi open",                     "Washington",    "United States"),
    ("delray beach open",             "Delray Beach",  "United States"),
    ("dallas open",                   "Dallas",        "United States"),
    ("u.s. national indoor",          "Memphis",       "United States"),
]


def _city_country_for(e: dict) -> tuple[str, str]:
    """Resolve (city, country) for a tournament event. Prefers
    venue.displayName; falls back to the _KNOWN_TOURNAMENT_CITY map
    for ESPN's chronically-blank entries."""
    venue = e.get("venue") or {}
    display = (venue.get("displayName") or "").strip()
    if display:
        return _parse_venue_city_country(display)
    name = (e.get("name") or "").lower()
    for needle, city, country in _KNOWN_TOURNAMENT_CITY:
        if needle in name:
            return city, country
    return "", ""


def _parse_venue_city_country(display_name: str) -> tuple[str, str]:
    """ESPN tennis stores venue.displayName as 'City, Country' (e.g.
    'Rome, Italy'). Split on the last comma so two-word cities like
    'New York, USA' still resolve correctly. Returns (city, country)
    with both possibly empty."""
    if not display_name or "," not in display_name:
        return display_name.strip(), ""
    city, _, country = display_name.rpartition(",")
    return city.strip(), country.strip()


def _parse_iso_dt(s: str) -> Optional[datetime]:
    """ESPN gives e.g. '2026-05-24T04:00Z'. Parse with explicit UTC
    tz — same approach as euroleague.py's utcDate handling."""
    if not s:
        return None
    try:
        # ESPN omits the seconds on tennis dates ('04:00Z' not '04:00:00Z'),
        # so we normalise to a parseable form before fromisoformat.
        clean = s.replace("Z", "+00:00")
        # Add seconds if absent
        m = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})(?!:)", clean)
        if m and len(clean) < len("YYYY-MM-DDTHH:MM:SS+00:00"):
            clean = clean[:16] + ":00" + clean[16:]
        return datetime.fromisoformat(clean)
    except (ValueError, AttributeError):
        return None


def _build_raw_event(tour: str, e: dict, requested_city: str) -> Optional[RawEvent]:
    """Convert one ESPN tournament dict to a RawEvent, scoped to
    `requested_city`. Returns None if the tournament isn't hosted there."""
    venue_city, venue_country = _city_country_for(e)
    if not venue_city or venue_city.lower() != requested_city.lower():
        return None

    start_dt = _parse_iso_dt(e.get("date") or "")
    end_dt = _parse_iso_dt(e.get("endDate") or "")
    if not start_dt:
        return None
    # Skip only when the WHOLE tournament is in the past. In-progress
    # tournaments (started earlier this week, ending later) are still
    # of interest — users planning trips this week want to see them.
    cutoff_end = end_dt.date() if end_dt else start_dt.date()
    if cutoff_end < date.today():
        return None

    tour_label = tour.upper()  # "ATP" or "WTA"
    name = e.get("name") or e.get("shortName") or "Tennis Tournament"
    identifier = e.get("id") or ""

    description = f"{tour_label} Tour — Tennis"
    if e.get("major"):
        description += " · Grand Slam"

    return RawEvent(
        name=f"{tour_label} - {name}",
        # Tournaments are multi-day; leave start_time unset so calendars
        # treat the entry as all-day. The end_date marks the closing
        # day. iCal export's _event_tz falls back to venue.city.timezone
        # if available; for tennis we don't have a per-tournament fine
        # grain (each match has its own time), so all-day is honest.
        start_date=start_dt.date(),
        start_time=None,
        end_date=end_dt.date() if end_dt else start_dt.date(),
        end_time=None,
        artist_name=None,
        sport="Tennis",
        description=description,
        venue_name=None,
        venue_city=venue_city,
        venue_country=venue_country,
        purchase_link=None,
        source="tennis_espn",
        source_id=f"tennis-{tour}-{identifier}",
        raw_categories=["Sports", "Tennis"],
    )


async def _fetch_scoreboard(
    client: httpx.AsyncClient, base_url: str, date_range: str
) -> list[dict]:
    """One scoreboard request. Returns the events list or [] on error."""
    try:
        r = await client.get(base_url, params={"dates": date_range})
        if r.status_code != 200:
            logger.warning(f"tennis: {base_url} dates={date_range} → {r.status_code}")
            return []
        d = r.json()
        return d.get("events") or []
    except Exception as e:
        logger.warning(f"tennis: fetch failed {base_url}: {type(e).__name__}: {e}")
        return []


class TennisCollector(BaseCollector):
    @property
    def source_name(self) -> str:
        return "tennis_espn"

    def is_configured(self) -> bool:
        return True

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if not city_name:
            return []

        today = date.today()
        end = today + timedelta(days=_LOOKAHEAD_DAYS)
        date_range = f"{today.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"

        all_events: list[tuple[str, dict]] = []
        async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS) as client:
            atp_events = await _fetch_scoreboard(client, _ATP_URL, date_range)
            wta_events = await _fetch_scoreboard(client, _WTA_URL, date_range)
            for e in atp_events:
                all_events.append(("atp", e))
            for e in wta_events:
                all_events.append(("wta", e))

        results: list[RawEvent] = []
        for tour, e in all_events:
            try:
                raw = _build_raw_event(tour, e, city_name)
                if raw:
                    results.append(raw)
            except Exception as exc:
                logger.debug(f"tennis: skipping {e.get('id')} — {exc}")

        logger.info(
            f"Tennis (ATP+WTA): {len(results)} upcoming tournaments in {city_name}"
        )
        return results
