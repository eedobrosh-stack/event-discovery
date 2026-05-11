"""Wanda Diamond League athletics circuit collector.

The Diamond League is a 14-meeting annual outdoor track & field circuit
running roughly May–September. Each meeting is a single-day event at a
specific stadium in a specific city. Schedule is stable year-over-year
(same 14-15 cities, only dates shift) so we hardcode the meeting roster
and re-validate dates against each meeting's official subdomain page
(https://{subdomain}.diamondleague.com/) at scrape time.

Phase A item #2 of the leagues-spreadsheet rollout. Covers 12 country-
rows from the user's source sheet: Belgium, France, Germany (Berlin
historically), Italy, Morocco, Netherlands (when applicable), Norway,
Poland, Spain (when applicable), Sweden, Switzerland, UK, US, China,
Qatar, Monaco — most of the European "Athletics" entries.

Granularity: one Event per meeting (e.g. "Diamond League — Bislett
Games"). Per-event start times aren't surfaced by the DL site
consistently — left null so calendars treat the row as all-day. Users
planning around a meeting want to see "DL is in Stockholm on June 7,"
not curtain time of the 100m heats.

Update cadence: dates are auto-refreshed from each meeting's subdomain
on every fire. The hardcoded fallback date is used only if the
subdomain returns no parseable future iso date. The meeting roster
itself (cities, friendly names, subdomains) needs a once-a-year PR
when the new season's calendar is published.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

_TIMEOUT = 20
_HEADERS = {"User-Agent": "Mozilla/5.0"}


@dataclass(frozen=True)
class _Meeting:
    subdomain: str        # "shanghai", "rome", ...
    friendly_name: str    # "Shanghai Diamond League", "Golden Gala Pietro Mennea"
    city: str             # canonical city name matching our City.name
    country: str          # canonical country name matching our City.country
    fallback_date: str    # ISO date used if live fetch fails (this season)


# 2026 Diamond League roster. Annual maintenance is one PR — replace
# fallback_date with the next season's published dates.
_MEETINGS: list[_Meeting] = [
    _Meeting("shanghai",   "Shanghai Diamond League",                "Shanghai",    "China",          "2026-05-16"),
    _Meeting("xiamen",     "Xiamen Diamond League",                  "Xiamen",      "China",          "2026-05-23"),
    _Meeting("rabat",      "Meeting International Mohammed VI",      "Rabat",       "Morocco",        "2026-05-31"),
    _Meeting("rome",       "Golden Gala Pietro Mennea",              "Rome",        "Italy",          "2026-06-04"),
    _Meeting("stockholm",  "Bauhaus-Galan",                          "Stockholm",   "Sweden",         "2026-06-07"),
    _Meeting("oslo",       "Bislett Games",                          "Oslo",        "Norway",         "2026-06-10"),
    _Meeting("doha",       "Doha Diamond League",                    "Doha",        "Qatar",          "2026-06-19"),
    _Meeting("paris",      "Meeting de Paris",                       "Paris",       "France",         "2026-06-28"),
    _Meeting("eugene",     "Prefontaine Classic",                    "Eugene",      "United States",  "2026-07-04"),
    _Meeting("monaco",     "Meeting Herculis EBS",                   "Monaco",      "Monaco",         "2026-07-10"),
    _Meeting("london",     "London Athletics Meet",                  "London",      "United Kingdom", "2026-07-18"),
    _Meeting("lausanne",   "Athletissima",                           "Lausanne",    "Switzerland",    "2026-08-21"),
    _Meeting("silesia",    "Silesia Kamila Skolimowska Memorial",    "Chorzów",     "Poland",         "2026-08-23"),
    _Meeting("zurich",     "Weltklasse Zürich",                      "Zurich",      "Switzerland",    "2026-08-27"),
    _Meeting("brussels",   "Memorial Van Damme",                     "Brussels",    "Belgium",        "2026-09-04"),
]


async def _live_date(client: httpx.AsyncClient, subdomain: str) -> Optional[str]:
    """Fetch the meeting's official subdomain and return the earliest
    future iso date found in the HTML. None if the page failed or no
    future date present (in which case the caller falls back to the
    hardcoded date)."""
    try:
        r = await client.get(f"https://{subdomain}.diamondleague.com/")
        if r.status_code != 200:
            return None
        iso = sorted(set(re.findall(r"\b(20\d\d-\d\d-\d\d)\b", r.text)))
        today_str = date.today().isoformat()
        future = [d for d in iso if d >= today_str]
        return future[0] if future else None
    except Exception as e:
        logger.debug(f"diamond_league: {subdomain} live-date fetch failed: {e}")
        return None


async def _resolve_dates(meetings: list[_Meeting]) -> dict[str, date]:
    """Resolve each meeting's date — live first, fallback otherwise."""
    out: dict[str, date] = {}
    async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS,
                                  follow_redirects=True) as client:
        results = await asyncio.gather(
            *[_live_date(client, m.subdomain) for m in meetings],
            return_exceptions=True,
        )
    for m, live in zip(meetings, results):
        chosen = live if isinstance(live, str) else None
        if not chosen:
            chosen = m.fallback_date
        try:
            out[m.subdomain] = datetime.fromisoformat(chosen).date()
        except ValueError:
            logger.warning(f"diamond_league: unparseable date for {m.subdomain}: {chosen!r}")
    return out


class DiamondLeagueCollector(BaseCollector):
    @property
    def source_name(self) -> str:
        return "diamond_league"

    def is_configured(self) -> bool:
        return True

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if not city_name:
            return []

        # Filter to the meetings hosted in the requested city. With 15
        # meetings total, the per-fire fetch cost is bounded — but we
        # still skip the live-date roundtrip for cities that don't
        # host any meeting.
        relevant = [m for m in _MEETINGS if m.city.lower() == city_name.lower()]
        if not relevant:
            return []

        dates = await _resolve_dates(relevant)
        today = date.today()

        results: list[RawEvent] = []
        for m in relevant:
            d = dates.get(m.subdomain)
            if not d or d < today:
                continue
            results.append(RawEvent(
                name=f"Diamond League — {m.friendly_name}",
                start_date=d,
                start_time=None,         # times vary per event; all-day in cal
                end_date=d,
                end_time=None,
                artist_name=None,
                sport="Athletics",
                description="Wanda Diamond League — outdoor track & field",
                venue_name=None,
                venue_city=m.city,
                venue_country=m.country,
                purchase_link=f"https://{m.subdomain}.diamondleague.com/",
                source="diamond_league",
                source_id=f"diamond-league-{m.subdomain}-{d.isoformat()}",
                raw_categories=["Sports", "Athletics"],
            ))

        logger.info(
            f"Diamond League: {len(results)} meetings in {city_name}"
        )
        return results
