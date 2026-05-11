"""UCI WorldTour road cycling collector.

Phase A item #4 — covers the 9 country-rows with "Cycling: UCI
WorldTour" from the leagues sheet (Belgium, France, Italy, Spain,
Netherlands, Luxembourg, Switzerland, Denmark, Colombia/Portugal
when applicable), plus extras (Australia, UAE, UK, Canada, Poland,
Germany, China).

Source: Wikipedia's "{YEAR} UCI World Tour" article — the canonical
season schedule. UCI's own site doesn't expose a programmatic API
and ProCyclingStats blocks scrapers with a Cloudflare wall.

  https://en.wikipedia.org/wiki/{year}_UCI_World_Tour

Wikipedia maintains one wikitable per season with columns
Race | Date | Winner | Second | Third. We parse the table, match
race name against a hardcoded race→(city, country) map (multi-stage
races traverse multiple cities, so we anchor each one to a canonical
host city — Tour de France→Paris, Liège-Bastogne-Liège→Liège, etc.),
filter to upcoming races, and emit one Event per race.

Granularity: one Event per race. Multi-stage tours (Giro, Tour de
France, Vuelta) span 2-3 weeks and we set start_date/end_date
accordingly. Single-day Monuments (Milan-San Remo, Tour of Flanders,
Paris-Roubaix, etc.) get a 1-day window.

Maintenance
===========
* Annual: update the year in `_WIKI_URL_TEMPLATE` (or compute from
  today's date once we cross the year boundary). Race set is stable
  ±2 races per season.
* If UCI promotes a new race into WorldTour status, add it to
  `_RACE_HOST` so it falls through cleanly.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime
from html import unescape
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

_TIMEOUT = 30
_HEADERS = {"User-Agent": "Mozilla/5.0"}
_WIKI_URL_TEMPLATE = "https://en.wikipedia.org/wiki/{year}_UCI_World_Tour"

# Hardcoded race → (host city, country). Anchored to the race's
# canonical host city — start city for one-day classics (Paris-Roubaix
# starts in Compiègne but Roubaix is the famous finish), traditional
# finish city for Grand Tours. Multi-stage races technically cross
# countries (Tour de France increasingly visits neighbours), but the
# canonical association is what matters for calendar lookup.
#
# Keys are matched case-insensitively against the Wikipedia race
# column. We strip leading/trailing slashes and whitespace before
# the lookup (Wiki occasionally has trailing "/" from broken markup).
_RACE_HOST: dict[str, tuple[str, str]] = {
    "tour down under":                       ("Adelaide", "Australia"),
    "cadel evans great ocean road race":     ("Geelong", "Australia"),
    "uae tour":                              ("Dubai", "United Arab Emirates"),
    "omloop het nieuwsblad":                 ("Ghent", "Belgium"),
    "strade bianche":                        ("Siena", "Italy"),
    "paris-nice":                            ("Paris", "France"),
    "tirreno-adriatico":                     ("Lido di Camaiore", "Italy"),
    "milan-san remo":                        ("Milan", "Italy"),
    "volta a catalunya":                     ("Barcelona", "Spain"),
    "tour of bruges":                        ("Bruges", "Belgium"),
    "e3 saxo classic":                       ("Harelbeke", "Belgium"),
    "gent-wevelgem":                         ("Ghent", "Belgium"),
    "dwars door vlaanderen":                 ("Roeselare", "Belgium"),
    "tour of flanders":                      ("Antwerp", "Belgium"),
    "tour of the basque country":            ("Bilbao", "Spain"),
    "paris-roubaix":                         ("Roubaix", "France"),
    "amstel gold race":                      ("Maastricht", "Netherlands"),
    "la flèche wallonne":                    ("Liège", "Belgium"),
    "liège-bastogne-liège":                  ("Liège", "Belgium"),
    "tour de romandie":                      ("Lausanne", "Switzerland"),
    "eschborn-frankfurt":                    ("Frankfurt", "Germany"),
    "giro d'italia":                         ("Rome", "Italy"),
    "tour auvergne-rhône-alpes":             ("Lyon", "France"),
    "copenhagen sprint":                     ("Copenhagen", "Denmark"),
    "tour de suisse":                        ("Bern", "Switzerland"),
    "tour de france":                        ("Paris", "France"),
    "clásica de san sebastián":              ("San Sebastián", "Spain"),
    "tour de pologne":                       ("Warsaw", "Poland"),
    "hamburg cyclassics":                    ("Hamburg", "Germany"),
    "renewi tour":                           ("Antwerp", "Belgium"),
    "vuelta a españa":                       ("Madrid", "Spain"),
    "bretagne classic":                      ("Plouay", "France"),
    "grand prix cycliste de québec":         ("Quebec City", "Canada"),
    "grand prix cycliste de montréal":       ("Montreal", "Canada"),
    "il lombardia":                          ("Como", "Italy"),
    "tour of guangxi":                       ("Beihai", "China"),
}


_MONTHS = {
    m.lower(): i for i, m in enumerate(
        ["", "January", "February", "March", "April", "May", "June",
         "July", "August", "September", "October", "November", "December"],
        start=0,
    ) if m
}


def _normalize_dash(s: str) -> str:
    # Wikipedia mixes en-dash (–), em-dash (—), and hyphen (-).
    # Normalize everything to plain hyphen for matching.
    return s.replace("–", "-").replace("—", "-").replace("\xa0", " ")


def _normalize_race(name: str) -> str:
    return _normalize_dash(name).strip().lstrip("/").strip().lower()


_RX_SINGLE = re.compile(r"^\s*(\d{1,2})\s+([A-Za-z]+)\s*$")
_RX_RANGE_SAME = re.compile(r"^\s*(\d{1,2})\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s*$")
_RX_RANGE_CROSS = re.compile(r"^\s*(\d{1,2})\s+([A-Za-z]+)\s*-\s*(\d{1,2})\s+([A-Za-z]+)\s*$")


def _parse_date_range(date_str: str, year: int) -> Optional[tuple[date, date]]:
    """Wikipedia date forms: '1 February', '20-25 January',
    '22 August - 13 September'. Returns (start, end), or None on
    parse failure."""
    s = _normalize_dash(date_str).strip()
    m = _RX_RANGE_CROSS.match(s)
    if m:
        d1, mo1, d2, mo2 = m.groups()
        try:
            start = date(year, _MONTHS[mo1.lower()], int(d1))
            end_year = year
            # If end month is earlier than start, the range wraps to
            # next year — Wikipedia uses this for January races that
            # start in late December (rare). We assume same-year here.
            end = date(end_year, _MONTHS[mo2.lower()], int(d2))
            return start, end
        except (KeyError, ValueError):
            return None
    m = _RX_RANGE_SAME.match(s)
    if m:
        d1, d2, mo = m.groups()
        try:
            month = _MONTHS[mo.lower()]
            return date(year, month, int(d1)), date(year, month, int(d2))
        except (KeyError, ValueError):
            return None
    m = _RX_SINGLE.match(s)
    if m:
        d, mo = m.groups()
        try:
            day = int(d)
            month = _MONTHS[mo.lower()]
            return date(year, month, day), date(year, month, day)
        except (KeyError, ValueError):
            return None
    return None


def _strip_html(s: str) -> str:
    return unescape(re.sub(r"<[^>]+>", "", s)).strip()


def _parse_wikitable(html: str) -> list[tuple[str, str]]:
    """Returns [(race_name, date_str), ...] from the first wikitable."""
    m = re.search(r'<table[^>]*class="[^"]*wikitable[^"]*"[^>]*>(.*?)</table>',
                  html, re.DOTALL)
    if not m:
        return []
    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", m.group(1), re.DOTALL)
    out: list[tuple[str, str]] = []
    for row in rows[1:]:  # skip header
        cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row, re.DOTALL)
        if len(cells) < 2:
            continue
        race = _strip_html(cells[0])
        date_str = _strip_html(cells[1])
        if race:
            out.append((race, date_str))
    return out


class UCIWorldTourCollector(BaseCollector):
    @property
    def source_name(self) -> str:
        return "uci_worldtour"

    def is_configured(self) -> bool:
        return True

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if not city_name:
            return []

        # Anchor on calendar year. If we're past November and there are
        # no future races left in the current year, the page may not
        # have published next year's calendar yet — caller would get
        # empty, which is fine.
        year = date.today().year
        url = _WIKI_URL_TEMPLATE.format(year=year)

        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS,
                                          follow_redirects=True) as client:
                r = await client.get(url)
                if r.status_code != 200:
                    logger.warning(f"uci_worldtour: {url} → {r.status_code}")
                    return []
                table_rows = _parse_wikitable(r.text)
        except Exception as e:
            logger.warning(f"uci_worldtour: fetch failed: {type(e).__name__}: {e}")
            return []

        today = date.today()
        results: list[RawEvent] = []
        for race_name, date_str in table_rows:
            host = _RACE_HOST.get(_normalize_race(race_name))
            if not host:
                continue
            host_city, host_country = host
            if host_city.lower() != city_name.lower():
                continue
            parsed = _parse_date_range(date_str, year)
            if not parsed:
                continue
            start, end = parsed
            # Skip events fully in the past.
            if end < today:
                continue
            results.append(RawEvent(
                name=f"UCI WorldTour — {race_name.strip().lstrip('/').strip()}",
                start_date=start,
                start_time=None,
                end_date=end,
                end_time=None,
                artist_name=None,
                sport="Cycling",
                description=f"UCI WorldTour road cycling — {host_country}",
                venue_name=None,
                venue_city=host_city,
                venue_country=host_country,
                purchase_link=None,
                source="uci_worldtour",
                source_id=f"uci-{year}-{_normalize_race(race_name).replace(' ', '-')}",
                raw_categories=["Sports", "Cycling"],
            ))

        logger.info(f"UCI WorldTour: {len(results)} races in {city_name}")
        return results
