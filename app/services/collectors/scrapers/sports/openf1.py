"""
OpenF1 Formula 1 calendar scraper.

Endpoint: https://api.openf1.org/v1/meetings?year=YYYY
Free API, no key required, no rate limit documented.

Returns upcoming F1 race weekends filtered to the requested country.
Each meeting (race weekend) becomes a single RawEvent spanning practice → race.

TV broadcast data: OpenF1 doesn't expose this, but F1 broadcast rights are
fixed annual national deals, so we hardcode a per-country broadcaster table.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

OPENF1_BASE = "https://api.openf1.org/v1"
TIMEOUT = 15


def _ch(name: str, country: str, kind: str = "TV") -> dict:
    return {"channel": name, "market": "national", "country": country, "type": kind}


# F1 broadcast rights 2025 — one entry per country we collect for.
# Sources: Formula1.com media partners page + Motorsport.com rights tracker.
F1_TV_BY_COUNTRY: dict[str, list[dict]] = {
    "United States": [
        _ch("ESPN",      "US"),
        _ch("ESPN2",     "US"),
        _ch("ABC",       "US"),
        _ch("ESPN+",     "US", "Streaming"),
    ],
    "United Kingdom": [
        _ch("Sky Sports F1",  "GB"),
        _ch("Sky Sports Main Event", "GB"),
        _ch("Channel 4",      "GB"),           # highlights only
    ],
    "Germany": [
        _ch("Sky Sport F1",  "DE"),
        _ch("Sky Sport 1",   "DE"),
        _ch("RTL",           "DE"),            # selected races free-to-air
    ],
    "France": [
        _ch("Canal+",        "FR"),
        _ch("Canal+ Sport",  "FR"),
    ],
    "Italy": [
        _ch("Sky Sport F1",  "IT"),
        _ch("Sky Sport 1",   "IT"),
        _ch("TV8",           "IT"),            # highlights + delayed free-to-air
    ],
    "Spain": [
        _ch("DAZN",          "ES", "Streaming"),
        _ch("DAZN F1",       "ES"),
        _ch("Antena 3",      "ES"),            # selected races
    ],
    "Netherlands": [
        _ch("Viaplay",       "NL", "Streaming"),
        _ch("Ziggo Sport",   "NL"),
    ],
    "Portugal": [
        _ch("Sport TV",      "PT"),
    ],
    "Belgium": [
        _ch("Play Sports",   "BE"),
        _ch("RTBF",          "BE"),            # Belgian GP free-to-air
    ],
    "Turkey": [
        _ch("S Sport",       "TR"),
        _ch("S Sport+",      "TR", "Streaming"),
    ],
    "Australia": [
        _ch("Fox Sports",    "AU"),
        _ch("Kayo Sports",   "AU", "Streaming"),
        _ch("Network 10",    "AU"),            # selected races free-to-air
    ],
    "Canada": [
        _ch("TSN",           "CA"),
        _ch("RDS",           "CA"),
    ],
    "Brazil": [
        _ch("Band",          "BR"),
        _ch("Bandplay",      "BR", "Streaming"),
        _ch("Canal+",        "BR"),
    ],
    "Mexico": [
        _ch("Canal 5",       "MX"),
        _ch("TUDN",          "MX"),
        _ch("Fox Sports",    "MX"),
    ],
    "Argentina": [
        _ch("ESPN",          "AR"),
        _ch("ESPN+",         "AR", "Streaming"),
        _ch("Disney+",       "AR", "Streaming"),
    ],
    "Japan": [
        _ch("Fuji TV",       "JP"),
        _ch("DAZN",          "JP", "Streaming"),
    ],
    "Israel": [
        _ch("Sport 5",       "IL"),
        _ch("Sport 5+",      "IL", "Streaming"),
    ],
    "Singapore": [
        _ch("Fox Sports",    "SG"),
        _ch("beIN Sports",   "SG"),
    ],
    "United Arab Emirates": [
        _ch("beIN Sports",   "AE"),
    ],
    "Saudi Arabia": [
        _ch("beIN Sports",   "SA"),
    ],
    "Bahrain": [
        _ch("beIN Sports",   "BH"),
    ],
    "Monaco": [
        _ch("Canal+",        "MC"),
    ],
    "Austria": [
        _ch("ServusTV",      "AT"),
        _ch("ORF",           "AT"),
    ],
    "Hungary": [
        _ch("M4 Sport",      "HU"),
    ],
    "Azerbaijan": [
        _ch("İctimai TV",    "AZ"),
    ],
}


def _parse_meeting(m: dict, tv_channels: list[dict]) -> Optional[RawEvent]:
    """Convert one OpenF1 meeting dict into a RawEvent."""
    date_str = m.get("date_start", "")
    if not date_str:
        return None
    try:
        start_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None

    today = date.today()
    # A race weekend is typically Thursday–Sunday (4 days).
    # date_start is the first session (practice); the race is ~3 days later.
    race_date = start_dt.date() + timedelta(days=3)
    if race_date < today:
        return None  # weekend already passed

    meeting_name = m.get("meeting_name") or "Formula 1 Grand Prix"
    circuit      = m.get("circuit_short_name") or ""
    location     = m.get("location") or ""
    country_name = m.get("country_name") or ""
    meeting_key  = m.get("meeting_key", "")
    year         = m.get("year") or start_dt.year

    base_name = meeting_name if str(year) in meeting_name else f"{meeting_name} {year}"
    # Prefix "Formula 1 ·" only if not already in the name
    full_name = base_name if base_name.startswith("Formula 1") else f"{base_name} · Formula 1"

    return RawEvent(
        name=full_name,
        start_date=start_dt.date(),
        end_date=race_date,
        start_time=None,
        end_time=None,
        artist_name=None,
        sport="Formula 1",
        home_team=None,
        away_team=None,
        tv_channels=tv_channels,
        venue_name=circuit or None,
        venue_city=location or None,
        venue_country=country_name or None,
        purchase_link=f"https://www.formula1.com/en/racing/{year}.html",
        image_url=None,
        source="openf1",
        source_id=f"openf1-{meeting_key}",
        raw_categories=["Sports", "Formula 1"],
    )


class OpenF1Collector(BaseCollector):
    """Collects upcoming F1 race weekends from the free OpenF1 API."""

    @property
    def source_name(self) -> str:
        return "openf1"

    def is_configured(self) -> bool:
        return True  # no API key needed

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        """
        Return F1 races in country_code (full country name, e.g. "Australia").
        Fetches the current year; if we're in Q4 also fetches next year.
        """
        if not country_code:
            return []

        # Look up static broadcaster list for this country (empty list if unknown)
        tv_channels = F1_TV_BY_COUNTRY.get(country_code, [])

        today = date.today()
        years = [today.year]
        if today.month >= 10:
            years.append(today.year + 1)

        all_meetings: list[dict] = []
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            for year in years:
                url = f"{OPENF1_BASE}/meetings?year={year}"
                try:
                    resp = await client.get(url)
                    if resp.status_code != 200:
                        logger.warning(f"OpenF1: HTTP {resp.status_code} for year {year}")
                        continue
                    all_meetings.extend(resp.json())
                except Exception as e:
                    logger.warning(f"OpenF1: fetch error for {year} — {e}")

        results: list[RawEvent] = []
        for m in all_meetings:
            m_country = (m.get("country_name") or "").strip()
            if m_country.lower() != country_code.lower():
                continue
            try:
                raw = _parse_meeting(m, tv_channels)
                if raw:
                    results.append(raw)
            except Exception as e:
                logger.debug(f"OpenF1: skipping meeting {m.get('meeting_key')} — {e}")

        logger.info(
            f"OpenF1: {len(results)} upcoming F1 races for '{country_code}' "
            f"({len(tv_channels)} TV channels)"
        )
        return results
