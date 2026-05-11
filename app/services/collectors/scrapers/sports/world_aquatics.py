"""World Aquatics (FINA) competition collector.

Phase A item #3 — 7 country-row coverage from the leagues sheet
(Swimming + Water Polo + Aquatics entries across Denmark, Greece,
Hungary, Israel, Italy, Luxembourg, Norway, Sweden, etc.).

Data source: the public Pulselive-backed API that powers the
worldaquatics.com /competitions calendar page. Discovered via
Chrome MCP network trace on 2026-05-12.

  https://api.worldaquatics.com/fina/competitions
    ?pageSize=100
    &venueDateFrom=YYYY-MM-DDT00:00:00+00:00
    &venueDateTo=YYYY-MM-DDT00:00:00+00:00
    &group=FINA
    &sort=dateFrom,asc
    &page=0

Each competition entry exposes:
  id, officialName, dateFrom, dateTo, location: {city, countryCode,
  countryName}, disciplines: [list of codes], competitionType, series.

No API key required.

Granularity: one Event row per competition. Multi-day meets (most
Championships and World Cup legs) span 2-5 days; we set start_date /
end_date accordingly. start_time left null — heat-by-heat times
aren't useful at the catalog level.

Disciplines → sport mapping
===========================
SW = Swimming · DV = Diving · WP = Water Polo · AS = Artistic Swimming ·
OW = Open Water Swimming · HD = High Diving · MS = Masters Swimming.
When a competition is multi-discipline, we tag it as "Aquatics" (the
generic catch-all).
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

_API_URL = "https://api.worldaquatics.com/fina/competitions"
_TIMEOUT = 30
_HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# How far ahead to scan. World Aquatics tends to publish the full
# year by early Q1, so 365 days is enough to capture the entire
# Championships + World Cup calendar without overshoot.
_LOOKAHEAD_DAYS = 365

_DISCIPLINE_LABELS = {
    "SW": "Swimming",
    "DV": "Diving",
    "WP": "Water Polo",
    "AS": "Artistic Swimming",
    "OW": "Open Water Swimming",
    "HD": "High Diving",
    "MS": "Masters Swimming",
}


def _label_disciplines(codes: list[str]) -> str:
    if not codes:
        return "Aquatics"
    if len(codes) == 1:
        return _DISCIPLINE_LABELS.get(codes[0], "Aquatics")
    # Multi-discipline (typical for Championships) → catch-all
    return "Aquatics"


def _parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


def _build_raw(comp: dict, requested_city: str) -> Optional[RawEvent]:
    loc = comp.get("location") or {}
    city = (loc.get("city") or "").strip()
    if not city or city.lower() != requested_city.lower():
        return None
    country = (loc.get("countryName") or "").strip()

    start = _parse_iso(comp.get("venueDateFrom") or comp.get("dateFrom") or "")
    end = _parse_iso(comp.get("venueDateTo") or comp.get("dateTo") or "")
    if not start:
        return None

    # Skip events fully in the past.
    cutoff_end = end.date() if end else start.date()
    if cutoff_end < date.today():
        return None

    name = (comp.get("officialName") or comp.get("name") or "").strip()
    if not name:
        return None
    comp_id = comp.get("id")

    disciplines = comp.get("disciplines") or []
    sport_label = _label_disciplines(disciplines)

    return RawEvent(
        name=f"World Aquatics — {name}",
        start_date=start.date(),
        start_time=None,
        end_date=end.date() if end else start.date(),
        end_time=None,
        artist_name=None,
        sport=sport_label,
        description=f"World Aquatics ({sport_label}) — {country}",
        venue_name=None,
        venue_city=city,
        venue_country=country,
        purchase_link=(
            f"https://www.worldaquatics.com/competitions/{comp_id}"
            if comp_id else None
        ),
        source="world_aquatics",
        source_id=f"world-aquatics-{comp_id}",
        raw_categories=["Sports", sport_label],
    )


class WorldAquaticsCollector(BaseCollector):
    @property
    def source_name(self) -> str:
        return "world_aquatics"

    def is_configured(self) -> bool:
        return True

    async def collect(self, city_name: str, country_code: str = "", **kwargs) -> list[RawEvent]:
        if not city_name:
            return []

        today = date.today()
        end = today + timedelta(days=_LOOKAHEAD_DAYS)
        params = {
            "pageSize": "100",
            "venueDateFrom": f"{today.isoformat()}T00:00:00+00:00",
            "venueDateTo":   f"{end.isoformat()}T00:00:00+00:00",
            "group": "FINA",
            "sort": "dateFrom,asc",
            "page": "0",
        }

        comps: list[dict] = []
        try:
            async with httpx.AsyncClient(timeout=_TIMEOUT, headers=_HEADERS) as client:
                r = await client.get(_API_URL, params=params)
                if r.status_code == 200:
                    d = r.json()
                    comps = d.get("content") or []
                else:
                    logger.warning(
                        f"world_aquatics: API status={r.status_code} body[:200]={r.text[:200]!r}"
                    )
        except Exception as e:
            logger.warning(f"world_aquatics: fetch failed: {type(e).__name__}: {e}")
            return []

        results: list[RawEvent] = []
        for comp in comps:
            try:
                raw = _build_raw(comp, city_name)
                if raw:
                    results.append(raw)
            except Exception as e:
                logger.debug(f"world_aquatics: skip comp {comp.get('id')}: {e}")

        logger.info(
            f"World Aquatics: {len(results)} competitions in {city_name}"
        )
        return results
