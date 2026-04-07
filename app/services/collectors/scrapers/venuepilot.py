from __future__ import annotations

"""VenuePilot venue scraper — fetches events via the VenuePilot GraphQL API.

VenuePilot is a ticketing/calendar platform used by many independent music &
dance venues.  Each venue has a numeric account_id visible in their widget
config.  We query the public GraphQL endpoint (no auth required) and filter to
future events only.
"""

import logging
from datetime import date
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent

logger = logging.getLogger(__name__)

VENUEPILOT_GRAPHQL = "https://www.venuepilot.co/graphql"

_QUERY = """
{
  publicEvents(accountIds: [%s], startDate: "%s") {
    id
    name
    date
    startTime
    endTime
    doorTime
    ticketsUrl
    tags
    venue { name city state }
    tickets { name price }
  }
}
"""

# ---------------------------------------------------------------------------
# Venue registry
# ---------------------------------------------------------------------------
# Add more VenuePilot venues here as needed.  `run_for_cities` lists the
# city names (lowercase) that trigger a scrape for this venue.
VENUEPILOT_VENUES = [
    {
        "name": "Ashkenaz Music & Dance Community Center",
        "account_id": 1228,
        "city": "Berkeley",
        "state": "CA",
        "country": "US",
        "address": "1317 San Pablo Ave, Berkeley, CA 94702",
        "website_url": "https://www.ashkenaz.com",
        "type": "Music/Dance Venue",
        # Run whenever the scheduler processes San Francisco (closest priority city)
        "run_for_cities": ["san francisco", "berkeley", "oakland"],
    },
]

# Tags → broad category mapping used for event-type resolution
_TAG_CATEGORY_MAP = [
    (["jazz", "blues", "soul", "r&b"], "Music"),
    (["rock", "pop", "indie", "metal", "punk", "hip hop", "rap", "electronic", "techno", "reggae", "latin", "gospel", "country", "folk", "classical", "orchestr"], "Music"),
    (["comedy", "stand-up", "standup", "improv"], "Comedy"),
    (["dance", "bachata", "salsa", "tango", "ballroom", "swing", "cumbia", "zouk"], "Dance"),
    (["film", "cinema", "movie", "screening"], "Film"),
    (["art", "exhibition", "gallery", "visual"], "Art"),
    (["festival", "fair"], "Festival"),
]


def _tags_to_category(tags: list[str]) -> str:
    tag_text = " ".join(tags).lower()
    for keywords, category in _TAG_CATEGORY_MAP:
        if any(kw in tag_text for kw in keywords):
            return category
    return "Music"  # Ashkenaz is primarily a music/dance venue


def _min_price(tickets: list[dict]) -> Optional[float]:
    prices = []
    for t in tickets:
        p = t.get("price")
        if p is not None:
            try:
                prices.append(float(p))
            except (ValueError, TypeError):
                pass
    return min(prices) if prices else None


def _fmt_time(t: Optional[str]) -> Optional[str]:
    """'HH:MM:SS' → 'HH:MM'; returns None for missing or midnight '00:00'."""
    if not t:
        return None
    parts = t.split(":")
    hh, mm = parts[0], parts[1] if len(parts) > 1 else "00"
    if hh == "00" and mm == "00":
        return None  # treat midnight as unknown (same as safe_time())
    return f"{hh}:{mm}"


class VenuePilotCollector(BaseCollector):
    """Fetches events from VenuePilot-powered venue calendars (GraphQL)."""

    @property
    def source_name(self) -> str:
        return "venuepilot"

    def is_configured(self) -> bool:
        return True  # no API key required

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        city_lower = city_name.lower()
        all_events: list[RawEvent] = []

        for cfg in VENUEPILOT_VENUES:
            if not any(c in city_lower for c in cfg.get("run_for_cities", [])):
                continue
            try:
                events = await self._fetch_venue_events(cfg)
                logger.info(f"VenuePilot {cfg['name']}: {len(events)} future events")
                all_events.extend(events)
            except Exception as e:
                logger.error(f"VenuePilot {cfg['name']} error: {e}")

        return all_events

    async def _fetch_venue_events(self, cfg: dict) -> list[RawEvent]:
        today = date.today()
        query = _QUERY % (cfg["account_id"], today.isoformat())

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                VENUEPILOT_GRAPHQL,
                json={"query": query},
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
            resp.raise_for_status()

        items = resp.json().get("data", {}).get("publicEvents", [])
        raw_events: list[RawEvent] = []

        for ev in items:
            try:
                date_str = ev.get("date")
                if not date_str:
                    continue
                event_date = date.fromisoformat(date_str)
                if event_date < today:
                    continue  # skip past events

                start_time = _fmt_time(ev.get("startTime"))
                end_time = _fmt_time(ev.get("endTime"))
                price = _min_price(ev.get("tickets") or [])
                tags = ev.get("tags") or []

                raw_events.append(RawEvent(
                    name=ev["name"],
                    start_date=event_date,
                    start_time=start_time,
                    end_date=event_date if end_time else None,
                    end_time=end_time,
                    venue_name=cfg["name"],
                    venue_city=cfg["city"],
                    venue_country=cfg["country"],
                    venue_address=cfg.get("address"),
                    venue_website_url=cfg.get("website_url"),
                    purchase_link=ev.get("ticketsUrl"),
                    price=price,
                    price_currency="USD" if price else None,
                    raw_categories=[_tags_to_category(tags)],
                    source=self.source_name,
                    source_id=str(ev["id"]),
                ))
            except Exception as e:
                logger.warning(f"VenuePilot: skipping event {ev.get('id')}: {e}")

        return raw_events
