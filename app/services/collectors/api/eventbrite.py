from __future__ import annotations
import logging
from datetime import date, datetime

import httpx

from app.config import settings
from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time
from app.services.collectors.category_mapper import map_category

logger = logging.getLogger(__name__)

_MAX_PAGES = 4   # up to 200 events per city run


class EventbriteCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "eventbrite"

    def is_configured(self) -> bool:
        return bool(settings.EVENTBRITE_TOKEN)

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        lat = kwargs.get("lat")
        lon = kwargs.get("lon")

        # Build location params — prefer lat/lon (reliable), fall back to
        # "City, Country" string (plain city name returns 404 on Eventbrite v3)
        if lat and lon:
            location_params = {
                "location.latitude": lat,
                "location.longitude": lon,
                "location.within": "50km",
            }
        else:
            location_params = {
                "location.address": f"{city_name}, {country_code}",
                "location.within": "50km",
            }

        base_params = {
            **location_params,
            "status": "live",
            "order_by": "start_asc",
            "expand": "venue,ticket_availability,category",
        }

        events: list[RawEvent] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for page in range(1, _MAX_PAGES + 1):
                params = {**base_params, "page": page}
                try:
                    resp = await client.get(
                        "https://www.eventbriteapi.com/v3/events/search/",
                        params=params,
                        headers={"Authorization": f"Bearer {settings.EVENTBRITE_TOKEN}"},
                    )
                    if resp.status_code in (404, 410):
                        logger.warning(
                            f"Eventbrite: {resp.status_code} for {city_name} "
                            f"(page {page}) — location may not be supported"
                        )
                        break
                    resp.raise_for_status()
                    data = resp.json()
                except httpx.HTTPStatusError as e:
                    logger.warning(f"Eventbrite HTTP error for {city_name}: {e}")
                    break

                page_events = data.get("events", [])
                for ev in page_events:
                    raw = self._transform(ev)
                    if raw:
                        events.append(raw)

                # Stop if this is the last page
                pagination = data.get("pagination", {})
                if not pagination.get("has_more_items", False):
                    break

        return events

    def _transform(self, ev: dict) -> RawEvent | None:
        start_str = ev.get("start", {}).get("utc") or ev.get("start", {}).get("local")
        if not start_str:
            return None

        try:
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        except ValueError:
            return None

        if start_dt.date() < date.today():
            return None

        end_dt = None
        end_str = ev.get("end", {}).get("utc") or ev.get("end", {}).get("local")
        if end_str:
            try:
                end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            except ValueError:
                pass

        event_name = ev.get("name", {}).get("text", "") or "Untitled Event"
        venue = ev.get("venue") or {}
        ticket = ev.get("ticket_availability", {})
        min_price = ticket.get("minimum_ticket_price", {})

        raw_cats = []
        cat_name = ev.get("category", {}).get("name") if ev.get("category") else None
        if cat_name:
            mapped = map_category("eventbrite", cat_name)
            if mapped:
                raw_cats.append(mapped)

        return RawEvent(
            name=event_name,
            start_date=start_dt.date(),
            start_time=safe_time(start_dt),
            end_date=end_dt.date() if end_dt else None,
            end_time=safe_time(end_dt) if end_dt else None,
            description=ev.get("description", {}).get("text"),
            price=float(min_price["value"]) if min_price.get("value") else None,
            price_currency=min_price.get("currency", "USD"),
            purchase_link=ev.get("url"),
            image_url=ev.get("logo", {}).get("url") if ev.get("logo") else None,
            is_online=ev.get("online_event", False),
            venue_name=venue.get("name"),
            venue_address=venue.get("address", {}).get("address_1"),
            venue_city=venue.get("address", {}).get("city"),
            venue_country=venue.get("address", {}).get("country"),
            venue_lat=float(venue["latitude"]) if venue.get("latitude") else None,
            venue_lon=float(venue["longitude"]) if venue.get("longitude") else None,
            source="eventbrite",
            source_id=str(ev.get("id", "")),
            raw_categories=raw_cats,
        )
