from __future__ import annotations
import httpx
from datetime import date, datetime

from app.config import settings
from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time
from app.services.collectors.category_mapper import map_category


class EventbriteCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "eventbrite"

    def is_configured(self) -> bool:
        return bool(settings.EVENTBRITE_TOKEN)

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        events = []
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://www.eventbriteapi.com/v3/events/search/",
                params={
                    "location.address": city_name,
                    "location.within": "50km",
                    "status": "live",
                    "order_by": "start_asc",
                    "expand": "venue,ticket_availability,category",
                },
                headers={"Authorization": f"Bearer {settings.EVENTBRITE_TOKEN}"},
            )
            resp.raise_for_status()
            data = resp.json()

        for ev in data.get("events", []):
            raw = self._transform(ev)
            if raw:
                events.append(raw)
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

        # Map category
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
