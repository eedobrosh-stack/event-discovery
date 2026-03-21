from __future__ import annotations
import httpx
from datetime import date, datetime

from app.config import settings
from app.services.collectors.base import BaseCollector, RawEvent
from app.services.collectors.category_mapper import map_category


class SeatGeekCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "seatgeek"

    def is_configured(self) -> bool:
        return bool(settings.SEATGEEK_CLIENT_ID)

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        events = []
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://api.seatgeek.com/2/events",
                params={
                    "venue.city": city_name,
                    "client_id": settings.SEATGEEK_CLIENT_ID,
                    "client_secret": settings.SEATGEEK_SECRET,
                    "per_page": 200,
                    "sort": "datetime_local.asc",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        for ev in data.get("events", []):
            raw = self._transform(ev)
            if raw:
                events.append(raw)
        return events

    def _transform(self, ev: dict) -> RawEvent | None:
        dt_str = ev.get("datetime_local")
        if not dt_str:
            return None

        try:
            dt = datetime.fromisoformat(dt_str)
        except ValueError:
            return None

        if dt.date() < date.today():
            return None

        venue = ev.get("venue") or {}
        performers = ev.get("performers") or []
        artist_name = performers[0].get("name") if performers else None

        event_name = ev.get("title", "")
        name = event_name or artist_name or "Untitled Event"
        if artist_name and artist_name != name and artist_name not in name:
            name = f"{name} - {artist_name}"

        # Map category
        raw_cats = []
        sg_type = ev.get("type")
        if sg_type:
            mapped = map_category("seatgeek", sg_type)
            if mapped:
                raw_cats.append(mapped)

        stats = ev.get("stats") or {}

        return RawEvent(
            name=name,
            start_date=dt.date(),
            start_time=dt.strftime("%H:%M"),
            artist_name=artist_name,
            price=stats.get("lowest_price"),
            price_currency="USD",
            purchase_link=ev.get("url"),
            image_url=performers[0].get("image") if performers else None,
            venue_name=venue.get("name"),
            venue_address=venue.get("address"),
            venue_city=venue.get("city"),
            venue_country=venue.get("country"),
            venue_lat=venue.get("location", {}).get("lat") if venue.get("location") else None,
            venue_lon=venue.get("location", {}).get("lon") if venue.get("location") else None,
            source="seatgeek",
            source_id=str(ev.get("id", "")),
            raw_categories=raw_cats,
        )
