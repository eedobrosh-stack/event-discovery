from __future__ import annotations
import httpx
from datetime import date

from app.config import settings
from app.services.collectors.base import BaseCollector, RawEvent
from app.services.collectors.category_mapper import map_category


class TicketmasterCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "ticketmaster"

    def is_configured(self) -> bool:
        return bool(settings.TICKETMASTER_KEY)

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        events = []
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://app.ticketmaster.com/discovery/v2/events.json",
                params={
                    "apikey": settings.TICKETMASTER_KEY,
                    "city": city_name,
                    "countryCode": country_code,
                    "size": 200,
                    "sort": "date,asc",
                },
            )
            resp.raise_for_status()
            data = resp.json()

        for ev in data.get("_embedded", {}).get("events", []):
            raw = self._transform(ev)
            if raw:
                events.append(raw)
        return events

    def _transform(self, ev: dict) -> RawEvent | None:
        start = ev.get("dates", {}).get("start", {})
        start_date_str = start.get("localDate")
        if not start_date_str:
            return None

        try:
            sd = date.fromisoformat(start_date_str)
        except ValueError:
            return None

        if sd < date.today():
            return None

        price_range = (ev.get("priceRanges") or [{}])[0] if ev.get("priceRanges") else {}
        event_name = ev.get("name", "")
        artist_name = None
        attractions = ev.get("_embedded", {}).get("attractions", [])
        if attractions:
            artist_name = attractions[0].get("name")

        name = event_name or artist_name or "Untitled Event"
        if artist_name and artist_name != name and artist_name not in name:
            name = f"{name} - {artist_name}"

        venue_data = (ev.get("_embedded", {}).get("venues") or [{}])[0]

        # Map categories
        raw_cats = []
        for clf in ev.get("classifications", []):
            seg = clf.get("segment", {})
            if seg.get("id"):
                mapped = map_category("ticketmaster_segment", seg["id"])
                if mapped:
                    raw_cats.append(mapped)
            genre = clf.get("genre", {})
            if genre.get("name"):
                mapped = map_category("ticketmaster_genre", genre["name"])
                if mapped and mapped not in raw_cats:
                    raw_cats.append(mapped)

        return RawEvent(
            name=name,
            start_date=sd,
            start_time=start.get("localTime", "")[:5] or None,
            artist_name=artist_name,
            description=ev.get("info"),
            price=price_range.get("min"),
            price_currency=price_range.get("currency", "USD"),
            purchase_link=ev.get("url"),
            image_url=(ev.get("images") or [{}])[0].get("url") if ev.get("images") else None,
            venue_name=venue_data.get("name"),
            venue_address=venue_data.get("address", {}).get("line1"),
            venue_city=venue_data.get("city", {}).get("name"),
            venue_country=venue_data.get("country", {}).get("countryCode"),
            venue_lat=float(venue_data["location"]["latitude"]) if venue_data.get("location", {}).get("latitude") else None,
            venue_lon=float(venue_data["location"]["longitude"]) if venue_data.get("location", {}).get("longitude") else None,
            source="ticketmaster",
            source_id=ev.get("id", ""),
            raw_categories=raw_cats,
        )
