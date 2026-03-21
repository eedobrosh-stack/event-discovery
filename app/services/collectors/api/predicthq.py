from __future__ import annotations
import httpx
from datetime import date, datetime

from app.config import settings
from app.services.collectors.base import BaseCollector, RawEvent
from app.services.collectors.category_mapper import map_category


class PredictHQCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "predicthq"

    def is_configured(self) -> bool:
        return bool(settings.PREDICTHQ_TOKEN)

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        lat = kwargs.get("lat")
        lon = kwargs.get("lon")
        if not lat or not lon:
            return []

        events = []
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://api.predicthq.com/v1/events/",
                params={
                    "location_around.origin": f"{lat},{lon}",
                    "location_around.offset": "50km",
                    "start.gte": date.today().isoformat(),
                    "sort": "start",
                    "limit": 200,
                },
                headers={"Authorization": f"Bearer {settings.PREDICTHQ_TOKEN}"},
            )
            resp.raise_for_status()
            data = resp.json()

        for ev in data.get("results", []):
            raw = self._transform(ev)
            if raw:
                events.append(raw)
        return events

    def _transform(self, ev: dict) -> RawEvent | None:
        start_str = ev.get("start")
        if not start_str:
            return None

        try:
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        except ValueError:
            return None

        end_dt = None
        if ev.get("end"):
            try:
                end_dt = datetime.fromisoformat(ev["end"].replace("Z", "+00:00"))
            except ValueError:
                pass

        # Map category
        raw_cats = []
        phq_cat = ev.get("category")
        if phq_cat:
            mapped = map_category("predicthq", phq_cat)
            if mapped:
                raw_cats.append(mapped)

        location = ev.get("location", [])
        lon = location[0] if len(location) > 0 else None
        lat = location[1] if len(location) > 1 else None

        return RawEvent(
            name=ev.get("title", "Untitled Event"),
            start_date=start_dt.date(),
            start_time=start_dt.strftime("%H:%M"),
            end_date=end_dt.date() if end_dt else None,
            end_time=end_dt.strftime("%H:%M") if end_dt else None,
            description=ev.get("description"),
            venue_name=ev.get("entities", [{}])[0].get("name") if ev.get("entities") else None,
            venue_lat=lat,
            venue_lon=lon,
            source="predicthq",
            source_id=str(ev.get("id", "")),
            raw_categories=raw_cats,
        )
