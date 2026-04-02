from __future__ import annotations
import httpx
import json
import re
from datetime import date, datetime

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time

# City slug → Dice.fm browse path
CITY_SLUGS = {
    "New York": "new_york-5bbf4db0f06331478e9b2c59",
    "London": "london-6f30c4af5c3b7d9e1a2b3456",
    "Los Angeles": "los_angeles-5bbf4db0f06331478e9b2c60",
    "Chicago": "chicago-5bbf4db0f06331478e9b2c61",
}


class DiceCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "dice"

    def is_configured(self) -> bool:
        return True  # No API key required — scrapes Next.js SSR data

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        slug = CITY_SLUGS.get(city_name)
        if not slug:
            return []

        url = f"https://dice.fm/browse/{slug}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
        }

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return []
            html = resp.text

        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html, re.DOTALL
        )
        if not match:
            return []

        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError:
            return []

        page_props = data.get("props", {}).get("pageProps", {})
        raw_events = page_props.get("events", [])

        events = []
        for ev in raw_events:
            raw = self._transform(ev)
            if raw:
                events.append(raw)
        return events

    def _transform(self, ev: dict) -> RawEvent | None:
        dates = ev.get("dates", {})
        start_str = dates.get("event_start_date")
        if not start_str:
            return None

        try:
            start_dt = datetime.fromisoformat(start_str)
        except ValueError:
            return None

        if start_dt.date() < date.today():
            return None

        end_dt = None
        end_str = dates.get("event_end_date")
        if end_str:
            try:
                end_dt = datetime.fromisoformat(end_str)
            except ValueError:
                pass

        # Price: amount_from is in cents
        price_data = ev.get("price", {})
        amount_from = price_data.get("amount_from")
        price = round(amount_from / 100, 2) if amount_from else None
        price_currency = price_data.get("currency", "USD")

        venues = ev.get("venues") or []
        venue = venues[0] if venues else {}
        venue_address = venue.get("address", "")
        venue_city_obj = venue.get("city", {})

        images = ev.get("images", {})
        image_url = images.get("landscape") or images.get("square")

        lineup = ev.get("summary_lineup", {})
        top_artists = lineup.get("top_artists", [])
        artist_name = top_artists[0].get("name") if top_artists else None

        perm_name = ev.get("perm_name", "")
        purchase_link = f"https://dice.fm/event/{perm_name}" if perm_name else None

        return RawEvent(
            name=ev.get("name", "Untitled Event"),
            start_date=start_dt.date(),
            start_time=safe_time(start_dt),
            end_date=end_dt.date() if end_dt else None,
            end_time=safe_time(end_dt) if end_dt else None,
            artist_name=artist_name,
            price=price,
            price_currency=price_currency,
            purchase_link=purchase_link,
            image_url=image_url,
            venue_name=venue.get("name"),
            venue_address=venue_address,
            venue_city=venue_city_obj.get("name"),
            source="dice",
            source_id=str(ev.get("id", "")),
            raw_categories=["Music"],
        )
