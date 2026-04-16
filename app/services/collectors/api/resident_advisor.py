from __future__ import annotations
import httpx
import re
from datetime import date, datetime

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time

RA_GRAPHQL = "https://ra.co/graphql"

# RA area IDs for supported cities
# Verified by querying ra.co/graphql — IDs with 0 results removed
CITY_AREA_IDS = {
    # Original 9
    "New York":      8,
    "London":        13,
    "Los Angeles":   23,
    "Chicago":       17,
    "Berlin":        34,
    "Amsterdam":     29,
    "Tel Aviv":      413,
    "Paris":         31,
    "Sydney":        61,
    # Priority-city gaps filled
    "San Francisco": 39,
    "Toronto":       52,
    # High-event new cities (verified > 50 events each)
    "Melbourne":     24,
    "Tokyo":         99,
    "Seoul":         37,
    "Barcelona":     57,
    "Dublin":        155,
    "Prague":        164,
    "Zurich":        73,
    "Ibiza":         45,
    "Vienna":        71,
    "Budapest":      169,
    "Lisbon":        146,
}

QUERY = """
query GET_EVENT_LISTINGS($filters: FilterInputDtoInput, $pageSize: Int, $page: Int) {
  eventListings(filters: $filters, pageSize: $pageSize, page: $page) {
    data {
      event {
        id title date startTime endTime contentUrl cost isTicketed
        artists { id name }
        venue { id name address contentUrl }
        images { filename }
      }
    }
    totalResults
  }
}
"""


class ResidentAdvisorCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "resident_advisor"

    def is_configured(self) -> bool:
        return True  # No API key required

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        area_id = CITY_AREA_IDS.get(city_name)
        if not area_id:
            return []

        events = []
        today = date.today()
        end_date = date(today.year, today.month + 3 if today.month <= 9 else today.month - 9,
                        today.day) if today.month <= 9 else date(today.year + 1, today.month - 9, today.day)

        headers = {
            "Content-Type": "application/json",
            "Referer": f"https://ra.co/events/{country_code.lower()}/{'newyorkcity' if city_name == 'New York' else city_name.lower().replace(' ', '')}",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }

        page = 1
        page_size = 50
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                payload = {
                    "operationName": "GET_EVENT_LISTINGS",
                    "variables": {
                        "filters": {
                            "areas": {"eq": area_id},
                            "listingDate": {
                                "gte": today.isoformat(),
                                "lte": end_date.isoformat(),
                            },
                        },
                        "pageSize": page_size,
                        "page": page,
                    },
                    "query": QUERY,
                }
                resp = await client.post(RA_GRAPHQL, json=payload, headers=headers)
                if resp.status_code != 200:
                    break

                data = resp.json()
                listings = data.get("data", {}).get("eventListings", {})
                items = listings.get("data", [])
                if not items:
                    break

                for item in items:
                    raw = self._transform(item.get("event", {}))
                    if raw:
                        events.append(raw)

                total = listings.get("totalResults", 0)
                if page * page_size >= total:
                    break
                page += 1

        return events

    def _transform(self, ev: dict) -> RawEvent | None:
        if not ev or not ev.get("date"):
            return None

        # RA: "date" is date-only (always midnight). Use "startTime" for real time.
        try:
            start_str = ev.get("startTime") or ev["date"]
            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        except (ValueError, KeyError):
            return None

        if start_dt.date() < date.today():
            return None

        end_dt = None
        if ev.get("endTime"):
            try:
                end_dt = datetime.fromisoformat(ev["endTime"].replace("Z", "+00:00"))
            except ValueError:
                pass

        # Parse price — RA returns a string like "$20+" or "£15"
        price = None
        price_currency = "USD"
        cost_str = ev.get("cost", "")
        if cost_str:
            currency_map = {"$": "USD", "£": "GBP", "€": "EUR"}
            for symbol, code in currency_map.items():
                if symbol in cost_str:
                    price_currency = code
                    nums = re.findall(r"[\d.]+", cost_str)
                    if nums:
                        price = float(nums[0])
                    break

        artists = ev.get("artists") or []
        artist_name = artists[0]["name"] if artists else None

        venue = ev.get("venue") or {}
        images = ev.get("images") or []
        image_url = images[0].get("filename") if images else None

        purchase_link = f"https://ra.co{ev['contentUrl']}" if ev.get("contentUrl") else None

        return RawEvent(
            name=ev.get("title", "Untitled Event"),
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
            venue_address=venue.get("address"),
            venue_website_url=f"https://ra.co{venue['contentUrl']}" if venue.get("contentUrl") else None,
            source="resident_advisor",
            source_id=str(ev.get("id", "")),
            raw_categories=["Music"],
        )
