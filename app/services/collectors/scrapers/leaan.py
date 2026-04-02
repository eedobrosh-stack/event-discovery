"""
Leaan.co.il collector — Israeli ticketing platform (Next.js SSR).
Scrapes the __NEXT_DATA__ JSON from the homepage, no API key needed.
Covers music, comedy, theatre, sports events across Israel.
"""
from __future__ import annotations
import httpx
import re
import json
import logging
from datetime import date, datetime

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time

logger = logging.getLogger(__name__)

# Hebrew city names → English
CITY_MAP = {
    "תל אביב": "Tel Aviv",
    "תל אביב-יפו": "Tel Aviv",
    "ירושלים": "Jerusalem",
    "חיפה": "Haifa",
    "ראשון לציון": "Rishon LeZion",
    "פתח תקווה": "Petah Tikva",
    "אשדוד": "Ashdod",
    "נתניה": "Netanya",
    "באר שבע": "Beersheba",
    "בני ברק": "Bnei Brak",
    "חולון": "Holon",
    "רמת גן": "Ramat Gan",
    "הרצליה": "Herzliya",
    "רעננה": "Ra'anana",
    "בת ים": "Bat Yam",
    "כפר סבא": "Kfar Saba",
    "רחובות": "Rehovot",
    "הוד השרון": "Hod HaSharon",
    "נס ציונה": "Nes Ziona",
}

# Hebrew category names → Supercaly categories
CATEGORY_MAP = {
    "מוזיקה": "Music",
    "ספורט": "Sports",
    "סטנדאפ": "Comedy",
    "תיאטרון": "Art",
    "ילדים": "Art",
    "אחר": "Music",
}


class LeaanCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "leaan"

    def is_configured(self) -> bool:
        return True  # No API key required

    async def collect(self, city_name: str, country_code: str = "IL", **kwargs) -> list[RawEvent]:
        # Map our city name to Hebrew for filtering
        heb_city_names = {v: k for k, v in CITY_MAP.items()}
        # Accept both "Tel Aviv" and direct Hebrew
        target_cities = set()
        if city_name in heb_city_names:
            target_cities.add(heb_city_names[city_name])
        # Allow Tel Aviv variants
        if city_name in ("Tel Aviv", "Tel Aviv-Yafo"):
            target_cities.update({"תל אביב", "תל אביב-יפו"})
        if city_name == "Jerusalem":
            target_cities.add("ירושלים")
        if city_name == "Haifa":
            target_cities.add("חיפה")

        if not target_cities:
            return []

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8",
        }

        async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers=headers) as client:
            resp = await client.get("https://www.leaan.co.il")
            if resp.status_code != 200:
                logger.warning(f"Leaan returned {resp.status_code}")
                return []
            html = resp.text

        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html, re.DOTALL
        )
        if not m:
            logger.warning("Leaan: __NEXT_DATA__ not found")
            return []

        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError as e:
            logger.warning(f"Leaan: JSON parse error: {e}")
            return []

        raw_events = data.get("props", {}).get("pageProps", {}).get(
            "initialState", {}
        ).get("search", {}).get("events", [])

        events = []
        for ev in raw_events:
            loc = ev.get("location", {})
            heb_city = loc.get("city", "")
            if heb_city not in target_cities:
                continue
            raw = self._transform(ev, loc)
            if raw:
                events.append(raw)

        logger.info(f"Leaan: found {len(events)} events for {city_name}")
        return events

    def _transform(self, ev: dict, loc: dict) -> RawEvent | None:
        name = ev.get("name") or ev.get("event_name", "")
        if not name or len(name.strip()) < 2:
            return None

        # Skip past events
        ts_start = ev.get("event_start")
        if not ts_start:
            return None
        try:
            start_dt = datetime.fromtimestamp(ts_start)
        except (OSError, ValueError):
            return None
        if start_dt.date() < date.today():
            return None

        ts_end = ev.get("event_end")
        end_dt = None
        if ts_end and ts_end != ts_start:
            try:
                end_dt = datetime.fromtimestamp(ts_end)
                # If end is same date and many hours later, it's a season pass, not end time
                if end_dt.date() > start_dt.date() + __import__('datetime').timedelta(days=1):
                    end_dt = None
            except (OSError, ValueError):
                end_dt = None

        start_time = safe_time(start_dt)
        end_time = safe_time(end_dt) if end_dt else None
        end_date_val = end_dt.date() if end_dt else None

        if end_time is None and start_time:
            end_date_val, end_time = default_end_time(start_time, start_dt.date(), None)

        # Price — leaan shows price in ILS (shekels)
        price = ev.get("starting_price")
        if price == 0:
            price = None

        # Image
        image_url = (
            ev.get("vivenu_image")
            or ev.get("gallery", {}).get("manui_card", {}).get("1")
        )

        # Category
        raw_cats = []
        for cat_id, cat_data in (ev.get("categories") or {}).items():
            heb_cat = cat_data.get("category_name", "")
            mapped = CATEGORY_MAP.get(heb_cat)
            if mapped:
                raw_cats.append(mapped)
        if not raw_cats:
            raw_cats = ["Music"]

        # Purchase link
        redirect = ev.get("redirect_url", "")
        vivenu_id = ev.get("vivenu_event_id", "")
        if redirect:
            purchase_link = redirect
        elif vivenu_id:
            purchase_link = f"https://www.leaan.co.il/event/{vivenu_id}"
        else:
            purchase_link = "https://www.leaan.co.il"

        # Venue & city
        venue_name = loc.get("name", "")
        venue_address = loc.get("street", "")
        heb_city = loc.get("city", "")
        city_en = CITY_MAP.get(heb_city, "Tel Aviv")

        return RawEvent(
            name=name,
            start_date=start_dt.date(),
            start_time=start_time,
            end_date=end_date_val,
            end_time=end_time,
            price=float(price) if price else None,
            price_currency="ILS",
            image_url=image_url,
            purchase_link=purchase_link,
            venue_name=venue_name if venue_name else None,
            venue_address=venue_address if venue_address else None,
            venue_city=city_en,
            venue_country="Israel",
            source="leaan",
            source_id=f"leaan_{ev.get('id', vivenu_id)}",
            raw_categories=raw_cats,
        )
