from __future__ import annotations
import httpx
from datetime import date

from app.config import settings

# Map full country names → ISO 2-letter codes for Ticketmaster API
COUNTRY_ISO = {
    "United States": "US", "United Kingdom": "GB", "Australia": "AU",
    "Austria": "AT", "Belgium": "BE", "Canada": "CA", "Chile": "CL",
    "Colombia": "CO", "Costa Rica": "CR", "Czechia": "CZ", "Denmark": "DK",
    "Estonia": "EE", "Finland": "FI", "France": "FR", "Germany": "DE",
    "Greece": "GR", "Hungary": "HU", "Iceland": "IS", "Ireland": "IE",
    "Israel": "IL", "Italy": "IT", "Japan": "JP", "South Korea": "KR",
    "Latvia": "LV", "Lithuania": "LT", "Mexico": "MX", "Netherlands": "NL",
    "New Zealand": "NZ", "Norway": "NO", "Poland": "PL", "Portugal": "PT",
    "Slovakia": "SK", "Slovenia": "SI", "Spain": "ES", "Sweden": "SE",
    "Switzerland": "CH", "Turkey": "TR",
}
from app.services.collectors.base import BaseCollector, RawEvent
from app.services.collectors.category_mapper import map_category


class TicketmasterCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "ticketmaster"

    def is_configured(self) -> bool:
        return bool(settings.TICKETMASTER_KEY)

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        country_code = COUNTRY_ISO.get(country_code, country_code)
        events = []
        _MAX_PAGES = 5  # TM caps at 5 pages × 200 = 1 000 events per city

        async with httpx.AsyncClient(timeout=30) as client:
            for page in range(_MAX_PAGES):
                resp = await client.get(
                    "https://app.ticketmaster.com/discovery/v2/events.json",
                    params={
                        "apikey": settings.TICKETMASTER_KEY,
                        "city": city_name,
                        "countryCode": country_code,
                        "size": 200,
                        "page": page,
                        "sort": "date,asc",
                        "includePriceRanges": "yes",
                    },
                )
                if resp.status_code == 400:
                    break  # page out of range — TM returns 400 when page > total pages
                resp.raise_for_status()
                data = resp.json()

                page_events = data.get("_embedded", {}).get("events", [])
                if not page_events:
                    break

                for ev in page_events:
                    raw = self._transform(ev)
                    if raw:
                        events.append(raw)

                # Stop if this was the last page
                pagination = data.get("page", {})
                total_pages = pagination.get("totalPages", 1)
                if page + 1 >= total_pages:
                    break

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

        # Map categories. Also detect the Sports segment so we can strip the
        # bogus "artist" (Ticketmaster stores the home team as the primary
        # attraction, which would otherwise go through the music-artist
        # enrichment path and end up tagged as Music/Fitness/etc.)
        raw_cats = []
        is_sports = False
        for clf in ev.get("classifications", []):
            seg = clf.get("segment", {})
            if seg.get("id"):
                if seg.get("id") == "KZFzniwnSyZfZ7v7nE":   # TM "Sports" segment
                    is_sports = True
                mapped = map_category("ticketmaster_segment", seg["id"])
                if mapped:
                    raw_cats.append(mapped)
            genre = clf.get("genre", {})
            if genre.get("name"):
                mapped = map_category("ticketmaster_genre", genre["name"])
                if mapped and mapped not in raw_cats:
                    raw_cats.append(mapped)

        # Infer specific sport from name/genre so the registry routes the
        # event into the sports priority chain (and picks a specific event
        # type like "Baseball Game" rather than just "Sports Event").
        sport_val = None
        home_team = away_team = None
        if is_sports:
            artist_name = None   # home team is not a music performer
            lower_name = name.lower()
            for kw, sv in (
                ("baseball",   "Baseball"),
                ("softball",   "Baseball"),
                ("basketball", "Basketball"),
                ("hockey",     "Ice Hockey"),
                ("football",   "American Football"),
                ("soccer",     "Soccer"),
                ("tennis",     "Tennis"),
                ("golf",       "Golf"),
            ):
                if kw in lower_name:
                    sport_val = sv
                    break
            sport_val = sport_val or "Sports"
            # Derive home/away from "X vs Y" or "X vs. Y" when present
            for sep in (" vs. ", " vs "):
                if sep in name:
                    parts = name.split(sep, 1)
                    home_team = parts[0].strip() or None
                    away_team = parts[1].strip() or None
                    break

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
            sport=sport_val,
            home_team=home_team,
            away_team=away_team,
        )
