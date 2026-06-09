from __future__ import annotations
import asyncio
import logging
from datetime import date, datetime, timedelta, timezone

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

_TM_ENDPOINT = "https://app.ticketmaster.com/discovery/v2/events.json"

# Request pacing + 429 handling. TM's free tier caps at ~5 req/sec; the
# 5-window × 5-page sweep (after the 2026-06-07 horizon extension to 24mo)
# can burst past that for an event-rich city and earn a 429 — which used to
# hit raise_for_status() and abort the whole city's TM collection, silently
# dropping its intake. We now space requests (~4.5/sec) and back off+retry on
# 429 instead of aborting.
_TM_REQ_DELAY = 0.22          # seconds between requests (~4.5 req/sec)
_TM_MAX_RETRIES = 4           # 429 retries per request
_TM_BACKOFF_BASE = 1.0        # backoff 1s, 2s, 4s … (or Retry-After if larger)

# Ticketmaster Discovery API caps responses at 1 000 events
# (5 pages × 200) per query. Major cities (NYC, LA, London, Berlin)
# routinely have 10k+ upcoming events, so a single un-windowed query
# only ever sees the chronologically-first slice. We slice the
# calendar into windows so each gets its own 1 000-event budget.
#
# The 12-18mo / 18-24mo windows were added 2026-06-07 to deepen the
# horizon: the catalog was front-loaded (only ~786 upcoming events sat
# beyond 1 year) because the windows stopped at 365 days. Far-dated
# events are *sticky* — they don't age out of the upcoming pool for a
# year+, so they accumulate instead of churning. An empirical probe
# confirmed real inventory out there (NYC: 799 @ 12-18mo, 123 @ 18-24mo;
# London: 917 / 59) and a hard floor of zero beyond 24mo — hence no
# 24-36mo window. Sparse far windows page out after 1-4 pages, so the
# real added cost is well under the 5×200 ceiling. Free tier is
# 5 000 calls/day; worst case here is ~200 calls/day (4 cities ×
# 2 fires/day × 5 windows × ≤5 pages).
_DATE_WINDOWS = [
    ("0-3mo",   0,    90),
    ("3-6mo",   90,   180),
    ("6-12mo",  180,  365),
    ("12-18mo", 365,  545),
    ("18-24mo", 545,  730),
]

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
from app.services.collectors.base import BaseCollector, RawEvent, CollectorAuthError
from app.services.collectors.category_mapper import map_category


class TicketmasterCollector(BaseCollector):

    @property
    def source_name(self) -> str:
        return "ticketmaster"

    def is_configured(self) -> bool:
        return bool(settings.TICKETMASTER_KEY)

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        country_code = COUNTRY_ISO.get(country_code, country_code)
        events: list[RawEvent] = []
        seen_ids: set[str] = set()  # belt-and-braces dedup within one collect()
        _MAX_PAGES = 5

        now_utc = datetime.now(timezone.utc).replace(microsecond=0)
        window_summary: list[str] = []

        async with httpx.AsyncClient(timeout=30) as client:
            for label, start_d, end_d in _DATE_WINDOWS:
                start_dt = now_utc + timedelta(days=start_d)
                end_dt = now_utc + timedelta(days=end_d)
                window_total_elements = None
                window_added = 0

                throttled = False
                for page in range(_MAX_PAGES):
                    params = {
                        "apikey": settings.TICKETMASTER_KEY,
                        "city": city_name,
                        "countryCode": country_code,
                        "size": 200,
                        "page": page,
                        "sort": "date,asc",
                        "startDateTime": start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "endDateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "includePriceRanges": "yes",
                    }
                    # Paced + 429-aware fetch (see _TM_REQ_DELAY block above).
                    for attempt in range(_TM_MAX_RETRIES):
                        await asyncio.sleep(_TM_REQ_DELAY)
                        resp = await client.get(_TM_ENDPOINT, params=params)
                        if resp.status_code != 429:
                            break
                        ra = resp.headers.get("Retry-After")
                        try:
                            ra = float(ra) if ra else 0.0
                        except ValueError:
                            ra = 0.0
                        backoff = max(ra, _TM_BACKOFF_BASE * (2 ** attempt))
                        logger.warning(
                            f"ticketmaster {city_name}/{country_code}: 429 on "
                            f"{label} p{page} — backoff {backoff:.1f}s "
                            f"(attempt {attempt + 1}/{_TM_MAX_RETRIES})"
                        )
                        await asyncio.sleep(backoff)
                    if resp.status_code == 429:
                        # Still throttled after retries — stop paging this
                        # window gracefully rather than aborting the whole
                        # city (raise_for_status would lose all of its TM).
                        logger.warning(
                            f"ticketmaster {city_name}/{country_code}: still 429 "
                            f"after {_TM_MAX_RETRIES} retries on {label}; "
                            f"skipping rest of window"
                        )
                        throttled = True
                        break
                    if resp.status_code in (401, 403):
                        # Systemic key rejection — fail loudly so it can't hide
                        # as found=0/success (the Bandsintown trap).
                        raise CollectorAuthError(
                            f"Ticketmaster {resp.status_code}: {resp.text[:160]}"
                        )
                    if resp.status_code == 400:
                        break  # page out of range — TM returns 400 when page > total pages
                    resp.raise_for_status()
                    data = resp.json()

                    page_events = data.get("_embedded", {}).get("events", [])
                    if not page_events:
                        break

                    for ev in page_events:
                        ev_id = ev.get("id")
                        if ev_id and ev_id in seen_ids:
                            continue
                        raw = self._transform(ev)
                        if raw:
                            events.append(raw)
                            window_added += 1
                            if ev_id:
                                seen_ids.add(ev_id)

                    pagination = data.get("page", {})
                    window_total_elements = pagination.get("totalElements", window_total_elements)
                    total_pages = pagination.get("totalPages", 1)
                    if page + 1 >= total_pages:
                        break

                # Surface per-window stats so we can spot windows that
                # are still capped at the 1 000-event ceiling (i.e. a
                # window where totalElements > 1 000 means we're still
                # missing events in that slice and should narrow further
                # with e.g. segmentId).
                te = window_total_elements if window_total_elements is not None else "?"
                capped = (
                    isinstance(window_total_elements, int)
                    and window_total_elements > 1000
                )
                window_summary.append(
                    f"{label}: te={te} added={window_added}"
                    + (" CAPPED" if capped else "")
                    + (" THROTTLED" if throttled else "")
                )

        logger.info(
            f"ticketmaster {city_name}/{country_code}: " + " | ".join(window_summary)
        )
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
