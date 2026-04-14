"""
Tel Aviv venue scraper — scrapes Israeli ticketing sites for events.
Supports: Smarticket, Barby, Zappa, Levontin 7, Shablul Jazz, and more.
"""
from __future__ import annotations
import httpx
import re
import json
from datetime import date, datetime, timedelta
from urllib.parse import urljoin, unquote

from app.services.collectors.base import BaseCollector, RawEvent, safe_time, default_end_time

HEBREW_MONTHS = {
    "ינואר": 1, "פברואר": 2, "מרץ": 3, "אפריל": 4,
    "מאי": 5, "יוני": 6, "יולי": 7, "אוגוסט": 8,
    "ספטמבר": 9, "אוקטובר": 10, "נובמבר": 11, "דצמבר": 12,
}
ENGLISH_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Tel Aviv venues with their websites and types
TEL_AVIV_VENUES = [
    # Music
    {"name": "Barby Club",         "url": "https://www.barby.co.il",          "address": "52 Kibbutz Galuyot St, Tel Aviv", "type": "Music Venue"},
    {"name": "Zappa Club Tel Aviv","url": "https://zappa-club.co.il/events",    "address": "24 Raoul Wallenberg St, Tel Aviv","type": "Music Venue"},
    {"name": "Levontin 7",         "url": "https://www.levontin7.com",         "address": "7 Levontin St, Tel Aviv",         "type": "Music Venue"},
    {"name": "Shablul Jazz Club",  "url": "https://shabluljazz.com",           "address": "40 Yordei HaSira St, Tel Aviv",   "type": "Jazz Club"},
    {"name": "Gagarin Club",       "url": "https://www.gagarin.co.il",         "address": "8 Karlibach St, Tel Aviv",        "type": "Music Venue"},
    {"name": "The Block",          "url": "https://www.theblock.co.il",        "address": "157 Ben Yehuda St, Tel Aviv",     "type": "Nightclub"},
    {"name": "Kuli Alma",          "url": "https://www.kulialma.com",          "address": "10 Mikveh Israel St, Tel Aviv",   "type": "Music Venue"},
    {"name": "Hangar 11",          "url": "https://www.hangar11.co.il",        "address": "Tel Aviv Port",                  "type": "Concert Hall"},
    {"name": "Reading 3",          "url": "https://www.reading3.co.il",        "address": "Reading Power Station, Tel Aviv","type": "Concert Hall"},
    {"name": "Radio EPGB",         "url": "https://radioepgb.com",             "address": "7 Shadal St, Tel Aviv",           "type": "Music Bar"},
    {"name": "Yellow Submarine",   "url": "https://www.yellowsubmarine.org.il","address": "13 HaRechev St, Tel Aviv",        "type": "Music Venue"},
    # Theatre
    {"name": "Habima Theatre",     "url": "https://www.habima.co.il",          "address": "Tarsat Blvd 2, Tel Aviv",         "type": "Theatre"},
    {"name": "Cameri Theatre",     "url": "https://www.cameri.co.il",          "address": "19 Shaul Hamelech Blvd, Tel Aviv","type": "Theatre"},
    {"name": "Beit Lessin Theatre","url": "https://www.lessin.co.il",          "address": "101 Dizengoff St, Tel Aviv",      "type": "Theatre"},
    {"name": "Gesher Theatre",     "url": "https://www.gesher-theatre.co.il",  "address": "7 Yordei HaSira St, Tel Aviv",    "type": "Theatre"},
    {"name": "Tmuna Theatre",      "url": "https://www.tmu-na.org.il",         "address": "8 Soncino St, Tel Aviv",          "type": "Theatre"},
    {"name": "Jaffa Theatre",      "url": "https://www.jaffatheatre.org.il",   "address": "10 Mifratz Shlomo St, Jaffa",    "type": "Theatre"},
    # Dance
    {"name": "Suzanne Dellal Centre","url": "https://www.suzannedellal.org.il","address": "5 Yehieli St, Tel Aviv",          "type": "Dance Center"},
    # Comedy
    {"name": "Comedy Bar Tel Aviv","url": "https://comedybar.co.il",           "address": "140 Dizengoff St, Tel Aviv",      "type": "Comedy Club"},
    # Art / Museum
    {"name": "Tel Aviv Museum of Art","url": "https://www.tamuseum.org.il",    "address": "27 Shaul HaMelech Blvd, Tel Aviv","type": "Museum"},
    {"name": "Eretz Israel Museum","url": "https://www.eretzmuseum.org.il",    "address": "2 Chaim Levanon St, Tel Aviv",    "type": "Museum"},
    # Cinema
    {"name": "Tel Aviv Cinematheque","url": "https://www.cinema.co.il",        "address": "2 Sprinzak St, Tel Aviv",         "type": "Cinema"},
]

GENERIC_WORDS = {
    "tickets", "buy", "home", "cart", "search", "login", "register",
    "contact", "about", "menu", "page", "gallery", "team", "shop",
    "בית", "כרטיסים", "יצירת קשר", "אודות", "חנות",
}


def _is_generic(name: str) -> bool:
    return len(name) < 3 or name.lower().strip() in GENERIC_WORDS


def _parse_hebrew_date(text: str) -> date | None:
    """Parse dates like '14 מרץ 2026', '14.03.2026', '14/3/26'."""
    # Hebrew month name
    for heb, month_num in HEBREW_MONTHS.items():
        m = re.search(rf'(\d{{1,2}})\s+{heb}\s*(\d{{4}})?', text)
        if m:
            day = int(m.group(1))
            year = int(m.group(2)) if m.group(2) else date.today().year
            try:
                return date(year, month_num, day)
            except ValueError:
                pass

    # English month name
    for eng, month_num in ENGLISH_MONTHS.items():
        m = re.search(rf'(\d{{1,2}})\s+{eng}\w*\s*(\d{{4}})?', text, re.IGNORECASE)
        if m:
            day = int(m.group(1))
            year = int(m.group(2)) if m.group(2) else date.today().year
            try:
                return date(year, month_num, day)
            except ValueError:
                pass

    # Numeric: DD/MM/YYYY or DD.MM.YYYY
    m = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{2,4})', text)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 2000
        try:
            return date(year, month, day)
        except ValueError:
            pass

    # ISO: YYYY-MM-DD
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    return None


def _parse_time(text: str) -> str | None:
    """Extract HH:MM from text."""
    m = re.search(r'(\d{1,2}):(\d{2})', text)
    if m:
        h, mn = int(m.group(1)), int(m.group(2))
        if 0 <= h <= 23 and 0 <= mn <= 59:
            return f"{h:02d}:{mn:02d}"
    return None


class TelAvivVenueScraper(BaseCollector):

    @property
    def source_name(self) -> str:
        return "tel_aviv_venues"

    def is_configured(self) -> bool:
        return True

    async def collect(self, city_name: str, country_code: str = "IL", **kwargs) -> list[RawEvent]:
        if city_name not in ("Tel Aviv", "Tel Aviv-Yafo"):
            return []

        all_events: list[RawEvent] = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
        }

        async with httpx.AsyncClient(
            timeout=20, follow_redirects=True, headers=headers,
            verify=False,   # some IL sites have cert mismatches (e.g. shabluljazz.com)
        ) as client:
            for venue in TEL_AVIV_VENUES:
                try:
                    events = await self._scrape_venue(client, venue)
                    all_events.extend(events)
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(
                        f"Tel Aviv scrape failed for {venue['name']}: {e}"
                    )

        return all_events

    async def _scrape_venue(self, client: httpx.AsyncClient, venue: dict) -> list[RawEvent]:
        url = venue["url"]
        resp = await client.get(url)
        if resp.status_code != 200:
            return []

        html = resp.text
        base_url = str(resp.url)

        # Try Smarticket embedded widget first
        if "smarticket.co.il" in html:
            events = self._parse_smarticket(html, base_url, venue)
            if events:
                return events

        # Try JSON-LD structured data
        events = self._parse_json_ld(html, base_url, venue)
        if events:
            return events

        # Generic: find event links + nearby dates
        return self._parse_generic(html, base_url, venue)

    def _parse_smarticket(self, html: str, base_url: str, venue: dict) -> list[RawEvent]:
        """Parse events from Smarticket-powered pages."""
        events = []
        seen = set()

        # Find all links pointing to smarticket.co.il
        for m in re.finditer(r'href=["\']([^"\']*smarticket\.co\.il/[^"\']+)["\']', html):
            href = m.group(1)
            if any(x in href for x in ["/?", "/#", "/cart", "/page/"]):
                continue

            # Extract event name from URL slug
            slug_m = re.search(r'smarticket\.co\.il/([^/?#]+)', href)
            if not slug_m:
                continue
            name = unquote(slug_m.group(1)).replace("_", " ").replace("+", " ").strip()
            if _is_generic(name):
                continue

            # Find date in surrounding HTML (~300 chars around the link)
            link_pos = m.start()
            context = html[max(0, link_pos - 400): link_pos + 400]
            start_date = _parse_hebrew_date(context)
            if not start_date or start_date < date.today():
                continue

            start_time = _parse_time(context)
            key = f"{name}_{start_date}"
            if key in seen:
                continue
            seen.add(key)

            purchase_link = href if href.startswith("http") else urljoin(base_url, href)
            end_date, end_time = default_end_time(start_time, start_date, None)

            events.append(RawEvent(
                name=name,
                start_date=start_date,
                start_time=start_time,
                end_date=end_date,
                end_time=end_time,
                purchase_link=purchase_link,
                venue_name=venue["name"],
                venue_address=venue["address"],
                venue_city="Tel Aviv",
                venue_country="Israel",
                source="tel_aviv_venues",
                source_id=purchase_link,
                raw_categories=[self._guess_category(venue["type"])],
            ))

        return events

    def _parse_json_ld(self, html: str, base_url: str, venue: dict) -> list[RawEvent]:
        """Extract events from JSON-LD structured data."""
        events = []
        for m in re.finditer(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL):
            try:
                data = json.loads(m.group(1))
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if item.get("@type") not in ("Event", "MusicEvent", "TheaterEvent",
                                                  "ComedyEvent", "DanceEvent", "ScreeningEvent"):
                        continue
                    name = item.get("name", "")
                    if not name or _is_generic(name):
                        continue

                    start_str = item.get("startDate", "")
                    if not start_str:
                        continue
                    try:
                        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    except ValueError:
                        continue
                    if start_dt.date() < date.today():
                        continue

                    end_str = item.get("endDate", "")
                    end_dt = None
                    if end_str:
                        try:
                            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        except ValueError:
                            pass

                    st = safe_time(start_dt)
                    et = safe_time(end_dt) if end_dt else None
                    if et is None:
                        _, et = default_end_time(st, start_dt.date(), None)

                    offers = item.get("offers", {})
                    if isinstance(offers, list):
                        offers = offers[0] if offers else {}
                    price = None
                    try:
                        price = float(offers.get("price", 0)) or None
                    except (TypeError, ValueError):
                        pass

                    purchase_link = offers.get("url") or item.get("url") or base_url
                    image = item.get("image")
                    image_url = image if isinstance(image, str) else (image[0] if isinstance(image, list) else None)

                    events.append(RawEvent(
                        name=name,
                        start_date=start_dt.date(),
                        start_time=st,
                        end_date=end_dt.date() if end_dt else start_dt.date(),
                        end_time=et,
                        price=price,
                        purchase_link=purchase_link,
                        image_url=image_url,
                        venue_name=venue["name"],
                        venue_address=venue["address"],
                        venue_city="Tel Aviv",
                        venue_country="Israel",
                        source="tel_aviv_venues",
                        source_id=purchase_link,
                        raw_categories=[self._guess_category(venue["type"])],
                    ))
            except (json.JSONDecodeError, KeyError):
                continue
        return events

    def _parse_generic(self, html: str, base_url: str, venue: dict) -> list[RawEvent]:
        """Fallback: find any links near a date pattern."""
        events = []
        seen = set()

        # Find all internal links
        for m in re.finditer(r'href=["\']([^"\'#?][^"\']*)["\']', html):
            href = m.group(1)
            full_url = href if href.startswith("http") else urljoin(base_url, href)
            if base_url not in full_url:
                continue

            link_pos = m.start()
            context = html[max(0, link_pos - 500): link_pos + 500]

            start_date = _parse_hebrew_date(context)
            if not start_date or start_date < date.today():
                continue

            # Try to extract a name from the link text
            link_text_m = re.search(r'href=["\'][^"\']+["\'][^>]*>([^<]{3,80})<', html[link_pos:link_pos+200])
            if not link_text_m:
                continue
            name = re.sub(r'\s+', ' ', link_text_m.group(1)).strip()
            if _is_generic(name):
                continue

            key = f"{name}_{start_date}"
            if key in seen:
                continue
            seen.add(key)

            start_time = _parse_time(context)
            end_date, end_time = default_end_time(start_time, start_date, None)

            events.append(RawEvent(
                name=name,
                start_date=start_date,
                start_time=start_time,
                end_date=end_date,
                end_time=end_time,
                purchase_link=full_url,
                venue_name=venue["name"],
                venue_address=venue["address"],
                venue_city="Tel Aviv",
                venue_country="Israel",
                source="tel_aviv_venues",
                source_id=full_url,
                raw_categories=[self._guess_category(venue["type"])],
            ))

        return events

    @staticmethod
    def _guess_category(venue_type: str) -> str:
        mapping = {
            "Theatre": "Art", "Theater": "Art", "Dance Center": "Dance",
            "Jazz Club": "Music", "Music Venue": "Music", "Concert Hall": "Music",
            "Music Bar": "Music", "Nightclub": "Music", "Comedy Club": "Comedy",
            "Museum": "Art", "Gallery": "Art", "Cinema": "Film",
        }
        return mapping.get(venue_type, "Music")
