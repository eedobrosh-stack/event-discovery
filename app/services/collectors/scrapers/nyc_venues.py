from __future__ import annotations

"""NYC venue web scraper — scrapes event listings from popular NYC venues."""

import re
import logging
from datetime import date
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.services.collectors.base import BaseCollector, RawEvent
from app.services.collectors.scrapers.base_scraper import (
    fetch_html, is_generic_name, extract_artist_from_name,
    parse_datetime, parse_price,
)

logger = logging.getLogger(__name__)

# NYC venues to scrape with their website URLs and types
NYC_VENUES = [
    {"name": "Blue Note Jazz Club", "url": "https://www.bluenotejazz.com/nyc/", "type": "Jazz Club", "scraper": "bluenote"},
    {"name": "Village Vanguard", "url": "https://villagevanguard.com/", "type": "Jazz Club", "scraper": "villagevanguard"},
    {"name": "Birdland Jazz Club", "url": "https://www.birdlandjazz.com/", "type": "Jazz Club", "scraper": "ticketweb"},
    {"name": "Bowery Ballroom", "url": "https://www.boweryballroom.com/", "type": "Concert Hall", "scraper": "generic"},
    {"name": "Mercury Lounge", "url": "https://www.mercuryloungenyc.com/", "type": "Concert Hall", "scraper": "generic"},
    {"name": "Joe's Pub", "url": "https://publictheater.org/programs/joes-pub/", "type": "Performance Venue", "scraper": "generic"},
    {"name": "Brooklyn Steel", "url": "https://www.bowerypresents.com/brooklyn-steel", "type": "Concert Hall", "scraper": "generic"},
    {"name": "Terminal 5", "url": "https://www.terminal5nyc.com/", "type": "Concert Hall", "scraper": "generic"},
    {"name": "Le Poisson Rouge", "url": "https://lpr.com/", "type": "Music Venue", "scraper": "generic"},
    {"name": "Carnegie Hall", "url": "https://www.carnegiehall.org/Calendar", "type": "Concert Hall", "scraper": "carnegiehall"},
    {"name": "Lincoln Center", "url": "https://www.lincolncenter.org/calendar", "type": "Performing Arts Center", "scraper": "generic"},
    {"name": "Beacon Theatre", "url": "https://www.msg.com/beacon-theatre", "type": "Theatre", "scraper": "msg"},
    {"name": "Radio City Music Hall", "url": "https://www.msg.com/radio-city-music-hall", "type": "Theatre", "scraper": "msg"},
    {"name": "Madison Square Garden", "url": "https://www.msg.com/madison-square-garden", "type": "Arena", "scraper": "msg"},
    {"name": "Brooklyn Academy of Music", "url": "https://www.bam.org/events", "type": "Performing Arts Center", "scraper": "generic"},
    {"name": "Irving Plaza", "url": "https://www.irvingplaza.com/", "type": "Concert Hall", "scraper": "generic"},
    {"name": "Webster Hall", "url": "https://www.websterhall.com/", "type": "Concert Hall", "scraper": "generic"},
    {"name": "Rough Trade NYC", "url": "https://www.roughtradenyc.com/", "type": "Record Store / Venue", "scraper": "generic"},
    {"name": "Smalls Jazz Club", "url": "https://www.smallslive.com/events/", "type": "Jazz Club", "scraper": "smallslive"},
    {"name": "Comedy Cellar", "url": "https://www.comedycellar.com/line-up/", "type": "Comedy Club", "scraper": "comedycellar"},
    {"name": "Gotham Comedy Club", "url": "https://gothamcomedyclub.com/", "type": "Comedy Club", "scraper": "generic"},
    {"name": "Museum of Modern Art", "url": "https://www.moma.org/calendar/", "type": "Museum", "scraper": "generic"},
    {"name": "Metropolitan Museum of Art", "url": "https://www.metmuseum.org/events", "type": "Museum", "scraper": "generic"},
    {"name": "Whitney Museum", "url": "https://whitney.org/events", "type": "Museum", "scraper": "generic"},
    {"name": "Guggenheim Museum", "url": "https://www.guggenheim.org/calendar", "type": "Museum", "scraper": "generic"},
]


class NYCVenueScraper(BaseCollector):
    """Scrapes events directly from NYC venue websites."""

    @property
    def source_name(self) -> str:
        return "nyc_venue_scraper"

    def is_configured(self) -> bool:
        return True  # No API keys needed

    async def collect(self, city_name: str, country_code: str = "US", **kwargs) -> list[RawEvent]:
        if city_name.lower() not in ("new york", "nyc", "new york city"):
            return []

        all_events = []
        for venue in NYC_VENUES:
            try:
                logger.info(f"Scraping {venue['name']}...")
                html = await fetch_html(venue["url"])
                if not html:
                    logger.warning(f"No HTML from {venue['name']}")
                    continue

                soup = BeautifulSoup(html, "lxml")
                scraper_type = venue.get("scraper", "generic")

                if scraper_type == "bluenote":
                    events = self._scrape_bluenote(soup, venue)
                elif scraper_type == "villagevanguard":
                    events = self._scrape_villagevanguard(soup, venue)
                elif scraper_type == "ticketweb":
                    events = self._scrape_ticketweb(soup, venue)
                elif scraper_type == "carnegiehall":
                    events = self._scrape_carnegiehall(soup, venue)
                elif scraper_type == "msg":
                    events = self._scrape_msg(soup, venue)
                elif scraper_type == "smallslive":
                    events = self._scrape_smallslive(soup, venue)
                elif scraper_type == "comedycellar":
                    events = self._scrape_comedycellar(soup, venue)
                else:
                    events = self._scrape_generic(soup, venue)

                logger.info(f"  Found {len(events)} events from {venue['name']}")
                all_events.extend(events)

            except Exception as e:
                logger.error(f"Error scraping {venue['name']}: {e}")

        return all_events

    def _clean_name(self, name: str) -> str:
        """Clean up event names — fix concatenated names, trim junk."""
        if not name:
            return name
        # Fix concatenated names like "Lady GagaThe MAYHEM Ball" -> "Lady Gaga - The MAYHEM Ball"
        # Detect lowercase->uppercase transition mid-word (e.g., "aT" in "GagaThe")
        name = re.sub(r"([a-z])([A-Z][a-z])", r"\1 - \2", name)
        # Remove trailing date suffixes like "Mar 19 at The Library"
        name = re.sub(r"\s*(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+at\s+.*$", "", name, flags=re.IGNORECASE)
        # Remove "on Mar 22" suffix
        name = re.sub(r"\s+on\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}$", "", name, flags=re.IGNORECASE)
        return name.strip()

    def _make_raw_event(
        self, name: str, venue: dict, start_date: str,
        start_time: str = "20:00", end_time: str = "22:30",
        artist_name: str = None, purchase_link: str = None,
        price: float = None, image_url: str = None,
        description: str = None, source_id: str = None,
    ) -> Optional[RawEvent]:
        """Create a RawEvent, validating name and date."""
        name = self._clean_name(name)
        if not name or is_generic_name(name):
            return None
        if not start_date:
            return None

        # Extract artist from name if not provided
        if not artist_name:
            name, artist_name = extract_artist_from_name(name)

        return RawEvent(
            name=name,
            start_date=date.fromisoformat(start_date),
            start_time=start_time,
            artist_name=artist_name,
            price=price,
            purchase_link=purchase_link,
            image_url=image_url,
            description=description,
            venue_name=venue["name"],
            venue_city="New York",
            venue_country="US",
            source="scraper",
            source_id=source_id or f"{venue['url']}_{name}_{start_date}",
            raw_categories=self._guess_category(venue),
        )

    def _guess_category(self, venue: dict) -> list[str]:
        """Guess event category from venue type."""
        vtype = venue.get("type", "").lower()
        if "jazz" in vtype or "music" in vtype or "concert" in vtype or "arena" in vtype:
            return ["Music"]
        if "comedy" in vtype:
            return ["Comedy"]
        if "theatre" in vtype or "theater" in vtype or "performing" in vtype:
            return ["Art"]
        if "museum" in vtype or "gallery" in vtype:
            return ["Art"]
        return []

    # --- Blue Note Jazz Club ---
    def _scrape_bluenote(self, soup: BeautifulSoup, venue: dict) -> list[RawEvent]:
        events = []
        seen = set()

        for el in soup.select(".event-wrap, li.show-slide"):
            dt = el.get("datetime", "")
            start_date_str = dt[:10] if dt else None

            # Get event name
            title_el = el.select_one(".listing-title, .listing-text")
            link_el = el.select_one('a[href*="/tm-event/"]')
            name = ""
            if title_el:
                name = title_el.get_text(strip=True)
            elif link_el:
                name = link_el.get_text(strip=True)

            if not name or not start_date_str:
                continue

            key = f"{name}_{start_date_str}"
            if key in seen:
                continue
            seen.add(key)

            link = link_el.get("href") if link_el else None
            if link and not link.startswith("http"):
                link = urljoin(venue["url"], link)

            time_el = el.select_one(".showtime-display")
            time_text = time_el.get_text(strip=True) if time_el else ""
            sd, st, et = parse_datetime(start_date_str, time_text)

            price_el = el.select_one(".price, [class*='price'], .event-price")
            price = parse_price(price_el.get_text(strip=True)) if price_el else None

            ev = self._make_raw_event(name, venue, sd, st, et, purchase_link=link, price=price)
            if ev:
                events.append(ev)

        return events

    # --- Village Vanguard ---
    def _scrape_villagevanguard(self, soup: BeautifulSoup, venue: dict) -> list[RawEvent]:
        events = []
        seen = set()

        for el in soup.select(".event-listing, .event-details, .upcoming-shows li, article"):
            title_el = el.select_one(".event-tagline, h3, h4, h2")
            link_el = el.select_one("a")
            name = title_el.get_text(strip=True) if title_el else ""
            if not name and link_el:
                name = link_el.get_text(strip=True)
            if not name:
                continue

            full_text = el.get_text()
            # Look for date range: "March 3 – March 8"
            date_m = re.search(
                r"(January|February|March|April|May|June|July|August|September|October|November|December)"
                r"\s+(\d{1,2})"
                r"(?:\s*[-–‑]\s*(?:January|February|March|April|May|June|July|August|September|October|November|December)?\s*(\d{1,2}))?"
                r"(?:\s*,?\s*(\d{4}))?",
                full_text, re.IGNORECASE,
            )

            if not date_m:
                continue

            from app.services.collectors.scrapers.base_scraper import ENGLISH_MONTHS
            month = ENGLISH_MONTHS.get(date_m.group(1).lower())
            day = int(date_m.group(2))
            year = int(date_m.group(4)) if date_m.group(4) else date.today().year
            if not month:
                continue

            start_date_str = f"{year}-{month:02d}-{day:02d}"
            key = f"{name}_{start_date_str}"
            if key in seen:
                continue
            seen.add(key)

            link = link_el.get("href") if link_el else None
            if link and not link.startswith("http"):
                link = urljoin(venue["url"], link)

            price_el = el.select_one(".price, [class*='price'], [class*='cost']")
            price = parse_price(price_el.get_text(strip=True)) if price_el else None

            ev = self._make_raw_event(name, venue, start_date_str, purchase_link=link, price=price)
            if ev:
                events.append(ev)

        return events

    # --- Ticketweb plugin (Birdland, etc.) ---
    def _scrape_ticketweb(self, soup: BeautifulSoup, venue: dict) -> list[RawEvent]:
        events = []
        seen = set()

        container = soup.select_one(".tw-plugin-upcoming-event-list, #tw-responsive.event-list")
        if not container:
            return self._scrape_generic(soup, venue)

        for block in container.find_all("div"):
            date_el = block.select_one(".tw-event-date-complete, .tw-event-date")
            if not date_el:
                continue

            date_text = date_el.get_text(strip=True)
            time_el = block.select_one(".tw-event-time-complete, .tw-event-time, .event-time")
            time_text = time_el.get_text(strip=True) if time_el else ""

            name_link = None
            for a in block.select('a[href*="/tm-event/"]'):
                text = a.get_text(strip=True)
                if text and len(text) > 2 and not re.search(r"Sold Out|Buy Tickets|Sales Ended", text, re.IGNORECASE):
                    name_link = a
                    break

            if not name_link:
                continue

            name = name_link.get_text(strip=True)
            href = name_link.get("href", "")
            if href and not href.startswith("http"):
                href = urljoin(venue["url"], href)

            sd, st, et = parse_datetime(date_text, time_text)
            if not sd:
                continue

            key = f"{name}_{sd}"
            if key in seen:
                continue
            seen.add(key)

            price_el = block.select_one(".tw-event-price, [class*='price']")
            price = parse_price(price_el.get_text(strip=True)) if price_el else None

            ev = self._make_raw_event(name, venue, sd, st, et, purchase_link=href, source_id=href, price=price)
            if ev:
                events.append(ev)

        return events

    # --- Carnegie Hall ---
    def _scrape_carnegiehall(self, soup: BeautifulSoup, venue: dict) -> list[RawEvent]:
        events = []
        seen = set()

        for card in soup.select(".ch-calendar-card, .event-card, .calendar-event, article, [class*='event']"):
            title_el = card.select_one("h3, h4, .event-title, .title, [class*='title']")
            if not title_el:
                continue
            name = title_el.get_text(strip=True)

            date_el = card.select_one(".date, time, [datetime], [class*='date']")
            date_text = ""
            if date_el:
                date_text = date_el.get("datetime", "") or date_el.get_text(strip=True)

            time_el = card.select_one(".time, [class*='time']")
            time_text = time_el.get_text(strip=True) if time_el else ""

            sd, st, et = parse_datetime(date_text, time_text)
            if not sd:
                continue

            key = f"{name}_{sd}"
            if key in seen:
                continue
            seen.add(key)

            link_el = card.select_one("a")
            link = link_el.get("href") if link_el else None
            if link and not link.startswith("http"):
                link = urljoin(venue["url"], link)

            price_el = card.select_one("[class*='price']")
            price = parse_price(price_el.get_text(strip=True)) if price_el else None

            img_el = card.select_one("img")
            img = img_el.get("src") if img_el else None

            ev = self._make_raw_event(name, venue, sd, st, et, purchase_link=link, price=price, image_url=img)
            if ev:
                events.append(ev)

        return events

    # --- MSG venues (Beacon Theatre, Radio City, MSG) ---
    def _scrape_msg(self, soup: BeautifulSoup, venue: dict) -> list[RawEvent]:
        events = []
        seen = set()

        for card in soup.select("[class*='event'], article, .card, li"):
            title_el = card.select_one("h2, h3, h4, .title, [class*='title'], [class*='name']")
            if not title_el:
                continue
            name = title_el.get_text(strip=True)
            if len(name) < 3 or is_generic_name(name):
                continue

            date_el = card.select_one("time, [datetime], .date, [class*='date']")
            date_text = ""
            if date_el:
                date_text = date_el.get("datetime", "") or date_el.get_text(strip=True)

            sd, st, et = parse_datetime(date_text, "")
            if not sd:
                continue

            key = f"{name}_{sd}"
            if key in seen:
                continue
            seen.add(key)

            link_el = card.select_one("a")
            link = link_el.get("href") if link_el else None
            if link and not link.startswith("http"):
                link = urljoin(venue["url"], link)

            img_el = card.select_one("img")
            img = img_el.get("src") if img_el else None

            price_el = card.select_one("[class*='price'], .cost, .ticket-price")
            price = parse_price(price_el.get_text(strip=True)) if price_el else None

            ev = self._make_raw_event(name, venue, sd, st, et, purchase_link=link, image_url=img, price=price)
            if ev:
                events.append(ev)

        return events

    # --- Smalls Live ---
    def _scrape_smallslive(self, soup: BeautifulSoup, venue: dict) -> list[RawEvent]:
        events = []
        seen = set()

        for card in soup.select(".event-card, .event, article, [class*='event']"):
            title_el = card.select_one("h3, h4, h2, .title, .event-title, [class*='title']")
            if not title_el:
                continue
            name = title_el.get_text(strip=True)

            date_el = card.select_one("time, [datetime], .date, [class*='date']")
            date_text = ""
            if date_el:
                date_text = date_el.get("datetime", "") or date_el.get_text(strip=True)

            time_el = card.select_one("[class*='time']")
            time_text = time_el.get_text(strip=True) if time_el else ""

            sd, st, et = parse_datetime(date_text, time_text)
            if not sd:
                continue

            key = f"{name}_{sd}"
            if key in seen:
                continue
            seen.add(key)

            link_el = card.select_one("a")
            link = link_el.get("href") if link_el else None
            if link and not link.startswith("http"):
                link = urljoin(venue["url"], link)

            price_el = card.select_one("[class*='price'], .cover, [class*='cover']")
            price = parse_price(price_el.get_text(strip=True)) if price_el else None

            ev = self._make_raw_event(name, venue, sd, st, et, purchase_link=link, price=price)
            if ev:
                events.append(ev)

        return events

    # --- Comedy Cellar ---
    def _scrape_comedycellar(self, soup: BeautifulSoup, venue: dict) -> list[RawEvent]:
        events = []
        seen = set()

        for card in soup.select(".show, .lineup-set, [class*='show'], [class*='lineup'], article"):
            title_el = card.select_one("h3, h4, h2, .comedian-name, .title, [class*='name']")
            if not title_el:
                continue
            name = title_el.get_text(strip=True)

            date_el = card.select_one("time, [datetime], .date, [class*='date']")
            date_text = ""
            if date_el:
                date_text = date_el.get("datetime", "") or date_el.get_text(strip=True)

            time_el = card.select_one("[class*='time']")
            time_text = time_el.get_text(strip=True) if time_el else ""

            sd, st, et = parse_datetime(date_text, time_text)
            if not sd:
                continue

            key = f"{name}_{sd}_{st}"
            if key in seen:
                continue
            seen.add(key)

            link = venue["url"]

            price_el = card.select_one("[class*='price'], .cover-charge, [class*='cover']")
            price = parse_price(price_el.get_text(strip=True)) if price_el else None

            ev = self._make_raw_event(name, venue, sd, st, et, purchase_link=link, price=price)
            if ev:
                ev.raw_categories = ["Comedy"]
                events.append(ev)

        return events

    # --- Generic scraper (fallback) ---
    def _scrape_generic(self, soup: BeautifulSoup, venue: dict) -> list[RawEvent]:
        events = []
        seen = set()

        selectors = [
            ".event", ".event-item", ".event-card", ".show",
            ".performance", "article", "[class*='event']",
            "[class*='show']", "[class*='performance']",
        ]

        for selector in selectors:
            for elem in soup.select(selector):
                # Get name
                title_el = elem.select_one("h1, h2, h3, h4, .event-title, .title, [class*='title']")
                name = ""
                if title_el:
                    name = title_el.get_text(strip=True)
                if not name:
                    link_el = elem.select_one("a")
                    if link_el:
                        name = link_el.get_text(strip=True)
                if not name or is_generic_name(name):
                    continue

                name = re.sub(r"\s+", " ", name).strip()

                # Get date
                date_el = elem.select_one(".date, .event-date, [class*='date'], time, [datetime]")
                date_text = ""
                if date_el:
                    date_text = date_el.get("datetime", "") or date_el.get_text(strip=True)

                time_el = elem.select_one(".time, .event-time, [class*='time']")
                time_text = time_el.get_text(strip=True) if time_el else ""

                sd, st, et = parse_datetime(date_text, time_text)
                if not sd:
                    continue

                key = f"{name}_{sd}"
                if key in seen:
                    continue
                seen.add(key)

                link_el = elem.select_one("a")
                link = link_el.get("href") if link_el else None
                if link and not link.startswith("http"):
                    link = urljoin(venue["url"], link)

                price_el = elem.select_one(".price, [class*='price']")
                price = parse_price(price_el.get_text(strip=True)) if price_el else None

                img_el = elem.select_one("img")
                img = img_el.get("src") if img_el else None

                desc_el = elem.select_one(".description, [class*='description']")
                desc = desc_el.get_text(strip=True) if desc_el else None

                ev = self._make_raw_event(
                    name, venue, sd, st, et,
                    purchase_link=link, price=price,
                    image_url=img, description=desc,
                )
                if ev:
                    events.append(ev)

        return events
