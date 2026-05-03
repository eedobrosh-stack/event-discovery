"""Tickchak.co.il — Israeli ticket aggregator.

Single-URL JSON-LD source: https://live.tickchak.co.il/ embeds 100+ Events
under @graph (canonical schema.org multi-entity wrapper). Discovered via
the find_city_guides probe after the @graph + Event-subtype fix in 6dc754d.

Event types observed: ComedyEvent, ChildrensEvent, SocialEvent,
EducationEvent, MusicEvent, TheaterEvent, generic Event. All accepted via
the shared EVENT_TYPES allowlist in app/services/collectors/_jsonld.py.

Coverage: nationwide IL — events at venues across Tel Aviv, Jerusalem,
Haifa, Yehud, Kfar Saba, etc. The collect() method runs once for any
Israeli city and returns all future events; downstream venue→city
matching assigns each event to its city in our DB.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

import httpx

from app.services.collectors.base import BaseCollector, RawEvent, safe_time
from app.services.collectors._jsonld import iter_events

logger = logging.getLogger(__name__)

URL = "https://live.tickchak.co.il/"

# Hebrew → English city names for the IL cities we care about. Mirrored
# (intentionally) from leaan.py and israel_sites.py — extract to a shared
# module if a 4th IL collector is ever added.
HEB_TO_EN_CITY = {
    "תל אביב": "Tel Aviv",
    "תל אביב-יפו": "Tel Aviv",
    "תל-אביב": "Tel Aviv",
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
    "אילת": "Eilat",
    "מודיעין-מכבים-רעות": "Modi'in",
    "מודיעין": "Modi'in",
    "יהוד-מונוסון": "Yehud",
    "יהוד": "Yehud",
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

ISRAEL_CITY_NAMES = set(HEB_TO_EN_CITY.values())


def _stable_source_id(name: str, start_iso: str, venue: str | None) -> str:
    """Tickchak doesn't expose per-event URLs in its JSON-LD, so we synthesize
    a deterministic ID from (name, start, venue). Hash collisions are
    practically zero at our scale and the same event in subsequent runs
    produces the same ID — idempotent ingestion."""
    import hashlib
    seed = f"{(name or '').strip()}|{start_iso}|{(venue or '').strip()}"
    return "tickchak_" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def _first_performer_name(performer) -> Optional[str]:
    if not performer:
        return None
    if isinstance(performer, list):
        performer = performer[0] if performer else None
    if isinstance(performer, dict):
        n = performer.get("name")
        return n.strip() if isinstance(n, str) else None
    if isinstance(performer, str):
        return performer.strip() or None
    return None


def _parse_event(ev: dict) -> RawEvent | None:
    """Map one tickchak JSON-LD entity to a RawEvent."""
    start_str = ev.get("startDate") or ""
    if not start_str:
        return None
    try:
        if "T" in start_str:
            start_dt = datetime.fromisoformat(start_str)
        else:
            start_dt = datetime.combine(date.fromisoformat(start_str), datetime.min.time())
    except ValueError:
        return None

    if start_dt.date() < date.today():
        return None

    end_dt = None
    end_str = ev.get("endDate") or ""
    if end_str:
        try:
            end_dt = datetime.fromisoformat(end_str) if "T" in end_str \
                else datetime.combine(date.fromisoformat(end_str), datetime.min.time())
        except ValueError:
            pass

    location = ev.get("location") or {}
    if isinstance(location, list):
        location = location[0] if location else {}
    address = location.get("address") or {} if isinstance(location, dict) else {}
    if isinstance(address, list):
        address = address[0] if address else {}

    venue_name = location.get("name") if isinstance(location, dict) else None
    raw_locality = address.get("addressLocality") if isinstance(address, dict) else None
    venue_city = HEB_TO_EN_CITY.get(raw_locality, raw_locality)
    venue_address = address.get("streetAddress") if isinstance(address, dict) else None

    offers = ev.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price = None
    low = offers.get("lowPrice") or offers.get("price") if isinstance(offers, dict) else None
    if low is not None:
        try:
            price = float(str(low).replace("₪", "").replace(",", "").strip())
        except (TypeError, ValueError):
            pass
    currency = "ILS"
    if isinstance(offers, dict) and offers.get("priceCurrency"):
        currency = offers["priceCurrency"]

    artist_name = _first_performer_name(ev.get("performer"))

    name = (ev.get("name") or "Untitled").strip()
    has_time = "T" in start_str
    start_time = safe_time(start_dt) if has_time else None
    end_time = safe_time(end_dt) if (end_dt and "T" in end_str) else None

    image = ev.get("image")
    if isinstance(image, list):
        image = image[0] if image else None
    if isinstance(image, dict):
        image = image.get("url")

    purchase_link = ev.get("url") or URL  # falls back to homepage if event-URL missing

    return RawEvent(
        name=name,
        start_date=start_dt.date(),
        start_time=start_time,
        end_date=end_dt.date() if end_dt else None,
        end_time=end_time,
        artist_name=artist_name,
        price=price,
        price_currency=currency,
        purchase_link=purchase_link,
        image_url=image if isinstance(image, str) else None,
        description=ev.get("description"),
        venue_name=venue_name,
        venue_address=venue_address,
        venue_city=venue_city,
        venue_country="Israel",
        source="tickchak",
        source_id=_stable_source_id(name, start_str, venue_name),
        raw_categories=[],
    )


class TickchakCollector(BaseCollector):
    """One-URL nationwide Israeli ticket aggregator."""

    @property
    def source_name(self) -> str:
        return "tickchak"

    def is_configured(self) -> bool:
        return True  # no API key

    async def collect(self, city_name: str, country_code: str = "IL", **kwargs) -> list[RawEvent]:
        # Only run for Israeli cities — tickchak is exclusively IL inventory.
        if country_code not in ("IL", "Israel") and city_name not in ISRAEL_CITY_NAMES:
            return []

        try:
            async with httpx.AsyncClient(
                timeout=30, follow_redirects=True, headers=_HEADERS,
            ) as client:
                resp = await client.get(URL)
                resp.raise_for_status()
                html = resp.text
        except Exception as exc:
            logger.warning(f"Tickchak: request error: {exc}")
            return []

        all_events: list[RawEvent] = []
        seen: set[str] = set()
        for ld in iter_events(html, future_only=True):
            ev = _parse_event(ld)
            if ev and ev.source_id and ev.source_id not in seen:
                seen.add(ev.source_id)
                all_events.append(ev)

        # Filter to the requested city when one was specified. Empty city_city
        # → kept (we don't know where to file it; assume Tel Aviv-region).
        if city_name and city_name not in ("Israel", ""):
            filtered = [
                e for e in all_events
                if not e.venue_city or e.venue_city == city_name
            ]
        else:
            filtered = all_events

        logger.info(
            f"Tickchak: {len(filtered)}/{len(all_events)} events for "
            f"{city_name or '(all IL)'}"
        )
        return filtered
