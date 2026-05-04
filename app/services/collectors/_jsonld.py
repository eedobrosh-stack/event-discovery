"""Shared schema.org JSON-LD parsing for event discovery + collection.

Single source of truth for two consumers:

  scripts/find_city_guides.py          counts Events on a probe page
  app/services/collectors/scrapers/    extracts Event dicts for ingestion

Handles three real-world wrapper patterns sites use to batch entities:

  bare list                    [{...}, {...}]
  {"@graph": [...]}             canonical schema.org multi-entity
  {"itemListElement": [...]}    ItemList containing Events / wrappers

Without descent through these wrappers, sites like tickchak.co.il (187
events under @graph) appear empty to the probe.
"""
from __future__ import annotations

import json
import re
from datetime import date
from typing import Iterator, Optional
from urllib.parse import urljoin


# Schema.org Event subtype hierarchy — accept any of these as a real event.
# Source: https://schema.org/Event#hierarchy
# Excludes BroadcastEvent / DeliveryEvent / OnDemandEvent / PublicationEvent /
# SaleEvent which aren't event-listings in the live-calendar sense.
EVENT_TYPES: frozenset = frozenset({
    "Event",
    "BusinessEvent", "ChildrensEvent", "ComedyEvent", "DanceEvent",
    "EducationEvent", "EventSeries", "ExhibitionEvent", "Festival",
    "FoodEvent", "Hackathon", "LiteraryEvent", "MusicEvent",
    "ScreeningEvent", "SocialEvent", "SportsEvent", "TheaterEvent",
    "VisualArtsEvent",
})


_LD_BLOCK_RE = re.compile(
    r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
    re.DOTALL,
)


def flatten_ld_items(data) -> Iterator[dict]:
    """Yield individual JSON-LD entities, descending into wrapper nodes.

    Sites batch entities a few different ways:
      - bare list                              [{...}, {...}]
      - {"@graph": [...]}                       canonical multi-entity
      - {"itemListElement": [...]}              ItemList (each may be {"item": ...})

    This recursively descends so callers see a flat stream of leaf entities.
    Non-dicts are silently skipped.
    """
    if isinstance(data, list):
        for item in data:
            yield from flatten_ld_items(item)
        return
    if not isinstance(data, dict):
        return
    if isinstance(data.get("@graph"), list):
        for item in data["@graph"]:
            yield from flatten_ld_items(item)
        return
    if isinstance(data.get("itemListElement"), list):
        for item in data["itemListElement"]:
            if isinstance(item, dict) and "item" in item:
                yield from flatten_ld_items(item["item"])
            else:
                yield from flatten_ld_items(item)
        return
    yield data


def iter_events(html: str, future_only: bool = True) -> Iterator[dict]:
    """Yield JSON-LD Event dicts from a page's HTML.

    Filters to schema.org Event subtypes (EVENT_TYPES). When future_only,
    skips events whose startDate (parsed YYYY-MM-DD prefix) is in the past.
    Malformed JSON blocks are silently skipped — a single bad block on a
    page shouldn't blind us to the others.
    """
    today = str(date.today())
    for raw_block in _LD_BLOCK_RE.findall(html):
        try:
            data = json.loads(raw_block)
        except (json.JSONDecodeError, TypeError):
            continue
        for item in flatten_ld_items(data):
            if not isinstance(item, dict):
                continue
            if item.get("@type") not in EVENT_TYPES:
                continue
            if future_only:
                sd = (item.get("startDate") or "")[:10]
                if sd and sd < today:
                    continue
            yield item


def count_events(html: str, future_only: bool = True) -> tuple[int, list[str]]:
    """Probe-side helper: (count, sample_names[:3]) — back-compat shape for
    scripts/find_city_guides.py."""
    names = [ev.get("name", "Untitled") for ev in iter_events(html, future_only=future_only)]
    return len(names), names[:3]


# ── Pagination detection (Move 1 of the pagination plan: log, don't follow) ──
#
# We look for the most-common pagination affordances and report whether any
# fired, which kind, and the inferred next-page URL when extractable. We do
# NOT follow the link — the goal of this iteration is to gather evidence of
# how often (and on which sources) pagination would actually unlock more
# events. Once we have that data, we'll design a follower that handles the
# patterns we actually see, instead of speculating.
#
# Patterns are tried in order of semantic clarity:
#   rel="next" link/anchor   ← HTML5 spec, gold-standard signal
#   aria-label="Next [page]" ← modern accessible UI
#   ?page=N / /page/N/ href  ← classic pagination links
#   Load More / Show More    ← XHR-based; we can't infer a next URL
_PAGINATION_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r'<link[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', re.I),
     "rel_next_link"),
    (re.compile(r'<a[^>]+rel=["\']next["\'][^>]+href=["\']([^"\']+)["\']', re.I),
     "rel_next_anchor"),
    (re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*rel=["\']next["\']', re.I),
     "rel_next_anchor"),
    (re.compile(r'<a[^>]+aria-label=["\']\s*Next(?:\s+page)?\s*["\'][^>]+href=["\']([^"\']+)["\']', re.I),
     "aria_next"),
    (re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*aria-label=["\']\s*Next(?:\s+page)?\s*["\']', re.I),
     "aria_next"),
    (re.compile(r'href=["\']([^"\']*(?:/page/\d+|[?&]page=\d+)[^"\']*)["\']', re.I),
     "page_param"),
]

# "Load More" family of XHR-driven pagination triggers. Catches the common
# button labels but stays generic on the surrounding tag — many sites use
# <a class="load-more"> rather than <button>. Some noise is acceptable;
# Move 1 is detection-for-evidence-gathering, not action.
_LOAD_MORE_RE = re.compile(
    r'(?:'
    r'<(?:button|a)[^<]*?>[^<]*(?:Load\s*More|View\s*More|Show\s*More|See\s*More)[^<]*</(?:button|a)>'
    r'|'
    r'class=["\'][^"\']*\bload[\-_]?more\b[^"\']*["\']'
    r')',
    re.I,
)


def jsonld_to_raw_event(ev: dict, source_name: str, source_url: str):
    """Convert a single schema.org Event-shaped JSON-LD dict to a RawEvent.

    Generic — works for any source that emits well-formed JSON-LD without
    site-specific quirks. Tickchak's parser stays separate because it has
    additional logic (Hebrew city translation, missing-URL synthesis).

    Drops events that:
      • lack a parseable startDate
      • are dated in the past
      • are explicitly cancelled / online-only

    Returns ``None`` for those — caller filters with `if ev` to skip.
    """
    from datetime import datetime, date
    from app.services.collectors.base import RawEvent

    if ev.get("eventStatus") == "https://schema.org/EventCancelled":
        return None
    if ev.get("eventAttendanceMode") == "https://schema.org/OnlineEventAttendanceMode":
        return None

    start_str = ev.get("startDate") or ""
    if not start_str:
        return None
    try:
        if "T" in start_str:
            start_dt = datetime.fromisoformat(start_str)
        else:
            start_dt = datetime.combine(date.fromisoformat(start_str[:10]),
                                        datetime.min.time())
    except (ValueError, TypeError):
        return None
    if start_dt.date() < date.today():
        return None

    end_dt = None
    end_str = ev.get("endDate") or ""
    if end_str:
        try:
            end_dt = (datetime.fromisoformat(end_str) if "T" in end_str
                      else datetime.combine(date.fromisoformat(end_str[:10]),
                                             datetime.min.time()))
        except (ValueError, TypeError):
            pass

    location = ev.get("location") or {}
    if isinstance(location, list):
        location = location[0] if location else {}
    address = location.get("address") if isinstance(location, dict) else {}
    if isinstance(address, list):
        address = address[0] if address else {}
    if not isinstance(address, dict):
        address = {}

    venue_name = location.get("name") if isinstance(location, dict) else None
    venue_city = address.get("addressLocality")
    venue_country = address.get("addressCountry")
    if isinstance(venue_country, dict):
        venue_country = venue_country.get("name")
    venue_address = address.get("streetAddress")

    offers = ev.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price = None
    currency = "USD"
    if isinstance(offers, dict):
        low = offers.get("lowPrice") or offers.get("price")
        if low is not None:
            try:
                price = float(str(low).replace(",", "").replace("$", "").strip())
            except (TypeError, ValueError):
                pass
        currency = offers.get("priceCurrency") or currency

    performer = ev.get("performer")
    artist_name = None
    if isinstance(performer, list) and performer:
        performer = performer[0]
    if isinstance(performer, dict):
        artist_name = (performer.get("name") or "").strip() or None
    elif isinstance(performer, str):
        artist_name = performer.strip() or None

    image = ev.get("image")
    if isinstance(image, list):
        image = image[0] if image else None
    if isinstance(image, dict):
        image = image.get("url")
    if not isinstance(image, str):
        image = None

    name = (ev.get("name") or "Untitled").strip()
    purchase_link = ev.get("url") or source_url

    # Stable source_id mirrors the architecture-doc pattern for cross-
    # collector dedup: hash(scrape_source | source_url | name | start_date).
    import hashlib
    seed = f"{source_name}|{source_url}|{name.lower().strip()}|{start_str[:10]}"
    sid = source_name + "_" + hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]

    has_time = "T" in start_str

    return RawEvent(
        name=name,
        start_date=start_dt.date(),
        start_time=start_dt.strftime("%H:%M") if has_time else None,
        end_date=end_dt.date() if end_dt else None,
        end_time=end_dt.strftime("%H:%M") if (end_dt and "T" in end_str) else None,
        artist_name=artist_name,
        description=ev.get("description"),
        price=price,
        price_currency=currency or "USD",
        purchase_link=purchase_link,
        image_url=image,
        venue_name=venue_name,
        venue_address=venue_address,
        venue_city=venue_city,
        venue_country=venue_country,
        source=source_name,
        source_id=sid,
        raw_categories=[],
    )


def detect_pagination(html: str, base_url: str = "") -> dict:
    """Detect pagination affordances. Does not follow them.

    Returns:
        {
          "has_pagination": bool,
          "signal":         str | None,   identifier of which heuristic fired
          "next_page_url":  str | None,   absolute URL when extractable (XHR
                                          patterns like Load More yield None
                                          even though has_pagination=True)
        }

    Empty/null html returns the no-signal shape rather than crashing.
    """
    if not html:
        return {"has_pagination": False, "signal": None, "next_page_url": None}

    for pattern, signal in _PAGINATION_PATTERNS:
        m = pattern.search(html)
        if not m:
            continue
        href = m.group(1)
        full = urljoin(base_url, href) if base_url else href
        return {"has_pagination": True, "signal": signal, "next_page_url": full}

    if _LOAD_MORE_RE.search(html):
        return {"has_pagination": True, "signal": "load_more_button", "next_page_url": None}

    return {"has_pagination": False, "signal": None, "next_page_url": None}
