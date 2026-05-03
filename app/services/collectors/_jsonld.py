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
from typing import Iterator


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
