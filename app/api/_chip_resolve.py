"""Typed text that IS a chip behaves like the chip.

Users often type "jazz" and press search without clicking the Jazz chip.
The text then went down the loose ``type_search`` word match (event
names / artist names / venue names containing "jazz") and missed every
event whose artist is *classified* Jazz but never says so. Rule
(2026-09-24): when a typed term equals — case-insensitively — the label
of a chip the autocomplete would offer for it, the search runs as if
that chip were selected.

The chip is picked by the autocomplete itself (``get_suggestions``), so
typed and clicked searches can never disagree on priority: Tournament >
Sub-genre / Genre > Artist > … . Only chip kinds whose filter differs
from the text match are rewritten:

    genre       → genres        ("opera" → Classical, like the chip)
    theme       → themes
    tournament  → tournaments
    performer   → artist_exact
    city name   → city_ids      (only when no location is selected;
                                 "תל אביב" → Tel Aviv, via city aliases;
                                 ranks above an artist of the same name)

Formats, categories, venues and event names already search the same way
typed or clicked, so they stay in ``type_search``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

_KIND_TO_FILTER = {
    "genre": "genres",
    "theme": "themes",
    "tournament": "tournaments",
    "performer": "artist_exact",
}


@dataclass
class Resolved:
    type_search: list[str] = field(default_factory=list)
    genres: list[str] = field(default_factory=list)
    themes: list[str] = field(default_factory=list)
    tournaments: list[str] = field(default_factory=list)
    artist_exact: list[str] = field(default_factory=list)
    city_ids: list[str] = field(default_factory=list)


def _split(v: str | None) -> list[str]:
    return [t.strip() for t in (v or "").split(",") if t.strip()]


def _chip_label(item: dict) -> str:
    # Venue labels carry " — City"; the chip's own text is before it.
    return (item.get("label") or item.get("value") or "").split(" — ")[0].strip()


def _match_chip(db, term: str) -> dict | None:
    from app.api.suggestions import get_suggestions  # lazy: avoid import cycle
    key = term.strip().lower()
    try:
        items = get_suggestions(q=term, limit=30, db=db)
    except Exception:  # never let the resolver break a search
        log.exception("chip resolve failed for %r", term)
        return None
    for item in items:
        if _chip_label(item).lower() == key or (item.get("value") or "").strip().lower() == key:
            return item
    return None


def _match_city(db, term: str) -> int | None:
    from app.api import cities as cities_mod  # lazy: avoid import cycle
    rows = cities_mod._cache or cities_mod._build_city_list(db)
    key = term.strip().lower()
    for c in rows:
        names = [c.name, *(getattr(c, "aliases", None) or [])]
        if any((n or "").strip().lower() == key for n in names):
            return c.id
    return None


def resolve_typed_terms(db, *, type_search: str | None, genres: str | None,
                        themes: str | None, tournaments: str | None,
                        artist_exact: str | None, city_ids: str | None,
                        country: str | None) -> dict:
    """Return the filter params with chip-equal typed terms moved into
    their chip filter. Terms that match no chip stay in type_search."""
    out = Resolved(genres=_split(genres), themes=_split(themes),
                   tournaments=_split(tournaments), artist_exact=_split(artist_exact),
                   city_ids=_split(city_ids))
    location_set = bool(out.city_ids or country)
    for term in _split(type_search):
        item = _match_chip(db, term)
        # A city name beats an artist of the same name ("Paris", "תל
        # אביב"), but not a genre / theme / tournament chip.
        if (item is None or item["kind"] == "performer") and not location_set:
            cid = _match_city(db, term)
            if cid is not None:
                out.city_ids.append(str(cid))
                location_set = True
                continue
        target = _KIND_TO_FILTER.get(item["kind"]) if item else None
        if target:
            bucket = getattr(out, target)
            if item["value"] not in bucket:
                bucket.append(item["value"])
            continue
        out.type_search.append(term)

    def join(xs: list[str]) -> str | None:
        return ",".join(xs) if xs else None

    return {
        "type_search": join(out.type_search), "genres": join(out.genres),
        "themes": join(out.themes), "tournaments": join(out.tournaments),
        "artist_exact": join(out.artist_exact), "city_ids": join(out.city_ids),
    }
