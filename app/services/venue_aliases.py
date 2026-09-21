from __future__ import annotations

import re
import unicodedata

from sqlalchemy import text

from app.models import Venue, VenueAlias


def normalize_venue_name(value: str | None) -> str:
    """Case/punctuation/diacritic-insensitive venue key, preserving scripts."""
    if not value:
        return ""
    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c)).lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^\w\s]+", " ", value, flags=re.UNICODE)
    return re.sub(r"\s+", " ", value).strip()


def find_venue_alias(db, city_id: int, name: str | None) -> Venue | None:
    key = normalize_venue_name(name)
    if not key:
        return None
    alias = (
        db.query(VenueAlias)
        .filter(VenueAlias.city_id == city_id, VenueAlias.normalized_alias == key)
        .first()
    )
    return db.get(Venue, alias.venue_id) if alias else None


def ensure_venue_alias(db, venue: Venue, alias: str | None, *, source: str = "ingest", confidence: float = 1.0) -> None:
    key = normalize_venue_name(alias)
    if not key:
        return
    existing = (
        db.query(VenueAlias)
        .filter(VenueAlias.city_id == venue.city_id, VenueAlias.normalized_alias == key)
        .first()
    )
    if existing:
        if existing.venue_id != venue.id and confidence > existing.confidence:
            existing.venue_id = venue.id
            existing.source = source
            existing.confidence = confidence
        return
    db.add(VenueAlias(
        venue_id=venue.id,
        city_id=venue.city_id,
        alias=alias.strip(),
        normalized_alias=key,
        source=source,
        confidence=confidence,
    ))
