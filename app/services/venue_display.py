"""One venue, one name — on every surface.

Events carry their own ``venue_name`` column: the raw string the
collector scraped ("מועדון שבלול תל אביב", "Shablul Jazz- שבלול ג'אז",
"שבלול ג'אז"). Showing that string made one venue look like three even
after the venue rows were merged, so display always reads the linked
``Venue.name`` instead, and search reads the name AND every spelling in
``venue_aliases`` (the dedupe passes write each merged-away spelling
there). Typing any alternate finds the venue; the results always show
the one canonical name.

The same goes for the city line: ``venues.physical_city`` holds every
spelling collectors emit ("Tel Aviv-Yafo", "תל אביב-יפו", "נמל יפו"),
so Israeli venues display their canonical city.
"""
from __future__ import annotations

from sqlalchemy import or_, select

from app.models import Venue
from app.models.venue_alias import VenueAlias
from app.services.il_places import canon_place


def display_venue_name(event) -> str | None:
    venue = getattr(event, "venue", None)
    if venue is not None and venue.name:
        return venue.name
    return event.venue_name


def display_venue_city(venue) -> str | None:
    if venue is None:
        return None
    city = getattr(venue, "city", None)
    country = venue.physical_country or (city.country if city is not None else None)
    if country == "Israel":
        return canon_place(venue.physical_city, city.name if city is not None else None)
    return venue.physical_city


def venue_name_clause(match, term: str):
    """``match(column, term)`` applied to Venue.name OR any alias of the
    venue. ``match`` is the caller's matcher (name_match_ilike, or a plain
    ilike in the export path) so each surface keeps its own semantics."""
    alias_ids = select(VenueAlias.venue_id).where(match(VenueAlias.alias, term))
    return or_(match(Venue.name, term), Venue.id.in_(alias_ids))
