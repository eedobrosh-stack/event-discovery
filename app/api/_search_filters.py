"""Shared SQL search-filter helpers.

The naive `column.ilike(f"%{term}%")` matches anywhere in a column, which
produces noisy hits like "testing" / "vesting" / "investing" when a user
searches for "sting". These helpers tighten matching so:

  * Long terms (≥4 chars) match at word starts only — "Stinging" hits,
    "testing" misses. This is the natural interpretation of a search:
    the user wants their term to begin a word, not embed inside one.

  * Short terms (<4 chars) require a strict whole-word match — "JAX"
    hits "JAX Conference" but not "Ajax Amsterdam". Without this short
    queries would be flooded with substring hits.

ILIKE patterns rather than regex so SQLite handles it natively without a
REGEXP extension. Word boundaries are approximated with spaces — column
text is generally well-tokenized event names, venue names, and artist
names, so a space-based boundary catches the vast majority of cases.
Punctuation-adjacent matches (e.g. "Sting!") are not perfectly handled
but in practice such names also appear with a space variant elsewhere
in the data set.
"""
from typing import Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

# Threshold below which we require a strict whole-word match. Longer terms
# (4+ chars) are forgiving — they match the start of any word so plurals
# and inflections ("Stinging", "Stings") still hit.
_WHOLE_WORD_BELOW = 4


def word_boundary_ilike(col, term: str):
    """Match `term` only as a complete word in `col`.

    Patterns:
      - "term"          (exact whole-column match)
      - "term %"        (at start)
      - "% term %"      (in middle)
      - "% term"        (at end)
    """
    return or_(
        col.ilike(term),
        col.ilike(f"{term} %"),
        col.ilike(f"% {term} %"),
        col.ilike(f"% {term}"),
    )


def word_start_ilike(col, term: str):
    """Match `term` at the start of any word in `col`.

    A "word start" is either the start of the column or immediately after
    a space. Trailing letters are allowed, so "sting" matches "Stinging"
    but not "testing".

    Patterns:
      - "term%"         (column starts with term)
      - "% term%"       (any word starts with term)
    """
    return or_(
        col.ilike(f"{term}%"),
        col.ilike(f"% {term}%"),
    )


def name_match_ilike(col, term: str):
    """Length-aware match: word-start for long terms, whole-word for short.

    Long terms (≥4 chars) use word-start so plurals/inflections still hit.
    Short terms (<4 chars) use strict whole-word so 2-3 char queries don't
    flood with substring noise.
    """
    if len(term) >= _WHOLE_WORD_BELOW:
        return word_start_ilike(col, term)
    return word_boundary_ilike(col, term)


def resolve_genre_artist_names(db: Session, genres: Optional[str]) -> Optional[list[str]]:
    """Expand a comma-separated list of parent genre names to the lowercase
    artist names tagged with any of their sub-genres.

    Returns:
      - None if `genres` is empty/None — caller should not apply a genre filter.
      - [] if the genres are valid parents but no tagged artists exist under
        them — caller should treat this as "no events match" (filter False).
      - list[str] of lowercased artist names otherwise.

    Single source of truth so /api/events and /api/export stay consistent.
    """
    if not genres:
        return None

    # Local imports keep this helper free of model coupling at import time —
    # genre.py is registered late and importing at module top would create
    # a small circular dance with app.models.__init__.
    from app.models.genre import GenreTaxonomy, ArtistGenre

    genre_list = [g.strip() for g in genres.split(",") if g.strip()]
    if not genre_list:
        return None

    sub_genres = [
        row[0] for row in (
            db.query(GenreTaxonomy.sub_genre)
            .filter(GenreTaxonomy.parent_genre.in_(genre_list))
            .all()
        )
    ]
    if not sub_genres:
        return []

    artist_norms = [
        row[0] for row in (
            db.query(ArtistGenre.normalized_name)
            .filter(or_(
                ArtistGenre.primary_genre.in_(sub_genres),
                ArtistGenre.secondary_1.in_(sub_genres),
                ArtistGenre.secondary_2.in_(sub_genres),
            ))
            .all()
        )
    ]
    return artist_norms
