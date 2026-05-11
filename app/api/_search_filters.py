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


def _single_word_match(col, term: str):
    """Length-aware single-word match.

    Long terms (≥4 chars) use word-start so plurals/inflections still hit.
    Short terms (<4 chars) use strict whole-word so 2-3 char queries don't
    flood with substring noise.
    """
    if len(term) >= _WHOLE_WORD_BELOW:
        return word_start_ilike(col, term)
    return word_boundary_ilike(col, term)


def name_match_ilike(col, term: str):
    """Length-aware, word-aware match.

    Single-word term: word-start for long terms, whole-word for short
    (see _single_word_match).

    Multi-word term: every word in the query must hit, in any order. Each
    word matches as a word-start (regardless of its length) — the
    co-required other words act as the noise filter, so we don't need
    the strict whole-word rule on short tokens. This is what makes
    "Tel A" match "Hapoel IBI Tel Aviv" (both "Tel" and "A" hit
    word-starts in the column) while still filtering out unrelated
    rows that happen to contain just one of the two tokens.
    """
    parts = term.split()
    if not parts:
        # Empty input — no filter; safe truthy expression so SQL stays
        # well-formed when this is OR'd / AND'd with other predicates.
        return col.ilike("%")
    if len(parts) == 1:
        return _single_word_match(col, parts[0])
    from sqlalchemy import and_
    # Per-word word-start match, AND'd. Order-independent by construction
    # (AND is commutative). Short tokens use the same word-start rule as
    # long ones in this multi-word path — the other tokens contain the
    # noise, so a 1-2 char token like "A" won't flood here even though it
    # would alone.
    return and_(*(word_start_ilike(col, p) for p in parts))


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

    # Cross-category secondary-tag leak guard.
    # 2026-05-12 bug: Stand-Up comedian "שחר חסון" (primary='Stand-Up',
    # parent=Comedy) had secondary_1='Contemporary Pop' assigned by
    # the brave-bridge classifier. Without this gate, filter "Pop" → he
    # surfaces because the secondary matches Contemporary Pop, even
    # though his primary lives entirely outside Music.
    #
    # Rule: when the user filters by ANY Music parent, only count
    # secondaries for artists whose primary is also under a Music
    # parent. The set of music-side sub-genres is the union of all
    # sub-genres parented by Rock/Pop/Jazz/etc. — computed once below
    # and used as the gate. Non-music filters (Comedy/Theatre/...)
    # keep the original open-secondary behavior because the asymmetry
    # only exists going INTO Music (a comedian never gets accidentally
    # tagged as a comedian).
    MUSIC_PARENTS = {
        "Rock", "Pop", "Jazz", "Hip-Hop", "Latin", "Country",
        "Classical", "Electronic", "World",
    }
    requesting_music = any(g in MUSIC_PARENTS for g in genre_list)

    primary_q = (
        db.query(ArtistGenre.normalized_name)
        .filter(ArtistGenre.primary_genre.in_(sub_genres))
    )
    if requesting_music:
        # Secondaries only count when primary's parent is also Music.
        music_subgenres_subq = (
            db.query(GenreTaxonomy.sub_genre)
            .filter(GenreTaxonomy.parent_genre.in_(MUSIC_PARENTS))
            .scalar_subquery()
        )
        secondary_q = (
            db.query(ArtistGenre.normalized_name)
            .filter(
                or_(
                    ArtistGenre.secondary_1.in_(sub_genres),
                    ArtistGenre.secondary_2.in_(sub_genres),
                ),
                ArtistGenre.primary_genre.in_(music_subgenres_subq),
            )
        )
    else:
        # Non-music parent — keep original wide secondary match.
        secondary_q = (
            db.query(ArtistGenre.normalized_name)
            .filter(or_(
                ArtistGenre.secondary_1.in_(sub_genres),
                ArtistGenre.secondary_2.in_(sub_genres),
            ))
        )
    artist_norms = [r[0] for r in primary_q.union(secondary_q).all()]
    return artist_norms


# Format-string fallback for the Genre filter.
#
# Problem: Genre=Jazz in Tel Aviv returns 2 events (only artists tagged with
# Jazz sub-genres), even though the catalog has many "Jazz Concert" events
# whose artist isn't classified (or is null). We complement the artist-genre
# match with an event_type-name match for events whose artist is unclassified.
#
# Each parent genre maps to a (category, name_keywords) pair:
#   - category restricts the event_type to a sane bucket (Music for music
#     genres, Comedy for Comedy) so we don't pick up "Rock Climbing
#     Competitions" (Outdoor) under genre=Rock.
#   - name_keywords match against EventType.name as a substring (lowercased).
#     None means category match alone is sufficient (e.g. Comedy).
#
# Maintained by hand because the parent-genre taxonomy is hand-curated;
# auto-deriving from name overlap gave false positives like Classical Ballet
# (Dance) sneaking in under genre=Classical.
_GENRE_FORMAT_MATCH = {
    "Rock":        ("Music", ["rock"]),
    "Pop":         ("Music", ["pop"]),
    "Jazz":        ("Music", ["jazz"]),
    "Hip-Hop":     ("Music", ["hip-hop", "hip hop", "rap"]),
    "Latin":       ("Music", ["latin"]),
    "Country":     ("Music", ["country"]),
    "Classical":   ("Music", ["classical", "symphony", "opera", "concerto",
                              "recital", "baroque", "renaissance", "string quartet",
                              "piano trio", "woodwind"]),
    "Electronic":  ("Music", ["electronic", "dj set"]),
    "World":       ("Music", ["world", "reggae", "gospel"]),
    "Comedy":      ("Comedy", None),
    # Theatre / Family / Spoken Word: skip for now — no clean event_type
    # category mapping yet, and the user's pain is music-genre coverage.
}


def build_genre_format_event_type_subquery(db: Session, genres: Optional[str]):
    """Subquery returning event IDs whose event_type matches any of the
    requested parent genres by category + name. Used as the fallback path
    when the artist-genre filter doesn't catch events whose artist isn't
    classified.

    Returns None if `genres` is empty or no genres have a format mapping.
    """
    if not genres:
        return None
    from sqlalchemy import select, and_, func
    from app.models import Event, EventType, event_event_types

    genre_list = [g.strip() for g in genres.split(",") if g.strip()]
    conditions = []
    for g in genre_list:
        spec = _GENRE_FORMAT_MATCH.get(g)
        if spec is None:
            continue
        cat, keywords = spec
        cond = EventType.category == cat
        if keywords:
            name_match = or_(*[
                func.lower(EventType.name).like(f"%{kw.lower()}%")
                for kw in keywords
            ])
            cond = and_(cond, name_match)
        conditions.append(cond)
    if not conditions:
        return None
    return (
        select(event_event_types.c.event_id)
        .join(EventType, EventType.id == event_event_types.c.event_type_id)
        .where(or_(*conditions))
        .scalar_subquery()
    )


# Venue-name fallback for the Genre filter.
#
# Problem: even after the artist-genre and format-fallback matches, events
# at a venue whose IDENTITY is the genre (e.g. "Shablul Jazz Club", "Comedy
# Cellar", "Royal Opera House") miss the filter when:
#   - the artist isn't classified, AND
#   - the per-event event_type tag is generic ("Concert", "Performance") or
#     a sibling-genre tag (we observed Hebrew-Shablul events tagged "Pop
#     Concert" by their collector, even though they're at a jazz club).
#
# The venue's name is a strong genre signal that the per-event metadata
# can drown out. This subquery surfaces events whose venue name contains
# any of the registered venue-name keywords for the requested parent
# genre.
#
# Conservative keyword list: only genres with unambiguous venue-name
# vocabulary. "Rock" / "Pop" / "Country" are deliberately omitted — they
# alias too easily ("Rock Climbing Centre", "Pop-up Theatre", "Country
# Club"). Hebrew variants included because Israeli venues mix scripts.
_GENRE_VENUE_NAME_MATCH = {
    # Latin English form + Hebrew variants (apostrophe / geresh / no-mark)
    "Jazz":      ["jazz", "ג'אז", "ג׳אז", "גאז"],
    # Comedy clubs are universally named with the word "comedy"
    "Comedy":    ["comedy", "סטנד אפ", "סטנדאפ", "קומדי"],
    # Classical venues are named after the institution form, not "classical"
    "Classical": ["philharmonic", "opera house", "symphony hall",
                  "פילהרמונית", "סימפונית", "אופרה"],
}


def build_genre_venue_name_subquery(db: Session, genres: Optional[str]):
    """Subquery returning event IDs at venues whose ``Venue.name`` contains
    any keyword registered for the requested parent genres. The third leg
    of the genre filter; OR'd alongside artist and format matches.

    Returns None if `genres` is empty or no requested genre has a venue
    keyword set."""
    if not genres:
        return None
    from sqlalchemy import select, func
    from app.models import Event, Venue

    genre_list = [g.strip() for g in genres.split(",") if g.strip()]
    keywords: list[str] = []
    for g in genre_list:
        keywords.extend(_GENRE_VENUE_NAME_MATCH.get(g, []))
    if not keywords:
        return None
    return (
        select(Event.id)
        .join(Venue, Event.venue_id == Venue.id)
        .where(or_(*[
            func.lower(Venue.name).like(f"%{kw.lower()}%") for kw in keywords
        ]))
        .scalar_subquery()
    )


def build_classified_artists_subquery(db: Session):
    """Subquery returning normalized_name of artists with a non-UNKNOWN
    classification — i.e. artists whose Genre column would NOT be null
    in the results table. Used to identify events that should fall back
    to the format match (artist null/unclassified)."""
    from sqlalchemy import select
    from app.models.genre import ArtistGenre
    return (
        select(ArtistGenre.normalized_name)
        .where(
            ArtistGenre.primary_genre.isnot(None),
            ArtistGenre.primary_genre != "UNKNOWN",
        )
        .scalar_subquery()
    )
