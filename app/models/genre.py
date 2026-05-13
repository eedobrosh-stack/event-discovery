"""Genre taxonomy + artist classification (populated from Gemini batch output).

Two tables:

  genre_taxonomy   — the canonical parent → sub-genre map (e.g. "Rock" → "Hard Rock").
                     Sub-genres are unique across the whole table so a single sub-genre
                     can never live under two parents (Gemini's first pass put "Opera"
                     under both Classical and Theatre — schema enforces uniqueness).

  artist_genre     — Gemini's per-artist classification:
                     primary_genre + up to two secondaries + confidence.
                     primary_genre may be the sentinel "UNKNOWN" with confidence="low"
                     when Gemini doesn't recognise the artist.

Why a separate table from `performers`:
  `performers.genres` already stores raw genre tags from MusicBrainz/Spotify ('jazz',
  'bebop', 'blues') — that's a flat list scraped from external APIs and meant for
  display. `artist_genre` is *our* curated 2-level taxonomy meant for query expansion
  (search "Rock" → match all artists whose primary/secondary is any sub-genre of Rock).
  Mixing the two would conflate "what an external service said about this artist" with
  "where this artist sits in our user-facing taxonomy".
"""
from sqlalchemy import Column, Integer, String, Float, DateTime, Index, ForeignKey, UniqueConstraint
from sqlalchemy.sql import func

from app.database import Base


class GenreTaxonomy(Base):
    """Canonical parent → sub-genre map. One row per sub-genre."""
    __tablename__ = "genre_taxonomy"

    # Sub-genre is the natural key — Gemini's prompt mandates uniqueness.
    sub_genre = Column(String(100), primary_key=True)
    parent_genre = Column(String(100), nullable=False)

    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_genre_taxonomy_parent", "parent_genre"),
    )


class ArtistGenre(Base):
    """Per-artist classification from Gemini.

    `artist_name` matches `events.artist_name` exactly (same string the scrapers
    write). `normalized_name` is the lowercase/trimmed version for case-insensitive
    joins, mirroring the Performer pattern.
    """
    __tablename__ = "artist_genre"

    id = Column(Integer, primary_key=True, autoincrement=True)

    artist_name = Column(String(500), nullable=False)
    normalized_name = Column(String(500), nullable=False, unique=True)

    # Sub-genre name from genre_taxonomy.sub_genre, OR the literal "UNKNOWN".
    # Not a hard FK because we want to keep the row even if a taxonomy entry
    # is later renamed; the validator catches drift at ingestion time.
    primary_genre = Column(String(100), nullable=True)
    secondary_1   = Column(String(100), nullable=True)
    secondary_2   = Column(String(100), nullable=True)

    # "high" / "medium" / "low" — Gemini's self-reported confidence.
    # Mandated to be "low" whenever primary_genre = "UNKNOWN".
    confidence = Column(String(10), nullable=True)

    source = Column(String(50), default="gemini")     # gemini / manual / ...
    classified_at = Column(DateTime, server_default=func.now())

    # Number of times this artist has been run through the classifier.
    # Bumped on every attempt regardless of outcome (UNKNOWN counts as an
    # attempt). Used by the auto-classification cron to park artists that
    # the classifier can't recognise even with Brave context: after
    # ``MAX_CLASSIFICATION_ATTEMPTS`` failed runs (UNKNOWN result), the
    # row stays in the table but is excluded from the retry pool. Manual
    # re-classification (or a new prompt) can override by direct edit.
    classification_attempts = Column(Integer, nullable=False, server_default="0")

    # Number of Brave search results returned when the classifier
    # queried this artist (capped at 20, Brave's max page size).
    # Captures a "web footprint" signal we already pay for during
    # classification — popular artists max out at 20, obscure artists
    # return 0-5. Used as one input to the internal-derived
    # popularity score (see scripts/recompute_popularity.py once
    # implemented). NULL when the artist hasn't been Brave-searched
    # yet, or pre-existed the column addition.
    brave_total_results = Column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_artist_genre_normalized", "normalized_name"),
        Index("ix_artist_genre_primary", "primary_genre"),
    )


class GenrePopularityThresholds(Base):
    """Per-parent-genre percentile thresholds over Performer.derived_popularity.

    Computed weekly by scripts/recompute_genre_thresholds.py from the
    current distribution of derived_popularity per parent genre. The
    API uses these to convert an artist's raw 0-100 score into a 1-5
    star rating *relative to their genre*: a 75 in Jazz is a top-tier
    jazz artist, while a 75 in Pop is mid-tier — they shouldn't render
    the same star count.

    One row per parent genre with at least N classified+scored
    performers (statistical floor — see MIN_N in
    scripts/recompute_genre_thresholds.py). Genres below the floor
    have no row, signalling to the API that stars are not yet
    displayable for that genre.
    """
    __tablename__ = "genre_popularity_thresholds"

    parent_genre = Column(String(100), primary_key=True)

    # 4 percentile boundaries against derived_popularity (0..100).
    # Mapping: score >= p80 → 5★, >= p60 → 4★, >= p40 → 3★, >= p20 → 2★, else 1★ (when displayable at all).
    p20 = Column(Integer, nullable=False)
    p40 = Column(Integer, nullable=False)
    p60 = Column(Integer, nullable=False)
    p80 = Column(Integer, nullable=False)

    # Sample size used to compute the percentiles. Surfaced for
    # debugging / audit; not used in star lookup.
    n_performers = Column(Integer, nullable=False)

    computed_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class ArtistRelated(Base):
    """Pre-computed top-N peer artists for each classified anchor artist.

    Populated by ``scripts/compute_artist_related.py`` from the full
    ``artist_genre`` set: each anchor's "bag" of {primary, secondary_1,
    secondary_2} is scored against every other artist via weighted-overlap
    plus a tightness penalty. Ties at the top score tier are broken by
    ``Performer.derived_popularity`` (DESC, NULLS LAST), with row id as
    final deterministic fallback. Top 20 per anchor are kept.

    Denormalised on purpose: ``peer_artist_name`` is stored directly so
    the read-time API (``GET /api/artists/related?name=X``) is a single
    indexed lookup with no join back to ``artist_genre``. Cost is a
    rename hazard (~free in practice — artists don't get renamed) in
    exchange for cheap reads.

    Recompute pattern is delete-by-anchor then bulk insert; the (anchor,
    rank) unique constraint guards against accidental duplicates.
    """
    __tablename__ = "artist_related"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Both join keys to artist_genre.normalized_name (no hard FK so the row
    # survives a classifier rename of either side; recompute fixes drift).
    anchor_normalized_name = Column(String(500), nullable=False)
    peer_normalized_name   = Column(String(500), nullable=False)

    # Denormalised display name (mirrors artist_genre.artist_name at the
    # peer side at compute time).
    peer_artist_name = Column(String(500), nullable=False)

    rank  = Column(Integer, nullable=False)   # 1..20, lower is better
    score = Column(Float,   nullable=False)   # weighted-overlap score after tightness penalty

    # Snapshot of Performer.derived_popularity at compute time; serves
    # both as the tie-break key (kept here for audit) and as a hint the
    # frontend can use (e.g. show top-5 stars on peer rows). NULL when
    # the peer has no Performer row or no derived score yet.
    peer_popularity = Column(Integer, nullable=True)

    computed_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_artist_related_anchor", "anchor_normalized_name"),
        UniqueConstraint("anchor_normalized_name", "rank", name="uq_anchor_rank"),
    )
