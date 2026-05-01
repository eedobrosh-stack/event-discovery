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
from sqlalchemy import Column, Integer, String, DateTime, Index, ForeignKey
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

    __table_args__ = (
        Index("ix_artist_genre_normalized", "normalized_name"),
        Index("ix_artist_genre_primary", "primary_genre"),
    )
