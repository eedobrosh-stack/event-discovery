from sqlalchemy import Column, Integer, String, Float, Text, DateTime, Index
from sqlalchemy.sql import func

from app.database import Base


class Performer(Base):
    """
    Cached artist/performer → category + event type mapping.
    Populated via MusicBrainz lookups in scripts/enrich_performers.py.
    """
    __tablename__ = "performers"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # The artist name exactly as it appears in events.artist_name
    name = Column(String(500), nullable=False)
    # Lowercase/stripped version for fast lookups
    normalized_name = Column(String(500), nullable=False, unique=True)

    # Our taxonomy
    category = Column(String(100), nullable=True)        # e.g. "Music", "Comedy"
    event_type_name = Column(String(255), nullable=True) # e.g. "Jazz Concert"

    # Raw data from lookup source
    genres = Column(Text, nullable=True)   # JSON array, e.g. '["jazz","bebop","blues"]'
    mb_id = Column(String(50), nullable=True)   # MusicBrainz artist MBID
    mb_type = Column(String(50), nullable=True) # MusicBrainz type: Person/Group/Orchestra/Choir

    # Lookup metadata
    source = Column(String(50), default="musicbrainz")  # musicbrainz / manual / fallback
    confidence = Column(Float, default=1.0)  # 0.0–1.0
    looked_up_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_performers_normalized", "normalized_name"),
    )
