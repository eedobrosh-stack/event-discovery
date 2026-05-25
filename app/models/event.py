from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Date, Text,
    ForeignKey, DateTime, Index, JSON,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(500), nullable=False)
    artist_name = Column(String(255), nullable=True)
    artist_youtube_channel = Column(String(1000), nullable=True)
    artist_popularity = Column(Integer, nullable=True)   # 1-10 derived from Spotify popularity
    artist_spotify_url = Column(String(500), nullable=True)
    start_date = Column(Date, nullable=False)
    start_time = Column(String(10), nullable=True)
    end_date = Column(Date, nullable=True)
    end_time = Column(String(10), nullable=True)
    purchase_link = Column(String(1000), nullable=True)
    price = Column(Float, nullable=True)
    price_currency = Column(String(10), default="USD")
    venue_id = Column(Integer, ForeignKey("venues.id"), nullable=True)
    venue_name = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)
    image_url = Column(String(1000), nullable=True)
    is_online = Column(Boolean, default=False)
    scrape_source = Column(String(100), nullable=True)
    source_id = Column(String(255), nullable=True)
    # Set when this row was extracted by Cadence A (Route 1, LLM extractor)
    # so we can trace events back to the LLMSource that produced them, and
    # via LLMSource.spotify_artist_id back to the Spotify-funnel query that
    # discovered the source. NULL for everything Route 2 collectors write.
    llm_source_id = Column(Integer, ForeignKey("llm_sources.id"), nullable=True)
    # ── Sports fields ────────────────────────────────────────────────────────
    sport      = Column(String(50),  nullable=True)   # "Soccer", "NFL", "AFL" …
    home_team  = Column(String(200), nullable=True)
    away_team  = Column(String(200), nullable=True)
    tv_channels = Column(JSON, nullable=True)          # [{channel, market, country, type}]
    # Named sport competition this event belongs to. Examples:
    # team-sport league labels ("FIFA World Cup", "NBA", "Premier League"),
    # tennis Grand Slams ("Wimbledon", "US Open"), etc. Set by sport
    # collectors at write time and used as the anchor for the Tournament
    # autocomplete chip (top-priority chip kind — clicking it filters to
    # every event with the same tournament value). NULL for non-sport
    # events and for sport rows from collectors that don't yet populate
    # it. Indexed for fast equality filter from /api/events.
    tournament = Column(String(200), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    venue = relationship("Venue", back_populates="events")
    event_types = relationship(
        "EventType", secondary="event_event_types", back_populates="events"
    )

    __table_args__ = (
        Index("ix_events_start", "start_date", "start_time"),
        Index("ix_events_venue", "venue_id"),
        Index("ix_events_dedup", "scrape_source", "source_id", unique=True),
        Index("ix_events_tournament", "tournament"),
        Index("ix_events_llm_source", "llm_source_id"),
    )
