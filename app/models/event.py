from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Date, Text,
    ForeignKey, DateTime, Index,
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
    )
