from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.sql import func

from app.database import Base


class PendingVenue(Base):
    """Tracks venue URLs submitted via the UI that need scraping."""
    __tablename__ = "pending_venues"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String(2000), nullable=False)
    venue_name = Column(String(500), nullable=True)
    city_name = Column(String(200), nullable=True)

    # "pending" → not yet attempted by agent
    # "success" → events saved
    # "partial" → some events found but 0 saved (already existed)
    # "failed"  → 0 events found, needs custom parser
    # "unsupported" → agent gave up (JS-only site, etc.)
    status = Column(String(20), nullable=False, default="pending")

    events_found = Column(Integer, nullable=True)
    events_saved = Column(Integer, nullable=True)
    agent_notes = Column(Text, nullable=True)  # what the agent tried / found

    created_at = Column(DateTime, server_default=func.now())
    handled_at = Column(DateTime, nullable=True)
