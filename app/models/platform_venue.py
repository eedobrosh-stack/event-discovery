from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy.sql import func

from app.database import Base


class PlatformVenue(Base):
    """A venue whose event calendar is powered by a known ticketing platform.

    Adding a new venue = paste URL → detect platform → save record → events
    auto-scrape daily with no code changes required.
    """
    __tablename__ = "platform_venues"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    city_id = Column(Integer, ForeignKey("cities.id"), nullable=True)

    # Which platform powers this venue's calendar
    platform = Column(String(50), nullable=False)    # venuepilot | dice | eventbrite | resident_advisor
    platform_id = Column(String(100), nullable=True)  # platform-specific account/venue ID

    website_url = Column(String(500), nullable=True)
    address = Column(String(500), nullable=True)

    # When False, the daily job skips this venue
    active = Column(Boolean, default=True, nullable=False)

    # Optional event-type override applied to all events scraped from this venue
    default_event_type_id = Column(Integer, ForeignKey("event_types.id"), nullable=True)

    last_scraped_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
