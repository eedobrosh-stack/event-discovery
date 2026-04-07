from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class Venue(Base):
    __tablename__ = "venues"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    city_id = Column(Integer, ForeignKey("cities.id"), nullable=False)
    timezone = Column(String(50), nullable=True)
    website_url = Column(String(500), nullable=True)
    street_address = Column(String(500), nullable=True)
    physical_city = Column(String(255), nullable=True)
    physical_country = Column(String(100), nullable=True)
    venue_type = Column(String(100), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    phone = Column(String(50), nullable=True)
    # Explicit event-type override: when set, all events at this venue get this
    # type (unless the artist is known in the Performer table, which takes priority).
    default_event_type_id = Column(Integer, ForeignKey("event_types.id"), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    city = relationship("City", back_populates="venues")
    events = relationship("Event", back_populates="venue")
    default_event_type = relationship("EventType", foreign_keys=[default_event_type_id])
