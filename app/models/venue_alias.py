from sqlalchemy import Column, DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base


class VenueAlias(Base):
    """A city-scoped alternate spelling for a canonical venue."""

    __tablename__ = "venue_aliases"
    __table_args__ = (
        UniqueConstraint("city_id", "normalized_alias", name="uq_venue_alias_city_key"),
        Index("ix_venue_alias_venue", "venue_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    venue_id = Column(Integer, ForeignKey("venues.id", ondelete="CASCADE"), nullable=False)
    city_id = Column(Integer, ForeignKey("cities.id", ondelete="CASCADE"), nullable=False)
    alias = Column(String(255), nullable=False)
    normalized_alias = Column(String(255), nullable=False)
    source = Column(String(100), nullable=True)
    confidence = Column(Float, nullable=False, default=1.0)
    created_at = Column(DateTime, server_default=func.now())

    venue = relationship("Venue")
