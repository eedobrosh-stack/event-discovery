from sqlalchemy import Column, Integer, String, Float, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship

from app.database import Base


class City(Base):
    __tablename__ = "cities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    country = Column(String(100), nullable=False)
    state = Column(String(100), nullable=True)
    timezone = Column(String(50), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    # ── Consolidation links ──────────────────────────────────────────────
    # When set, this city row is a duplicate (alias) of another — same
    # physical city under a different name/label (e.g. Tel Aviv-Yafo
    # alias of Tel Aviv, City Of London alias of London, New York City
    # without state alias of New York with state=NY). Autocomplete
    # hides aliased rows; the events query rewrites a city_id selection
    # to include the canonical row + all rows aliased to it. NULL
    # means this row IS the canonical version (or hasn't been touched
    # by the consolidation pass yet).
    canonical_city_id = Column(Integer, ForeignKey("cities.id"), nullable=True)
    # When set, this city is a sub-area of a larger city (e.g. North
    # London inside London, East New York inside New York). Distinct
    # from canonical_city_id: sub-areas are still real, selectable
    # cities. The events query expands "London" to include all
    # descendants via parent_city_id, but selecting "North London"
    # directly still filters to just that sub-area.
    parent_city_id = Column(Integer, ForeignKey("cities.id"), nullable=True)

    venues = relationship("Venue", back_populates="city")

    __table_args__ = (
        UniqueConstraint("name", "country", "state", name="uq_city_name_country_state"),
        Index("ix_cities_canonical", "canonical_city_id"),
        Index("ix_cities_parent", "parent_city_id"),
    )
