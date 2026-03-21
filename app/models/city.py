from sqlalchemy import Column, Integer, String, Float, UniqueConstraint
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

    venues = relationship("Venue", back_populates="city")

    __table_args__ = (
        UniqueConstraint("name", "country", "state", name="uq_city_name_country_state"),
    )
