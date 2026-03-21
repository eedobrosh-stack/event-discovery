"""Create tables and seed initial data."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import Base, engine, SessionLocal
from app.models import City, Venue, Event, EventType, event_event_types
from app.seed.cities import CITIES
from app.seed.event_types import EVENT_TYPES


def seed():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        # Seed cities
        for city_data in CITIES:
            existing = db.query(City).filter_by(
                name=city_data["name"], country=city_data["country"]
            ).first()
            if not existing:
                db.add(City(**city_data))
        db.commit()
        print(f"Seeded {len(CITIES)} cities")

        # Seed event types
        for et_data in EVENT_TYPES:
            existing = db.query(EventType).filter_by(name=et_data["name"]).first()
            if not existing:
                db.add(EventType(**et_data))
        db.commit()
        print(f"Seeded {len(EVENT_TYPES)} event types")

    finally:
        db.close()


if __name__ == "__main__":
    seed()
    print("Database seeded successfully!")
