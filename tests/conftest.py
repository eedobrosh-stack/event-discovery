"""Shared fixtures: an in-memory SQLite session with the full schema and a
City row, for tests that drive CollectorRegistry._save_events directly."""
from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.database import Base
import app.models  # noqa: F401  — registers every table on Base.metadata
from app.models import City


@pytest.fixture
def db():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = Session()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


@pytest.fixture
def city(db):
    c = City(name="Testville", country="US", latitude=0.0, longitude=0.0)
    db.add(c)
    db.commit()
    return c
