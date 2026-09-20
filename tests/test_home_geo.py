"""Roadmap #2/#3 (2026-09-21): v2 is the homepage, classic at /v2; browser
position → nearest canonical city with a venue-weighted tie-break."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.models  # noqa: F401
from app.database import Base, get_db
from app.main import app
from app.models import City, Venue


@pytest.fixture
def client(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/geo.db")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    tlv = City(name="Tel Aviv", country="Israel", latitude=32.0853, longitude=34.7818)
    giv = City(name="Givatayim", country="Israel", latitude=32.0723, longitude=34.8100)
    yafo = City(name="Tel Aviv-yafo", country="Israel", latitude=32.078, longitude=34.794)   # alias row
    haifa = City(name="Haifa", country="Israel", latitude=32.7940, longitude=34.9896)
    db.add_all([tlv, giv, yafo, haifa]); db.commit()
    yafo.canonical_city_id = tlv.id
    db.add_all([Venue(name=f"TLV venue {i}", city_id=tlv.id) for i in range(30)])
    db.add_all([Venue(name=f"Giv venue {i}", city_id=giv.id) for i in range(3)])
    db.add_all([Venue(name=f"Haifa venue {i}", city_id=haifa.id) for i in range(25)])
    db.commit(); db.close()

    def override():
        s = Session()
        try:
            yield s
        finally:
            s.close()
    app.dependency_overrides[get_db] = override
    try:
        yield TestClient(app)
    finally:
        app.dependency_overrides.clear()


def test_homepage_is_v2_and_classic_moved(client):
    home = client.get("/").text
    classic = client.get("/v2").text
    assert 'id="v2-geo"' in home and "supercaly_location" in home
    assert 'id="v2-geo"' not in classic and "home-city-input" in classic
    assert 'href="/v2">Classic Homepage' in home and 'href="/">✨ New Homepage' in classic


def test_nearest_prefers_major_city_over_closer_suburb(client):
    # standing in Givatayim: Givatayim is 3 km away but has 3 venues → Tel Aviv
    r = client.get("/api/geo/nearest", params={"lat": 32.0723, "lon": 34.8100}).json()
    assert r["name"] == "Tel Aviv" and r["nearest_any"] == "Givatayim" and r["venues"] == 30


def test_nearest_skips_alias_rows_and_finds_haifa(client):
    r = client.get("/api/geo/nearest", params={"lat": 32.79, "lon": 34.99}).json()
    assert r["name"] == "Haifa"
    # a point next to the alias row still resolves to the canonical city
    r = client.get("/api/geo/nearest", params={"lat": 32.078, "lon": 34.794}).json()
    assert r["name"] == "Tel Aviv"


def test_nearest_empty_when_far_from_everything(client):
    assert client.get("/api/geo/nearest", params={"lat": 51.5, "lon": -0.12}).json() == {}
    assert client.get("/api/geo/nearest", params={"lat": 95, "lon": 0}).status_code == 422
