"""Stats v2 endpoint (roadmap #5): one JSON, optional country scope."""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.models  # noqa: F401
from app.database import Base, get_db
from app.main import app
from app.models import City, Event, Venue


@pytest.fixture
def client(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path}/s.db")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    il = City(name="Tel Aviv", country="Israel"); uk = City(name="London", country="United Kingdom")
    db.add_all([il, uk]); db.commit()
    v1 = Venue(name="Barby", city_id=il.id); v2 = Venue(name="O2", city_id=uk.id)
    db.add_all([v1, v2]); db.commit()
    nxt = date.today() + timedelta(days=3)
    db.add_all([
        Event(name="Local show", artist_name="Omri Mor", start_date=nxt, venue_id=v1.id, scrape_source="mevalim", source_id="a"),
        Event(name="Tour A", artist_name="Deep Purple", start_date=nxt, venue_id=v1.id, scrape_source="tmisrael", source_id="b"),
        Event(name="Tour B", artist_name="Deep Purple", start_date=nxt, venue_id=v2.id, scrape_source="ticketmaster", source_id="c"),
        Event(name="Old", artist_name="Gone", start_date=date.today() - timedelta(days=1), venue_id=v2.id, scrape_source="x", source_id="d"),
    ])
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


def test_stats_v2_worldwide_and_scoped(client):
    d = client.get("/api/stats/v2").json()
    assert d["country"] is None and d["totals"]["upcoming"] == 3 and d["totals"]["artists_upcoming"] == 2
    assert d["flow"]["added_24h"] == 3 and d["flow"]["aged_out_24h"] == 1 and d["flow"]["net_24h"] == 2
    assert d["artists"] == {"single_country": 1, "international": 1,
                            "top_international": [{"artist": "Deep Purple", "countries": 2, "upcoming": 2}]}
    assert {c["country"]: c["upcoming"] for c in d["by_country"]} == {"Israel": 2, "United Kingdom": 1}
    assert d["enrichment"]["artists_upcoming"] == 2 and d["enrichment"]["pct_genre"] == 0.0
    for k in ("taxonomy", "aggregation", "failures"):
        assert k in d
    il = client.get("/api/stats/v2", params={"country": "Israel"}).json()
    assert il["totals"]["upcoming"] == 2 and il["artists"]["in_scope_single"] == 1 and il["artists"]["in_scope_international"] == 1
    assert client.get("/stats_v2.html").status_code == 200
