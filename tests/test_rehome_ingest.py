"""Ingest files an Israeli venue under its own city, not the collector's."""
from datetime import date, timedelta

from app.models import City
from app.services.collectors.base import RawEvent
from app.services.collectors.registry import CollectorRegistry
from app.services.il_places import canon_place


def _raw(**kw):
    base = dict(name="Show", start_date=date.today() + timedelta(days=5), venue_name="היכל התרבות קריית גת",
                source="test", source_id="x1")
    base.update(kw)
    return RawEvent(**base)


def test_home_city_follows_venue_city(db):
    tlv = City(name="Tel Aviv", country="Israel")
    kg = City(name="Kiryat Gat", country="Israel")
    other = City(name="Israel - Other", country="Israel")
    db.add_all([tlv, kg, other]); db.flush()
    home = CollectorRegistry._venue_home_city
    assert home(_raw(venue_city="קריית גת"), tlv, db).id == kg.id
    assert home(_raw(venue_city="כפר עציון"), tlv, db).id == other.id
    assert home(_raw(venue_city="תל אביב-יפו"), tlv, db).id == tlv.id
    assert home(_raw(venue_city=None), tlv, db).id == tlv.id


def test_bare_country_is_not_a_place():
    assert canon_place("Israel", "Jerusalem") == "Jerusalem"
    assert canon_place("Jerusalem, Israel") == "Jerusalem"
