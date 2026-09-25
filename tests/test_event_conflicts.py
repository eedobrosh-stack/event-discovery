"""Rules A (same title → shared artist / types) and B (same show at
different times → keep the earliest), app/services/event_conflicts.py."""
from datetime import date, timedelta

from app.models import City, Event, EventType, Venue
from app.services.event_conflicts import _conflict, propagate_same_title, resolve_time_conflicts

D = date.today() + timedelta(days=5)


def _setup(db):
    tlv = City(name="Tel Aviv", country="Israel")
    db.add(tlv); db.flush()
    barby = Venue(name="בארבי", city_id=tlv.id)
    addr = Venue(name="נמל יפו 1, תל אביב", city_id=tlv.id)
    db.add_all([barby, addr]); db.flush()
    for i in range(5):                                   # Barby is the established venue
        db.add(Event(name=f"x{i}", start_date=D + timedelta(days=30 + i), venue_id=barby.id, scrape_source="barby", source_id=f"x{i}"))
    concert = EventType(name="Concert", category="Music")
    db.add(concert); db.flush()
    return tlv, barby, addr, concert


def _ev(db, **kw):
    e = Event(start_date=D, **kw); db.add(e); db.flush(); return e


def test_conflict_guards():
    a = {"start_time": "20:30", "scrape_source": "makore", "venue_id": 1}
    assert _conflict(a, {"start_time": "22:00", "scrape_source": "muzi", "venue_id": 2}, "name")
    assert not _conflict(a, {"start_time": "17:00", "scrape_source": "makore", "venue_id": 1}, "name")  # matinee
    assert not _conflict(a, {"start_time": "20:30", "scrape_source": "makore", "venue_id": 3}, "name")  # tour points
    assert not _conflict(a, {"start_time": "23:45", "scrape_source": "muzi", "venue_id": 1}, "name")    # > 3 h
    assert not _conflict(a, {"start_time": "21:00", "scrape_source": "muzi", "venue_id": 2}, "artist")  # artist elsewhere


def test_jimbo_case(db):
    _, barby, addr, concert = _setup(db)
    title = "ג׳ימבו ג׳יי ותזמורת הרחוב"
    early = _ev(db, name=title, start_time="20:30", venue_id=addr.id, scrape_source="makore", source_id="m")
    b = _ev(db, name=title, start_time="21:00", venue_id=barby.id, scrape_source="barby", source_id="b")
    b.event_types.append(concert)
    t = _ev(db, name=title, start_time="21:00", venue_id=barby.id, scrape_source="tickchak", source_id="t",
            artist_name="ג'ימבו ג'יי", artist_youtube_channel="https://yt/x")
    _ev(db, name=title, start_time="22:00", venue_id=barby.id, scrape_source="muzi", source_id="z",
        artist_name="ג'ימבו ג'יי")
    db.commit()

    prop = propagate_same_title(db, apply=True)
    assert {p["event_id"] for p in prop if "artist_name" in p} == {early.id, b.id}
    db.expire_all()
    assert db.get(Event, early.id).artist_name == "ג'ימבו ג'יי"
    assert [x.name for x in db.get(Event, early.id).event_types] == ["Concert"]

    folds = resolve_time_conflicts(db, apply=True)
    assert len(folds) == 1 and folds[0]["keep_id"] == early.id and len(folds[0]["drop"]) == 3
    db.expire_all()
    kept = db.query(Event).filter(Event.name == title).all()
    assert len(kept) == 1
    k = kept[0]
    assert k.start_time == "20:30" and k.venue_id == barby.id and k.artist_youtube_channel == "https://yt/x"
    assert resolve_time_conflicts(db, apply=False) == []               # idempotent


def test_two_artists_do_not_propagate(db):
    _, barby, _, _ = _setup(db)
    _ev(db, name="Jazz Night", start_time="20:00", venue_id=barby.id, scrape_source="a", source_id="1", artist_name="A")
    e2 = Event(name="Jazz Night", start_date=D + timedelta(days=1), start_time="20:00", venue_id=barby.id,
               scrape_source="a", source_id="2", artist_name="B")
    e3 = Event(name="Jazz Night", start_date=D + timedelta(days=2), start_time="20:00", venue_id=barby.id,
               scrape_source="a", source_id="3")
    db.add_all([e2, e3]); db.commit()
    assert not [p for p in propagate_same_title(db, apply=False) if "artist_name" in p]


def test_ingest_folds_cross_source_time_shift(db, city):
    from app.services.collectors.base import RawEvent
    from app.services.collectors.registry import CollectorRegistry

    def raw(src, t, artist=None):
        return RawEvent(name="Kind of Blue", start_date=D, start_time=t, venue_name="Blue Note",
                        source=src, source_id=f"{src}-1", artist_name=artist)

    reg = CollectorRegistry()
    reg._save_events([raw("barby", "21:00")], city, db)
    reg._save_events([raw("makore", "20:30", artist="Miles Tribute Band")], city, db)   # earlier, other source
    reg._save_events([RawEvent(name="Kind of Blue", start_date=D, start_time="17:00", venue_name="Blue Note",
                               source="barby", source_id="barby-2")], city, db)        # same source, matinee
    rows = sorted(db.query(Event.start_time, Event.artist_name, Event.scrape_source).all())
    assert rows == [("17:00", None, "barby"), ("20:30", "Miles Tribute Band", "barby")]
