"""mevalim duplicate rows after a ticket-provider URL flip (2026-09-19).

mevalim keys rows on the provider's offer URL. smarticket / mishkan7 moved
from ``/<hebrew_slug>_<hash>`` to ``/event/<id>``; the job's exact
(scrape_source, source_id) check missed and one run created 943 second
rows. These tests pin the three layers of the fix: the content-key guard
in the job's save path (which also migrates source_id to the new URL), the
in-run RawEvent dedup, and the ``--source`` intra-source mode of
scripts/dedupe_events.py used for the backfill."""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

from app.models import City, Event, EventType, Venue
from app.scheduler.jobs import _upsert_mevalim_event
from app.services.collectors.base import RawEvent
from app.services.collectors.scrapers.mevalim import dedup_raw_events
from app.services.dedup import find_venue_duplicate

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dedupe_events as DE  # noqa: E402

D = date.today() + timedelta(days=12)
VENUE = "היכל התרבות אשקלון"
OLD_URL = "https://ashkelon.smarticket.co.il/בת_הים_הקטנה_6b49c0568d"
NEW_URL = "https://ashkelon.smarticket.co.il/event/5941"


@pytest.fixture
def il(db):
    city = City(name="Ashkelon", country="Israel")
    db.add(city)
    db.flush()
    venue = Venue(name=VENUE, city_id=city.id, physical_country="Israel")
    db.add(venue)
    db.flush()
    et = EventType(name="Kids Show", category="Theatre")
    db.add(et)
    db.commit()
    return city, venue, {"kids-shows": et}


def _existing(db, venue, *, source_id=OLD_URL, source="mevalim", name="בת הים הקטנה",
              start_time="17:30"):
    ev = Event(name=name, artist_name=name, start_date=D, start_time=start_time,
               venue_id=venue.id, venue_name=venue.name, purchase_link=source_id,
               scrape_source=source, source_id=source_id, price=120.0, price_currency="ILS")
    db.add(ev)
    db.commit()
    return ev


def _raw(*, source_id=NEW_URL, name="בת הים הקטנה", start_time="17:30", venue=VENUE,
         start_date=D, categories=("kids-shows",)):
    return RawEvent(name=name, artist_name=name, start_date=start_date, start_time=start_time,
                    price=130.0, price_currency="ILS", purchase_link=source_id,
                    venue_name=venue, venue_city="Ashkelon", venue_country="Israel",
                    source="mevalim", source_id=source_id, raw_categories=list(categories))


def test_url_flip_updates_existing_row_and_migrates_source_id(db, il):
    _city, venue, et_cache = il
    old = _existing(db, venue)
    outcome = _upsert_mevalim_event(db, _raw(), venue, et_cache)
    db.commit()
    assert outcome == "migrated"
    rows = db.query(Event).filter_by(scrape_source="mevalim").all()
    assert len(rows) == 1 and rows[0].id == old.id
    assert rows[0].source_id == NEW_URL and rows[0].purchase_link == NEW_URL
    assert rows[0].price == 130.0  # refreshed like a normal re-scrape
    # Next run: the new URL is now an exact hit → plain update, still one row.
    assert _upsert_mevalim_event(db, _raw(), venue, et_cache) == "updated"
    assert db.query(Event).count() == 1


def test_different_showtime_same_day_is_a_new_row(db, il):
    _city, venue, et_cache = il
    _existing(db, venue, start_time="17:30")
    outcome = _upsert_mevalim_event(
        db, _raw(source_id="https://ashkelon.smarticket.co.il/event/5942", start_time="20:30"),
        venue, et_cache)
    db.commit()
    assert outcome == "saved"
    assert sorted(t for (t,) in db.query(Event.start_time)) == ["17:30", "20:30"]


def test_null_time_on_one_side_still_matches(db, il):
    _city, venue, et_cache = il
    _existing(db, venue, start_time=None)
    assert _upsert_mevalim_event(db, _raw(start_time="17:30"), venue, et_cache) == "migrated"
    assert db.query(Event).count() == 1


def test_title_punctuation_variant_matches(db, il):
    _city, venue, et_cache = il
    _existing(db, venue, name="עדי אשכנזי")
    assert _upsert_mevalim_event(db, _raw(name="עדי אשכנזי - מופע חדש!"[:10]), venue, et_cache) == "migrated"
    assert _upsert_mevalim_event(db, _raw(name="עדי  אשכנזי."), venue, et_cache) == "updated"
    assert db.query(Event).count() == 1


def test_different_show_same_venue_same_day_is_saved(db, il):
    _city, venue, et_cache = il
    _existing(db, venue, name="בת הים הקטנה")
    outcome = _upsert_mevalim_event(
        db, _raw(name="שלגיה ושבעת הגמדים", source_id="https://ashkelon.smarticket.co.il/event/6000"),
        venue, et_cache)
    assert outcome == "saved"
    assert db.query(Event).count() == 2


def test_new_row_gets_event_type_from_category(db, il):
    _city, venue, et_cache = il
    assert _upsert_mevalim_event(db, _raw(), venue, et_cache) == "saved"
    ev = db.query(Event).one()
    assert [t.name for t in ev.event_types] == ["Kids Show"]


def test_cross_source_twin_is_skipped_like_the_registry_does(db, il):
    _city, venue, et_cache = il
    _existing(db, venue, source="smarticket", source_id="smarticket-1")
    assert _upsert_mevalim_event(db, _raw(), venue, et_cache) == "skipped_dup"
    assert db.query(Event).count() == 1
    assert db.query(Event).one().scrape_source == "smarticket"


def test_find_venue_duplicate_matches_by_venue_name_when_venue_ids_differ(db, il):
    _city, venue, _ = il
    other = Venue(name=VENUE, city_id=venue.city_id)
    db.add(other)
    db.flush()
    ev = _existing(db, other)
    hit = find_venue_duplicate(db, name="בת הים הקטנה", start_date=D, venue_name=VENUE,
                               venue_id=venue.id, start_time="17:30")
    assert hit is not None and hit.id == ev.id
    assert find_venue_duplicate(db, name="בת הים הקטנה", start_date=D + timedelta(days=1),
                                venue_name=VENUE, venue_id=venue.id) is None


def test_in_run_dedup_collapses_two_url_shapes_of_one_show():
    per_page = [
        [_raw(source_id=OLD_URL)],                       # detail page, slug URL
        [_raw(source_id=NEW_URL)],                       # archive page, /event/<id> URL
        [_raw(source_id=NEW_URL, start_time="20:30")],   # evening show → kept
        [_raw(source_id=OLD_URL, name="בת הים")],          # name variant, same URL → dropped
    ]
    out = dedup_raw_events(per_page)
    assert [(e.source_id, e.start_time) for e in out] == [(OLD_URL, "17:30"), (NEW_URL, "20:30")]


def test_dedupe_script_source_mode_folds_intra_mevalim_pairs(db, il):
    _city, venue, _ = il
    old = _existing(db, venue, source_id=OLD_URL)
    new = _existing(db, venue, source_id=NEW_URL)
    _existing(db, venue, source_id=NEW_URL + "x", start_time="20:30")  # evening, separate
    _existing(db, venue, source="smarticket", source_id="s-1")          # other source, untouched

    # Default (cross-source) mode: the mevalim pair is invisible …
    events = DE._load_events(db, None, None, None, None)
    assert [len(c) for c in DE._group_candidates(events)] == [3]  # old+new+smarticket cluster
    # … --source mevalim mode sees exactly the pair and keeps the older row.
    events = DE._load_events(db, None, None, None, None, source="mevalim")
    assert all(e.scrape_source == "mevalim" for e in events) and len(events) == 3
    clusters = DE._group_candidates(events, same_source=True)
    assert len(clusters) == 1 and sorted(e.id for e in clusters[0]) == [old.id, new.id]
    canonical, dups = DE._pick_canonical(clusters[0])
    assert canonical.id == old.id and [d.id for d in dups] == [new.id]
    DE.fold_cluster(db, canonical, dups, apply=True)
    assert db.query(Event).filter_by(scrape_source="mevalim").count() == 2
