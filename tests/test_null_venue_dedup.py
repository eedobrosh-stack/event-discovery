"""Venue-less / online event dedupe: same normalised title + same date +
no venue → duplicate, at ingest and in scripts/dedupe_events.py."""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

from app.models import Event
from app.services.collectors.base import RawEvent
from app.services.collectors.registry import CollectorRegistry
from app.services.dedup import find_null_venue_duplicate, normalize_title

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dedupe_events as DE  # noqa: E402

D = date.today() + timedelta(days=10)


@pytest.mark.parametrize("a,b", [
    ("TECHSPO Philadelphia", "techspo philadelphia"),
    ("Live 2026: Chicago Marketing", "Live 2026 - Chicago Marketing!"),
    ("October Virtual: Marketing Analytics", "october  virtual — marketing analytics"),
    ("Café Tacvba", "CAFÉ TACVBA"),
])
def test_normalize_title_folds_case_and_punctuation(a, b):
    assert normalize_title(a) == normalize_title(b) != ""


def test_normalize_title_keeps_distinct_titles_apart():
    assert normalize_title("TECHSPO Philadelphia") != normalize_title("TECHSPO Miami")
    assert normalize_title("") == "" and normalize_title(None) == ""


def _online(name, source_id, source="llm_extractor", start_time=None, venue=None):
    return RawEvent(name=name, start_date=D, start_time=start_time, venue_name=venue,
                    source=source, source_id=source_id)


def test_ingest_skips_venueless_duplicate_across_source_pages(db, city):
    reg = CollectorRegistry()
    raws = [
        _online("TECHSPO Philadelphia", "page-1"),
        _online("TECHSPO Philadelphia", "page-2"),
        _online("techspo philadelphia!", "ld-1", source="ld_online_marketing"),
        _online("TECHSPO Miami", "page-3"),   # different title → kept
    ]
    saved = reg._save_events(raws, city, db)
    assert saved == 2
    assert sorted(n for (n,) in db.query(Event.name)) == ["TECHSPO Miami", "TECHSPO Philadelphia"]


def test_ingest_keeps_same_title_when_either_side_has_a_venue(db, city):
    reg = CollectorRegistry()
    saved = reg._save_events([
        _online("Jazz Night", "a", venue="Blue Note"),
        _online("Jazz Night", "b", venue="Smalls"),      # different venue → kept
        _online("Jazz Night", "c"),                       # venue-less; no venue-less twin → kept
    ], city, db)
    assert saved == 3


def test_ingest_keeps_venueless_twins_with_conflicting_times(db, city):
    reg = CollectorRegistry()
    saved = reg._save_events([
        _online("Ultimate 4D Experience", "a", source="ticketmaster", start_time="10:00"),
        _online("Ultimate 4D Experience", "b", source="ticketmaster", start_time="14:00"),
        _online("Ultimate 4D Experience", "c", source="ticketmaster", start_time="10:00"),  # dup of a
        _online("Ultimate 4D Experience", "d", source="ticketmaster"),  # null time → matches a
    ], city, db)
    assert saved == 2


def test_ingest_online_flag_triggers_rule_even_with_venue_name(db, city):
    reg = CollectorRegistry()
    db.add(Event(name="Remote Summit", start_date=D, venue_id=None, scrape_source="x", source_id="1"))
    db.commit()
    raw = RawEvent(name="Remote Summit", start_date=D, venue_name="Zoom", is_online=True,
                   source="y", source_id="2")
    assert reg._save_events([raw], city, db) == 0


def test_find_null_venue_duplicate_ignores_other_dates(db):
    db.add(Event(name="TECHSPO Dubai", start_date=D, scrape_source="x", source_id="1"))
    db.commit()
    assert find_null_venue_duplicate(db, "TECHSPO Dubai", D) is not None
    assert find_null_venue_duplicate(db, "TECHSPO Dubai", D + timedelta(days=1)) is None
    assert find_null_venue_duplicate(db, "", D) is None


def _ev(name, src, sid, venue_id=None, start_time=None, link=None):
    return Event(name=name, start_date=D, venue_id=venue_id, start_time=start_time,
                 scrape_source=src, source_id=sid, purchase_link=link)


def test_dedupe_script_null_venue_mode_clusters_intra_source(db):
    rows = [
        _ev("Live 2026: Chicago Marketing", "llm_extractor", "p1", link="https://x/1"),
        _ev("Live 2026: Chicago Marketing", "llm_extractor", "p2"),
        _ev("live 2026 - chicago marketing", "ld_online_marketing", "p3"),
        _ev("Live 2026: Chicago Marketing", "llm_extractor", "p4", start_time="09:00"),  # null-time rows join it
        _ev("Other Webinar", "llm_extractor", "p5"),
    ]
    db.add_all(rows)
    db.commit()
    # Default (venue-keyed) mode ignores venue-less rows entirely.
    assert DE._group_candidates(rows) == []
    clusters = DE._group_candidates(rows, null_venue=True)
    assert len(clusters) == 1 and len(clusters[0]) == 4
    canonical, dups = DE._pick_canonical(clusters[0])
    assert canonical.purchase_link == "https://x/1"
    assert len(dups) == 3


def test_dedupe_script_null_venue_mode_splits_on_conflicting_times(db):
    rows = [
        _ev("Ultimate 4D Experience", "ticketmaster", "a", start_time="10:00"),
        _ev("Ultimate 4D Experience", "ticketmaster", "b", start_time="14:00"),
        _ev("Ultimate 4D Experience", "ticketmaster", "c", start_time="14:00"),
        _ev("Ultimate 4D Experience", "ticketmaster", "d"),   # ambiguous null time → left alone
    ]
    db.add_all(rows)
    db.commit()
    clusters = DE._group_candidates(rows, null_venue=True)
    assert [sorted(e.source_id for e in c) for c in clusters] == [["b", "c"]]
