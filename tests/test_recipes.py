"""Unit tests for the Route 3 recipe engine — no network, no DB.

Run:  PYTHONPATH=. python3 -m pytest tests/test_recipes.py -q
"""
from __future__ import annotations

import json
from datetime import date, timedelta

import pytest

from app.services.recipes import parse as P
from app.services.recipes.fetch import Response
from app.services.recipes.normalize import normalize
from app.services.recipes.runner import run_recipe, expand_entry_urls
from app.services.recipes.schema import validate_recipe, registered_domain

TOMORROW = (date.today() + timedelta(days=1)).isoformat()
NEXT_WEEK = (date.today() + timedelta(days=7)).isoformat()
YESTERDAY = (date.today() - timedelta(days=1)).isoformat()


class FakeFetcher:
    """Serves canned bodies keyed by URL; counts requests like the real one."""

    def __init__(self, pages: dict, status: dict | None = None):
        self.pages = pages
        self.status = status or {}
        self.requests_made = 0
        self.max_requests = 400
        self.urls: list[str] = []

    def get(self, url, *, values=None, method=None):
        self.requests_made += 1
        self.urls.append(url)
        self.methods = getattr(self, "methods", []) + [method]
        if url not in self.pages:
            return Response(url, 404, "", {})
        return Response(url, self.status.get(url, 200), self.pages[url], {})

    def close(self):
        pass


# ── schema ───────────────────────────────────────────────────────────────
def _base(**over):
    doc = {
        "recipe_version": 1, "domain": "example.org", "source_name": "example",
        "country": "Israel", "timezone": "Asia/Jerusalem", "language": "he",
        "entry": {"urls": ["https://example.org/events"]},
        "fetch": {"delay_seconds": 0.5},
        "parse": {"kind": "html", "item": ".ev",
                  "fields": {"name": ".t", "start_date": {"sel": "time", "attr": "datetime"}}},
    }
    doc.update(over)
    return doc


def test_registered_domain():
    assert registered_domain("https://www.ICM.org.il/jazz") == "icm.org.il"
    assert registered_domain("shablul.smarticket.co.il") == "shablul.smarticket.co.il"


def test_schema_valid_minimal():
    assert validate_recipe(_base()) == []


def test_schema_catches_common_mistakes():
    bad = _base(domain="www.example.org", source_name="Bad Name")
    bad["parse"]["fields"]["venue"] = ".v"          # typo'd RawEvent field
    bad["entry"]["urls"].append("https://other.com/x")
    errs = validate_recipe(bad)
    joined = "\n".join(errs)
    assert "domain" in joined
    assert "source_name" in joined
    assert "unknown RawEvent field 'venue'" in joined
    assert "other.com" in joined


def test_schema_template_needs_iteration_config():
    doc = _base(entry={"urls": [], "template": "https://example.org/e/{value}"})
    assert any("values" in e for e in validate_recipe(doc))


def test_schema_browser_render_rejected():
    doc = _base(fetch={"render": "browser", "delay_seconds": 1})
    assert any("browser" in e for e in validate_recipe(doc))


# ── get_path ─────────────────────────────────────────────────────────────
def test_get_path():
    o = {"data": {"events": [{"id": 1, "tags": [{"n": "a"}, {"n": "b"}],
                              "venue": {"loc": {"lat": 32.1}}}]}}
    assert P.get_path(o, "data.events[0].id") == 1
    assert P.get_path(o, "data.events[0].venue.loc.lat") == 32.1
    assert P.get_path(o, "data.events[0].tags[*].n") == ["a", "b"]
    assert P.get_path(o, "data.missing.x") is None
    assert P.get_path(o, "") is o


# ── html parse + normalise ───────────────────────────────────────────────
HTML = f"""
<html><body>
<div class="ev"><a class="t" href="/event/101">ג'אז בבית</a>
  <time datetime="{TOMORROW}T20:30:00">מחר</time><span class="v">שבלול</span><span class="p">₪120</span></div>
<div class="ev"><a class="t" href="/event/102">Old show</a>
  <time datetime="{YESTERDAY}">yesterday</time><span class="v">X</span></div>
<div class="ev"><a class="t" href="/event/103">No date</a><span class="v">Y</span></div>
<div class="ev"><a class="t" href="/event/104">Next week</a>
  <time datetime="{NEXT_WEEK}">soon</time><span class="v">Z</span></div>
<a class="next" href="/events?page=2">next</a>
</body></html>"""

HTML_P2 = f"""<div class="ev"><a class="t" href="/event/201">Page two</a>
<time datetime="{NEXT_WEEK}T19:00">x</time><span class="v">Q</span></div>"""


def _html_recipe(**over):
    doc = _base(
        entry={"urls": ["https://example.org/events"],
               "paginate": {"mode": "next_link", "selector": "a.next", "max_pages": 5}},
        parse={"kind": "html", "item": ".ev",
               "fields": {
                   "source_id": {"sel": "a.t", "attr": "href", "regex": r"/event/(\d+)"},
                   "name": "a.t",
                   "start_datetime": {"sel": "time", "attr": "datetime"},
                   "venue_name": ".v",
                   "price": ".p",
                   "purchase_link": {"sel": "a.t", "attr": "href", "absolute": True},
               }},
        defaults={"price_currency": "ILS", "raw_categories": ["Jazz"]})
    doc.update(over)
    return doc


def test_html_parse_fields():
    rows = P.parse_html(HTML, _html_recipe()["parse"], "https://example.org/events")
    assert len(rows) == 4
    r = rows[0]
    assert r["source_id"] == "101"
    assert r["name"] == "ג'אז בבית"
    assert r["purchase_link"] == "https://example.org/event/101"
    assert r["price"] == "₪120"


def test_next_link_and_run_recipe_pagination():
    f = FakeFetcher({
        "https://example.org/events": HTML,
        "https://example.org/events?page=2": HTML_P2,
    })
    res = run_recipe(_html_recipe(), fetcher=f)
    assert res.fatal is None, res.fatal
    assert res.pages == ["https://example.org/events", "https://example.org/events?page=2"]
    assert res.fetched == 5
    names = sorted(e.name for e in res.events)
    assert names == sorted(["ג'אז בבית", "Next week", "Page two"])
    assert res.dropped["past"] == 1
    assert res.dropped["no_date"] == 1
    ev = next(e for e in res.events if e.source_id == "101")
    assert ev.start_time == "20:30"
    assert ev.price == 120.0 and ev.price_currency == "ILS"
    assert ev.raw_categories == ["Jazz"]
    assert ev.venue_country == "Israel"
    assert ev.source == "example"


def test_page_param_stops_on_empty_page():
    f = FakeFetcher({
        "https://example.org/events?page=1": HTML_P2,
        "https://example.org/events?page=2": HTML_P2.replace("201", "202"),
        "https://example.org/events?page=3": "<html></html>",
    })
    doc = _html_recipe(entry={"urls": ["https://example.org/events"],
                              "paginate": {"mode": "page_param", "param": "page", "max_pages": 10}})
    res = run_recipe(doc, fetcher=f)
    assert len(res.pages) == 3
    assert res.fetched == 2


def test_source_id_derived_when_missing():
    doc = _html_recipe()
    del doc["parse"]["fields"]["source_id"]
    f = FakeFetcher({"https://example.org/events": HTML_P2})
    res = run_recipe(doc, fetcher=f)
    assert len(res.events) == 1
    assert len(res.events[0].source_id) == 16


# ── api parse ────────────────────────────────────────────────────────────
API = {
    "meta": {"next_cursor": "abc"},
    "data": {"events": [
        {"id": 7, "title": "Show A", "startsAt": f"{TOMORROW}T21:00:00+03:00",
         "venue": {"name": "Barby", "city": "Tel Aviv", "location": {"lat": 32.05, "lng": 34.76}},
         "tickets": {"minPrice": 90}, "url": "https://example.org/e/7",
         "performers": [{"name": "Band"}], "tags": [{"name": "Rock"}, {"name": "Indie"}]},
        {"id": 8, "title": "Show B", "startsAt": 4102444800,  # epoch 2100-01-01
         "venue": {"name": "Zappa"}, "url": "/e/8"},
    ]},
}
API_P2 = {"meta": {"next_cursor": None}, "data": {"events": [
    {"id": 9, "title": "Show C", "startsAt": f"{NEXT_WEEK}", "venue": {"name": "Q"}, "url": "/e/9"}]}}


def _api_recipe():
    return _base(
        entry={"urls": ["https://example.org/api/events"],
               "paginate": {"mode": "cursor", "cursor_path": "meta.next_cursor",
                            "param": "cursor", "max_pages": 5}},
        parse={"kind": "api", "items": "data.events",
               "fields": {
                   "source_id": "id", "name": "title", "start_datetime": "startsAt",
                   "artist_name": "performers[0].name",
                   "venue_name": "venue.name", "venue_city": "venue.city",
                   "venue_lat": "venue.location.lat", "venue_lon": "venue.location.lng",
                   "price": "tickets.minPrice",
                   "purchase_link": {"path": "url", "absolute": True},
                   "raw_categories": "tags[*].name",
               }})


def test_api_parse_and_cursor_pagination():
    f = FakeFetcher({
        "https://example.org/api/events": json.dumps(API),
        "https://example.org/api/events?cursor=abc": json.dumps(API_P2),
    })
    res = run_recipe(_api_recipe(), fetcher=f)
    assert res.fatal is None, res.fatal
    assert len(res.pages) == 2
    assert res.fetched == 3
    a = next(e for e in res.events if e.source_id == "7")
    assert a.artist_name == "Band"
    assert a.venue_lat == 32.05 and a.venue_lon == 34.76
    assert a.raw_categories == ["Rock", "Indie"]
    assert a.start_time == "21:00"
    b = next(e for e in res.events if e.source_id == "8")
    assert b.start_date.year == 2100
    assert b.purchase_link == "https://example.org/e/8"


# ── jsonld + ics ─────────────────────────────────────────────────────────
def test_jsonld_recipe_uses_existing_converter():
    html = f"""<html><head><script type="application/ld+json">
    {{"@context":"https://schema.org","@type":"Event","name":"LD Show",
      "startDate":"{TOMORROW}T20:00:00","url":"https://example.org/ld/1",
      "location":{{"@type":"Place","name":"Hall","address":{{"addressLocality":"Haifa","addressCountry":"IL"}}}}}}
    </script></head><body/></html>"""
    doc = _base(parse={"kind": "jsonld"})
    res = run_recipe(doc, fetcher=FakeFetcher({"https://example.org/events": html}))
    assert res.fatal is None, res.fatal
    assert len(res.events) == 1
    assert res.events[0].name == "LD Show"
    assert res.events[0].source == "example"


def test_ics_recipe():
    ics = f"""BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
UID:evt-1@example.org
SUMMARY:Cal Show
DTSTART:{NEXT_WEEK.replace('-', '')}T190000Z
LOCATION:Main Hall
URL:https://example.org/c/1
END:VEVENT
END:VCALENDAR
"""
    doc = _base(entry={"urls": ["https://example.org/feed.ics"]}, parse={"kind": "ics"})
    res = run_recipe(doc, fetcher=FakeFetcher({"https://example.org/feed.ics": ics}))
    assert res.fatal is None, res.fatal
    assert len(res.events) == 1
    e = res.events[0]
    assert e.source_id == "evt-1@example.org" and e.venue_name == "Main Hall"
    assert e.start_time == "19:00"


# ── detail hop ───────────────────────────────────────────────────────────
def test_detail_hop_fills_missing_fields_only_within_budget():
    listing = f"""<div class="ev"><a class="t" href="/event/1">A</a><time datetime="{NEXT_WEEK}">x</time></div>
                  <div class="ev"><a class="t" href="/event/2">B</a><time datetime="{NEXT_WEEK}">x</time></div>"""
    doc = _html_recipe(
        entry={"urls": ["https://example.org/events"]},
        detail={"url_field": "purchase_link", "max_per_run": 1, "only_new": False,
                "parse": {"kind": "html", "fields": {"venue_name": ".venue", "price": ".price"}}})
    doc["parse"]["fields"].pop("venue_name")
    doc["parse"]["fields"].pop("price")
    f = FakeFetcher({
        "https://example.org/events": listing,
        "https://example.org/event/1": "<div class='venue'>Deep Hall</div><div class='price'>50</div>",
        "https://example.org/event/2": "<div class='venue'>Never fetched</div>",
    })
    res = run_recipe(doc, fetcher=f)
    assert res.detail_fetched == 1
    ev1 = next(e for e in res.events if e.source_id == "1")
    ev2 = next(e for e in res.events if e.source_id == "2")
    assert ev1.venue_name == "Deep Hall" and ev1.price == 50.0
    assert ev2.venue_name is None


# ── dates ────────────────────────────────────────────────────────────────
def test_hebrew_free_text_date_parses_via_dateparser():
    rows = [{"name": "x", "start_date": "15 באוקטובר 2030", "_page_url": "u"}]
    res = normalize(rows, _base())
    assert len(res.events) == 1
    assert res.events[0].start_date == date(2030, 10, 15)


def test_strptime_format_preferred():
    doc = _base()
    doc["parse"]["date_format"] = "%d.%m.%Y"
    rows = [{"name": "x", "start_date": "05.11.2031", "start_time": "21:15", "_page_url": "u"}]
    res = normalize(rows, doc)
    assert res.events[0].start_date == date(2031, 11, 5)
    assert res.events[0].start_time == "21:15"


# ── entry expansion / failure modes ──────────────────────────────────────
def test_expand_entry_values_template():
    urls = expand_entry_urls({"urls": [], "template": "https://example.org/e/{value}",
                              "values": ["tlv", "haifa"]})
    assert [u for u, _ in urls] == ["https://example.org/e/tlv", "https://example.org/e/haifa"]
    assert urls[0][1] == {"value": "tlv"}


def test_http_error_is_fatal_when_nothing_fetched():
    f = FakeFetcher({"https://example.org/events": ""}, status={"https://example.org/events": 503})
    res = run_recipe(_html_recipe(entry={"urls": ["https://example.org/events"]}), fetcher=f)
    assert res.fatal and "HTTP 503" in res.fatal
    assert res.events == []


def test_invalid_recipe_is_fatal_without_fetching():
    f = FakeFetcher({})
    res = run_recipe(_base(domain="www.example.org"), fetcher=f)
    assert res.fatal.startswith("invalid recipe")
    assert f.requests_made == 0


# ── sync (git → table) on a throwaway SQLite DB ──────────────────────────
def test_sync_recipes_from_dir_upsert_semantics(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401  (register every table on Base)
    from app.database import Base
    from app.models import SourceRecipe
    from app.services.recipes.sync import sync_recipes_from_dir

    engine = create_engine(f"sqlite:///{tmp_path}/t.db")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)

    rdir = tmp_path / "recipes"
    rdir.mkdir()
    doc = _html_recipe()
    doc["priority"] = 7
    (rdir / "example.org.json").write_text(json.dumps(doc), encoding="utf-8")
    (rdir / "broken.json").write_text("{not json", encoding="utf-8")
    bad = _html_recipe(domain="www.bad.org")
    (rdir / "bad.json").write_text(json.dumps(bad), encoding="utf-8")

    db = Session()
    s = sync_recipes_from_dir(db, str(rdir))
    assert s["created"] == 1 and len(s["invalid"]) == 2, s
    row = db.query(SourceRecipe).filter_by(domain="example.org").one()
    assert row.recipe_version == 1 and row.priority == 7 and row.enabled

    # simulate a night of health, then redeploy with an unchanged file
    row.last_status = "ok"; row.saved_total = 99; row.drift_flag = True
    row.next_run_at = None
    db.commit()
    s = sync_recipes_from_dir(db, str(rdir))
    assert s["unchanged"] == 1, s
    row = db.query(SourceRecipe).filter_by(domain="example.org").one()
    assert row.recipe_version == 1 and row.saved_total == 99 and row.drift_flag is True

    # edited recipe → version bump, drift cleared, due now
    doc["parse"]["fields"]["description"] = ".d"
    (rdir / "example.org.json").write_text(json.dumps(doc), encoding="utf-8")
    s = sync_recipes_from_dir(db, str(rdir))
    assert s["updated"] == 1, s
    row = db.query(SourceRecipe).filter_by(domain="example.org").one()
    assert row.recipe_version == 2 and row.drift_flag is False and row.next_run_at is not None
    db.close()


def test_html_parser_falls_back_on_markup_lxml_rejects():
    # a bare ':' attribute name makes lxml's sax target raise
    html = f'<div : class="ev"><a class="t" href="/event/9">Odd</a><time datetime="{NEXT_WEEK}">x</time></div>'
    rows = P.parse_html(html, _html_recipe()["parse"], "https://example.org/e")
    assert len(rows) == 1 and rows[0]["name"] == "Odd"


def test_budget_exhaustion_keeps_partial_rows():
    from app.services.recipes.fetch import BudgetExhausted

    class CappedFetcher(FakeFetcher):
        def get(self, url, *, values=None):
            if self.requests_made >= 1:
                raise BudgetExhausted("1 requests")
            return super().get(url, values=values)

    f = CappedFetcher({
        "https://example.org/events?page=1": HTML_P2,
        "https://example.org/events?page=2": HTML_P2.replace("201", "202"),
    })
    doc = _html_recipe(entry={"urls": ["https://example.org/events"],
                              "paginate": {"mode": "page_param", "param": "page", "max_pages": 10}})
    res = run_recipe(doc, fetcher=f)
    assert res.fetched == 1 and len(res.events) == 1
    assert res.budget_hit and res.fatal is None
    assert any("budget" in e for e in res.errors)


def test_ongoing_if_end_only_maps_exhibitions_to_today():
    doc = _base()
    doc["parse"]["date_format"] = "%d/%m/%Y"
    rows = [
        {"name": "Open show", "end_date": (date.today() + timedelta(days=30)).strftime("%d/%m/%Y"), "_page_url": "u"},
        {"name": "Closed show", "end_date": (date.today() - timedelta(days=1)).strftime("%d/%m/%Y"), "_page_url": "u"},
        {"name": "Undated", "_page_url": "u"},
    ]
    res = normalize(rows, doc)
    assert len(res.events) == 0 and res.dropped["no_date"] == 3      # flag off → dropped
    doc["parse"]["ongoing_if_end_only"] = True
    res = normalize(rows, doc)
    assert [e.name for e in res.events] == ["Open show"]
    assert res.events[0].start_date == date.today() and res.events[0].start_time is None
    assert res.events[0].end_date == date.today() + timedelta(days=30)
    assert res.dropped["no_date"] == 2


def test_follow_mode_fans_listing_into_show_pages_with_page_fields():
    listing = ('<ul><li><a class="show" href="/announce/1">A</a></li>'
               '<li><a class="show" href="/announce/2">B</a></li>'
               '<li><a class="show" href="https://other.com/x">ext</a></li></ul>')
    def show(n, title):
        return (f'<h1>{title}</h1><meta property="og:image" content="/img/{n}.jpg">'
                f'<table><tr data-city="Haifa"><td class="c">Haifa</td>'
                f'<td><time class="d">א׳, {NEXT_WEEK[8:10]}.{NEXT_WEEK[5:7]}.{NEXT_WEEK[:4]}<br>20:30</time></td>'
                f'<td class="last" data-hall="Hall {n}"></td><td><a href="/announce/buy/{n}1">x</a></td></tr>'
                f'<tr data-city="Tel Aviv"><td class="c">Tel Aviv</td>'
                f'<td><time class="d">ב׳, {NEXT_WEEK[8:10]}.{NEXT_WEEK[5:7]}.{NEXT_WEEK[:4]}<br>21:00</time></td>'
                f'<td class="last" data-hall="Hall {n}b"></td><td><a href="/announce/buy/{n}2">x</a></td></tr></table>')
    f = FakeFetcher({
        "https://example.org/list": listing,
        "https://example.org/announce/1": show(1, "Show One"),
        "https://example.org/announce/2": show(2, "Show Two"),
    })
    doc = _base(
        entry={"urls": ["https://example.org/list"],
               "follow": {"selector": "a.show", "max_links": 10}},
        parse={"kind": "html", "item": "tr[data-city]", "date_format": "%d.%m.%Y",
               "page_fields": {"name": "h1",
                               "image_url": {"sel": "meta[property='og:image']", "attr": "content", "absolute": True}},
               "fields": {"source_id": {"sel": "a[href^='/announce/buy/']", "attr": "href", "regex": r"buy/(\d+)"},
                          "start_date": {"sel": "time.d", "regex": r"(\d{2}\.\d{2}\.\d{4})"},
                          "start_time": {"sel": "time.d", "regex": r"(\d{1,2}:\d{2})"},
                          "venue_name": {"sel": "td.last", "attr": "data-hall"},
                          "venue_city": "td.c"}})
    res = run_recipe(doc, fetcher=f)
    assert res.fatal is None, (res.fatal, res.errors)
    assert res.followed == 2                      # other.com link filtered out
    assert len(res.pages) == 3
    assert len(res.events) == 4
    ev = next(e for e in res.events if e.source_id == "21")
    assert ev.name == "Show Two" and ev.venue_name == "Hall 2" and ev.venue_city == "Haifa"
    assert ev.start_time == "20:30" and ev.image_url == "https://example.org/img/2.jpg"
    assert ev.purchase_link == "https://example.org/announce/2"


def test_hebrew_city_aliases_applied_for_he_recipes():
    rows = [{"name": "x", "start_date": NEXT_WEEK, "venue_city": "תל אביב-יפו", "_page_url": "u"},
            {"name": "y", "start_date": NEXT_WEEK, "venue_city": "באר שבע", "_page_url": "u"},
            {"name": "z", "start_date": NEXT_WEEK, "venue_city": "כפר יונה", "_page_url": "u"}]
    doc = _base()
    doc["city_aliases"] = {"כפר יונה": "Kfar Yona"}
    res = normalize(rows, doc)
    assert [e.venue_city for e in res.events] == ["Tel Aviv", "Beersheba", "Kfar Yona"]


def test_group_events_by_city_uses_venue_city_then_default(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401
    from app.database import Base
    from app.models import City
    from app.services.recipes.runner import group_events_by_city
    engine = create_engine(f"sqlite:///{tmp_path}/c.db")
    Base.metadata.create_all(bind=engine)
    db = sessionmaker(bind=engine)()
    tlv = City(name="Tel Aviv", country="Israel", timezone="Asia/Jerusalem", latitude=32.0, longitude=34.7)
    bsh = City(name="Beersheba", country="Israel", timezone="Asia/Jerusalem", latitude=31.2, longitude=34.7)
    db.add_all([tlv, bsh]); db.commit()
    rows = [{"name": "a", "start_date": NEXT_WEEK, "venue_city": "Beersheba", "_page_url": "u"},
            {"name": "b", "start_date": NEXT_WEEK, "venue_city": "Tel Aviv", "_page_url": "u"},
            {"name": "c", "start_date": NEXT_WEEK, "venue_city": "Nowhere", "_page_url": "u"},
            {"name": "d", "start_date": NEXT_WEEK, "_page_url": "u"}]
    events = normalize(rows, _base()).events
    groups, unresolved, skipped = group_events_by_city(db, events, "Israel", tlv)
    by_name = {c.name: sorted(e.name for e in evs) for c, evs in groups}
    assert by_name == {"Beersheba": ["a"], "Tel Aviv": ["b", "c", "d"]}
    assert unresolved == {"Nowhere": 1}
    assert skipped == {}


def test_group_events_by_city_resolves_foreign_venue_country_before_default(tmp_path):
    """QA 2026-09-20: livenation.com recipe #243 (country=Israel, city=Tel
    Aviv) emitted US venues; 'Houston' missed the Israel-only lookup and
    fell back to Tel Aviv → 'House of Blues (Houston)' under Tel Aviv."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401
    from app.database import Base
    from app.models import City
    from app.services.recipes.runner import group_events_by_city
    engine = create_engine(f"sqlite:///{tmp_path}/c.db")
    Base.metadata.create_all(bind=engine)
    db = sessionmaker(bind=engine)()
    tlv = City(name="Tel Aviv", country="Israel", timezone="Asia/Jerusalem", latitude=32.0, longitude=34.7)
    hou = City(name="Houston", country="United States", state="TX", timezone="America/Chicago")
    # a city that exists in two countries: the venue's country must decide
    par_fr = City(name="Paris", country="France", timezone="Europe/Paris")
    par_ca = City(name="Paris", country="Canada", timezone="America/Toronto")
    db.add_all([tlv, hou, par_fr, par_ca]); db.commit()
    rows = [
        # venue_country "US" → canon "United States" → Houston found there
        {"name": "houston", "start_date": NEXT_WEEK, "venue_city": "Houston", "venue_country": "US", "_page_url": "u"},
        # foreign country we know (City rows exist) but city we don't track → skipped, not Tel Aviv
        {"name": "anaheim", "start_date": NEXT_WEEK, "venue_city": "Anaheim", "venue_country": "US", "_page_url": "u"},
        # ambiguous name resolved inside the event's own country
        {"name": "paris", "start_date": NEXT_WEEK, "venue_city": "Paris", "venue_country": "Canada", "_page_url": "u"},
        # country we have no City rows for → old behaviour: default city + unresolved
        {"name": "elbonia", "start_date": NEXT_WEEK, "venue_city": "Elbon", "venue_country": "Elbonia", "_page_url": "u"},
        # no venue_country in the row → normalize fills the recipe country → default city
        {"name": "local", "start_date": NEXT_WEEK, "venue_city": "Tel Aviv", "_page_url": "u"},
    ]
    events = normalize(rows, _base()).events
    assert events[0].venue_country == "US" and events[4].venue_country == "Israel"
    groups, unresolved, skipped = group_events_by_city(db, events, "Israel", tlv)
    by_city = {(c.name, c.country): sorted(e.name for e in evs) for c, evs in groups}
    assert by_city == {("Houston", "United States"): ["houston"],
                       ("Paris", "Canada"): ["paris"],
                       ("Tel Aviv", "Israel"): ["elbonia", "local"]}
    assert unresolved == {"Elbon": 1}
    assert skipped == {"Anaheim (United States)": 1}


def test_auto_enroll_jsonld_groups_domains_and_respects_git_recipes(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401
    from app.database import Base
    from app.models import LLMSource, SourceRecipe
    from app.services.recipes.auto_enroll import auto_enroll_jsonld, plan_jsonld_recipes
    engine = create_engine(f"sqlite:///{tmp_path}/a.db")
    Base.metadata.create_all(bind=engine)
    db = sessionmaker(bind=engine)()

    def src(url, **kw):
        d = dict(url=url, state="recurring", last_method="jsonld", country="Germany",
                 city_name="Berlin", events_saved_total=5, last_event_count=2)
        d.update(kw)
        db.add(LLMSource(**d))
    src("https://www.alpha.de/events", events_saved_total=50)
    src("https://alpha.de/konzerte", events_saved_total=120)
    src("https://alpha.de/theater", events_saved_total=1)
    src("https://beta.co.uk/whats-on", country="United Kingdom", city_name="Leeds", events_saved_total=9)
    src("https://beta.co.uk/blocked", country="United Kingdom", state="blocked")
    src("https://gamma.org/cal", country=None, city_name=None)          # no country → skipped
    src("https://delta.fr/agenda", last_method="html")                   # not jsonld → ignored
    src("https://example.org/e")                                         # has a git recipe → left alone
    db.add(SourceRecipe(domain="example.org", source_name="example", recipe=_base(),
                        recipe_version=1, enabled=True, written_by="git-deploy"))
    db.commit()

    plan = plan_jsonld_recipes(db, max_urls=2)
    doms = {d["domain"]: d for d in plan["recipes"]}
    assert set(doms) == {"alpha.de", "beta.co.uk"}
    assert doms["alpha.de"]["entry"]["urls"] == ["https://alpha.de/konzerte", "https://www.alpha.de/events"]
    assert doms["alpha.de"]["priority"] == 17 and doms["alpha.de"]["cadence_hours"] == 48
    assert doms["alpha.de"]["source_name"] == "ld_alpha_de" and doms["alpha.de"]["parse"] == {"kind": "jsonld"}
    assert doms["beta.co.uk"]["city_name"] == "Leeds"
    assert plan["skipped"]["no_country"] == 1 and plan["skipped"]["has_recipe"] == 1

    s = auto_enroll_jsonld(db, max_urls=2)
    assert s["created"] == 2 and s["invalid"] == []
    rows = {r.domain: r for r in db.query(SourceRecipe).all()}
    assert rows["alpha.de"].written_by == "auto-jsonld" and rows["alpha.de"].enabled
    assert rows["example.org"].written_by == "git-deploy"
    # LLMSource rows of enrolled domains are graduated; others untouched
    states = {r.url: r.state for r in db.query(LLMSource).all()}
    assert states["https://alpha.de/konzerte"] == "graduated"
    assert states["https://beta.co.uk/blocked"] == "graduated"
    assert states["https://delta.fr/agenda"] == "recurring"
    # idempotent
    s2 = auto_enroll_jsonld(db, max_urls=2)
    assert s2["created"] == 0 and s2["domains_planned"] == 0


def test_fetcher_falls_back_to_impersonation_on_403(monkeypatch):
    from app.services.recipes.fetch import Fetcher, Response
    f = Fetcher({"delay_seconds": 0.5}, respect_robots=False)
    calls = {"plain": 0, "imp": 0}

    class R:  # minimal httpx-like response
        def __init__(self, code, text): self.status_code, self.text, self.url, self.headers = code, text, "https://x.test/e", {}
    monkeypatch.setattr(f._client, "get", lambda url: calls.__setitem__("plain", calls["plain"] + 1) or R(403, "blocked"))
    monkeypatch.setattr(f, "_get_impersonated", lambda url, values, method=None: calls.__setitem__("imp", calls["imp"] + 1) or Response(url, 200, "<html>ok</html>", {}))
    r = f.get("https://x.test/e")
    assert r.status == 200 and f.impersonate is True and f.switched_to_impersonation
    r2 = f.get("https://x.test/e2")           # subsequent calls go straight to impersonation
    assert r2.status == 200 and calls == {"plain": 1, "imp": 2}
    f.close()


# ── prober ───────────────────────────────────────────────────────────────
def _probe_fetcher(pages):
    f = FakeFetcher(pages)
    f.max_requests = 8
    return f


def test_probe_detects_jsonld_first():
    from app.services.recipes.probe import probe_domain
    html = "".join(f'<script type="application/ld+json">{{"@type":"Event","name":"E{i}","startDate":"{NEXT_WEEK}T20:00:00","location":{{"@type":"Place","name":"H"}}}}</script>' for i in range(4))
    f = _probe_fetcher({"https://a.test/events": f"<html>{html}</html>"})
    res = probe_domain("a.test", ["https://a.test/events"], "Israel", fetcher=f)
    assert res["hit"] and res["hit"]["detector"] == "jsonld" and res["hit"]["events"] == 4


def test_probe_finds_ics_feed_link():
    from app.services.recipes.probe import probe_domain, build_recipe
    ics = "BEGIN:VCALENDAR\nVERSION:2.0\n" + "".join(
        f"BEGIN:VEVENT\nUID:u{i}\nSUMMARY:Show {i}\nDTSTART:{NEXT_WEEK.replace('-', '')}T190000Z\nEND:VEVENT\n" for i in range(3)) + "END:VCALENDAR\n"
    f = _probe_fetcher({
        "https://b.test/whats-on": '<html><head><link rel="alternate" type="text/calendar" href="/feed.ics"></head><body>no jsonld</body></html>',
        "https://b.test/feed.ics": ics,
    })
    res = probe_domain("b.test", ["https://b.test/whats-on"], "Ireland", fetcher=f)
    assert res["hit"]["detector"] == "ics" and res["hit"]["feed"] == "https://b.test/feed.ics"
    doc = build_recipe("b.test", res["hit"], ["https://b.test/whats-on"], "Ireland", None, 12)
    assert validate_recipe(doc) == [] and doc["parse"]["kind"] == "ics" and doc["source_name"] == "ics_b_test"


def test_probe_finds_wp_events_calendar_rest():
    from app.services.recipes.probe import probe_domain, build_recipe
    events = [{"id": i, "title": f"Gig {i}", "start_date": f"{NEXT_WEEK} 20:00:00", "end_date": f"{NEXT_WEEK} 22:00:00",
               "url": f"https://c.test/event/{i}", "cost": "$10", "venue": {"venue": "Barn", "city": "Austin"},
               "categories": [{"name": "Music"}], "image": {"url": "https://c.test/i.jpg"}} for i in range(5)]
    f = _probe_fetcher({
        "https://c.test/calendar": '<html><link rel="stylesheet" href="/wp-content/themes/x.css"><div class="tribe-events">…</div></html>',
        "https://c.test/wp-json/tribe/events/v1/events?per_page=50&start_date=now": json.dumps({"events": events, "next_rest_url": None}),
    })
    res = probe_domain("c.test", ["https://c.test/calendar"], "United States", fetcher=f)
    assert res["hit"]["detector"] == "tribe_rest" and res["hit"]["events"] == 5
    doc = build_recipe("c.test", res["hit"], ["https://c.test/calendar"], "United States", "Austin", 0)
    assert validate_recipe(doc) == [] and doc["entry"]["paginate"]["mode"] == "next_url"
    # and the built recipe actually runs against the same fake API
    r = run_recipe(doc, fetcher=_probe_fetcher(f.pages))
    assert r.fatal is None and len(r.events) == 5 and r.events[0].venue_city == "Austin" and r.events[0].price == 10.0


def test_probe_none_when_nothing_free_and_budget_respected():
    from app.services.recipes.probe import probe_domain
    f = _probe_fetcher({"https://d.test/": "<html><body>plain listing, no structured data</body></html>",
                        "https://d.test/2": "<html><body>still nothing</body></html>"})
    res = probe_domain("d.test", ["https://d.test/", "https://d.test/2", "https://d.test/3"], "Germany", fetcher=f)
    assert res["hit"] is None and res["pages_checked"] == 2 and res["requests"] <= 8 and res["error"] is None


def test_probe_batch_records_outcomes_and_creates_recipe(tmp_path, monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401
    from app.database import Base
    from app.models import LLMSource, SourceRecipe, SourceProbe
    from app.services.recipes import probe as PR
    engine = create_engine(f"sqlite:///{tmp_path}/p.db"); Base.metadata.create_all(bind=engine)
    db = sessionmaker(bind=engine)()
    db.add(LLMSource(url="https://hit.test/events", state="recurring", last_method="html", country="Spain", events_saved_total=40))
    db.add(LLMSource(url="https://miss.test/cal", state="recurring", last_method="error", country="Spain", events_saved_total=2))
    db.add(LLMSource(url="https://done.test/x", state="graduated", last_method="jsonld", country="Spain"))
    db.add(SourceRecipe(domain="done.test", source_name="ld_done_test", recipe=_base(), recipe_version=1, enabled=True))
    db.commit()
    html_hit = "".join(f'<script type="application/ld+json">{{"@type":"Event","name":"E{i}","startDate":"{NEXT_WEEK}T20:00:00"}}</script>' for i in range(3))
    pages = {"https://hit.test/events": html_hit, "https://miss.test/cal": "<html>nothing</html>"}
    real = PR.probe_domain
    monkeypatch.setattr(PR, "probe_domain", lambda dom, urls, country, fetcher=None: real(dom, urls, country, fetcher=_probe_fetcher(pages)))

    cands = PR.select_candidates(db, 10)
    assert [c["domain"] for c in cands] == ["hit.test", "miss.test"]        # done.test excluded, yield order
    s = PR.run_probe_batch(db, limit=10)
    assert s["probed"] == 2 and s["recipes"] == 1 and s["none"] == 1 and s["by_detector"] == {"jsonld": 1}
    rec = db.query(SourceRecipe).filter_by(domain="hit.test").one()
    assert rec.written_by == "auto-probe" and rec.recipe["parse"]["kind"] == "jsonld"
    pr = {p.domain: p for p in db.query(SourceProbe).all()}
    assert pr["hit.test"].outcome == "recipe" and pr["miss.test"].outcome == "none"
    # LLMSource graduated by the upsert; second batch finds nothing new to probe
    assert db.query(LLMSource).filter_by(url="https://hit.test/events").one().state == "graduated"
    assert PR.select_candidates(db, 10) == []


def test_probe_infers_country_from_events_then_tld():
    from app.services.recipes.probe import probe_domain, infer_country
    html = "".join(f'<script type="application/ld+json">{{"@type":"Event","name":"E{i}","startDate":"{NEXT_WEEK}T20:00:00","location":{{"@type":"Place","name":"H","address":{{"addressLocality":"Haifa","addressCountry":"IL"}}}}}}</script>' for i in range(3))
    f = _probe_fetcher({"https://x.example/events": f"<html>{html}</html>"})
    res = probe_domain("x.example", ["https://x.example/events"], None, fetcher=f)
    assert res["hit"]["country"] == "Israel"                      # from addressCountry IL
    assert infer_country([], "venue.co.uk") == "United Kingdom"    # TLD fallback
    assert infer_country([], "venue.com") is None


# ── 2026-09-18: country aliases, queue pins, no_country cooldown ─────────
def test_canon_country_aliases():
    from app.services.recipes.countries import canon_country
    assert canon_country("Deutschland") == "Germany"
    assert canon_country("United States of America") == "United States"
    assert canon_country("102#Italy") == "Italy"
    assert canon_country(" UK ") == "United Kingdom"
    assert canon_country("Elbonia") == "Elbonia"        # unknown passes through
    assert canon_country(None) is None and canon_country("  ") is None


def test_resolve_city_uses_canonical_country_and_city_fallback(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401
    from app.database import Base
    from app.models import City
    from app.services.recipes.runner import _resolve_city
    engine = create_engine(f"sqlite:///{tmp_path}/c.db"); Base.metadata.create_all(bind=engine)
    db = sessionmaker(bind=engine)()
    db.add(City(name="Munich", country="Germany", latitude=48.1, longitude=11.6))
    db.commit()
    assert _resolve_city(db, "Deutschland", "München").name == "Munich"   # alias + unknown city → any city in country
    assert _resolve_city(db, "102#Germany", None).name == "Munich"
    assert _resolve_city(db, "France", None) is None


def test_select_candidates_pins_first_and_no_country_cooldown(tmp_path):
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401
    from app.database import Base
    from app.models import LLMSource, SourceProbe, QueuePin
    from app.services.recipes import probe as PR
    engine = create_engine(f"sqlite:///{tmp_path}/q.db"); Base.metadata.create_all(bind=engine)
    db = sessionmaker(bind=engine)()
    now = datetime.utcnow()
    db.add(LLMSource(url="https://big.test/events", state="recurring", country="Spain", events_saved_total=500))
    db.add(LLMSource(url="https://small.test/events", state="recurring", country="Deutschland", events_saved_total=5))
    db.add(LLMSource(url="https://nc.test/events", state="recurring", country=None, events_saved_total=900))
    db.add(LLMSource(url="https://old.test/events", state="recurring", country=None, events_saved_total=50))
    # nc.test: a fresh no_country hit → must NOT be re-probed every hour any more
    db.add(SourceProbe(domain="nc.test", outcome="no_country", last_probed_at=now - timedelta(hours=1), detector="jsonld", events_found=7))
    # old.test: no_country but past the 7-day window → eligible again
    db.add(SourceProbe(domain="old.test", outcome="no_country", last_probed_at=now - timedelta(days=8)))
    # small.test was probed 'none' yesterday → parked … unless pinned
    db.add(SourceProbe(domain="small.test", outcome="none", last_probed_at=now - timedelta(days=1)))
    db.add(QueuePin(domain="small.test", rank=2))
    db.add(QueuePin(domain="outside.test", rank=1, country="Israel"))     # not in the pool at all
    # cityonly.test: no country on the row, but the city is unambiguous → country derived
    db.add(LLMSource(url="https://cityonly.test/e", state="recurring", country=None, city_name="Brussels", events_saved_total=40))
    db.add(LLMSource(url="https://ambig.test/e", state="recurring", country=None, city_name="Dublin", events_saved_total=30))
    from app.models import City
    db.add(City(name="Brussels", country="Belgium", latitude=50.8, longitude=4.3))
    db.add(City(name="Dublin", country="Ireland", latitude=53.3, longitude=-6.2))
    db.add(City(name="Dublin", country="United States", latitude=40.1, longitude=-83.1))
    db.commit()

    cands = PR.select_candidates(db, 10)
    doms = [c["domain"] for c in cands]
    assert doms[:2] == ["outside.test", "small.test"], doms           # pins first, by rank
    assert "nc.test" not in doms                                        # cooling down, still no country
    assert doms[2:] == ["big.test", "old.test", "cityonly.test", "ambig.test"]   # then yield order
    by = {c["domain"]: c for c in cands}
    assert by["cityonly.test"]["country"] == "Belgium"
    assert by["ambig.test"]["country"] is None                          # Dublin IE vs Dublin OH: leave it
    # a cooling-down no_country hit becomes eligible as soon as its city resolves a country
    db.add(LLMSource(url="https://nc.test/more", state="recurring", country=None, city_name="Brussels", events_saved_total=1))
    db.commit()
    assert "nc.test" in [c["domain"] for c in PR.select_candidates(db, 10)]
    by = {c["domain"]: c for c in cands}
    assert by["outside.test"]["urls"][0] == "https://outside.test/" and by["outside.test"]["country"] == "Israel"
    assert by["small.test"]["country"] == "Germany"                     # LLMSource alias canonicalised
    assert by["big.test"]["pin_rank"] is None and by["small.test"]["pin_rank"] == 2


def test_parse_pin_lines():
    from app.api.admin import _parse_pin_lines
    lines = _parse_pin_lines("""
        # comment
        eventim.de
        https://www.koelnticket.de/tickets, Deutschland
        somesite.com | United States of America
        Eventim.de
        notadomain
    """)
    assert lines == [("eventim.de", None), ("koelnticket.de", "Germany"), ("somesite.com", "United States")]


# ── relay (off-box fetch) ────────────────────────────────────────────────
def test_fetcher_body_format_form_posts_urlencoded(monkeypatch):
    """WordPress admin-ajax.php reads $_POST: a JSON body is ignored and the
    handler answers '0'. fetch.body_format=form must send data=, not json=."""
    from app.services.recipes.fetch import Fetcher
    calls = []

    class FakeResp:
        url = "https://x.test/wp-admin/admin-ajax.php"; status_code = 200; text = "<ok/>"; headers = {}

    class FakeClient:
        def post(self, url, **kw):
            calls.append(kw); return FakeResp()

        def get(self, url):
            raise AssertionError("GET used for a POST recipe")

    for fmt, key in (("form", "data"), ("json", "json"), (None, "json")):
        cfg = {"method": "POST", "body": {"action": "load_more", "offset": 0}, "delay_seconds": 0.5}
        if fmt:
            cfg["body_format"] = fmt
        f = Fetcher(cfg, respect_robots=False)
        f._client = FakeClient()
        f.get("https://x.test/wp-admin/admin-ajax.php")
        assert list(calls[-1].keys()) == [key], (fmt, calls[-1])
        assert calls[-1][key] == {"action": "load_more", "offset": 0}


def test_detail_hop_gets_even_when_listing_posts():
    """katedra.co.il: the listing is a POST to admin-ajax.php, the detail
    pages are ordinary GET pages. The hop must not inherit fetch.method."""
    from app.services.recipes.fetch import Fetcher
    calls = []

    class FakeResp:
        def __init__(self, url): self.url = url; self.status_code = 200; self.headers = {}
        text = "<html><div class='p'>75 ₪</div></html>"

    class FakeClient:
        def post(self, url, **kw): calls.append(("POST", url)); return FakeResp(url)
        def get(self, url): calls.append(("GET", url)); return FakeResp(url)

    f = Fetcher({"method": "POST", "body_format": "form", "body": {"a": 1}, "delay_seconds": 0.5}, respect_robots=False)
    f._client = FakeClient()
    f.get("https://x.test/ajax")
    f.get("https://x.test/detail/1", method="GET")
    assert calls == [("POST", "https://x.test/ajax"), ("GET", "https://x.test/detail/1")]
    doc = _base()
    doc["fetch"] = {"method": "POST", "body": {"a": 1}, "delay_seconds": 0.5}
    doc["detail"] = {"url_field": "purchase_link", "parse": {"kind": "html", "fields": {"price": {"sel": ".p", "regex": "(\\d+)"}}}}
    doc["parse"]["fields"]["purchase_link"] = {"sel": "a", "attr": "href"}
    listing = f"<div class='ev'><span class='t'>A</span><time datetime='{NEXT_WEEK}'></time><a href='https://example.org/d/1'>x</a></div>"
    ff = FakeFetcher({"https://example.org/events": listing, "https://example.org/d/1": "<div class='p'>75 ₪</div>"})
    res = run_recipe(doc, fetcher=ff)
    assert ff.methods == [None, "GET"] and res.events[0].price == 75.0


def test_validate_recipe_relay_and_body_format():
    doc = _base()
    doc["relay"] = "mac"
    doc["fetch"] = {"method": "POST", "body_format": "form", "body": {"a": 1}}
    assert validate_recipe(doc) == []
    doc["relay"] = "moon"
    assert any(e.startswith("relay:") for e in validate_recipe(doc))
    doc["relay"] = "mac"
    doc["fetch"]["body_format"] = "xml"
    assert any(e.startswith("fetch.body_format") for e in validate_recipe(doc))


def test_result_payload_roundtrip_preserves_events_and_health_counts():
    from app.services.recipes.runner import result_to_payload, result_from_payload
    doc = _base()
    doc["parse"] = {"kind": "api", "items": "", "fields": {
        "source_id": "id", "name": "n", "start_datetime": "d", "venue_name": "v",
        "venue_city": "c", "venue_country": "k", "price": "p", "raw_categories": "cats"}}
    body = json.dumps([{"id": "a1", "n": "Alpha", "d": f"{NEXT_WEEK}T19:30:00", "v": "Hall", "c": "Haifa",
                        "k": "Israel", "p": "75", "cats": ["x", "y"]}])
    res = run_recipe(doc, fetcher=FakeFetcher({"https://example.org/events": body}))
    res.errors.append("detail https://example.org/e/1: HTTP 500")
    payload = json.loads(json.dumps(result_to_payload(res)))     # what travels over HTTPS
    back = result_from_payload(payload, domain=doc["domain"], source_name=doc["source_name"])
    assert back.fetched == res.fetched == 1 and back.requests == res.requests
    assert back.errors == res.errors and back.fatal is None
    a, b = res.events[0], back.events[0]
    assert (a.name, a.start_date, a.start_time, a.venue_name, a.venue_city, a.venue_country, a.price,
            a.price_currency, a.source, a.source_id, a.raw_categories) == \
           (b.name, b.start_date, b.start_time, b.venue_name, b.venue_city, b.venue_country, b.price,
            b.price_currency, b.source, b.source_id, b.raw_categories)
    assert b.start_date == date.fromisoformat(NEXT_WEEK)


def _relay_db(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401
    from app.database import Base
    from app.models import City, SourceRecipe
    engine = create_engine(f"sqlite:///{tmp_path}/relay.db")
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    db.add_all([City(name="Tel Aviv", country="Israel", timezone="Asia/Jerusalem", latitude=32.0, longitude=34.7),
                City(name="Haifa", country="Israel", timezone="Asia/Jerusalem", latitude=32.8, longitude=35.0)])
    doc = _base()
    doc.update({"relay": "mac", "country": "Israel", "city_name": "Tel Aviv", "source_name": "relay_test"})
    db.add(SourceRecipe(domain=doc["domain"], source_name="relay_test", recipe=doc, recipe_version=1,
                        enabled=True, priority=5, cadence_hours=48, country="Israel", city_name="Tel Aviv"))
    db.commit()
    return Session, db


def test_relay_endpoint_persists_through_the_recipe_path(tmp_path, monkeypatch):
    """POST /api/admin/recipes/{domain}/relay: token gate, persist via
    persist_result (venue_city grouping), health columns updated, and the
    known-ids feed reflects what was saved."""
    from fastapi.testclient import TestClient
    from app.main import app
    from app.database import get_db
    from app.config import settings
    from app.models import Event, SourceRecipe
    Session, db = _relay_db(tmp_path)

    def override():
        s = Session()
        try:
            yield s
        finally:
            s.close()
    app.dependency_overrides[get_db] = override
    try:
        client = TestClient(app)
        payload = {"events": [
            {"name": "Lecture A", "start_date": NEXT_WEEK, "start_time": "19:00", "venue_name": "Hall A",
             "venue_city": "Haifa", "venue_country": "Israel", "source": "relay_test", "source_id": "a1",
             "price": 75.0, "price_currency": "ILS", "purchase_link": "https://x.test/a1", "raw_categories": []},
            {"name": "Lecture B", "start_date": NEXT_WEEK, "venue_name": "Hall B", "venue_city": "Tel Aviv",
             "venue_country": "Israel", "source": "relay_test", "source_id": "b1", "raw_categories": []}],
            "rows": 2, "pages": ["https://x.test/events"], "requests": 3, "duration_s": 4.2, "errors": []}
        # disabled when the env var is unset
        monkeypatch.setattr(settings, "RELAY_TOKEN", "")
        assert client.post("/api/admin/recipes/example.org/relay", json=payload).status_code == 503
        monkeypatch.setattr(settings, "RELAY_TOKEN", "s3cret")
        assert client.post("/api/admin/recipes/example.org/relay", json=payload,
                           headers={"X-Relay-Token": "nope"}).status_code == 401
        assert client.get("/api/admin/recipes/example.org/known-ids", headers={"X-Relay-Token": "s3cret"}).json()["known_ids"] == []
        assert client.post("/api/admin/recipes/nope.test/relay", json=payload,
                           headers={"X-Relay-Token": "s3cret"}).status_code == 404
        r = client.post("/api/admin/recipes/example.org/relay", json=payload, headers={"X-Relay-Token": "s3cret"})
        assert r.status_code == 200, r.text
        out = r.json()
        assert out["saved"] == 2 and out["city_groups"] == 2 and out["last_status"] == "ok" and out["relay"] == "mac"
        s = Session()
        evs = {e.source_id: e for e in s.query(Event).filter(Event.scrape_source == "relay_test").all()}
        assert set(evs) == {"a1", "b1"}
        assert evs["a1"].venue.city.name == "Haifa" and evs["b1"].venue.city.name == "Tel Aviv"
        assert evs["a1"].price == 75.0 and evs["a1"].price_currency == "ILS"
        row = s.query(SourceRecipe).filter_by(domain="example.org").one()
        assert (row.runs_total, row.last_fetched, row.last_saved, row.last_requests) == (1, 2, 2, 3)
        assert row.next_run_at is not None and row.consecutive_zero_fetch == 0
        s.close()
        ids = client.get("/api/admin/recipes/example.org/known-ids", headers={"X-Relay-Token": "s3cret"}).json()
        assert sorted(ids["known_ids"]) == ["a1", "b1"] and ids["recipe"]["relay"] == "mac"
        # second post of the same events: nothing new saved, health still ok, no duplicates
        r2 = client.post("/api/admin/recipes/example.org/relay", json=payload, headers={"X-Relay-Token": "s3cret"}).json()
        assert r2["saved"] == 0 and r2["last_status"] in ("ok", "empty")
        s = Session(); assert s.query(Event).filter(Event.scrape_source == "relay_test").count() == 2; s.close()
        # a fatal remote run is recorded as an error run, events untouched
        r3 = client.post("/api/admin/recipes/example.org/relay", headers={"X-Relay-Token": "s3cret"},
                         json={"events": [], "rows": 0, "requests": 1, "fatal": "HTTP 403 at listing"}).json()
        assert r3["fatal"] == "HTTP 403 at listing" and r3["last_status"] == "error"
    finally:
        app.dependency_overrides.clear()
        db.close()
