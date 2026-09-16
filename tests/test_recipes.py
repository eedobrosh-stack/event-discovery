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

    def get(self, url, *, values=None):
        self.requests_made += 1
        self.urls.append(url)
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
    groups, unresolved = group_events_by_city(db, events, "Israel", tlv)
    by_name = {c.name: sorted(e.name for e in evs) for c, evs in groups}
    assert by_name == {"Beersheba": ["a"], "Tel Aviv": ["b", "c", "d"]}
    assert unresolved == {"Nowhere": 1}
