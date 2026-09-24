"""Typed text equal to a chip label runs as that chip."""
from types import SimpleNamespace as NS

import app.api.cities as cities_mod
import app.api.suggestions as sugg
from app.api._chip_resolve import resolve_typed_terms

CHIPS = {
    "jazz": [{"kind": "genre", "value": "Jazz", "label": "Jazz"},
             {"kind": "performer", "value": "Jazz", "label": "Jazz"}],
    "opera": [{"kind": "genre", "value": "Classical", "label": "Opera"}],
    "ai": [{"kind": "theme", "value": "AI", "label": "AI"}],
    "coldplay": [{"kind": "performer", "value": "Coldplay", "label": "Coldplay"}],
    "barby": [{"kind": "venue", "value": "בארבי", "label": "בארבי — Tel Aviv"}],
    "paris": [{"kind": "performer", "value": "Paris", "label": "Paris"}],
    "jazzy": [{"kind": "genre", "value": "Jazz", "label": "Jazz"}],
}


def _setup(monkeypatch):
    monkeypatch.setattr(sugg, "get_suggestions", lambda q, limit, db: CHIPS.get(q.lower(), []))
    monkeypatch.setattr(cities_mod, "_cache", [NS(id=239, name="Tel Aviv", aliases=["תל אביב"]),
                                               NS(id=5, name="Paris", aliases=[])])


def _r(**kw):
    base = dict(type_search=None, genres=None, themes=None, tournaments=None,
                artist_exact=None, city_ids=None, country=None)
    base.update(kw)
    return resolve_typed_terms(None, **base)


def test_exact_label_becomes_chip(monkeypatch):
    _setup(monkeypatch)
    assert _r(type_search="JAZZ")["genres"] == "Jazz"
    assert _r(type_search="opera")["genres"] == "Classical"
    assert _r(type_search="ai")["themes"] == "AI"
    assert _r(type_search="Coldplay")["artist_exact"] == "Coldplay"
    assert _r(type_search="jazz")["type_search"] is None


def test_non_exact_and_text_kinds_stay_text(monkeypatch):
    _setup(monkeypatch)
    assert _r(type_search="jazzy")["type_search"] == "jazzy"      # prefix, not equal
    assert _r(type_search="barby")["type_search"] == "barby"      # venue: same either way


def test_city_names(monkeypatch):
    _setup(monkeypatch)
    assert _r(type_search="תל אביב")["city_ids"] == "239"
    assert _r(type_search="Paris")["city_ids"] == "5"             # city beats same-name artist
    kept = _r(type_search="Paris", city_ids="239")                # a location is already chosen
    assert kept["city_ids"] == "239" and kept["artist_exact"] == "Paris"
