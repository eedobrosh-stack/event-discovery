"""Artist-spelling canonicalisation (ingest + backfill) and duplicate-city
merge planning — added 2026-09-19 after the first QA run found 394 artists
under 2+ spellings and 90+ city rows that were the same place."""
from __future__ import annotations

from collections import Counter

import pytest


# ── artist spellings ──────────────────────────────────────────────────────
def test_artist_key_unifies_case_punct_diacritics_but_keeps_leading_the():
    from scripts.canonicalize_artist_names import artist_key
    assert artist_key("RUSH") == artist_key("Rush") == "rush"
    assert artist_key("Six: The Musical") == artist_key("SIX the Musical")
    assert artist_key("Romeo & Juliet") == artist_key("Romeo and Juliet")
    assert artist_key("André Rieu") == artist_key("Andre Rieu")
    assert artist_key("Public Image Ltd.") == artist_key("Public Image Ltd")
    assert artist_key("The Beat") != artist_key("BEAT")          # different acts
    assert artist_key("הדרדסים LIVE") == artist_key("הדרדסים Live")  # non-Latin kept


def test_pick_canonical_prefers_frequency_then_known_then_lowercase():
    from scripts.canonicalize_artist_names import pick_canonical
    assert pick_canonical(Counter({"RUSH": 3, "Rush": 10}), known=set()) == "Rush"
    assert pick_canonical(Counter({"RUSH": 5, "Rush": 5}), known={"RUSH"}) == "RUSH"
    assert pick_canonical(Counter({"CARL COX": 2, "Carl Cox": 2}), known=set()) == "Carl Cox"


def test_ingest_reuses_known_spelling(tmp_path):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import app.models  # noqa: F401
    from app.database import Base
    from app.models import Performer
    from app.models.genre import ArtistGenre
    from app.services.collectors.registry import canonical_artist_spelling
    engine = create_engine(f"sqlite:///{tmp_path}/a.db"); Base.metadata.create_all(bind=engine)
    db = sessionmaker(bind=engine)()
    db.add(ArtistGenre(artist_name="Bill Bailey", normalized_name="bill bailey", primary_genre="Comedy"))
    db.add(Performer(name="Carl Cox", normalized_name="carl cox"))
    db.commit()
    assert canonical_artist_spelling(db, "BILL BAILEY") == "Bill Bailey"      # artist_genre wins
    assert canonical_artist_spelling(db, "  carl cox ") == "Carl Cox"          # performers fallback
    assert canonical_artist_spelling(db, "Brand New Act") == "Brand New Act"   # unknown → unchanged
    assert canonical_artist_spelling(db, None) is None and canonical_artist_spelling(db, "  ") == "  "
    # second lookup is served from the per-session cache
    assert db._artist_spelling_cache["bill bailey"] == "Bill Bailey"


# ── duplicate cities ──────────────────────────────────────────────────────
def _city(id, name, country, venues, upcoming=0, state=None, canonical=None):
    return {"id": id, "name": name, "country": country, "state": state, "canonical_city_id": canonical,
            "parent_city_id": None, "venues": venues, "upcoming": upcoming}


def test_city_merge_plan_rules():
    from scripts.merge_duplicate_cities import plan, name_key
    assert name_key("Köln") == name_key("Cologne") == "cologne"
    assert name_key("São Paulo") == name_key("Sao Paulo")
    assert name_key("渋谷区") != name_key("大阪市")                  # non-Latin names stay distinct
    cities = [
        _city(1, "Cologne", "Germany", 90), _city(2, "Köln", "Germany", 804),          # exonym wins
        _city(3, "Montreal", "Canada", 485), _city(4, "Montréal", "Canada", 434),      # priority spelling / more venues
        _city(5, "Prague", "Czech Republic", 531), _city(6, "Prague", "Czechia", 79),  # country variants → one group
        _city(7, "Springfield", "United States", 10, state="IL"), _city(8, "Springfield", "United States", 5, state="MO"),
        _city(9, "Zürich", "Switzerland", 128, canonical=10), _city(10, "Zurich", "Switzerland", 445),  # already linked → skipped
        _city(11, "Łódź", "Poland", 11), _city(12, "Łódź", "Poland", 1), _city(13, "Łódź", "Poland", 1),
    ]
    merges = {m["key"]: m for m in plan(cities)}
    assert merges["cologne"]["canonical"]["id"] == 1 and [d["id"] for d in merges["cologne"]["dups"]] == [2]
    assert merges["montreal"]["canonical"]["id"] == 3
    assert merges["prague"]["canonical"]["id"] == 5 and [d["id"] for d in merges["prague"]["dups"]] == [6]
    assert "springfield" not in merges                              # different states, not a duplicate
    assert "zurich" not in merges                                   # canonical_city_id already set
    lodz = merges[name_key("Łódź")]                                  # "ł" has no NFKD decomposition; key is "łodz"
    assert lodz["canonical"]["id"] == 11 and len(lodz["dups"]) == 2
