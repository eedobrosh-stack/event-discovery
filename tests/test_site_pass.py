"""dedupe_venues --site-pass: site keys, page folding, place reading."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import dedupe_venues as DV  # noqa: E402
from app.services.il_places import canon_place, name_places  # noqa: E402


def test_junk_urls_identify_nothing():
    for u in ("https://www.waze.com/ul?ll=32.1", "https://youtube.com/watch?v=x",
              "https://maps.apple.com/?q=a", "https://linktr.ee/club"):
        assert DV._is_junk_url(u)
        assert DV._site_key(u)[0] == ""


def test_own_site_keys_on_host_shared_site_on_page():
    assert DV._site_key("https://www.d-one.co.il/shows") == ("d-one.co.il", False)
    assert DV._site_key("https://hotcinema.co.il/theater/2") == ("hotcinema.co.il/theater/2", True)
    assert DV._site_key("https://hotcinema.co.il/theater/16")[0] != DV._site_key("https://hotcinema.co.il/theater/2")[0]
    # a shared site's homepage says nothing about which venue
    assert DV._site_key("https://www.tel-aviv.gov.il/en/Pages/HomePage.aspx")[0] == ""
    assert DV._site_key("https://www.secrettelaviv.com")[0] == ""


def test_page_key_folds_language_and_homepage():
    assert DV._page_key("https://www.habima.co.il/en/homepage/") == DV._page_key("https://habima.co.il/")
    assert DV._page_key("https://www.shows.org.il/") == DV._page_key("https://shows.org.il")


def test_places():
    assert canon_place("תל אביב-יפו") == canon_place("Tel Aviv-Jaffa") == "Tel Aviv"
    assert canon_place("", "Haifa") == "Haifa"
    assert name_places("היכל התרבות אופקים") != name_places("היכל התרבות נתיבות")
    assert name_places("בארבי נמל יפו") == name_places("בארבי תל אביב") == {"Tel Aviv"}
    # everyday words are not read as places inside names
    assert name_places("מוזיאון ארץ ישראל") == set()


def test_identity_words_drop_kind_and_place():
    assert DV._identity_words("היכל התרבות, כפר יונה") == set()
    assert "d" in DV._identity_words("אולם דיוואן D-one") or "one" in DV._identity_words("אולם דיוואן D-one")


def test_strip_place_suffix_keeps_city_that_is_the_name():
    assert DV._strip_place_suffix("תיאטרון הקאמרי, תל אביב-יפו", "Tel Aviv") == "תיאטרון הקאמרי"
    assert DV._strip_place_suffix("תיאטרון חיפה חיפה", "Haifa") == "תיאטרון חיפה"
    assert DV._strip_place_suffix("היכל התרבות נתניה", "Netanya") == "היכל התרבות נתניה"
    assert DV._strip_place_suffix("Jimmy who tel aviv, תל אביב-יפו", "Tel Aviv") == "Jimmy who"


def test_display_name_override_and_majority():
    shablul = [{"name": n} for n in ("מועדון שבלול", "מועדון שבלול תל אביב", "שבלול ג'אז")]
    assert DV._display_name(shablul, "Tel Aviv", "shablul.smarticket.co.il") == "שבלול ג'אז"
    barby = [{"name": n} for n in ("Barby", "בארבי נמל יפו", "בארבי תל אביב", "מועדון הבארבי")]
    assert DV._display_name(barby, "Tel Aviv", "barby.co.il") == "בארבי"
    # a 1-of-2 Hebrew spelling does not replace the survivor's name
    pair = [{"name": "Hilton Tel Aviv"}, {"name": "מועדון היסוד"}]
    assert DV._display_name(pair, "Tel Aviv", "hilton.com") == "Hilton"


def test_site_move_rewrites_old_host():
    assert DV.SITE_MOVES["shabluljazz.com"].startswith("https://shablul.smarticket.co.il")


def test_canon_place_keeps_unmapped_spelling():
    assert canon_place("Israel - Other") == "Israel - Other"
    assert canon_place("", "Israel - Other") == "Israel - Other"
    assert canon_place("תל אביב -יפו") == "Tel Aviv"
