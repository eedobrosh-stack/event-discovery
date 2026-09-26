"""Live lookup: which recipes can search, and the per-term dedupe."""
from app.services import live_lookup as LL


def test_search_url_explicit_and_tribe():
    kupat = {"entry": {"search": {"url": "https://www.kupat.co.il/api/events?f={q}&d={today}"}}}
    assert LL._search_url(kupat, "פאר טסי").startswith("https://www.kupat.co.il/api/events?f=%D7%A4")
    assert "{today}" not in LL._search_url(kupat, "x y")
    tribe = {"entry": {"urls": ["https://youticket.co.il/wp-json/tribe/events/v1/events?per_page=50"]}}
    assert LL._search_url(tribe, "jazz").endswith("&search=jazz")
    assert LL._search_url({"entry": {"urls": ["https://x.com/events.ics"]}}, "jazz") is None


def test_short_terms_are_skipped():
    assert LL.start("a")["state"] == "skipped"
