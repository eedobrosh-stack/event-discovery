"""Bulk-seed LLMSource rows from a curated list of regional aggregators.

Lever 2 of the source-inventory growth path: bypass Gemini's roulette
and onboard URLs we KNOW are high-yield event calendars. The seed list
below is hand-picked from public super-aggregators (Time Out city pages,
official tourism boards, large dedicated venues).

Behaviour:
  • Idempotent — skips URLs already in the registry.
  • Skips reserved-domain URLs (a hand-coded collector already covers them).
  • Probes each URL with the same logic as Cadence B (JSON-LD count OR
    visible-content heuristic). URLs that fail both gates are reported
    and NOT registered, so the registry stays clean.
  • Registered rows go in as state='trial' with a clear seed-attribution
    note, so the recurring-extraction job picks them up on the next cycle.

Run:
    cd /Users/eedo.b/supercaly && PYTHONPATH=. python3 scripts/seed_llm_sources.py
"""
from __future__ import annotations

import logging
from datetime import datetime
from dotenv import load_dotenv

# Load .env explicitly so api keys + DB url are available even when invoked
# outside the FastAPI app boot path.
load_dotenv("/Users/eedo.b/supercaly/.env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("seed_llm_sources")

from app.database import SessionLocal  # noqa: E402
from app.models.llm_source import LLMSource  # noqa: E402
from app.extractors.llm_extractor import _fetch_html  # noqa: E402
from app.extractors.discovery import looks_like_event_listing  # noqa: E402
from app.services.collectors._jsonld import iter_events, detect_pagination  # noqa: E402
from app.scheduler.jobs import _is_reserved_discovery_url  # noqa: E402

# Curated seed list — (city, country, url, source_type, language, why)
#
# Picking criteria:
#   • Domain is NOT in our reserved-domain list (those are already covered
#     by hand-coded collectors).
#   • Public site with a stable "what's on" / events page.
#   • Reasonable expectation of either JSON-LD or visible event listings.
#
# Notes on Time Out: their /things-to-do path tends to be heavily React-
# rendered. Some pages will fail the visible-content heuristic — those
# get reported and skipped, no registry pollution.
SEED_URLS: list[tuple[str, str, str, str, str, str]] = [
    # ── Time Out city pages (global aggregator) ──
    ("New York", "United States",
     "https://www.timeout.com/newyork/things-to-do",
     "city_magazine", "en", "Time Out New York — daily-curated event listings"),
    ("Los Angeles", "United States",
     "https://www.timeout.com/los-angeles/things-to-do",
     "city_magazine", "en", "Time Out Los Angeles"),
    ("Chicago", "United States",
     "https://www.timeout.com/chicago/things-to-do",
     "city_magazine", "en", "Time Out Chicago"),
    ("San Francisco", "United States",
     "https://www.timeout.com/san-francisco/things-to-do",
     "city_magazine", "en", "Time Out San Francisco"),
    ("London", "United Kingdom",
     "https://www.timeout.com/london/things-to-do",
     "city_magazine", "en", "Time Out London"),
    ("Manchester", "United Kingdom",
     "https://www.timeout.com/manchester/things-to-do",
     "city_magazine", "en", "Time Out Manchester"),
    ("Paris", "France",
     "https://www.timeout.com/paris/things-to-do",
     "city_magazine", "en", "Time Out Paris (English)"),
    ("Rome", "Italy",
     "https://www.timeout.com/rome/things-to-do",
     "city_magazine", "en", "Time Out Rome"),
    ("Madrid", "Spain",
     "https://www.timeout.com/madrid/things-to-do",
     "city_magazine", "en", "Time Out Madrid"),
    ("Barcelona", "Spain",
     "https://www.timeout.com/barcelona/things-to-do",
     "city_magazine", "en", "Time Out Barcelona"),
    ("Amsterdam", "Netherlands",
     "https://www.timeout.com/amsterdam/things-to-do",
     "city_magazine", "en", "Time Out Amsterdam"),
    ("Lisbon", "Portugal",
     "https://www.timeout.com/lisbon/things-to-do",
     "city_magazine", "en", "Time Out Lisbon"),
    ("Istanbul", "Turkey",
     "https://www.timeout.com/istanbul/things-to-do",
     "city_magazine", "en", "Time Out Istanbul"),
    ("São Paulo", "Brazil",
     "https://www.timeout.com/sao-paulo/coisas-fazer",
     "city_magazine", "pt", "Time Out São Paulo (Portuguese)"),
    ("Buenos Aires", "Argentina",
     "https://www.timeout.com/buenos-aires/cosas-para-hacer",
     "city_magazine", "es", "Time Out Buenos Aires (Spanish)"),
    ("Mexico City", "Mexico",
     "https://www.timeout.com/ciudad-de-mexico/cosas-que-hacer",
     "city_magazine", "es", "Time Out Mexico City (Spanish)"),
    ("Sydney", "Australia",
     "https://www.timeout.com/sydney/things-to-do",
     "city_magazine", "en", "Time Out Sydney"),
    ("Melbourne", "Australia",
     "https://www.timeout.com/melbourne/things-to-do",
     "city_magazine", "en", "Time Out Melbourne"),
    ("Tel Aviv", "Israel",
     "https://www.timeout.com/israel/things-to-do",
     "city_magazine", "en", "Time Out Israel"),

    # ── Local tourism boards / official what's-on portals ──
    ("Berlin", "Germany",
     "https://www.visitberlin.de/en/events-berlin",
     "tourism_board", "en", "visitBerlin official events portal"),
    ("Amsterdam", "Netherlands",
     "https://www.iamsterdam.com/en/whats-on/events",
     "tourism_board", "en", "I Amsterdam official events"),
    ("Paris", "France",
     "https://www.parisinfo.com/what-to-do-in-paris/events-and-festivals-in-paris",
     "tourism_board", "en", "Paris tourist office events"),
    ("Rome", "Italy",
     "https://www.turismoroma.it/en/events",
     "tourism_board", "en", "Roma tourist office events"),
    ("Sydney", "Australia",
     "https://www.sydney.com/events",
     "tourism_board", "en", "Destination Sydney events portal"),
    ("Melbourne", "Australia",
     "https://www.thatsmelbourne.com.au/whats-on",
     "tourism_board", "en", "City of Melbourne what's on"),

    # ── Major dedicated calendars / lifestyle sites ──
    ("London", "United Kingdom",
     "https://www.barbican.org.uk/whats-on",
     "venue", "en", "Barbican Centre — major London arts venue calendar"),
    ("London", "United Kingdom",
     "https://www.southbankcentre.co.uk/whats-on",
     "venue", "en", "Southbank Centre London — major arts complex"),
    ("New York", "United States",
     "https://www.lincolncenter.org/calendar",
     "venue", "en", "Lincoln Center NYC — major performing arts complex"),
    ("Los Angeles", "United States",
     "https://www.discoverlosangeles.com/things-to-do/events/calendar",
     "tourism_board", "en", "Discover Los Angeles events calendar"),
    ("Athens", "Greece",
     "https://www.thisisathens.org/events",
     "tourism_board", "en", "This is Athens (city of Athens what's on)"),
]


def main() -> None:
    db = SessionLocal()
    stats = {
        "input": len(SEED_URLS),
        "registered": 0,
        "registered_jsonld": 0,
        "registered_visible": 0,
        "skipped_existing": 0,
        "skipped_reserved": 0,
        "fetch_errors": 0,
        "no_signal": 0,
    }
    failures: list[tuple[str, str]] = []

    try:
        for city, country, url, source_type, language, why in SEED_URLS:
            url = url.strip()

            if _is_reserved_discovery_url(url):
                stats["skipped_reserved"] += 1
                log.info(f"skip-reserved: {url} (covered by hand-coded collector)")
                continue

            existing = db.query(LLMSource).filter(LLMSource.url == url).first()
            if existing:
                stats["skipped_existing"] += 1
                log.info(f"skip-existing: {url} (already registered as id={existing.id}, state={existing.state})")
                continue

            html = _fetch_html(url)
            if not html:
                stats["fetch_errors"] += 1
                failures.append((url, "fetch_failed"))
                log.warning(f"fetch-failed: {url}")
                continue

            ld_events = list(iter_events(html, future_only=True))
            jsonld_pass = len(ld_events) >= 3
            visible_pass, visible_reason = (False, "")
            if not jsonld_pass:
                visible_pass, visible_reason = looks_like_event_listing(html, url)

            if not (jsonld_pass or visible_pass):
                stats["no_signal"] += 1
                failures.append((url, f"no-signal: {visible_reason}"))
                log.warning(f"no-signal: {url} ({visible_reason})")
                continue

            pag = detect_pagination(html, base_url=url)
            if jsonld_pass:
                method = f"jsonld ({len(ld_events)} events)"
                stats["registered_jsonld"] += 1
            else:
                method = f"visible ({visible_reason})"
                stats["registered_visible"] += 1

            note = (
                f"[seeded {datetime.utcnow().date()}] "
                f"{source_type} / {language} — {why} "
                f"[via {method}] (lever 2 manual seed)"
            )
            new_src = LLMSource(
                url=url,
                city_name=city,
                country=country,
                state="trial",
                runs_total=0,
                events_seen_total=0,
                events_saved_total=0,
                has_pagination=bool(pag["has_pagination"]),
                pagination_signal=pag["signal"],
                next_page_url=(pag["next_page_url"] or "")[:1000] or None,
                notes=note,
            )
            db.add(new_src)
            db.commit()
            stats["registered"] += 1
            log.info(f"seeded: {url} via {method} → state=trial for {city}")
    finally:
        db.close()

    # Final report
    print()
    print("=" * 70)
    print("SEED RUN SUMMARY")
    print("=" * 70)
    for k, v in stats.items():
        print(f"  {k:>20} = {v}")
    if failures:
        print()
        print(f"{len(failures)} URLs did not register:")
        for url, reason in failures:
            print(f"  ✗ {url}\n      {reason}")


if __name__ == "__main__":
    main()
