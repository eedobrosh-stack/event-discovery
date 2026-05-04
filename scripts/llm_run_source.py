"""Manually run the LLM extractor against one URL → ingest events → upsert
the LLMSource registry row.

This is the trial-state driver from the Route 1 architecture (Step 4):

  • Onboard a candidate by running it once and watching what comes out.
  • Re-run it to verify stability across days.
  • Promote with --promote (state=recurring) when ready for the scheduler.
  • Block with --block when it's hallucinating or dups exhaustively.

Usage:
    python3 scripts/llm_run_source.py URL --city "Tel Aviv" --country Israel
    python3 scripts/llm_run_source.py URL --dry-run        # no DB writes
    python3 scripts/llm_run_source.py URL --promote        # state → recurring
    python3 scripts/llm_run_source.py URL --block "..."    # state → blocked

When --city is omitted the source is treated as nationwide (city_name=NULL
on the LLMSource row); each event's venue_city, when present, drives
city resolution downstream.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Reuse the .env loader pattern used by other scripts.
ENV_PATH = ROOT / ".env"
if ENV_PATH.is_file():
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from app.database import SessionLocal, Base, engine                   # noqa: E402
from app.models import City, LLMSource, LLM_SOURCE_STATES              # noqa: E402
from app.extractors.llm_extractor import extract_auto, ExtractorUnconfigured  # noqa: E402
from app.services.collectors.registry import CollectorRegistry         # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("llm_run_source")


def _ensure_tables():
    """create_all() once on first run so a fresh dev DB has llm_sources."""
    Base.metadata.create_all(bind=engine)


def _resolve_city(db, city_name: str | None, country: str | None) -> City | None:
    if not city_name:
        return None
    q = db.query(City).filter(City.name == city_name)
    if country:
        q = q.filter(City.country == country)
    city = q.first()
    if not city and country:
        log.warning(
            f"city {city_name!r} (country={country!r}) not in DB. "
            "Events will still ingest but city resolution falls back to "
            "venue_city per-event."
        )
    return city


def _upsert_source_row(db, *, url, city_name, country) -> LLMSource:
    src = db.query(LLMSource).filter(LLMSource.url == url).first()
    if src:
        return src
    src = LLMSource(
        url=url,
        city_name=city_name,
        country=country,
        state="trial",
        runs_total=0,
        events_seen_total=0,
        events_saved_total=0,
    )
    db.add(src)
    db.commit()
    db.refresh(src)
    log.info(f"new LLMSource (state=trial): {url}")
    return src


def _record_run(db, src: LLMSource, *, result, saved: int) -> None:
    """Update the LLMSource row with the latest run's outcome."""
    src.last_run_at = datetime.utcnow()
    src.runs_total = (src.runs_total or 0) + 1
    src.last_event_count = len(result.events)
    src.last_method = result.method
    src.last_error = result.error[:500] if result.error else None
    src.events_seen_total = (src.events_seen_total or 0) + len(result.events)
    src.events_saved_total = (src.events_saved_total or 0) + saved
    src.has_pagination = bool(result.has_pagination)
    src.pagination_signal = result.pagination_signal
    src.next_page_url = (result.next_page_url or "")[:1000] or None

    # Streak counters — symmetric reset; the scheduler uses these for
    # auto-demote (consecutive_empty_runs) and auto-promote
    # (consecutive_success_runs). A "successful run" = events extracted
    # AND no error.
    run_was_successful = bool(result.events) and not result.error
    if run_was_successful:
        src.consecutive_success_runs = (src.consecutive_success_runs or 0) + 1
        src.consecutive_empty_runs = 0
    else:
        src.consecutive_empty_runs = (src.consecutive_empty_runs or 0) + 1
        src.consecutive_success_runs = 0
    db.commit()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("url", help="URL of an events listing page")
    ap.add_argument("--city", help="City name in our DB, e.g. 'Tel Aviv'")
    ap.add_argument("--country", help="Country (full name, e.g. 'Israel')")
    ap.add_argument("--max-events", type=int, default=50)
    ap.add_argument("--model", default="gemini-2.5-flash")
    ap.add_argument("--dry-run", action="store_true",
                    help="Extract + print, but don't persist events or update LLMSource")
    ap.add_argument("--promote", action="store_true",
                    help="On success, set LLMSource.state='recurring'")
    ap.add_argument("--block", metavar="REASON",
                    help="Set LLMSource.state='blocked' with REASON in notes")
    ap.add_argument("--note", help="Free-form note to append to LLMSource.notes")
    args = ap.parse_args()

    _ensure_tables()

    log.info(f"extracting from {args.url}…")
    try:
        # extract_auto so manual onboarding sees JSON-LD wins for free,
        # exactly as the scheduled recurring job does. Earlier the script
        # called plain extract() which always hit the LLM — masked sources
        # like allevents.in/tel-aviv that actually have JSON-LD events.
        result = extract_auto(
            args.url, source_name="llm_extractor",
            model=args.model, max_events=args.max_events,
        )
    except ExtractorUnconfigured as e:
        sys.exit(f"extractor unconfigured: {e}")

    log.info(
        f"  method={result.method}  events={len(result.events)}  "
        f"raw={result.raw_html_bytes:,}B  cleaned={result.cleaned_html_bytes:,}B  "
        f"hallucination_dropped={result.dropped_for_hallucination}"
    )
    if result.has_pagination:
        log.info(
            f"  pagination: signal={result.pagination_signal} "
            f"next={result.next_page_url or '(not extractable)'}"
        )
    if result.error:
        log.warning(f"  error: {result.error}")

    print(f"\n📋  Extracted {len(result.events)} events from {args.url} "
          f"(via {result.method})\n")
    for i, ev in enumerate(result.events, 1):
        date_part = str(ev.start_date)
        if ev.start_time:
            date_part += f" {ev.start_time}"
        print(f"  {i:2}. [{date_part}] {ev.name}")
        if ev.venue_name:
            print(f"      venue: {ev.venue_name}")
        if ev.artist_name:
            print(f"      artist: {ev.artist_name}")
        if ev.price is not None:
            print(f"      price: {ev.price} {ev.price_currency or ''}")

    if args.dry_run:
        print("\n(dry run — no DB writes, LLMSource not updated)")
        return 0

    # ── Persist ──────────────────────────────────────────────────────────
    db = SessionLocal()
    try:
        src = _upsert_source_row(
            db, url=args.url, city_name=args.city, country=args.country,
        )

        # Promote / block flags take effect even on empty extracts —
        # operators may want to adjust state without re-extracting.
        if args.promote:
            src.state = "recurring"
            log.info(f"  state → recurring")
        if args.block:
            src.state = "blocked"
            blocked_note = f"[blocked {datetime.utcnow().date()}] {args.block}"
            src.notes = (src.notes + "\n" + blocked_note) if src.notes else blocked_note
            log.info(f"  state → blocked: {args.block!r}")
        if args.note:
            stamped = f"[{datetime.utcnow().date()}] {args.note}"
            src.notes = (src.notes + "\n" + stamped) if src.notes else stamped

        # Save events via the same path the scheduled collectors use, so
        # cross-source dedup, venue mapping, normalization, etc. all run.
        saved = 0
        if result.events:
            city = _resolve_city(db, args.city, args.country)
            if city is None:
                # _save_events requires a City object. For nationwide sources
                # the user can pass --country and we'll grab the first matching
                # city (events will dedup against any matching venue_city
                # downstream).
                if args.country:
                    city = db.query(City).filter(
                        City.country == args.country,
                    ).first()
                if city is None:
                    sys.exit(
                        "Cannot persist without a resolvable City. "
                        "Pass --city and/or --country, or use --dry-run."
                    )
            registry = CollectorRegistry()
            saved = registry._save_events(result.events, city, db)
            log.info(f"  saved={saved}/{len(result.events)} after dedup")

        _record_run(db, src, result=result, saved=saved)

        # Final state summary
        print(
            f"\n→ LLMSource id={src.id}  state={src.state}  "
            f"runs_total={src.runs_total}  "
            f"last_event_count={src.last_event_count}  "
            f"events_saved_total={src.events_saved_total}"
        )
        if src.notes:
            print(f"  notes: {src.notes}")
    finally:
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
