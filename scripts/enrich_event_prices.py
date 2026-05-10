"""Enrich Event.price by fetching the event's purchase_link page and
parsing JSON-LD for offers.price.

Symptom (observed 2026-05-10): the catalog has very thin price coverage.
Top-20 most-popular performers had prices_n=0 across 1k+ events apiece.
Many event pages DO publish JSON-LD with an `offers.price` field — but
the original collectors either didn't capture it or didn't navigate to
the detail page. This script closes the gap for events whose
``purchase_link`` is set, by re-fetching the linked page and re-running
the same JSON-LD parser the JSON-LD-aware collectors use at scrape
time.

Pipeline (v1, accuracy-first):
  1. Pick targets — upcoming events with purchase_link set, no price.
     Order by start_date ASC (sooner-first).
  2. For each: fetch the page, run iter_events() to extract any
     JSON-LD events on the page.
  3. Match the right one — same start_date AND artist/name overlap
     with the target event. If exactly one JSON-LD event matches and
     it carries a price, write it to Event.price + Event.price_currency.
     If 0 matches or >1 ambiguous matches: skip — better to have null
     than a wrong price.
  4. No fallback to regex / Gemini / etc. in v1. We see how much
     JSON-LD covers first. Layer-2 fallback follows once we have data.

Cap per run via ``--limit``. Default ``--limit 500`` keeps the wall
time and bandwidth bounded for nightly cron use.

Usage:
    PYTHONPATH=. python3 scripts/enrich_event_prices.py
    PYTHONPATH=. python3 scripts/enrich_event_prices.py --apply
    PYTHONPATH=. python3 scripts/enrich_event_prices.py --apply --limit 200
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("enrich_event_prices")

from sqlalchemy import or_  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event  # noqa: E402
from app.extractors.llm_extractor import _fetch_html  # noqa: E402
from app.services.collectors._jsonld import iter_events  # noqa: E402

# Sanity bounds. A "price" outside this range almost certainly came from
# parsing a wrong number on the page (a phone number, a year, a runtime).
MIN_PRICE = 1.0
MAX_PRICE = 5000.0

# Polite pacing between HTTP fetches — we're walking event pages on
# venue/ticket sites that didn't ask to be re-fetched.
FETCH_DELAY_SEC = 0.6


def _norm_text(s: str | None) -> str:
    return (s or "").strip().lower()


def _matches_target(jsonld_ev, target_event: Event) -> bool:
    """True when a JSON-LD event from the fetched page corresponds to
    the target Event we're trying to enrich.

    Match rules (must satisfy ALL):
      • same start_date — strict, no fuzzy date matching.
      • name OR artist overlap: at least one of the target's name /
        artist_name (lowercased) is a substring of the JSON-LD event's
        name OR artist, or vice-versa. Substring rather than equality
        because event-detail pages often append "Live in <City>" or
        " - <YYYY>" and we shouldn't reject those.
    """
    if jsonld_ev.start_date != target_event.start_date:
        return False
    target_strs = [
        _norm_text(target_event.name),
        _norm_text(target_event.artist_name),
    ]
    target_strs = [s for s in target_strs if s]
    cand_strs = [
        _norm_text(jsonld_ev.name),
        _norm_text(jsonld_ev.artist_name),
    ]
    cand_strs = [s for s in cand_strs if s]
    if not target_strs or not cand_strs:
        return False
    for t in target_strs:
        for c in cand_strs:
            if t in c or c in t:
                return True
    return False


def _is_valid_price(price: float | None) -> bool:
    if price is None:
        return False
    try:
        p = float(price)
    except (TypeError, ValueError):
        return False
    return MIN_PRICE <= p <= MAX_PRICE


def _fetch_targets(db, limit: int):
    """Upcoming events with purchase_link set + price still NULL."""
    today = date.today()
    return (
        db.query(Event)
        .filter(
            Event.start_date >= today,
            Event.purchase_link.isnot(None),
            Event.purchase_link != "",
            or_(Event.price.is_(None), Event.price == 0),
        )
        .order_by(Event.start_date.asc(), Event.id.asc())
        .limit(limit)
        .all()
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    parser.add_argument("--limit", type=int, default=500,
                        help="Cap on events processed (default 500). Each "
                             "event is one HTTP fetch — keep this bounded "
                             "for wall-time / politeness.")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    log.info(f"mode={mode} limit={args.limit}")

    db = SessionLocal()
    stats = {
        "targeted":          0,
        "fetch_errors":      0,
        "no_jsonld_events":  0,
        "no_match":          0,
        "ambiguous_match":   0,
        "match_no_price":    0,
        "match_invalid_price": 0,
        "wrote_price":       0,
    }
    samples = []  # [(event_id, name, old_price, new_price, currency, source_url)]

    try:
        targets = _fetch_targets(db, args.limit)
        log.info(f"target events (upcoming, purchase_link set, price null): "
                 f"{len(targets):,}")

        for i, ev in enumerate(targets, start=1):
            stats["targeted"] += 1
            html = _fetch_html(ev.purchase_link)
            if not html:
                stats["fetch_errors"] += 1
                continue

            jsonld_events = list(iter_events(html, future_only=False))
            if not jsonld_events:
                stats["no_jsonld_events"] += 1
                continue

            matches = [j for j in jsonld_events if _matches_target(j, ev)]
            if len(matches) == 0:
                stats["no_match"] += 1
                continue
            if len(matches) > 1:
                stats["ambiguous_match"] += 1
                continue

            matched = matches[0]
            price = matched.price
            currency = matched.price_currency or ev.price_currency or "USD"

            if price is None:
                stats["match_no_price"] += 1
                continue
            if not _is_valid_price(price):
                stats["match_invalid_price"] += 1
                log.debug(f"rejected price={price!r} for ev_id={ev.id} {ev.name!r}")
                continue

            stats["wrote_price"] += 1
            samples.append({
                "event_id": ev.id,
                "name": ev.name,
                "artist_name": ev.artist_name,
                "start_date": str(ev.start_date),
                "old_price": ev.price,
                "new_price": float(price),
                "currency": currency,
                "purchase_link": ev.purchase_link,
            })

            if args.apply:
                ev.price = float(price)
                ev.price_currency = currency
                # Commit per-event so a fetch failure halfway through
                # doesn't roll back hours of progress.
                db.commit()

            if i % 25 == 0:
                log.info(
                    f"  progress {i}/{len(targets)}  "
                    f"wrote={stats['wrote_price']}  "
                    f"no_match={stats['no_match']}  "
                    f"no_jsonld={stats['no_jsonld_events']}  "
                    f"fetch_err={stats['fetch_errors']}"
                )

            time.sleep(FETCH_DELAY_SEC)

        log.info(
            "done. "
            f"targeted={stats['targeted']}  "
            f"wrote_price={stats['wrote_price']}  "
            f"fetch_errors={stats['fetch_errors']}  "
            f"no_jsonld={stats['no_jsonld_events']}  "
            f"no_match={stats['no_match']}  "
            f"ambiguous={stats['ambiguous_match']}  "
            f"match_no_price={stats['match_no_price']}  "
            f"match_invalid_price={stats['match_invalid_price']}"
        )

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = "apply" if args.apply else "dryrun"
        audit_path = ROOT / "data" / f"enrich_event_prices_{ts}_{suffix}.json"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(json.dumps({
            "stats": stats,
            "samples": samples[:50],   # cap audit size
            "samples_total": len(samples),
        }, ensure_ascii=False, indent=2))
        log.info(f"audit written: {audit_path}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
