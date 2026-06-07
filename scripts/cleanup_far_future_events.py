#!/usr/bin/env python3
"""One-time cleanup: delete events dated implausibly far in the future.

Pairs with the MAX_FUTURE_DAYS write-time bound added to
app/services/collectors/registry._save_events on 2026-06-07. Removes the
rows that predate that bound — date-parse errors (LLM hallucinating years
like 3030/6960, tel_aviv_venues misreading blog dates as 2382) and
Ticketmaster placeholder/accounting SKUs pinned at 2099-12-31.

Threshold matches the write bound exactly: start_date > today + 1095d (~3y).
This keeps the legitimate 2-3y conference inventory (DigiMarCon 2028,
NBAA 2028, etc.) while removing the long-tail garbage.

ORM-driven delete so the event_themes / event_event_types m2m association
rows cascade (same approach as dedupe_events.py).

Usage:
    PYTHONPATH=. python3 scripts/cleanup_far_future_events.py            # dry-run
    PYTHONPATH=. python3 scripts/cleanup_far_future_events.py --apply    # delete
"""
import sys
from datetime import date, timedelta
from collections import Counter

from app.database import SessionLocal
from app.models import Event
from app.services.collectors.registry import MAX_FUTURE_DAYS

APPLY = "--apply" in sys.argv


def main():
    db = SessionLocal()
    cutoff = date.today() + timedelta(days=MAX_FUTURE_DAYS)
    rows = (
        db.query(Event)
        .filter(Event.start_date > cutoff)
        .order_by(Event.start_date)
        .all()
    )
    print(f"cutoff (today + {MAX_FUTURE_DAYS}d) = {cutoff}")
    print(f"events beyond cutoff: {len(rows)}")

    by_src = Counter(r.scrape_source or "(null)" for r in rows)
    print("\nby source:")
    for src, n in by_src.most_common():
        print(f"  {src:>18}: {n}")

    print("\nsample (first 20):")
    for r in rows[:20]:
        print(f"  {r.start_date}  [{r.scrape_source}]  {(r.name or '')[:55]}")

    if not APPLY:
        print(f"\nDRY-RUN — would delete {len(rows)} rows. Re-run with --apply.")
        return

    for r in rows:
        db.delete(r)
    db.commit()
    print(f"\nDELETED {len(rows)} far-future rows.")


if __name__ == "__main__":
    main()
