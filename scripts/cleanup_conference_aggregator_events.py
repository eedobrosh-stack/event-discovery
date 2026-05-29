"""One-off cleanup: delete the LLM-extracted events that came from the
9 conference-aggregator hosts surfaced in the 2026-05-18 QA review.

The Cadence-B query taxonomy was rebalanced (2026-05-18) so we'll
discover fewer of these going forward, but the existing 7K+ events
already in events.db are stale templated-aggregator junk (e.g. the
Malala Yousafzai "Leadership Conference" replicated across multiple
city-pair titles). This script removes them.

Match: scrape_source = 'llm_extractor' AND purchase_link matches one
of the 9 hosts (substring match, picks up subdomains + URL params).

Run examples (per the project convention, dry-run → --apply):
    PYTHONPATH=. python3 scripts/cleanup_conference_aggregator_events.py
    PYTHONPATH=. python3 scripts/cleanup_conference_aggregator_events.py --apply
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter

from sqlalchemy import or_

from app.database import SessionLocal
from app.models import Event
from app.scheduler.jobs import _BLOCKED_BY_POLICY_DOMAINS

# Single source of truth lives in jobs.py — the same set that drives
# the discovery filter. Keeping them in lockstep avoids the drift bug
# where someone adds a domain to the runtime filter but forgets the
# cleanup script.
JUNK_DOMAINS = sorted(_BLOCKED_BY_POLICY_DOMAINS)


def _matching_filter():
    """An OR-of-LIKE clause across all junk domains, restricted to
    llm_extractor rows."""
    clauses = [Event.purchase_link.ilike(f"%{d}%") for d in JUNK_DOMAINS]
    return (Event.scrape_source == "llm_extractor", or_(*clauses))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="actually delete (default is dry-run)")
    args = ap.parse_args()

    s = SessionLocal()
    try:
        filt = _matching_filter()
        events = s.query(Event).filter(*filt).all()
        total = len(events)
        print(f"Matched {total:,} llm_extractor events on the 9 junk hosts.")
        if total == 0:
            print("Nothing to do.")
            return 0

        # Per-domain breakdown so the dry-run output is verifiable
        per_domain: Counter[str] = Counter()
        for e in events:
            link = (e.purchase_link or "").lower()
            for dom in JUNK_DOMAINS:
                if dom in link:
                    per_domain[dom] += 1
                    break
        print("\nPer-domain match counts:")
        for dom in JUNK_DOMAINS:
            n = per_domain.get(dom, 0)
            print(f"  {dom:<38} {n:>5}")

        # Show 3 samples for spot-check
        print("\nFirst 5 matched events:")
        for e in events[:5]:
            name = (e.name or "")[:60]
            print(f"  id={e.id} {e.start_date} | {name:<60} | {e.purchase_link[:60] if e.purchase_link else ''}")

        if not args.apply:
            print("\nDRY-RUN — pass --apply to delete.")
            return 0

        # ORM delete-one-at-a-time so any m2m cascade rules
        # (event_event_types etc.) fire correctly.
        for e in events:
            s.delete(e)
        s.commit()
        print(f"\nDeleted {total:,} events.")
        return 0
    except Exception as exc:
        s.rollback()
        sys.stderr.write(f"cleanup failed: {exc}\n")
        return 1
    finally:
        s.close()


if __name__ == "__main__":
    sys.exit(main())
