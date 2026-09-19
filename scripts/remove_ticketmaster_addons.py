"""Remove Ticketmaster add-on listings that were saved as events.

Ticketmaster's Discovery API returns upsell SKUs — "Zac Brown Band
Premium Seating", "Zac Brown Band Bar Rails", "Parking permit Masego",
"Metallica - Suite Reservation", "Hieroglyphics - VIP M&G Add-On - NOT
A CONCERT TICKET" — as ordinary events on the same date and venue as
the real show. Until 2026-09-19 the collector saved them verbatim, so
the catalog carried ~1.3k such rows (2026-09-18 QA report: ~1,441
duplicate groups were this class). The ingest path now drops them via
``app.services.collectors.addon_filter.ticket_addon_reason``; this
script applies the *same* predicate retroactively so the rule cannot
drift between ingest and backfill.

Scope: ``scrape_source = 'ticketmaster'`` only. Other sources are not
touched even if a title happens to match.

Default is a dry-run: prints per-class counts and 30 samples, writes an
audit JSON to data/remove_tm_addons_<ts>_dryrun.json. ``--apply``
deletes the rows through the ORM (so event_types / event_themes m2m
rows cascade) in batches of 200 and writes the *_apply.json audit.

Usage:
    PYTHONPATH=. python3 scripts/remove_ticketmaster_addons.py
    PYTHONPATH=. python3 scripts/remove_ticketmaster_addons.py --apply
    DATABASE_URL=sqlite:////tmp/copy.db PYTHONPATH=. python3 scripts/remove_ticketmaster_addons.py
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("remove_tm_addons")

from app.database import SessionLocal  # noqa: E402
from app.models import Event  # noqa: E402
from app.services.collectors.addon_filter import ticket_addon_reason  # noqa: E402

SAMPLE_N = 30
BATCH = 200


def find_addon_rows(db) -> list[tuple[int, str, str, str | None, str]]:
    """(id, name, start_date, venue_name, class) for every TM row that
    the ingest predicate would drop today."""
    hits = []
    q = (
        db.query(Event.id, Event.name, Event.start_date, Event.venue_name)
        .filter(Event.scrape_source == "ticketmaster")
        .yield_per(2000)
    )
    for ev_id, name, sd, venue in q:
        reason = ticket_addon_reason(name)
        if reason:
            hits.append((ev_id, name, str(sd), venue, reason))
    return hits


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--apply", action="store_true",
                    help="Delete the rows. Without this, dry-run only.")
    ap.add_argument("--samples", type=int, default=SAMPLE_N)
    args = ap.parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"

    db = SessionLocal()
    try:
        total_tm = db.query(Event.id).filter(Event.scrape_source == "ticketmaster").count()
        hits = find_addon_rows(db)
        by_class = Counter(h[4] for h in hits)
        log.info(f"mode={mode} ticketmaster rows={total_tm} add-on rows={len(hits)}")
        for cls, n in by_class.most_common():
            log.info(f"  {cls:<16} {n}")

        # Spread samples across classes so the dry-run shows every kind.
        per_class: dict[str, list] = defaultdict(list)
        for h in hits:
            per_class[h[4]].append(h)
        samples: list = []
        while len(samples) < min(args.samples, len(hits)):
            progressed = False
            for cls in by_class:
                if per_class[cls] and len(samples) < args.samples:
                    samples.append(per_class[cls].pop(0))
                    progressed = True
            if not progressed:
                break
        log.info(f"--- {len(samples)} samples ---")
        for ev_id, name, sd, venue, cls in samples:
            log.info(f"  id={ev_id} {sd} [{cls}] {name[:70]!r} @ {(venue or '-')[:30]}")

        deleted = 0
        if args.apply and hits:
            ids = [h[0] for h in hits]
            for i in range(0, len(ids), BATCH):
                chunk = ids[i:i + BATCH]
                for ev in db.query(Event).filter(Event.id.in_(chunk)).all():
                    db.delete(ev)
                    deleted += 1
                db.commit()
                db.expire_all()
            log.info(f"deleted {deleted} rows")

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        audit = ROOT / "data" / f"remove_tm_addons_{ts}_{'apply' if args.apply else 'dryrun'}.json"
        audit.parent.mkdir(parents=True, exist_ok=True)
        audit.write_text(json.dumps({
            "mode": mode, "ticketmaster_rows": total_tm, "addon_rows": len(hits),
            "by_class": dict(by_class), "deleted": deleted,
            "rows": [dict(zip(("id", "name", "start_date", "venue_name", "class"), h)) for h in hits],
        }, ensure_ascii=False, indent=1, default=str))
        log.info(f"audit written: {audit}")
        if not args.apply:
            log.info(f"DRY-RUN — {len(hits)} rows would be deleted. Re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
