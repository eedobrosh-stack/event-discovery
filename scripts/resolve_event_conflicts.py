"""Propagate artist / types across same-title events, then fold
same-show rows that disagree on time (app/services/event_conflicts.py).

    PYTHONPATH=. python3 scripts/resolve_event_conflicts.py [--country Israel]
    PYTHONPATH=. python3 scripts/resolve_event_conflicts.py --apply [--country Israel]

Upcoming, non-sport events only. Dry run by default; audit JSON in data/.
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

from app.database import SessionLocal  # noqa: E402
from app.services.event_conflicts import propagate_same_title, resolve_time_conflicts  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("event_conflicts")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--country", default=None)
    args = ap.parse_args()
    db = SessionLocal()
    try:
        prop = propagate_same_title(db, apply=args.apply, country=args.country)
        log.info(f"propagation: {len(prop)} events "
                 f"(artist {sum('artist_name' in p for p in prop)}, "
                 f"category/format {sum('event_type_ids' in p for p in prop)})")
        folds = resolve_time_conflicts(db, apply=args.apply, country=args.country)
        for _ in range(2 if args.apply else 0):   # folds can create new neighbours
            more = resolve_time_conflicts(db, apply=True, country=args.country)
            if not more:
                break
            folds += more
        log.info(f"time conflicts: {len(folds)} shows, {sum(len(f['drop']) for f in folds)} rows folded "
                 f"(matched on {dict(Counter(f['matched_on'] for f in folds))})")
        for f in sorted(folds, key=lambda f: -len(f["drop"]))[:6]:
            log.info(f"  keep {f['keep_time']} {f['keep_source']:<14} {f['name'][:40]!r} {f['date']} "
                     f"← {[(d['time'], d['source']) for d in f['drop']]}")
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = ROOT / "data" / f"resolve_event_conflicts_{ts}_{'apply' if args.apply else 'dryrun'}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"propagation": prop, "folds": folds}, ensure_ascii=False, indent=1, default=str))
        log.info(f"audit written: {out}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
