"""One-off: backfill artist_name from name on existing mevalim rows.

mevalim events were historically ingested with artist_name='' or NULL
because the collector left the field unset. The collector now mirrors
``name`` into ``artist_name`` (see app/services/collectors/scrapers/
mevalim.py), and the ingest job's "refresh existing" path will gradually
fix older rows over the daily cycle. This script does the same fix in
one pass so the backfill lands immediately.

Idempotent — only touches rows where artist_name is NULL or empty.

Same fold approach is safe for techconf (conference speaker names live
in Event.name with empty artist_name there too); add that source to
the WHERE clause if/when we want to backfill that source too.

Usage:
    python3 scripts/backfill_mevalim_artist_name.py --dry-run
    python3 scripts/backfill_mevalim_artist_name.py
"""
from __future__ import annotations

import argparse
import logging
import sys
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
log = logging.getLogger("backfill_mevalim_artist_name")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="Report only — no DB writes.")
    parser.add_argument("--source", default="mevalim",
                        help="scrape_source to backfill (default: mevalim).")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        # Count what's about to change
        n_total = db.execute(text("""
            SELECT COUNT(*) FROM events
            WHERE scrape_source = :src
        """), {"src": args.source}).scalar() or 0
        n_to_fix = db.execute(text("""
            SELECT COUNT(*) FROM events
            WHERE scrape_source = :src
              AND name IS NOT NULL AND name != ''
              AND (artist_name IS NULL OR artist_name = '')
        """), {"src": args.source}).scalar() or 0

        log.info(f"Source {args.source!r}: {n_total} rows total, "
                 f"{n_to_fix} need backfill")

        if n_to_fix == 0:
            log.info("Nothing to do.")
            return

        if args.dry_run:
            sample = db.execute(text("""
                SELECT id, name FROM events
                WHERE scrape_source = :src
                  AND name IS NOT NULL AND name != ''
                  AND (artist_name IS NULL OR artist_name = '')
                LIMIT 5
            """), {"src": args.source}).fetchall()
            log.info("Sample rows that would update:")
            for r in sample:
                log.info(f"  id={r[0]}  name={r[1]!r}")
            log.info("Dry-run complete. Re-run without --dry-run to apply.")
            return

        result = db.execute(text("""
            UPDATE events
            SET artist_name = name
            WHERE scrape_source = :src
              AND name IS NOT NULL AND name != ''
              AND (artist_name IS NULL OR artist_name = '')
        """), {"src": args.source})
        db.commit()
        log.info(f"Updated {result.rowcount} rows")
    finally:
        db.close()


if __name__ == "__main__":
    main()
