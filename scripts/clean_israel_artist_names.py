"""Clean events.artist_name for Israeli events: show titles are not artists.

Applies app/services/artist_names.clean_israeli_artist (the same rule
ingest now applies) to every Israeli event that has an artist_name:

  * a show / event title naming a performer → that performer
    ("דרור קרן במופע סטנדאפ חדש!" → "דרור קרן");
  * a show / event title naming no one → NULL
    ("אור לגויים - תיאטרון בית ליסין"); the event's own name keeps the title;
  * a known duplicate spelling → the kept spelling ("דיויד ברוזה" → "דויד ברוזה").

Sport rows are left alone. Dry run by default; audit JSON in data/.

    PYTHONPATH=. python3 scripts/clean_israel_artist_names.py
    PYTHONPATH=. python3 scripts/clean_israel_artist_names.py --apply
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.services.artist_names import clean_israeli_artist  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("clean_artists")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry run).")
    args = ap.parse_args()
    db = SessionLocal()
    try:
        known = {r[0] for r in db.execute(text(
            "SELECT normalized_name FROM performers WHERE COALESCE(mb_id,'') != '' "
            "OR COALESCE(spotify_id,'') != ''"))}
        genre = dict(db.execute(text("SELECT normalized_name, primary_genre FROM artist_genre")).fetchall())
        today = date.today().isoformat()
        rows = db.execute(text("""
            SELECT e.artist_name, COUNT(*), SUM(CASE WHEN e.start_date >= :t THEN 1 ELSE 0 END)
            FROM events e JOIN venues v ON v.id = e.venue_id JOIN cities c ON c.id = v.city_id
            WHERE c.country = 'Israel' AND e.sport IS NULL
              AND e.artist_name IS NOT NULL AND TRIM(e.artist_name) != ''
            GROUP BY e.artist_name
        """), {"t": today}).fetchall()

        changes, kinds = [], Counter()
        for old, n, upcoming in rows:
            new = clean_israeli_artist(old, known_performer=lambda k: k in known,
                                       sub_genre=lambda k: genre.get(k))
            if new == old:
                continue
            kind = ("cleared" if new is None else
                    "spelling" if new.lower() != old.strip().lower() and len(new.split()) == len(old.split()) else
                    "performer_from_title")
            kinds[kind] += n
            changes.append({"from": old, "to": new, "kind": kind, "events": n, "upcoming": upcoming or 0})
        changes.sort(key=lambda c: -c["events"])
        log.info(f"distinct artist values: {len(rows)}; changed: {len(changes)} "
                 f"({sum(c['events'] for c in changes)} events, "
                 f"{sum(c['upcoming'] for c in changes)} upcoming) by kind: {dict(kinds)}")
        for c in changes[:8]:
            log.info(f"  {c['kind']:<20} {c['events']:>4}  {c['from'][:50]!r} → {c['to']!r}")

        if args.apply:
            for c in changes:
                db.execute(text("""
                    UPDATE events SET artist_name = :new
                    WHERE artist_name = :old AND sport IS NULL AND venue_id IN (
                        SELECT v.id FROM venues v JOIN cities c ON c.id = v.city_id
                        WHERE c.country = 'Israel')
                """), {"new": c["to"], "old": c["from"]})
            db.commit()
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = ROOT / "data" / f"clean_israel_artist_names_{ts}_{'apply' if args.apply else 'dryrun'}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(changes, ensure_ascii=False, indent=1))
        log.info(f"audit written: {out}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
