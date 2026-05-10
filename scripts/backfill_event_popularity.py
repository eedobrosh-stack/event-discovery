"""Backfill Event.artist_popularity / .artist_spotify_url / .image_url
from already-enriched Performer rows.

Symptom (observed 2026-05-10): the events table had
``Event.artist_popularity`` populated on 0 of 145,178 rows despite 708
performers carrying ``Performer.popularity`` from the
enrich_spotify_job. Cause: the per-event propagation in that job used
``Event.artist_name == Performer.name`` — case-sensitive — while the
SELECT-pending join one screen up uses ``func.lower(...) ==
func.lower(...)``. Any event whose collector wrote ``artist_name`` in a
different case than ``Performer.name`` silently failed to receive the
popularity / Spotify URL / image.

The cron itself (the case-sensitivity fix lands alongside this commit)
only re-processes performers without a ``spotify_id``. Already-
enriched performers are skipped, so without an explicit backfill
their events stay un-popularized indefinitely. This script closes the
gap once.

For each performer with ``spotify_id``:
  - LOWER(Event.artist_name) == LOWER(Performer.name) → backfill:
      • Event.artist_popularity   = round(Performer.popularity / 10)
      • Event.artist_spotify_url  = Performer.spotify_url   (if missing)
      • Event.image_url           = Performer.image_url     (if missing)

The popularity 0–100 → 1–10 mapping mirrors the cron's logic so the
column ends up consistent regardless of source.

Idempotent — re-runs only update rows whose target column is still
null (popularity already-set rows are left alone, in case manual
overrides exist). Safe to re-run after each enrich_spotify_job cycle
during the catch-up phase.

Usage:
    PYTHONPATH=. python3 scripts/backfill_event_popularity.py            # dry-run
    PYTHONPATH=. python3 scripts/backfill_event_popularity.py --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
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
log = logging.getLogger("backfill_event_popularity")

from sqlalchemy import func  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event, Performer  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap performers processed (testing).")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    log.info(f"mode={mode}")

    db = SessionLocal()
    try:
        # Already-enriched performers — those that ran through the cron
        # successfully but whose events never received the propagation.
        q = (
            db.query(Performer)
            .filter(
                Performer.spotify_id.isnot(None),
                Performer.popularity.isnot(None),
            )
            .order_by(Performer.id)
        )
        if args.limit:
            q = q.limit(args.limit)
        performers = q.all()
        log.info(f"already-enriched performers in scope: {len(performers)}")

        total_pop_set = 0
        total_url_set = 0
        total_img_set = 0
        per_artist: list[dict] = []

        for p in performers:
            name_lower = (p.name or "").lower().strip()
            if not name_lower:
                continue

            score_1_10 = max(1, round((p.popularity or 0) / 10)) if p.popularity else None

            # Count first so dry-run reports useful numbers without
            # writing. Three independent matches because the WHERE
            # clauses differ (only fill missing image / spotify_url
            # but always set popularity, mirroring the cron's logic).
            base_filter = func.lower(Event.artist_name) == name_lower
            cand_pop = (
                db.query(func.count(Event.id))
                .filter(base_filter, Event.artist_popularity.is_(None))
                .scalar()
            ) if score_1_10 else 0
            cand_url = (
                db.query(func.count(Event.id))
                .filter(base_filter, Event.artist_spotify_url.is_(None))
                .scalar()
            ) if p.spotify_url else 0
            cand_img = (
                db.query(func.count(Event.id))
                .filter(base_filter, Event.image_url.is_(None))
                .scalar()
            ) if p.image_url else 0

            if (cand_pop + cand_url + cand_img) == 0:
                continue

            per_artist.append({
                "performer_id": p.id,
                "name": p.name,
                "popularity_raw": p.popularity,
                "popularity_1_10": score_1_10,
                "events_to_fill_popularity": cand_pop,
                "events_to_fill_spotify_url": cand_url,
                "events_to_fill_image_url": cand_img,
            })

            if args.apply:
                if score_1_10 and cand_pop:
                    db.query(Event).filter(
                        base_filter,
                        Event.artist_popularity.is_(None),
                    ).update(
                        {"artist_popularity": score_1_10},
                        synchronize_session=False,
                    )
                    total_pop_set += cand_pop
                if p.spotify_url and cand_url:
                    db.query(Event).filter(
                        base_filter,
                        Event.artist_spotify_url.is_(None),
                    ).update(
                        {"artist_spotify_url": p.spotify_url},
                        synchronize_session=False,
                    )
                    total_url_set += cand_url
                if p.image_url and cand_img:
                    db.query(Event).filter(
                        base_filter,
                        Event.image_url.is_(None),
                    ).update(
                        {"image_url": p.image_url},
                        synchronize_session=False,
                    )
                    total_img_set += cand_img
            else:
                total_pop_set += cand_pop
                total_url_set += cand_url
                total_img_set += cand_img

        if args.apply:
            db.commit()

        log.info(
            f"performers touched={len(per_artist)} "
            f"events: popularity_set={total_pop_set} "
            f"spotify_url_set={total_url_set} "
            f"image_url_set={total_img_set}"
        )

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = "apply" if args.apply else "dryrun"
        audit_path = ROOT / "data" / f"backfill_event_popularity_{ts}_{suffix}.json"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(json.dumps(per_artist, ensure_ascii=False, indent=2))
        log.info(f"audit written: {audit_path}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
