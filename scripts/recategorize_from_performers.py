"""
Quick re-apply: reads the current performers table and re-categorizes all events
that have a matching artist entry.  Safe to run while enrich_performers.py is
still running in the background (no conflicts — just reads performers, writes events).

Usage:
    python3 scripts/recategorize_from_performers.py
    python3 scripts/recategorize_from_performers.py --dry-run
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from app.models import Event, EventType, Performer
from app.services.performer_lookup import normalize


def run(dry_run: bool = False):
    db = SessionLocal()
    try:
        et_by_name = {et.name: et for et in db.query(EventType).all()}
        performer_map = {
            p.normalized_name: (p.category, p.event_type_name)
            for p in db.query(Performer).all()
            if p.event_type_name
        }

        print(f"Performers in DB: {len(performer_map)}")

        updated = 0
        skipped = 0

        events = (
            db.query(Event)
            .filter(Event.artist_name.isnot(None))
            .filter(Event.artist_name != "")
            .all()
        )

        for event in events:
            norm = normalize(event.artist_name.strip())
            if norm not in performer_map:
                skipped += 1
                continue

            _, type_name = performer_map[norm]
            et = et_by_name.get(type_name)
            if not et:
                skipped += 1
                continue

            # Skip if already correctly tagged
            current_types = [t.name for t in event.event_types]
            if current_types == [type_name]:
                skipped += 1
                continue

            if not dry_run:
                event.event_types = [et]
            updated += 1

        if not dry_run:
            db.commit()

        print(f"Updated: {updated:,}  |  Skipped (no match or unchanged): {skipped:,}")
        if dry_run:
            print("[DRY RUN — nothing committed]")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
