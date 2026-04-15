"""
Deduplicate venues within the same city by exact case-insensitive name match.

This is a one-off fix for the 908 venues whose names were cleaned from
"Event Title @ Real Venue Name" → "Real Venue Name". Some of those cleaned
names now collide with an existing canonical record for the same venue.

Strategy:
  For each (city_id, lower(name)) group with > 1 venue:
    - Keep the record with the most events (tie-break: lowest id)
    - Reassign all events from duplicate records to the canonical one
    - Delete the duplicate venue rows

Usage:
    python3 scripts/deduplicate_venues.py
    python3 scripts/deduplicate_venues.py --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from sqlalchemy import text


def get_event_count(db, venue_id: int) -> int:
    return db.execute(
        text("SELECT COUNT(*) FROM events WHERE venue_id = :vid"),
        {"vid": venue_id},
    ).scalar() or 0


def run(dry_run: bool = False):
    db = SessionLocal()
    total_merged = 0
    total_events_reassigned = 0

    try:
        # Find all (city_id, lowercase name) groups with more than one venue
        dupes = db.execute(text("""
            SELECT city_id, LOWER(name) AS lname,
                   COUNT(*) AS cnt,
                   GROUP_CONCAT(id ORDER BY id) AS ids
            FROM venues
            GROUP BY city_id, LOWER(name)
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC, lname
        """)).fetchall()

        print(f"Found {len(dupes)} duplicate venue groups")

        for row in dupes:
            city_id = row[0]
            lname = row[1]
            ids = [int(i) for i in row[3].split(",")]

            # Canonical = most events; tie-break = lowest id
            scored = sorted(ids, key=lambda vid: (-get_event_count(db, vid), vid))
            keep_id = scored[0]
            drop_ids = scored[1:]

            keep_name = db.execute(
                text("SELECT name FROM venues WHERE id = :id"), {"id": keep_id}
            ).scalar()
            keep_events = get_event_count(db, keep_id)

            for drop_id in drop_ids:
                drop_name = db.execute(
                    text("SELECT name FROM venues WHERE id = :id"), {"id": drop_id}
                ).scalar()
                drop_events = get_event_count(db, drop_id)

                print(
                    f"  {'[DRY] ' if dry_run else ''}"
                    f"Merge venue #{drop_id} {drop_name!r} ({drop_events} events) → "
                    f"#{keep_id} {keep_name!r} ({keep_events} events)"
                    f"  [city_id={city_id}]"
                )

                if not dry_run:
                    # Reassign events
                    db.execute(
                        text("UPDATE events SET venue_id = :keep WHERE venue_id = :drop"),
                        {"keep": keep_id, "drop": drop_id},
                    )
                    # Delete the duplicate venue
                    db.execute(
                        text("DELETE FROM venues WHERE id = :id"), {"id": drop_id}
                    )

                total_events_reassigned += drop_events
                total_merged += 1

        if not dry_run:
            db.commit()
            print(
                f"\nDone — {total_merged} duplicate venues removed, "
                f"{total_events_reassigned} events reassigned."
            )
        else:
            print(
                f"\n[DRY RUN] Would remove {total_merged} duplicate venues, "
                f"reassigning {total_events_reassigned} events."
            )

        remaining = db.execute(text("SELECT COUNT(*) FROM venues")).scalar()
        print(f"Venues remaining: {remaining:,}")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deduplicate venue records by name+city")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, make no changes")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
