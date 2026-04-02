"""
Deduplicate cities in two passes:
  Pass 1 — Known variant merges (hand-curated: "New York City" → "New York", etc.)
  Pass 2 — Exact case-insensitive duplicates (same name+country after lower())
            Keep the one with the most events; reassign venues, then delete dupes.

NOTE: Pass 1 must run before Pass 2 so variant merges resolve before the
      case-fold pass tries to pick a canonical.

Usage:
    python3 scripts/deduplicate_cities.py
    python3 scripts/deduplicate_cities.py --dry-run
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import SessionLocal
from sqlalchemy import text

# ---------------------------------------------------------------------------
# VARIANT MERGES: (keep_name, keep_country) → [(drop_name, drop_country), ...]
# ---------------------------------------------------------------------------
VARIANT_MERGES = [
    # New York City → New York
    (("New York", "United States"), [("New York City", "United States")]),
    # Frankfurt variants → Frankfurt am Main
    (("Frankfurt am Main", "Germany"), [("Frankfurt Am Main", "Germany"), ("Frankfurt", "Germany")]),
    # London sub-areas / boroughs → London, United Kingdom
    (("London", "United Kingdom"), [
        ("East London", "United Kingdom"),
        ("North London", "United Kingdom"),
        ("South West London", "United Kingdom"),
        ("City Of London", "United Kingdom"),
        ("Tower Of London", "United Kingdom"),
        ("West London", "United Kingdom"),
        ("South London", "United Kingdom"),
        ("Central London", "United Kingdom"),
    ]),
]


def get_event_count(db, city_id):
    return db.execute(text(
        "SELECT COUNT(*) FROM events e JOIN venues v ON e.venue_id = v.id WHERE v.city_id = :cid"
    ), {"cid": city_id}).scalar() or 0


def merge_city(db, keep_id, drop_id, dry_run):
    """Reassign all venues from drop_id to keep_id, then delete drop city."""
    venue_count = db.execute(text(
        "SELECT COUNT(*) FROM venues WHERE city_id = :cid"
    ), {"cid": drop_id}).scalar() or 0

    if not dry_run:
        db.execute(text("UPDATE venues SET city_id = :keep WHERE city_id = :drop"),
                   {"keep": keep_id, "drop": drop_id})
        db.execute(text("DELETE FROM cities WHERE id = :cid"), {"cid": drop_id})

    return venue_count


def run(dry_run=False):
    db = SessionLocal()
    total_merged = 0

    try:
        # ── Pass 1: known variant merges ───────────────────────────────────
        print("=== Pass 1: Known variant merges ===")
        for (keep_name, keep_country), drop_list in VARIANT_MERGES:
            keep_row = db.execute(text(
                "SELECT id FROM cities WHERE name=:n AND country=:c"
            ), {"n": keep_name, "c": keep_country}).fetchone()

            if not keep_row:
                print(f"  SKIP: canonical '{keep_name}, {keep_country}' not found")
                continue

            keep_id = keep_row[0]

            for drop_name, drop_country in drop_list:
                drop_row = db.execute(text(
                    "SELECT id FROM cities WHERE name=:n AND country=:c"
                ), {"n": drop_name, "c": drop_country}).fetchone()

                if not drop_row:
                    print(f"  SKIP: '{drop_name}, {drop_country}' not found")
                    continue

                drop_id = drop_row[0]
                venues_moved = merge_city(db, keep_id, drop_id, dry_run)
                print(f"  {'[DRY] ' if dry_run else ''}Merge '{drop_name}' (id={drop_id}) → '{keep_name}' (id={keep_id})  [{venues_moved} venues]")
                total_merged += 1

        if not dry_run:
            db.commit()

        # ── Pass 2: exact case-insensitive duplicates ──────────────────────
        print("\n=== Pass 2: Exact case-insensitive duplicates ===")
        dupes = db.execute(text("""
            SELECT LOWER(name) as lname, LOWER(country) as lcountry,
                   COUNT(*) as cnt, GROUP_CONCAT(id) as ids
            FROM cities
            GROUP BY LOWER(name), LOWER(country)
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
        """)).fetchall()

        print(f"Found {len(dupes)} duplicate groups")

        for row in dupes:
            ids = [int(i) for i in row[3].split(",")]
            # Pick canonical = most events; tie-break = lowest id
            scored = sorted(ids, key=lambda cid: (-get_event_count(db, cid), cid))
            keep_id = scored[0]
            drop_ids = scored[1:]

            keep_name = db.execute(text("SELECT name, country FROM cities WHERE id=:id"),
                                   {"id": keep_id}).fetchone()

            for drop_id in drop_ids:
                drop_name = db.execute(text("SELECT name, country FROM cities WHERE id=:id"),
                                       {"id": drop_id}).fetchone()
                if not drop_name:
                    continue  # already deleted in Pass 1
                venues_moved = merge_city(db, keep_id, drop_id, dry_run)
                print(f"  {'[DRY] ' if dry_run else ''}Merge {drop_name[0]!r} (id={drop_id}) → {keep_name[0]!r} (id={keep_id})  [{venues_moved} venues]")
                total_merged += 1

        if not dry_run:
            db.commit()

        print(f"\n{'[DRY RUN] ' if dry_run else ''}Done — {total_merged} cities merged.")
        final = db.execute(text("SELECT COUNT(*) FROM cities")).scalar()
        print(f"Cities remaining: {final:,}")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
