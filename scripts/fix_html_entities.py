"""Backfill HTML-entity decoder for text fields stored entity-encoded.

Some scrapers (mostly Israeli-venue collectors that fetched HTML
attributes) wrote text containing literal `&#039;`, `&amp;`, `&quot;`
etc. into Event.name / venue_name / artist_name / description. The
frontend's `esc()` helper re-encodes those `&`, so users see literal
`&#039;` in the UI instead of `'`.

Run:
    PYTHONPATH=. python3 scripts/fix_html_entities.py --dry-run
    PYTHONPATH=. python3 scripts/fix_html_entities.py --apply

Idempotent: re-running on already-clean data is a no-op (html.unescape
returns input unchanged when there are no entities to decode).
"""
import argparse
import html
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import or_

from app.database import SessionLocal
from app.models import Event, Venue, Performer

# Most common HTML entity fingerprints in our DB. SQL `LIKE '%&#%'` or
# `LIKE '%&amp;%'` is the cheap way to scope the candidate set without
# loading the whole table.
ENTITY_LIKES = ["%&#%", "%&amp;%", "%&quot;%", "%&apos;%", "%&lt;%", "%&gt;%"]


def _needs_decode(s):
    if not s:
        return False
    return html.unescape(s) != s


def _candidates(query, col):
    """Filter `query` to rows where `col` matches any entity-like
    fingerprint. Cheap pre-filter; the real check is in Python."""
    return query.filter(or_(*[col.like(p) for p in ENTITY_LIKES]))


def run(*, apply: bool = False) -> dict:
    db = SessionLocal()
    stats = {"events_name": 0, "events_artist": 0, "events_venue": 0, "events_desc": 0, "venues": 0, "performers": 0}
    try:
        # Events: name, artist_name, venue_name (denorm), description
        for col_attr, stat_key in [
            ("name", "events_name"),
            ("artist_name", "events_artist"),
            ("venue_name", "events_venue"),
            ("description", "events_desc"),
        ]:
            col = getattr(Event, col_attr)
            rows = _candidates(db.query(Event), col).all()
            for ev in rows:
                v = getattr(ev, col_attr)
                if _needs_decode(v):
                    new_v = html.unescape(v)
                    if apply:
                        setattr(ev, col_attr, new_v)
                    stats[stat_key] += 1

        # Venues: name
        v_rows = _candidates(db.query(Venue), Venue.name).all()
        for v in v_rows:
            if _needs_decode(v.name):
                new_v = html.unescape(v.name)
                if apply:
                    v.name = new_v
                stats["venues"] += 1

        # Performers: name
        p_rows = _candidates(db.query(Performer), Performer.name).all()
        for p in p_rows:
            if _needs_decode(p.name):
                new_v = html.unescape(p.name)
                if apply:
                    p.name = new_v
                stats["performers"] += 1

        if apply:
            db.commit()
        return stats
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write decoded values. Without this, runs dry.")
    args = parser.parse_args()
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"mode={mode}")
    stats = run(apply=args.apply)
    print("rows needing decode:")
    for k, v in stats.items():
        print(f"  {k:.<25} {v:>6}")
    if not args.apply and any(stats.values()):
        print("\nRe-run with --apply to write changes.")


if __name__ == "__main__":
    main()
