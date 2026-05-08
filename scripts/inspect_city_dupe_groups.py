"""Read-only diagnostic: print details for each city dupe-group.

For each group of US cities sharing a canonicalised name (lowercase,
state-suffix-stripped where applicable), print every row's id, state,
venue/event counts, and a couple of sample venue names so we can tell
"genuine duplicate" from "two distinct cities that happen to share a
name across states".

Use case: dedupe_us_cities.py only handles the "exactly one with-state,
one without" shape. Other shapes (0/2, 2/0, etc.) need human eyes
before we know how to merge — this script gives us those eyes.

Usage:
    python3 scripts/inspect_city_dupe_groups.py

Read-only — never writes.
"""
from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.api._us_states import US_STATE_NAME_SET  # noqa: E402


def _canonicalise_name(s: str) -> str:
    s = (s or "").strip().lower()
    if s.endswith(" city"):
        prefix = s[: -len(" city")].strip()
        if prefix.title() in US_STATE_NAME_SET:
            return prefix
    return s


def main():
    db = SessionLocal()
    try:
        rows = db.execute(text("""
            SELECT c.id, c.name, c.state,
                   (SELECT COUNT(*) FROM venues v WHERE v.city_id = c.id) AS venue_count,
                   (SELECT COUNT(*) FROM events e
                      JOIN venues v ON v.id = e.venue_id
                      WHERE v.city_id = c.id) AS event_count
            FROM cities c
            WHERE c.country = 'United States'
        """)).fetchall()

        # Group by canonicalised name
        by_key: dict[str, list[dict]] = {}
        for r in rows:
            key = _canonicalise_name(r[1])
            if not key:
                continue
            by_key.setdefault(key, []).append({
                "id": int(r[0]),
                "name": r[1],
                "state": r[2],
                "venues": int(r[3] or 0),
                "events": int(r[4] or 0),
            })

        # Only show groups with >1 row
        multi = {k: g for k, g in by_key.items() if len(g) > 1}
        if not multi:
            print("No multi-row groups found.")
            return

        print(f"Found {len(multi)} group(s) with multiple rows:")
        print("=" * 78)

        for key in sorted(multi.keys()):
            group = multi[key]
            with_state = sum(1 for g in group if g["state"])
            print(f"\n[{key!r}]  ({len(group)} rows: {with_state} with-state, "
                  f"{len(group) - with_state} without)")
            for g in sorted(group, key=lambda x: -x["venues"]):
                # Sample 3 venue names so we can see if the rows actually
                # describe the same place or two different ones.
                samples = db.execute(text("""
                    SELECT name FROM venues WHERE city_id = :id
                    ORDER BY id LIMIT 3
                """), {"id": g["id"]}).fetchall()
                sample_str = ", ".join(s[0] for s in samples) if samples else "(no venues)"
                print(f"  id={g['id']:>5}  name={g['name']!r:<22}  "
                      f"state={str(g['state'])!r:<14}  "
                      f"venues={g['venues']:>4}  events={g['events']:>5}")
                print(f"           sample venues: {sample_str[:120]}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
