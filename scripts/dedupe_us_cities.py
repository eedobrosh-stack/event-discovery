"""De-duplicate US city rows that differ only in state attribution.

Symptom (observed 2026-05-08): the cities table has two rows for the
same place — one with the proper state code (``name='New York',
state='NY'``) carrying the bulk of venues and events, and a near-
duplicate with no state (``name='New York City', state=None``) carrying
trace data. The display layer renders both as "New York City" once
the state-suffix UI is on, so users see the same place twice in
autocomplete.

This script folds the "no-state" duplicate into the "has-state"
canonical row by:
  1. Scanning US cities for pairs where the lowercased+trimmed name
     resolves to the same place but exactly one of the rows is
     missing ``state``.
  2. Choosing the one WITH state as canonical.
  3. Repointing every venue from the duplicate to the canonical row.
  4. Deleting the duplicate from cities.

Idempotent. ``--dry-run`` previews actions without writing.

Names are matched on lowercased+trimmed display name AFTER stripping
a trailing " City" suffix from the no-state side, so the "New York"
↔ "New York City" pair correctly reconciles. Add other normalisation
rules to ``_canonicalise_name()`` as we discover further dupe shapes.

Usage:
    python3 scripts/dedupe_us_cities.py --dry-run
    python3 scripts/dedupe_us_cities.py
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
log = logging.getLogger("dedupe_us_cities")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.api._us_states import US_STATE_NAME_SET  # noqa: E402


def _canonicalise_name(s: str | None) -> str:
    """Reduce a city name to a comparison key.

    Lowercase, strip whitespace, and drop a trailing " city" suffix —
    BUT ONLY when the prefix (everything before " City") matches a
    real US state name. That captures the "New York City" ↔ "New
    York" pattern (state-name disambiguation) without falsely
    collapsing genuine "X City" town names.

    Counter-example (caught during dev): "Lincoln, Nebraska" and
    "Lincoln City, Indiana" are TWO DIFFERENT REAL CITIES — Lincoln
    City IN is its own town, not a disambiguated form of Lincoln NE.
    The state-name guard prevents that fold.
    """
    s = (s or "").strip().lower()
    if s.endswith(" city"):
        prefix_raw = s[: -len(" city")].strip()
        prefix_titled = prefix_raw.title()
        # Only chop the suffix when the prefix is a US state name
        # (state-name disambiguation form). Lincoln, Iowa City, Park
        # City, etc. all keep their full proper names.
        if prefix_titled in US_STATE_NAME_SET:
            return prefix_raw
    return s


def find_duplicate_pairs(db) -> list[tuple[dict, dict]]:
    """Return [(canonical, duplicate), …] for US-city dupes.

    Pair criteria (BOTH must hold):
      • Same canonicalised name (see _canonicalise_name).
      • country = 'United States'.
      • Exactly one of the two rows has a state value; the other is
        NULL/empty.

    The "with state" row is canonical. Venue counts are loaded so
    callers can sanity-check (canonical should generally hold the
    bulk of the data).
    """
    rows = db.execute(text("""
        SELECT c.id, c.name, c.state,
               (SELECT COUNT(*) FROM venues v WHERE v.city_id = c.id) AS venue_count,
               (SELECT COUNT(*) FROM events e
                  JOIN venues v ON v.id = e.venue_id
                  WHERE v.city_id = c.id) AS event_count
        FROM cities c
        WHERE c.country = 'United States'
    """)).fetchall()
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

    pairs: list[tuple[dict, dict]] = []
    for key, group in by_key.items():
        if len(group) < 2:
            continue
        with_state = [g for g in group if g["state"]]
        without_state = [g for g in group if not g["state"]]
        if len(with_state) != 1 or len(without_state) != 1:
            # Skip more complex shapes (multiple has-state rows, etc.)
            # — those need a human eye, log them so we don't silently
            # ignore.
            log.warning(
                f"skip group {key!r} — non-trivial shape: "
                f"{len(with_state)} with-state, {len(without_state)} without"
            )
            continue
        pairs.append((with_state[0], without_state[0]))
    return pairs


def fold_duplicate(db, canonical: dict, duplicate: dict, *, dry_run: bool) -> None:
    """Repoint venues from ``duplicate`` to ``canonical``, then delete
    the duplicate row. Single transaction — either both writes apply
    or neither does."""
    log.info(
        f"  fold {duplicate['name']!r}(id={duplicate['id']}, "
        f"venues={duplicate['venues']}, events={duplicate['events']}) "
        f"→ {canonical['name']!r}(id={canonical['id']}, "
        f"venues={canonical['venues']}, events={canonical['events']})"
    )
    if dry_run:
        return

    # 1. Move venues. Update returns affected rows so we log what
    #    actually moved (handy if a partial run already moved some).
    res = db.execute(
        text("UPDATE venues SET city_id = :new WHERE city_id = :old"),
        {"new": canonical["id"], "old": duplicate["id"]},
    )
    log.info(f"    {res.rowcount} venue rows repointed")

    # 2. Delete the now-empty duplicate row.
    db.execute(
        text("DELETE FROM cities WHERE id = :id"),
        {"id": duplicate["id"]},
    )
    db.commit()
    log.info(f"    duplicate city row deleted")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="Report only — no DB writes.")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        pairs = find_duplicate_pairs(db)
        if not pairs:
            log.info("No duplicate pairs found — nothing to do.")
            return
        log.info(f"Found {len(pairs)} duplicate pair(s):")
        for can, dup in pairs:
            fold_duplicate(db, can, dup, dry_run=args.dry_run)
        if args.dry_run:
            log.info("Dry-run complete. Re-run without --dry-run to apply.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
