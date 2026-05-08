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


def _trim_lower(s: str | None) -> str:
    return (s or "").strip().lower()


def _is_whitespace_dupe(a: dict, b: dict) -> bool:
    """True when two rows have the same trimmed-lowercase name AND
    compatible state attribution (both null OR both same value).

    Catches the "Pittsburgh" / "Pittsburgh " (trailing space) and
    " Des Moines" / "Des Moines" (leading space) cases. Pure name
    duplicates differing only in whitespace.
    """
    if _trim_lower(a["name"]) != _trim_lower(b["name"]):
        return False
    a_st = _trim_lower(a["state"])
    b_st = _trim_lower(b["state"])
    return a_st == b_st


def _is_city_suffix_dupe(a: dict, b: dict) -> bool:
    """True when one row's trimmed name = "<X>" and the other's =
    "<X> City", AND <X> is a US state name (so the " City" is the
    state-disambiguation form), AND at least one side has a state
    value.

    Catches the New York pattern (one side state=NY, other state=null)
    AND the Washington-DC pattern (both sides state=DC). The "at
    least one has state" guard keeps us from folding pairs like
    "Kansas City" / "Kansas" when neither side has a state — those
    are likely bad-data shapes that need human judgment, not an
    auto-merge.
    """
    a_name = _trim_lower(a["name"])
    b_name = _trim_lower(b["name"])
    short_long_pairs = [(a_name, b_name), (b_name, a_name)]
    for n_short, n_long in short_long_pairs:
        if n_long == n_short + " city" and n_short.title() in US_STATE_NAME_SET:
            # State-name guard: prefix must really be a state.
            if a["state"] or b["state"]:
                return True
    return False


def find_duplicate_pairs(db) -> list[tuple[dict, dict]]:
    """Return [(canonical, duplicate), …] for US-city dupes.

    Strategy:
      1. Group US cities by canonicalised name (state-disambiguation-
         aware — see _canonicalise_name).
      2. Inside each multi-row group, rank by (venues + events) desc;
         the heaviest row is canonical.
      3. For each non-canonical row, fold IF it matches the canonical
         under one of these safe rules:
           • whitespace-only dupe (same trimmed name)
           • NY/Washington-style " City" suffix where the prefix is a
             state name AND at least one side carries a state value.
      4. Otherwise log a warning and leave both rows alone.

    Counter-examples kept safe by these rules:
      • Lincoln, NE  vs  Lincoln City, IN — different canonical
        groups (Lincoln isn't a state name, so " City" isn't stripped)
      • Kansas City  vs  Kansas (no state) — same group, but neither
        side has a state, so the suffix rule doesn't fire.
      • Washington, DC  vs  Washington City, DC — same group, both
        have DC state, suffix rule fires → fold.
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
        # Canonical = heaviest row by (venues + events). When tied,
        # prefer the row that already carries a state value.
        ranked = sorted(
            group,
            key=lambda g: (-(g["venues"] + g["events"]), 0 if g["state"] else 1),
        )
        canonical = ranked[0]
        skipped: list[dict] = []
        for other in ranked[1:]:
            if _is_whitespace_dupe(canonical, other):
                pairs.append((canonical, other))
            elif _is_city_suffix_dupe(canonical, other):
                pairs.append((canonical, other))
            else:
                skipped.append(other)
        if skipped:
            extra = ", ".join(f"{o['name']!r}(id={o['id']}, "
                              f"v={o['venues']}/e={o['events']})"
                              for o in skipped)
            log.warning(
                f"skip group {key!r} — couldn't fold canonical "
                f"{canonical['name']!r}(id={canonical['id']}) with: {extra}"
            )
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
