"""Apply approved city consolidations from a reviewed detect CSV.

Reads the CSV produced by scripts/detect_city_duplicates.py (default
path data/city_duplicates.csv), filters to rows where `approved` is
'y' / 'yes' / 'true' (case-insensitive), and writes the
canonical_city_id / parent_city_id links on the cities table.

Per-row actions:
  relationship='alias'   →  set alias_id's canonical_city_id = canonical_id
  relationship='nested'  →  set the nested row's parent_city_id = canonical_id
                            (the nested row is the "alias_id" in the CSV
                            schema; canonical_id is its containing parent)

Idempotency:
  - If a target FK is already set to the same id, no-op.
  - If it's set to a DIFFERENT id, surface a warning + skip (operator
    decides which one is right).
  - If an alias_id is already canonical for OTHER rows (the row is a
    transitive intermediate), warn + skip (operator resolves the chain
    by re-running detect after the deeper alias has been merged).

Convention (per CLAUDE.md): dry-run by default, --apply to commit.

Usage:
    PYTHONPATH=. python3 scripts/apply_city_consolidation.py
    PYTHONPATH=. python3 scripts/apply_city_consolidation.py --apply
    PYTHONPATH=. python3 scripts/apply_city_consolidation.py -i data/city_duplicates.csv --apply
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from collections import defaultdict
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
log = logging.getLogger("apply_city_consolidation")

from app.database import SessionLocal  # noqa: E402
from app.models import City  # noqa: E402


APPROVED_VALUES = {"y", "yes", "true", "1", "approved"}


def _truthy(s: str) -> bool:
    return (s or "").strip().lower() in APPROVED_VALUES


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("-i", "--input", default="data/city_duplicates.csv",
                    help="Path to the reviewed detect CSV.")
    ap.add_argument("--apply", action="store_true",
                    help="Actually write the FKs (default is dry-run).")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        log.error(f"Input CSV not found: {in_path}")
        return 1

    with in_path.open() as f:
        rows = [r for r in csv.DictReader(f) if _truthy(r.get("approved", ""))]
    log.info(f"Loaded {len(rows):,} approved rows from {in_path}")
    if not rows:
        log.info("Nothing approved — nothing to do.")
        return 0

    by_rel = defaultdict(int)
    for r in rows:
        by_rel[r["relationship"]] += 1
    log.info(f"  breakdown: {dict(by_rel)}")

    db = SessionLocal()
    try:
        applied = 0
        skipped_conflict = 0
        skipped_already_set = 0
        skipped_chain = 0

        # Build a quick view of which rows are already targets of someone
        # else (so we can detect alias-of-alias chains and refuse them).
        existing_canon_targets = {
            row.id: row.canonical_city_id
            for row in db.query(City).filter(City.canonical_city_id.isnot(None)).all()
        }

        for r in rows:
            rel = r["relationship"]
            try:
                target_id = int(r["canonical_id"])
                source_id = int(r["alias_id"])
            except (KeyError, ValueError):
                log.warning(f"  skip — malformed ids: {r}")
                continue

            source = db.query(City).filter(City.id == source_id).first()
            target = db.query(City).filter(City.id == target_id).first()
            if source is None or target is None:
                log.warning(f"  skip — row not found (source={source_id}, target={target_id})")
                continue

            if rel == "alias":
                # Chain check: target itself should not already be aliased
                # to a third row. If it is, the operator should re-run
                # detect after merging the deeper chain.
                if target.canonical_city_id is not None:
                    log.warning(
                        f"  CHAIN — target #{target_id} ({target.name}) is already "
                        f"aliased to #{target.canonical_city_id}. Re-run detect "
                        f"after the deeper merge."
                    )
                    skipped_chain += 1
                    continue
                if source.canonical_city_id == target_id:
                    skipped_already_set += 1
                    continue
                if source.canonical_city_id is not None:
                    log.warning(
                        f"  CONFLICT — #{source_id} ({source.name}) already aliased to "
                        f"#{source.canonical_city_id}, not #{target_id} ({target.name}). "
                        f"Skipping; resolve manually."
                    )
                    skipped_conflict += 1
                    continue
                # Also reject self-alias (shouldn't happen but be safe)
                if source_id == target_id:
                    log.warning(f"  skip — self-alias ({source_id})")
                    continue

                log.info(
                    f"  alias  #{source_id:5d} {source.name!r} → "
                    f"#{target_id:5d} {target.name!r}"
                )
                if args.apply:
                    source.canonical_city_id = target_id
                applied += 1

            elif rel == "nested":
                if source.parent_city_id == target_id:
                    skipped_already_set += 1
                    continue
                if source.parent_city_id is not None:
                    log.warning(
                        f"  CONFLICT — #{source_id} ({source.name}) already has "
                        f"parent #{source.parent_city_id}, not #{target_id} "
                        f"({target.name}). Skipping; resolve manually."
                    )
                    skipped_conflict += 1
                    continue
                if source_id == target_id:
                    log.warning(f"  skip — self-parent ({source_id})")
                    continue

                log.info(
                    f"  nested #{source_id:5d} {source.name!r} ⊂ "
                    f"#{target_id:5d} {target.name!r}"
                )
                if args.apply:
                    source.parent_city_id = target_id
                applied += 1

            else:
                log.warning(f"  skip — unknown relationship {rel!r}")

        if args.apply:
            db.commit()
            log.info(f"COMMITTED {applied} links.")
        else:
            db.rollback()
            log.info(f"DRY-RUN — would apply {applied} links (re-run with --apply to commit).")

        if skipped_already_set:
            log.info(f"  no-op (already set): {skipped_already_set}")
        if skipped_conflict:
            log.info(f"  conflicts (manual review needed): {skipped_conflict}")
        if skipped_chain:
            log.info(f"  chain conflicts: {skipped_chain}")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
