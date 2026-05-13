"""Backfill `events.tournament` for historical sport rows.

The `tournament` column is set at write time by sport collectors going
forward (espn.py, tennis.py). This script populates the column for rows
that pre-date the collector change.

Heuristics — all derived from existing data, no external lookup:

  - **ESPN team sports** (`scrape_source = 'espn_sports'`): the event
    name is constructed as ``"{cfg.label} - {home} vs {away}"`` (see
    espn.py `_build_raw_event`). The tournament is everything before
    the last ``" - "``. Robust to labels that contain " - " themselves
    (no real league label does today, but `rsplit(' - ', 1)` future-
    proofs that).
  - **Tennis** (`scrape_source = 'tennis_espn'`): the event name is
    ``"{ATP|WTA} - {tournament name}"``. Tournament is the part after
    the prefix — Grand Slam events should group ATP and WTA draws
    under a single tournament label (the bare name).
  - **Ticketmaster World Cup** (`scrape_source = 'ticketmaster'`): TM
    ingests the 2026 FIFA World Cup fixture list as Sports events
    with names like ``"World Cup: Match 1 Group A - Mexico vs South
    Africa - 2026 World Cup"`` and ``"World Cup Round of 32: 1A
    vs. TBD (Match 79) - 2026 World Cup"``. Both are tagged with
    ``"FIFA World Cup"`` so they group under the same Tournament chip
    as ESPN's fifa.world rows. Other Ticketmaster sport rows
    (concerts, generic listings) are left untouched.

Other sport collectors (Euroleague, MLB, OpenF1, Diamond League, BSL,
cricapi, world_aquatics, uci_worldtour) are intentionally not handled
here for v1. The Tournament chip is allowlisted to "FIFA World Cup"
only on the AC side, so widening collector coverage is a separate
ship.

Safety:
  - ``--apply`` is required to write. Default is dry-run (prints
    summary + sample, exits clean).
  - Only touches rows where ``tournament IS NULL`` AND
    ``scrape_source IN ('espn_sports', 'tennis_espn')``. Re-running
    after a successful apply is a no-op (no rows match).
  - Skips rows whose name doesn't split cleanly on " - " (preserves
    NULL rather than writing a degenerate value like the whole name).

Usage:
    PYTHONPATH=. python3 scripts/backfill_event_tournament.py
    PYTHONPATH=. python3 scripts/backfill_event_tournament.py --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
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
log = logging.getLogger("backfill_event_tournament")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event  # noqa: E402

ESPN_SOURCE = "espn_sports"
TENNIS_SOURCE = "tennis_espn"
TICKETMASTER_SOURCE = "ticketmaster"
TENNIS_PREFIXES = ("ATP - ", "WTA - ")

# Ticketmaster's World Cup 2026 fixture naming pattern. They publish
# the full schedule as Sports events with names like
# "World Cup: Match 1 Group A - Mexico vs South Africa - 2026 World Cup"
# and "World Cup Round of 32: 1A vs. TBD (Match 79) - 2026 World Cup".
# The label embedded in the name varies, so we match the prefix shape
# rather than the trailing year-tag (which is more inconsistent).
_TICKETMASTER_WC_PREFIXES = (
    "World Cup: Match",
    "World Cup Round of",
)


def derive_tournament(name: str, source: str) -> str | None:
    """Return the tournament label for a row, or None if it can't be
    cleanly extracted (caller should leave the row's tournament NULL)."""
    if not name:
        return None
    if source == ESPN_SOURCE:
        # "FIFA World Cup - Mexico vs South Africa" → "FIFA World Cup"
        if " - " not in name:
            return None
        label = name.rsplit(" - ", 1)[0].strip()
        return label or None
    if source == TENNIS_SOURCE:
        for prefix in TENNIS_PREFIXES:
            if name.startswith(prefix):
                return name[len(prefix):].strip() or None
        return None
    if source == TICKETMASTER_SOURCE:
        # Ticketmaster ingests the World Cup 2026 fixture list as Sports
        # events. We tag them with "FIFA World Cup" so they group under
        # the same Tournament chip as ESPN's fifa.world rows once both
        # sources are populated.
        for prefix in _TICKETMASTER_WC_PREFIXES:
            if name.startswith(prefix):
                return "FIFA World Cup"
        return None
    return None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--apply", action="store_true",
                   help="Write to events.tournament. Default: dry-run.")
    args = p.parse_args()

    db = SessionLocal()
    try:
        # Pull only the rows we might touch — tournament NULL and source
        # in the set we support. Rows that already have a tournament
        # value (set at write time by the new collector code) are
        # skipped automatically.
        candidate_sources = [ESPN_SOURCE, TENNIS_SOURCE, TICKETMASTER_SOURCE]
        rows = (
            db.query(Event.id, Event.name, Event.scrape_source)
            .filter(
                Event.tournament.is_(None),
                Event.scrape_source.in_(candidate_sources),
            )
            .all()
        )
        log.info("Candidate rows (tournament NULL, source in %s): %d",
                 candidate_sources, len(rows))

        updates: list[tuple[int, str]] = []
        skipped = 0
        sample_by_label: dict[str, str] = {}
        label_counts: Counter[str] = Counter()
        for rid, name, src in rows:
            label = derive_tournament(name, src)
            if not label:
                skipped += 1
                continue
            updates.append((rid, label))
            label_counts[label] += 1
            sample_by_label.setdefault(label, name)

        log.info("Derivable: %d, skipped (no clean split): %d", len(updates), skipped)
        log.info("Distinct tournament labels: %d", len(label_counts))
        log.info("Top 15 by count:")
        for label, n in label_counts.most_common(15):
            sample = sample_by_label[label]
            log.info("  %5d  %-30s  e.g. %s", n, label, sample[:60])

        if args.apply:
            log.info("APPLYING — writing %d rows …", len(updates))
            # Batch with executemany — SQLite handles ~5k rows/sec on the
            # Render disk easily, so chunking is for connection sanity
            # not throughput.
            CHUNK = 1000
            for i in range(0, len(updates), CHUNK):
                batch = updates[i:i + CHUNK]
                db.execute(
                    text("UPDATE events SET tournament = :label WHERE id = :id"),
                    [{"id": rid, "label": label} for rid, label in batch],
                )
            db.commit()
            log.info("  wrote %d rows", len(updates))
        else:
            log.info("Dry-run only. Re-run with --apply to write.")

        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
