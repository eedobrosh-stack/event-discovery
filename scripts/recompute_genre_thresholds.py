"""Recompute per-parent-genre percentile thresholds for popularity stars.

Pre-requisite: scripts/recompute_popularity.py has populated
Performer.derived_popularity. This script reads those scores, partitions
them by the artist's parent genre, and writes the 20/40/60/80 percentile
boundaries to genre_popularity_thresholds.

The API (step #4) uses the boundaries to convert an artist's raw 0-100
score into a 1-5 star rating *relative to that genre*. A score of 75 in
Jazz might be top-tier (5★) because the jazz distribution maxes out
around 75-80; the same 75 in Pop is mid-tier (3★) because pop's elite
band stretches into the 90s.

Computation per parent genre:
  1. Resolve all classified+scored artists under this parent
     (artist_genre.primary_genre → genre_taxonomy.parent_genre →
      Performer with non-null derived_popularity).
  2. Skip the genre if the sample is below MIN_N (default 30) — too
     few datapoints for stable percentiles. Genres below the floor
     get NO threshold row at all; the API treats absence as
     "no stars displayable for this genre yet".
  3. statistics.quantiles(scores, n=5) gives the 4 internal cutpoints
     (i.e. the 20 / 40 / 60 / 80 percentiles).

Output: rows in genre_popularity_thresholds, one per qualifying parent
genre. Replaces existing rows in-place (UPSERT). Genres that drop below
MIN_N on a re-run get their existing row deleted, so stars stop
appearing rather than going stale.

Usage:
    PYTHONPATH=. python3 scripts/recompute_genre_thresholds.py
    PYTHONPATH=. python3 scripts/recompute_genre_thresholds.py --apply
    PYTHONPATH=. python3 scripts/recompute_genre_thresholds.py --apply --min-n 15
"""
from __future__ import annotations

import argparse
import json
import logging
import statistics
import sys
from collections import defaultdict
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
log = logging.getLogger("recompute_genre_thresholds")

from sqlalchemy import func  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Performer  # noqa: E402
from app.models.genre import (  # noqa: E402
    ArtistGenre, GenreTaxonomy, GenrePopularityThresholds,
)

# Statistical floor: genres with fewer than this many scored
# performers don't get thresholds. 30 is a conservative tradeoff
# between "show stars sooner for niche genres" and "don't compute
# percentiles from N=8 datapoints". Tunable via CLI.
MIN_N_DEFAULT = 30


def _gather_scores_per_parent(db) -> dict[str, list[int]]:
    """Walk the artist_genre + genre_taxonomy + performer chain,
    bucketing derived_popularity scores by parent genre.

    Performer is matched by lower-cased name == ArtistGenre.normalized_name
    (same key used everywhere else in the codebase). An artist whose
    primary_genre maps to a parent gets their score added to that
    parent's bucket.
    """
    # Resolve sub_genre → parent_genre once.
    sub_to_parent = dict(
        db.query(GenreTaxonomy.sub_genre, GenreTaxonomy.parent_genre).all()
    )

    # Pull all classified non-UNKNOWN artists' (normalized_name,
    # primary_genre) — these are eligible for star ranking.
    classified = (
        db.query(ArtistGenre.normalized_name, ArtistGenre.primary_genre)
        .filter(
            ArtistGenre.primary_genre.isnot(None),
            ArtistGenre.primary_genre != "UNKNOWN",
        )
        .all()
    )
    log.info(f"classified artists in scope: {len(classified):,}")

    name_to_parent: dict[str, str] = {}
    for norm, primary in classified:
        parent = sub_to_parent.get(primary)
        if parent and norm:
            name_to_parent[norm] = parent

    # Pull performers with derived_popularity in one shot.
    perfs = (
        db.query(Performer.name, Performer.derived_popularity)
        .filter(Performer.derived_popularity.isnot(None))
        .all()
    )
    log.info(f"performers with derived_popularity: {len(perfs):,}")

    by_parent: dict[str, list[int]] = defaultdict(list)
    for name, score in perfs:
        key = (name or "").lower().strip()
        if not key:
            continue
        parent = name_to_parent.get(key)
        if parent:
            by_parent[parent].append(int(score))
    return by_parent


def _percentiles(scores: list[int]) -> tuple[int, int, int, int]:
    """Return (p20, p40, p60, p80) as integers. Uses
    statistics.quantiles(n=5) which gives 4 internal cutpoints by
    inclusive/exclusive defaults (the 20/40/60/80 boundaries)."""
    cuts = statistics.quantiles(scores, n=5)
    return tuple(int(round(c)) for c in cuts)  # type: ignore[return-value]


def run(*, apply: bool = False, min_n: int = MIN_N_DEFAULT) -> dict:
    """Programmatic entry. Returns a result dict with per-parent
    thresholds + which parents were skipped due to thin samples."""
    db = SessionLocal()
    try:
        by_parent = _gather_scores_per_parent(db)
        log.info(f"parent-genre buckets: {len(by_parent)}")

        kept: dict[str, dict] = {}
        skipped: dict[str, int] = {}
        for parent, scores in sorted(by_parent.items()):
            if len(scores) < min_n:
                skipped[parent] = len(scores)
                log.info(f"  SKIP {parent:<14}  n={len(scores)} (< min_n={min_n})")
                continue
            p20, p40, p60, p80 = _percentiles(scores)
            kept[parent] = {
                "p20": p20, "p40": p40, "p60": p60, "p80": p80,
                "n": len(scores),
                "min": min(scores), "max": max(scores),
            }
            log.info(
                f"  KEEP {parent:<14}  n={len(scores):<5} "
                f"min={min(scores):>3}  p20={p20:>3}  p40={p40:>3}  "
                f"p60={p60:>3}  p80={p80:>3}  max={max(scores):>3}"
            )

        if apply:
            now = datetime.utcnow()
            existing_rows = {
                r.parent_genre: r
                for r in db.query(GenrePopularityThresholds).all()
            }
            for parent, t in kept.items():
                row = existing_rows.get(parent)
                if row:
                    row.p20 = t["p20"]
                    row.p40 = t["p40"]
                    row.p60 = t["p60"]
                    row.p80 = t["p80"]
                    row.n_performers = t["n"]
                    row.computed_at = now
                else:
                    db.add(GenrePopularityThresholds(
                        parent_genre=parent,
                        p20=t["p20"], p40=t["p40"],
                        p60=t["p60"], p80=t["p80"],
                        n_performers=t["n"],
                        computed_at=now,
                    ))
            # Drop rows whose parent dropped below the floor on this
            # run — better to hide stars than to keep stale ones.
            for parent, row in existing_rows.items():
                if parent not in kept:
                    db.delete(row)
            db.commit()
            log.info(f"wrote {len(kept)} threshold rows; dropped "
                     f"{sum(1 for p in existing_rows if p not in kept)} stale")

        return {
            "kept": kept,
            "skipped": skipped,
            "min_n": min_n,
            "applied": apply,
        }
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    parser.add_argument("--min-n", type=int, default=MIN_N_DEFAULT,
                        help=f"Minimum sample size per parent genre "
                             f"(default {MIN_N_DEFAULT}). Genres with "
                             f"fewer scored performers are skipped — "
                             f"no thresholds row written, no stars in UI.")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    log.info(f"mode={mode} min_n={args.min_n}")

    result = run(apply=args.apply, min_n=args.min_n)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = "apply" if args.apply else "dryrun"
    audit_path = ROOT / "data" / f"recompute_genre_thresholds_{ts}_{suffix}.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(json.dumps(result, ensure_ascii=False, indent=2))
    log.info(f"audit written: {audit_path}")
    if not args.apply:
        log.info("DRY-RUN — re-run with --apply to write.")


if __name__ == "__main__":
    main()
