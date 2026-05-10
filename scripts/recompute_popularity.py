"""Recompute internally-derived popularity for every Performer.

Background — Spotify removed `popularity` / `genres` / `followers` from
the public Web API for Client Credentials apps in late 2024. The
existing `Performer.popularity` column (Spotify-sourced) has been
empty/zero ever since. This script derives a replacement 0–100
popularity score from internal signals — none of which depend on a
fragile vendor commitment.

Inputs (per performer; lower-cased name as the join key):
  upcoming_events      — count of events with start_date >= today
  past_events          — count of events with start_date <  today
  distinct_cities      — distinct Venue.city_id across this performer's events
  median_ticket_price  — median Event.price across non-null prices
  brave_total_results  — ArtistGenre.brave_total_results (capped at 20),
                         the count Brave returned during classification —
                         a "web footprint" signal that especially helps
                         non-English / long-tail artists where event-
                         count alone underweights them

Score is a weighted log-scaled blend, normalised to 0–100. Weights
chosen so that:
  - a brand-new local act with 1 upcoming event scores low single-digits
  - a regularly-touring regional act (~10 upcoming, 5 cities) scores
    in the 30–50 band
  - a major touring artist with 50+ events across 20+ cities and a
    high Brave footprint scores 80+

Per-genre banding (UI stars) is computed in a separate step from this
output; this script only writes the raw score on Performer.

Usage:
    PYTHONPATH=. python3 scripts/recompute_popularity.py
    PYTHONPATH=. python3 scripts/recompute_popularity.py --apply
    PYTHONPATH=. python3 scripts/recompute_popularity.py --apply --limit 100
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import statistics
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
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
log = logging.getLogger("recompute_popularity")

from sqlalchemy import func  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event, Venue, Performer  # noqa: E402
from app.models.genre import ArtistGenre  # noqa: E402

# ── Score formula ─────────────────────────────────────────────────────
#
# Each input is normalised to 0..1 with a log-scale or linear cap, then
# weighted-summed with absent-signal renormalisation (so the total
# weight of present signals always sums to 1.0 — a performer with no
# brave_total_results doesn't get penalised, just assessed on the
# remaining inputs).
#
# Anchors:
#   upcoming  log10(x+1) / log10(101)    → 100 events ≈ score 1.0 contribution
#   past      log10(x+1) / log10(1001)   → 1000 events ≈ 1.0
#   cities    log10(x+1) / log10(51)     → 50 cities ≈ 1.0
#   price     min(median/200, 1.0)       → $200 median ≈ 1.0 (linear)
#   brave     brave / 20                 → 20 results = max page = 1.0
#
# Weights when ALL inputs are present:
#   upcoming = 0.25
#   past     = 0.20
#   cities   = 0.20
#   price    = 0.10
#   brave    = 0.25
#   (sum 1.00)

W_UPCOMING = 0.25
W_PAST = 0.20
W_CITIES = 0.20
W_PRICE = 0.10
W_BRAVE = 0.25

PRICE_ANCHOR = 200.0   # USD-equivalent; "high signal" median ticket price
BRAVE_MAX = 20         # Brave's max page size; what we ask for and cap at


def _score(
    upcoming: int,
    past: int,
    cities: int,
    median_price: float | None,
    brave: int | None,
) -> int:
    """0-100 derived popularity. Returns 0 when no signals are present."""
    upcoming_n = min(1.0, math.log10(max(upcoming, 0) + 1) / math.log10(101))
    past_n     = min(1.0, math.log10(max(past, 0) + 1)     / math.log10(1001))
    cities_n   = min(1.0, math.log10(max(cities, 0) + 1)   / math.log10(51))

    if median_price is not None and median_price > 0:
        price_n = min(1.0, float(median_price) / PRICE_ANCHOR)
        w_price = W_PRICE
    else:
        price_n = 0.0
        w_price = 0.0

    if brave is not None:
        brave_n = min(1.0, brave / BRAVE_MAX)
        w_brave = W_BRAVE
    else:
        brave_n = 0.0
        w_brave = 0.0

    total_w = W_UPCOMING + W_PAST + W_CITIES + w_price + w_brave
    if total_w == 0:
        return 0

    raw = (
        W_UPCOMING * upcoming_n
        + W_PAST * past_n
        + W_CITIES * cities_n
        + w_price * price_n
        + w_brave * brave_n
    ) / total_w

    return max(0, min(100, round(raw * 100)))


# ── Aggregation ───────────────────────────────────────────────────────
def _aggregate_signals(db) -> dict[str, dict]:
    """Walk all events with non-null artist_name once and aggregate per
    lower-cased name. Returns {key → {upcoming, past, cities (set),
    prices (list), brave (int|None)}}."""
    today = date.today()

    # Pull all events + their venue's city_id in one shot. ~145k rows
    # at the time of writing — fits in memory comfortably and is much
    # faster than per-performer queries.
    rows = (
        db.query(
            Event.artist_name, Event.start_date, Event.price,
            Venue.city_id,
        )
        .outerjoin(Venue, Venue.id == Event.venue_id)
        .filter(Event.artist_name.isnot(None), Event.artist_name != "")
        .all()
    )
    log.info(f"loaded {len(rows):,} event rows for aggregation")

    agg: dict[str, dict] = defaultdict(
        lambda: {"upcoming": 0, "past": 0, "cities": set(), "prices": [], "brave": None}
    )
    for an, sd, price, city_id in rows:
        key = (an or "").lower().strip()
        if not key:
            continue
        a = agg[key]
        if sd and sd >= today:
            a["upcoming"] += 1
        else:
            a["past"] += 1
        if city_id is not None:
            a["cities"].add(city_id)
        if price is not None and price > 0:
            a["prices"].append(float(price))

    # Layer in Brave footprint from artist_genre. Same lower-case key
    # the Brave classifier uses (normalized_name).
    brave_rows = (
        db.query(ArtistGenre.normalized_name, ArtistGenre.brave_total_results)
        .filter(ArtistGenre.brave_total_results.isnot(None))
        .all()
    )
    for name, n in brave_rows:
        if name and name in agg:
            agg[name]["brave"] = int(n)
    log.info(f"layered Brave footprint for {len(brave_rows):,} artists "
             f"({sum(1 for k in agg if agg[k]['brave'] is not None):,} matched against event aggregates)")
    return agg


def _median(prices: list[float]) -> float | None:
    if not prices:
        return None
    return statistics.median(prices)


# ── Apply ─────────────────────────────────────────────────────────────
def _apply_scores(
    db, agg: dict[str, dict], *, apply: bool, limit: int | None,
) -> dict:
    """Compute scores per performer (lookup by lower(Performer.name))
    and write to Performer.derived_popularity. Returns summary stats."""
    # Pull all performers in one pass — much faster than per-row lookups.
    performers = db.query(Performer.id, Performer.name).all()
    log.info(f"loaded {len(performers):,} performer rows")

    by_norm: dict[str, list[int]] = defaultdict(list)
    for pid, name in performers:
        key = (name or "").lower().strip()
        if key:
            by_norm[key].append(pid)

    written = 0
    distribution = defaultdict(int)
    score_map: dict[int, int] = {}  # performer_id → score

    iterated = 0
    for key, signals in agg.items():
        if key not in by_norm:
            continue   # no Performer row matches this artist_name; skip
        score = _score(
            upcoming=signals["upcoming"],
            past=signals["past"],
            cities=len(signals["cities"]),
            median_price=_median(signals["prices"]),
            brave=signals["brave"],
        )
        bucket = "0-9" if score < 10 else (
                 "10-24" if score < 25 else (
                 "25-49" if score < 50 else (
                 "50-74" if score < 75 else "75-100")))
        distribution[bucket] += 1
        for pid in by_norm[key]:
            score_map[pid] = score
            iterated += 1
            if limit and iterated >= limit:
                break
        if limit and iterated >= limit:
            break

    if apply and score_map:
        # Bulk-update in chunks so we don't blow the SQL parameter cap.
        ids = list(score_map.keys())
        CHUNK = 500
        for i in range(0, len(ids), CHUNK):
            chunk_ids = ids[i:i + CHUNK]
            for pid in chunk_ids:
                db.query(Performer).filter(Performer.id == pid).update(
                    {"derived_popularity": score_map[pid]},
                    synchronize_session=False,
                )
            db.commit()
        written = len(score_map)

    return {
        "performers_total": len(performers),
        "performers_matched": len({pid for pids in by_norm.values() for pid in pids if pid in score_map}),
        "performers_scored": len(score_map),
        "performers_written": written,
        "distribution": dict(distribution),
    }


# ── Programmatic entry ────────────────────────────────────────────────
def run(*, apply: bool = False, limit: int | None = None) -> dict:
    """Programmatic entry point so the scheduler can invoke recompute
    in-process. Mirrors the CLI ``main()``. Returns the result dict
    augmented with the top-20 list (handy for the audit log)."""
    db = SessionLocal()
    try:
        agg = _aggregate_signals(db)
        log.info(f"aggregated {len(agg):,} distinct artist keys")

        result = _apply_scores(db, agg, apply=apply, limit=limit)
        log.info(
            f"performers_total={result['performers_total']:,}  "
            f"matched={result['performers_matched']:,}  "
            f"scored={result['performers_scored']:,}  "
            f"written={result['performers_written']:,}"
        )
        log.info(f"distribution: {result['distribution']}")

        top = sorted(
            ((k, _score(
                s["upcoming"], s["past"], len(s["cities"]),
                _median(s["prices"]), s["brave"],
            )) for k, s in agg.items()),
            key=lambda x: -x[1],
        )[:20]
        result["top"] = [{"name": n, "score": s} for n, s in top]
        result["aggregated_keys"] = len(agg)
        return result
    finally:
        db.close()


# ── Main ──────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap performers scored (testing).")
    args = parser.parse_args()

    mode = "APPLY" if args.apply else "DRY-RUN"
    log.info(f"mode={mode} limit={args.limit}")

    db = SessionLocal()
    try:
        agg = _aggregate_signals(db)
        log.info(f"aggregated {len(agg):,} distinct artist keys")

        result = _apply_scores(db, agg, apply=args.apply, limit=args.limit)
        log.info(
            f"performers_total={result['performers_total']:,}  "
            f"matched={result['performers_matched']:,}  "
            f"scored={result['performers_scored']:,}  "
            f"written={result['performers_written']:,}"
        )
        log.info(f"distribution: {result['distribution']}")

        # Show top 20 by score as a sanity check
        top = sorted(
            ((k, _score(
                s["upcoming"], s["past"], len(s["cities"]),
                _median(s["prices"]), s["brave"],
            )) for k, s in agg.items()),
            key=lambda x: -x[1],
        )[:20]
        log.info("=== top 20 by derived popularity ===")
        for name, score in top:
            s = agg[name]
            log.info(
                f"  score={score:>3}  upcoming={s['upcoming']:<3}  "
                f"past={s['past']:<4}  cities={len(s['cities']):<2}  "
                f"prices_n={len(s['prices']):<3}  brave={s['brave']!s:<4}  "
                f"{name}"
            )

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = "apply" if args.apply else "dryrun"
        audit_path = ROOT / "data" / f"recompute_popularity_{ts}_{suffix}.json"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(json.dumps({
            "result": result,
            "weights": {
                "upcoming": W_UPCOMING, "past": W_PAST, "cities": W_CITIES,
                "price": W_PRICE, "brave": W_BRAVE,
                "price_anchor": PRICE_ANCHOR, "brave_max": BRAVE_MAX,
            },
            "top": [{"name": n, "score": s} for n, s in top],
        }, ensure_ascii=False, indent=2))
        log.info(f"audit written: {audit_path}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
