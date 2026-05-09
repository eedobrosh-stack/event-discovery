"""De-duplicate event rows that the same show was scraped twice from
different collectors.

Symptom (observed 2026-05-09 after running the venue dedupe): the
events table holds duplicate rows for one real show when two collectors
scrape the same venue. Concrete example — "Kind of Blue 2026-05-14"
at Shablul Jazz Club appears twice: one row from the Smarticket
collector tagged ``Jazz Concert``, another from the mevalim collector
tagged ``Pop Concert``. Both rows have the same date and venue but
different ``scrape_source`` / ``source_id`` pairs, so the
``ix_events_dedup`` unique index doesn't catch them — that index is
``(scrape_source, source_id)``-scoped.

After scripts/dedupe_venues.py merged the venue rows, the duplicate
events now share ``venue_id``, which makes them trivially detectable
on ``(start_date, venue_id, identifier)``.

Detection — within scope, two events are flagged as a duplicate pair
when ALL of these hold:

  1. Same ``start_date``.
  2. Same non-null ``venue_id``. (Events with null venue can't be
     reliably cross-deduped on venue identity; they're skipped.)
  3. Same primary identifier — ``LOWER(strip(coalesce(artist_name, name)))``.
     Different collectors stash the title in different fields (Smarticket
     leaves ``artist_name`` null and puts the title in ``name``; mevalim
     duplicates the title into both). The identifier collapses to one
     string so the title doesn't need to live in the same column on
     each side.
  4. Different ``scrape_source`` (so we don't fight ``ix_events_dedup``).
  5. Time check — if BOTH have non-null ``start_time`` and they differ,
     DO NOT match (different shows on the same day, e.g. matinee +
     evening). If at least one is null, match on date alone.

Canonical pick (highest first):
  1. Has ``purchase_link`` set (the row that lets users buy tickets).
  2. Has non-null ``artist_name``.
  3. More populated optional columns.
  4. Lower ``id`` (stable tie-break).

Apply phase, in one transaction per cluster:
  - Backfill canonical's null fields from the first dup that has them
    (artist_name, artist_youtube_channel, artist_popularity,
    artist_spotify_url, start_time, end_date, end_time, purchase_link,
    price, price_currency, description, image_url, venue_name).
  - Union ``event_types`` from all rows onto canonical so a Pop+Jazz
    pair becomes a single row tagged with BOTH. Search filters are
    ``any event_type matches`` so two tags won't double-count results;
    cleaning up genuinely-wrong tags is item #3 (event_type classifier
    improvement).
  - Delete dup rows via the ORM so the m2m entries cascade cleanly.

Safety:
  - ``--apply`` is required to write. Default mode is dry-run.
  - ``--city-id N`` scopes via venue→city.
  - ``--venue-id N`` scopes to a single venue.
  - ``--start-date YYYY-MM-DD`` / ``--end-date YYYY-MM-DD`` scope by
    event start_date — recommended for first runs.
  - Audit JSON written to data/dedupe_events_<ts>_{dryrun,apply}.json.

Idempotent — re-running after a clean apply finds nothing.

Usage:
    PYTHONPATH=. python3 scripts/dedupe_events.py --city-id 239 \\
        --start-date 2026-05-01 --end-date 2026-07-01
    PYTHONPATH=. python3 scripts/dedupe_events.py --city-id 239 \\
        --start-date 2026-05-01 --end-date 2026-07-01 --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import sys
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
log = logging.getLogger("dedupe_events")

from sqlalchemy import and_, or_  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.models import Event, Venue  # noqa: E402

# Optional columns we backfill from duplicates onto canonical when
# canonical has null/empty in that column. The first dup with a
# non-null value wins. Order does not matter.
BACKFILL_COLS = [
    "artist_name", "artist_youtube_channel", "artist_popularity",
    "artist_spotify_url", "start_time", "end_date", "end_time",
    "purchase_link", "price", "price_currency",
    "description", "image_url", "venue_name",
]

# Columns whose non-null state contributes to the "more populated"
# canonical-pick tiebreak.
POPULATED_TIEBREAK_COLS = BACKFILL_COLS


def _norm(s: str | None) -> str:
    """Lowercase, collapse whitespace, drop trailing punct. Used for
    artist_name / event-name comparison only."""
    if not s:
        return ""
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return re.sub(r"[,.;:!?]+$", "", s).strip()


def _identifier(e: Event) -> str | None:
    """Primary normalized identifier for equality comparison. Uses
    ``artist_name`` when set, falling back to ``name``. Different
    collectors store the same show's title in different fields:
    Smarticket leaves ``artist_name`` null and puts the title in
    ``name``; mevalim duplicates the title into both fields. The
    identifier collapses both shapes to one string so cross-source
    matches don't require the title to live in the same column on
    each row. Returns None when both fields are empty."""
    a = _norm(e.artist_name)
    if a:
        return a
    n = _norm(e.name)
    if n:
        return n
    return None


def _populated_count(e: Event) -> int:
    return sum(
        1 for c in POPULATED_TIEBREAK_COLS
        if getattr(e, c, None) not in (None, "")
    )


def _times_compatible(a: Event, b: Event) -> bool:
    """True when start_times don't contradict each other.
    If both have non-empty start_time and they differ → False.
    Else (at least one null) → True."""
    ta = (a.start_time or "").strip()
    tb = (b.start_time or "").strip()
    if ta and tb and ta != tb:
        return False
    return True


def _load_events(
    db,
    city_id: int | None,
    venue_id: int | None,
    start: date | None,
    end: date | None,
) -> list[Event]:
    q = db.query(Event).join(Venue, Event.venue_id == Venue.id)
    if city_id is not None:
        q = q.filter(Venue.city_id == city_id)
    if venue_id is not None:
        q = q.filter(Event.venue_id == venue_id)
    if start is not None:
        q = q.filter(Event.start_date >= start)
    if end is not None:
        q = q.filter(Event.start_date <= end)
    return q.all()


def _group_candidates(events: list[Event]) -> list[list[Event]]:
    """Bucket by (start_date, venue_id, identifier). Within each bucket,
    keep only buckets where ≥2 distinct ``scrape_source`` values appear
    AND time predicates are compatible across all members. Returns a
    list of dup clusters (length ≥ 2)."""
    buckets: dict[tuple, list[Event]] = {}
    for e in events:
        if e.venue_id is None:
            continue
        ident = _identifier(e)
        if ident is None:
            continue
        key = (e.start_date, e.venue_id, ident)
        buckets.setdefault(key, []).append(e)

    clusters: list[list[Event]] = []
    for members in buckets.values():
        if len(members) < 2:
            continue
        # Require ≥2 distinct scrape_sources in the bucket — otherwise
        # whatever multiplicity is here is an intra-source artifact
        # we don't want to touch.
        sources = {(m.scrape_source or "") for m in members}
        if len(sources) < 2:
            continue
        # Time-compatibility check: all pairs must be compatible. If
        # not, split the bucket into sub-clusters where each sub-cluster
        # is mutually compatible. Simple approach: pin each member's
        # effective time (its non-empty start_time, or "" for null) and
        # group by that — null members get joined to whichever pinned
        # time appears, but if multiple distinct non-empty times exist
        # they form separate sub-clusters.
        non_empty_times = sorted({(m.start_time or "").strip()
                                  for m in members
                                  if (m.start_time or "").strip()})
        if len(non_empty_times) <= 1:
            # Either no explicit time on any row, or all explicit times
            # agree — cluster is mutually compatible.
            clusters.append(members)
            continue
        # Multiple distinct explicit times → distinct shows on the same
        # day at the same venue. Split into per-time sub-clusters.
        # Null-time rows are ambiguous — leave them alone (don't fold
        # into either sub-cluster, since either pairing could be wrong).
        sub: dict[str, list[Event]] = {t: [] for t in non_empty_times}
        for m in members:
            t = (m.start_time or "").strip()
            if t:
                sub[t].append(m)
        for t in non_empty_times:
            m_list = sub[t]
            if len(m_list) >= 2 and len({(m.scrape_source or "") for m in m_list}) >= 2:
                clusters.append(m_list)
    return clusters


def _pick_canonical(cluster: list[Event]) -> tuple[Event, list[Event]]:
    """Order by (has purchase_link, has artist_name, populated count,
    lower id). First is canonical."""
    ranked = sorted(cluster, key=lambda e: (
        0 if (e.purchase_link or "").strip() else 1,
        0 if (e.artist_name or "").strip() else 1,
        -_populated_count(e),
        e.id,
    ))
    return ranked[0], ranked[1:]


def fold_cluster(db, canonical: Event, dups: list[Event], *, apply: bool) -> dict:
    backfilled: list[str] = []
    for col in BACKFILL_COLS:
        if getattr(canonical, col, None) in (None, ""):
            for d in dups:
                v = getattr(d, col, None)
                if v not in (None, ""):
                    if apply:
                        setattr(canonical, col, v)
                    backfilled.append(col)
                    break

    # Union event_types — set on canonical to the unique combination.
    types_before = {t.id for t in canonical.event_types}
    types_added: list[str] = []
    if apply:
        for d in dups:
            for t in d.event_types:
                if t.id not in types_before:
                    canonical.event_types.append(t)
                    types_before.add(t.id)
                    types_added.append(t.name)
    else:
        seen = set(types_before)
        for d in dups:
            for t in d.event_types:
                if t.id not in seen:
                    seen.add(t.id)
                    types_added.append(t.name)

    if apply:
        for d in dups:
            db.delete(d)
        db.commit()

    return {
        "canonical_id": canonical.id,
        "canonical_name": canonical.name,
        "canonical_artist": canonical.artist_name,
        "start_date": str(canonical.start_date),
        "venue_id": canonical.venue_id,
        "scrape_sources": sorted({
            (e.scrape_source or "") for e in [canonical, *dups]
        }),
        "duplicates": [
            {
                "id": d.id,
                "name": d.name,
                "artist_name": d.artist_name,
                "scrape_source": d.scrape_source,
                "source_id": d.source_id,
                "start_time": d.start_time,
                "purchase_link_present": bool((d.purchase_link or "").strip()),
            }
            for d in dups
        ],
        "fields_backfilled_to_canonical": backfilled,
        "event_types_added_to_canonical": types_added,
    }


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true",
                        help="Write changes. Without this, runs as a dry-run.")
    parser.add_argument("--city-id", type=int, default=None,
                        help="Restrict to events at venues in this city.")
    parser.add_argument("--venue-id", type=int, default=None,
                        help="Restrict to a single venue.")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Inclusive lower bound on Event.start_date (YYYY-MM-DD).")
    parser.add_argument("--end-date", type=str, default=None,
                        help="Inclusive upper bound on Event.start_date (YYYY-MM-DD).")
    args = parser.parse_args()

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)

    mode = "APPLY" if args.apply else "DRY-RUN"
    scope_parts = []
    if args.city_id: scope_parts.append(f"city_id={args.city_id}")
    if args.venue_id: scope_parts.append(f"venue_id={args.venue_id}")
    if start: scope_parts.append(f"start≥{start}")
    if end: scope_parts.append(f"end≤{end}")
    scope = ", ".join(scope_parts) if scope_parts else "ALL EVENTS"
    log.info(f"mode={mode} scope=[{scope}]")

    db = SessionLocal()
    try:
        events = _load_events(db, args.city_id, args.venue_id, start, end)
        log.info(f"loaded {len(events)} event rows in scope")
        clusters = _group_candidates(events)
        log.info(f"duplicate clusters: {len(clusters)}")
        if not clusters:
            log.info("nothing to do.")
            return

        plan = []
        for cluster in clusters:
            canonical, dups = _pick_canonical(cluster)
            log.info(
                f"cluster venue_id={canonical.venue_id} date={canonical.start_date} "
                f"keep id={canonical.id} {(canonical.artist_name or canonical.name)[:50]!r} "
                f"src={canonical.scrape_source}"
            )
            for d in dups:
                log.info(
                    f"  drop id={d.id} src={d.scrape_source} "
                    f"src_id={d.source_id} {(d.artist_name or d.name)[:50]!r}"
                )
            res = fold_cluster(db, canonical, dups, apply=args.apply)
            if res["fields_backfilled_to_canonical"]:
                log.info(f"  backfill→canonical: {res['fields_backfilled_to_canonical']}")
            if res["event_types_added_to_canonical"]:
                log.info(f"  +event_types: {res['event_types_added_to_canonical']}")
            plan.append(res)

        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        suffix = "apply" if args.apply else "dryrun"
        audit_path = ROOT / "data" / f"dedupe_events_{ts}_{suffix}.json"
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2, default=str))
        log.info(f"audit written: {audit_path}")
        log.info(f"{len(plan)} clusters, "
                 f"{sum(len(p['duplicates']) for p in plan)} duplicate rows "
                 f"{'deleted' if args.apply else 'would be deleted'}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
