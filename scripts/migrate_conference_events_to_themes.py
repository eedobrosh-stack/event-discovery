"""One-off migration: collapse the conference-specific event_types into
the existing 'Tech Conference' type + per-event theme assignment.

Before  (event_type bakes the topic into the name):
  Tech Conference                108 events   (canonical — stays)
  AI Tech Conferences            364          (merge → Tech Conference + AI theme)
  Startup Showcases              131          (merge → Tech Conference + Startup theme)
  Cybersecurity Conferences        1          (merge → Tech Conference + Cybersecurity theme)
  Consumer Electronics Shows       6          (merge → Tech Conference + Consumer Electronics theme)

After:
  Tech Conference                ~610 events  (one canonical type)
  + theme association via event_themes (AI, Cybersecurity, Startup,
    Consumer Electronics, …) where the keyword classifier matches
    the event name + description, with a per-retired-type fallback
    theme when no keyword hits.

The 4 retired event_types are deleted at the end since nothing should
reference them post-migration (cascade via event_event_types).

Per the project convention: dry-run by default, --apply to commit.

Usage:
    PYTHONPATH=. python3 scripts/migrate_conference_events_to_themes.py
    PYTHONPATH=. python3 scripts/migrate_conference_events_to_themes.py --apply
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
log = logging.getLogger("migrate_conference_events_to_themes")

from app.database import SessionLocal  # noqa: E402
from app.models import Event, EventType, EventTheme  # noqa: E402
from scripts.categorize_events import theme_match  # noqa: E402


# Canonical type — 'Tech Conference' stays as the umbrella event_type
# for all conference-shaped events. Per the operator's call on
# 2026-05-30, this is preferred over creating a new generic 'Conference'
# type: 'Tech Conference' already exists with 108 events, integrating
# with the existing autocomplete + classifier surface; the topic
# specialization moves to themes.
CANONICAL_TYPE_NAME = "Tech Conference"

# Event types being merged into the canonical. Loop skips any name not
# present (idempotent on re-runs and tolerant of earlier cleanups).
RETIRED_TYPES = [
    "AI Tech Conferences",
    "Startup Showcases",
    "Cybersecurity Conferences",
    "Consumer Electronics Shows",
]

# Per-old-type fallback themes when the keyword classifier doesn't
# match any of the seeded themes against an event's name/description.
# Example: an "AI Tech Conferences" event whose name is just "Annual
# Conference 2026" won't trigger any of the AI keywords; we fall back
# to the theme that the original event_type implied.
FALLBACK_THEME_BY_TYPE = {
    "AI Tech Conferences":        "AI",
    "Startup Showcases":          "Startup",
    "Cybersecurity Conferences":  "Cybersecurity",
    "Consumer Electronics Shows": "Consumer Electronics",
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Actually mutate the DB (default is dry-run).")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        # ── Step 1: ensure the canonical type exists ─────────────────
        # 'Tech Conference' should already be in the table (108 events
        # link to it pre-migration). This is a safety check + a way to
        # keep the script idempotent if it gets re-run on a system
        # where the row was somehow dropped.
        canonical = db.query(EventType).filter_by(name=CANONICAL_TYPE_NAME).first()
        if canonical is None:
            log.warning(
                f"Canonical type {CANONICAL_TYPE_NAME!r} doesn't exist — "
                f"this is unexpected. Aborting; please seed it first."
            )
            return 1
        log.info(f"Canonical type: #{canonical.id} {canonical.name} ({canonical.category})")

        # ── Step 2: find all events whose only / primary event_type is
        # one of the retired set. We migrate them by stripping the
        # old type and assigning Conference.
        retired_rows = (
            db.query(EventType)
            .filter(EventType.name.in_(RETIRED_TYPES))
            .all()
        )
        if not retired_rows:
            log.info("No retired event_type rows found — nothing to migrate.")
            return 0

        retired_ids = [t.id for t in retired_rows]
        retired_id_to_name = {t.id: t.name for t in retired_rows}
        log.info(f"Found retired event_types: {[t.name for t in retired_rows]}")

        # Pull every event linked to one of the retired types. Some
        # events may have multiple type rows; we strip ONLY the retired
        # one(s) and keep the rest.
        from app.models import event_event_types as _eet
        from sqlalchemy import select
        affected_event_ids = [
            r[0] for r in db.execute(
                select(_eet.c.event_id)
                .where(_eet.c.event_type_id.in_(retired_ids))
                .distinct()
            ).fetchall()
        ]
        log.info(f"Affected events: {len(affected_event_ids):,}")
        if not affected_event_ids:
            return 0

        # ── Step 3: walk events, swap event_types, assign themes ─────
        stats = Counter()
        themes_assigned = 0
        for eid in affected_event_ids:
            ev = db.query(Event).filter_by(id=eid).first()
            if ev is None:
                continue

            # Track which retired types this event had — used for the
            # fallback-theme heuristic when no keyword matches.
            prior_retired_names = [
                retired_id_to_name[t.id] for t in ev.event_types
                if t.id in set(retired_ids)
            ]
            for rn in prior_retired_names:
                stats[f"from:{rn}"] += 1

            # New event_types list: everything that wasn't retired, plus
            # the canonical Tech Conference (avoid dupe if it's already
            # attached — common when an event was previously tagged with
            # both AI Tech Conferences AND Tech Conference).
            new_types = [t for t in ev.event_types if t.id not in set(retired_ids)]
            if canonical not in new_types:
                new_types.append(canonical)
            if args.apply:
                ev.event_types = new_types

            # Theme assignment: run keyword classifier; fall back to
            # the retired-type-implied theme if no keyword match.
            text = " ".join([ev.name or "", ev.description or ""])
            matched = theme_match(text)
            if not matched:
                # Fallback: use the most-specific retired type's implied theme.
                for rn in prior_retired_names:
                    fallback = FALLBACK_THEME_BY_TYPE.get(rn)
                    if fallback and fallback not in matched:
                        matched.append(fallback)

            if matched and args.apply:
                existing = {
                    et.theme_name for et in
                    db.query(EventTheme).filter(EventTheme.event_id == eid).all()
                }
                for theme in matched:
                    if theme not in existing:
                        db.add(EventTheme(event_id=eid, theme_name=theme))
                        themes_assigned += 1
            for theme in matched:
                stats[f"theme:{theme}"] += 1

        log.info(f"Stats: {dict(stats)}")
        log.info(f"Themes assigned (new rows): {themes_assigned}")

        # ── Step 4: delete the retired event_types ───────────────────
        # ev.event_types reassignment above strips the m2m rows; once
        # every event has been migrated, the retired event_type rows
        # have no remaining references and are safe to delete.
        if args.apply:
            db.commit()  # flush the m2m changes first
            for et in retired_rows:
                # Sanity: confirm no remaining references before delete.
                remaining = db.execute(
                    select(_eet.c.event_id).where(_eet.c.event_type_id == et.id)
                ).first()
                if remaining:
                    log.warning(
                        f"  SKIP DELETE — {et.name!r} still has m2m references; "
                        f"check for events the loop missed"
                    )
                    continue
                log.info(f"  deleting EventType {et.name!r}")
                db.delete(et)
            db.commit()
            log.info("APPLIED — migration complete.")
        else:
            db.rollback()
            log.info(f"DRY-RUN — would migrate {len(affected_event_ids):,} events, "
                     f"assign {themes_assigned} themes, delete {len(retired_rows)} event_types")

        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
