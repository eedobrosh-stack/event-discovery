#!/usr/bin/env python3
"""Unify artist spellings across events: "RUSH" / "Rush", "BILL BAILEY" /
"Bill Bailey", "Six: The Musical" / "SIX the Musical", "Public Image Ltd" /
"Public Image Ltd." become one spelling.

Symptom (QA run 2026-09-18): 394 artists appeared under 2+ spellings across
5,702 upcoming events. Each spelling is its own autocomplete chip, its own
artist_genre lookup key and its own YouTube lookup, so coverage numbers and
search results split.

Key = lowercase, diacritics stripped, punctuation removed, whitespace
collapsed, "&" = "and" (a leading "The" is kept: "BEAT" ≠ "The Beat"). Canonical spelling per key:
  1. the most frequent spelling among events,
  2. else the spelling artist_genre / performers already use,
  3. else the one with the most lowercase letters (i.e. not SHOUTING),
  4. else lexicographic (stable).
Ingest-time counterpart: app.services.collectors.registry.canonical_artist_spelling
(case/whitespace variants only). Read-only unless --apply. Idempotent.

    PYTHONPATH=. python3 scripts/canonicalize_artist_names.py            # dry run
    PYTHONPATH=. python3 scripts/canonicalize_artist_names.py --apply
    PYTHONPATH=. python3 scripts/canonicalize_artist_names.py --all-dates  # not just upcoming
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("canonicalize_artist_names")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402


def artist_key(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower().replace("&", " and ")
    s = re.sub(r"[^\w ]+", " ", s).replace("_", " ")
    # NB: a leading "The" is kept — "BEAT" (Belew/Vai/Levin/Carey) is not
    # "The Beat", so "the X" and "X" stay separate artists.
    return re.sub(r"\s+", " ", s).strip()


def pick_canonical(variants: Counter, known: set[str]) -> str:
    def rank(name: str):
        return (-variants[name],
                0 if name in known else 1,
                -sum(1 for c in name if c.islower()),
                name)
    return sorted(variants, key=rank)[0]


def plan(db, all_dates: bool) -> list[dict]:
    where = "" if all_dates else "and start_date >= date('now')"
    rows = db.execute(text(f"""select artist_name, count(*) from events
                               where artist_name is not null and trim(artist_name) <> '' {where}
                               group by artist_name""")).all()
    known = {r[0].strip() for r in db.execute(text("select artist_name from artist_genre where artist_name is not null")).all()}
    known |= {r[0].strip() for r in db.execute(text("select name from performers where name is not null")).all()}
    by_key: dict = defaultdict(Counter)
    for name, n in rows:
        by_key[artist_key(name)][name] = n
    plan_rows = []
    for key, variants in by_key.items():
        if len(variants) < 2 or not key:
            continue
        canon = pick_canonical(variants, known)
        others = {v: n for v, n in variants.items() if v != canon}
        plan_rows.append({"key": key, "canonical": canon, "others": others, "events": sum(others.values())})
    plan_rows.sort(key=lambda r: -r["events"])
    return plan_rows


def apply(db, plan_rows: list[dict], all_dates: bool) -> int:
    where = "" if all_dates else "and start_date >= date('now')"
    n = 0
    for r in plan_rows:
        for other in r["others"]:
            res = db.execute(text(f"update events set artist_name = :c where artist_name = :o {where}"),
                             {"c": r["canonical"], "o": other})
            n += res.rowcount or 0
    db.commit()
    return n


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--all-dates", action="store_true", help="include past events (default: upcoming only)")
    args = ap.parse_args()
    db = SessionLocal()
    try:
        rows = plan(db, args.all_dates)
        log.info("%d artists with 2+ spellings → %d event rows to rewrite", len(rows), sum(r["events"] for r in rows))
        for r in rows[:40]:
            log.info("  %-45s ← %s", r["canonical"][:45], ", ".join(f"{o} ×{n}" for o, n in r["others"].items())[:110])
        if len(rows) > 40:
            log.info("  … %d more", len(rows) - 40)
        if not args.apply:
            log.info("DRY RUN — nothing written. Re-run with --apply.")
            return
        log.info("APPLIED: %d rows updated", apply(db, rows, args.all_dates))
    finally:
        db.close()


if __name__ == "__main__":
    main()
