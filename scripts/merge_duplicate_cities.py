#!/usr/bin/env python3
"""Merge city rows that are the same place under two spellings.

Symptom (QA run 2026-09-18, 90 groups): the cities table holds one row per
spelling — diacritics ("Montréal" 434 venues / "Montreal" 485, "Zürich" /
"Zurich", "São Paulo" / "Sao Paulo"), exonym vs endonym ("Wien" / "Vienna",
"Köln" 804 / "Cologne" 90, "München" 617 / "Munich" 469, "Praha" / "Prague",
"Warszawa" / "Warsaw", "Lisboa" / "Lisbon"), and country-spelling splits
("Prague, Czechia" 79 / "Prague, Czech Republic" 531). Users see the place
twice in autocomplete, event counts split, and PRIORITY_CITIES scraped both
spellings as separate cities.

What it does — same shape as dedupe_us_cities.py:
  1. Group cities by (canonical country, canonical name key). The name key
     strips diacritics/case/punctuation and maps known endonyms to their
     English exonym (EXONYMS below). Country goes through
     app.services.recipes.countries.canon_country ("Czechia" and "Czech
     Republic" land in one group; the row keeps its own country string).
  2. Canonical row = the one whose display name is the English exonym (or
     the ASCII spelling) when present, else the row with the most venues;
     lowest id as tie-break. Rows already pointing at another canonical
     (canonical_city_id) are skipped.
  3. Repoint venues.city_id and platform_venues.city_id; repoint other
     cities' canonical_city_id / parent_city_id; rewrite llm_sources /
     source_recipes / pending_venues city_name strings; delete the dup.

Read-only unless --apply. Idempotent.

    PYTHONPATH=. python3 scripts/merge_duplicate_cities.py            # dry run
    PYTHONPATH=. python3 scripts/merge_duplicate_cities.py --apply
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("merge_duplicate_cities")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.services.recipes.countries import canon_country  # noqa: E402
from app.scheduler.jobs import PRIORITY_CITIES  # noqa: E402

PRIORITY_NAMES = {(n, canon_country(c) or c) for n, c in PRIORITY_CITIES}

# endonym / alt spelling (normalised) → English exonym (normalised)
EXONYMS = {
    "wien": "vienna", "koln": "cologne", "munchen": "munich", "praha": "prague",
    "warszawa": "warsaw", "lisboa": "lisbon", "sevilla": "seville", "firenze": "florence",
    "roma": "rome", "milano": "milan", "napoli": "naples", "venezia": "venice",
    "torino": "turin", "genova": "genoa", "bruxelles": "brussels", "brussel": "brussels",
    "antwerpen": "antwerp", "kobenhavn": "copenhagen", "goteborg": "gothenburg",
    "geneve": "geneva", "athina": "athens",
    "tel aviv yafo": "tel aviv", "tel aviv jaffa": "tel aviv", "yerushalayim": "jerusalem",
    "den haag": "the hague", "s gravenhage": "the hague", "nurnberg": "nuremberg",
    "hannover": "hanover", "frankfurt am main": "frankfurt", "bucuresti": "bucharest",
    "beograd": "belgrade", "zagreb": "zagreb", "lyon": "lyon", "marseille": "marseille",
    "krakow": "krakow", "gdansk": "gdansk", "wroclaw": "wroclaw", "lodz": "lodz",
    "montreal": "montreal", "quebec": "quebec", "zurich": "zurich",
}


def norm(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^\w ]+", " ", s).replace("_", " ")   # keep non-Latin letters (渋谷区 ≠ 大阪市)
    return re.sub(r"\s+", " ", s).strip()


def name_key(name: str | None) -> str:
    n = norm(name)
    return EXONYMS.get(n, n)


def is_ascii(s: str) -> bool:
    return all(ord(c) < 128 for c in s)


def load_cities(db) -> list[dict]:
    venues = dict(db.execute(text("select city_id, count(*) from venues group by city_id")).all())
    upcoming = dict(db.execute(text("""select v.city_id, count(*) from events e join venues v on v.id = e.venue_id
                                       where e.start_date >= date('now') group by v.city_id""")).all())
    rows = db.execute(text("select id, name, country, state, canonical_city_id, parent_city_id from cities")).mappings().all()
    return [{**dict(r), "venues": venues.get(r["id"], 0), "upcoming": upcoming.get(r["id"], 0)} for r in rows]


def plan(cities: list[dict]) -> list[dict]:
    groups: dict = defaultdict(list)
    for c in cities:
        if c["canonical_city_id"]:
            continue                      # already consolidated as a sub-area / alias
        if not c["name"] or not c["country"] or not name_key(c["name"]):
            continue
        country = canon_country(c["country"]) or c["country"]
        # US cities: same name in different states are different places
        state = (c["state"] or "").strip().upper() if country == "United States" else ""
        groups[(country, state, name_key(c["name"]))].append(c)
    merges = []
    for (country, _state, key), rows in groups.items():
        if len(rows) < 2:
            continue
        has_exonym = any(norm(r["name"]) in EXONYMS for r in rows)

        def rank(c):
            n = norm(c["name"])
            return (
                # 1. English exonym when the group mixes endonym/exonym (Munich over München)
                0 if (has_exonym and n == key) else 1,
                # 2. the spelling the scraper rotation targets
                0 if (c["name"], country) in PRIORITY_NAMES else 1,
                # 3. the row that already carries the data
                -c["venues"], -c["upcoming"],
                # 4. ASCII, then stable
                0 if is_ascii(c["name"]) else 1, c["id"])
        rows.sort(key=rank)
        canon, dups = rows[0], rows[1:]
        merges.append({"country": country, "key": key, "canonical": canon, "dups": dups})
    merges.sort(key=lambda m: -sum(d["venues"] + d["upcoming"] for d in m["dups"]))
    return merges


def apply(db, merges: list[dict]) -> dict:
    n_venues = n_cities = 0
    for m in merges:
        canon = m["canonical"]
        for d in m["dups"]:
            cid, did = canon["id"], d["id"]
            r = db.execute(text("UPDATE venues SET city_id = :c WHERE city_id = :d"), {"c": cid, "d": did})
            n_venues += r.rowcount or 0
            db.execute(text("UPDATE platform_venues SET city_id = :c WHERE city_id = :d"), {"c": cid, "d": did})
            db.execute(text("UPDATE cities SET canonical_city_id = :c WHERE canonical_city_id = :d"), {"c": cid, "d": did})
            db.execute(text("UPDATE cities SET parent_city_id = :c WHERE parent_city_id = :d"), {"c": cid, "d": did})
            for table in ("llm_sources", "source_recipes", "pending_venues"):
                db.execute(text(f"UPDATE {table} SET city_name = :cn WHERE city_name = :dn"),
                           {"cn": canon["name"], "dn": d["name"]})
            db.execute(text("DELETE FROM cities WHERE id = :d"), {"d": did})
            n_cities += 1
        db.commit()
    return {"cities_removed": n_cities, "venues_repointed": n_venues}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--limit", type=int, default=None, help="only the first N merge groups")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        cities = load_cities(db)
        merges = plan(cities)
        if args.limit:
            merges = merges[: args.limit]
        tot_dups = sum(len(m["dups"]) for m in merges)
        tot_venues = sum(d["venues"] for m in merges for d in m["dups"])
        tot_up = sum(d["upcoming"] for m in merges for d in m["dups"])
        log.info("%d merge groups → %d duplicate city rows, %d venues and %d upcoming events to repoint",
                 len(merges), tot_dups, tot_venues, tot_up)
        for m in merges[:60]:
            c = m["canonical"]
            log.info("  %-22s ← %s   [%s]", f"{c['name']} #{c['id']} ({c['venues']}v/{c['upcoming']}e)",
                     ", ".join(f"{d['name']} #{d['id']} ({d['venues']}v/{d['upcoming']}e, {d['country']})" for d in m["dups"]),
                     m["country"])
        if len(merges) > 60:
            log.info("  … %d more groups", len(merges) - 60)
        if not args.apply:
            log.info("DRY RUN — nothing written. Re-run with --apply.")
            return
        res = apply(db, merges)
        log.info("APPLIED: %s", res)
    finally:
        db.close()


if __name__ == "__main__":
    main()
