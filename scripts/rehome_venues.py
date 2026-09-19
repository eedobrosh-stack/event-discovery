#!/usr/bin/env python3
"""Re-home venues that sit under the wrong City row.

Symptom (QA venue_city check, 2026-09-20: 2,275 venues / 3,976 upcoming
events): "House of Blues (Houston)" (venue 140381, physical_city=Houston,
physical_country=US) and "House of Blues (Anaheim)" (140384) are attached to
Tel Aviv, Israel, so they show up under "Jazz in Tel Aviv". Same shape for
ld_happeningnext_com → Tel Aviv, ld_eventbrite_sg → Curitiba, ld_luma_com →
Berlin, ld_stayhappening_com → New York, skiddle → Berlin.

Root cause: runner.group_events_by_city looked venue_city up ONLY inside the
recipe's country, so a foreign city missed and the event fell back to the
recipe's default city. Fixed at ingest (venue_country-aware lookup, foreign
unknowns skipped); this script repairs the rows already written.

What it does — same shape as merge_duplicate_cities.py / dedupe_events.py:
  1. For every venue with physical_city: canonicalise physical_country
     (countries.canon_country: "US" → "United States"); when it is empty the
     attached city's country is assumed, so only the distance test can fire.
  2. Resolve (physical_city, country) to a City row: exact name match after
     stripping case/diacritics, inside that country only. Preference:
     canonical row (canonical_city_id IS NULL) > most venues > lowest id.
     When several rows share the name (Birmingham AL / MI …) and the venue
     has coordinates, the nearest candidate wins.
  3. Propose a re-home when the target is not the attached city, is not
     linked to it (canonical_city_id / parent_city_id either way) and the
     two cities' countries differ. With --geo, same-country targets whose
     centres are > 150 km apart are proposed too; that is opt-in because on
     the 2026-09-19 snapshot it mostly surfaced duplicate city rows with
     bogus coordinates (Cesena / Cesenà, St Charles / St. Charles — that is
     merge_duplicate_cities.py's job) and ambiguous same-name cities
     (Albany CA vs Albany NY) — 232 rows vs 4,487 country mismatches.
  4. Collision: the target city already has a venue with the same name
     (the Ticketmaster row for "House of Blues (Houston)" already lives in
     Houston). A plain move would create a duplicate pair, so by default
     those are only listed. --merge-collisions repoints the mis-homed
     venue's events (events.venue_id is the only reference) to the existing
     venue and deletes the emptied row.
  --apply: UPDATE venues SET city_id = target (+ the collision merges when
  asked). Idempotent.

    PYTHONPATH=. python3 scripts/rehome_venues.py                       # dry run
    PYTHONPATH=. python3 scripts/rehome_venues.py --apply
    PYTHONPATH=. python3 scripts/rehome_venues.py --merge-collisions --apply
    PYTHONPATH=. python3 scripts/rehome_venues.py --min-upcoming 1       # only venues with future events
    PYTHONPATH=. python3 scripts/rehome_venues.py --source ld_livenation_com
"""
from __future__ import annotations

import argparse
import logging
import math
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
log = logging.getLogger("rehome_venues")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.services.recipes.countries import canon_country  # noqa: E402

FAR_KM = 150.0
ONLINE_RE = re.compile(r"\bonline\b|\bvirtual\b|\blivestream\b", re.I)


def norm(s: str | None) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^\w ]+", " ", s).replace("_", " ")
    return re.sub(r"\s+", " ", s).strip()


def km(lat1, lon1, lat2, lon2) -> float | None:
    if None in (lat1, lon1, lat2, lon2):
        return None
    if (abs(lat1) < 0.01 and abs(lon1) < 0.01) or (abs(lat2) < 0.01 and abs(lon2) < 0.01):
        return None
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(a))


def load(db):
    cities = {r["id"]: dict(r) for r in db.execute(text(
        "select id, name, country, state, latitude, longitude, canonical_city_id, parent_city_id from cities"
    )).mappings().all()}
    venue_counts = dict(db.execute(text("select city_id, count(*) from venues group by city_id")).all())
    for c in cities.values():
        c["ccountry"] = canon_country(c["country"])
        c["venues"] = venue_counts.get(c["id"], 0)
    by_key = defaultdict(list)                       # (norm name, canonical country) → [city]
    for c in cities.values():
        if c["ccountry"]:
            by_key[(norm(c["name"]), c["ccountry"])].append(c)
    upcoming = defaultdict(Counter)                  # venue_id → Counter(scrape_source)
    for vid, src, n in db.execute(text("""select venue_id, coalesce(scrape_source, '?'), count(*) from events
                                          where start_date >= date('now') and venue_id is not null
                                          group by 1, 2""")).all():
        upcoming[vid][src] += n
    venues = db.execute(text("""select id, name, city_id, physical_city, physical_country, latitude, longitude
                                from venues where city_id is not null and physical_city is not null
                                and trim(physical_city) <> ''""")).mappings().all()
    names_in_city = defaultdict(dict)                # city_id → {norm venue name: lowest venue id}
    for vid, cid, vname in db.execute(text("select id, city_id, name from venues order by id")).all():
        names_in_city[cid].setdefault(norm(vname), vid)
    return cities, by_key, upcoming, venues, names_in_city


def linked(a: dict, b: dict) -> bool:
    ids = {a["id"], a.get("canonical_city_id"), a.get("parent_city_id")} - {None}
    idb = {b["id"], b.get("canonical_city_id"), b.get("parent_city_id")} - {None}
    return bool(ids & idb)


def pick_target(cands: list[dict], vlat=None, vlon=None) -> dict:
    if len(cands) > 1 and vlat is not None and vlon is not None:
        near = [(d, c) for c in cands if (d := km(vlat, vlon, c["latitude"], c["longitude"])) is not None]
        if near:
            return min(near, key=lambda t: t[0])[1]
    return sorted(cands, key=lambda c: (c["canonical_city_id"] is not None, -c["venues"], c["id"]))[0]


def plan(cities, by_key, upcoming, venues, names_in_city, *, source=None, min_upcoming=0, venue_id=None,
         geo=False):
    moves, collisions = [], []
    for v in venues:
        if venue_id and v["id"] != venue_id:
            continue
        if ONLINE_RE.search(v["name"] or ""):
            continue
        cur = cities.get(v["city_id"])
        if not cur:
            continue
        up = upcoming.get(v["id"], Counter())
        n_up = sum(up.values())
        if n_up < min_upcoming or (source and source not in up):
            continue
        pcountry = canon_country(v["physical_country"]) or cur["ccountry"]
        cands = by_key.get((norm(v["physical_city"]), pcountry))
        if not cands:
            continue
        cands = [c for c in cands if c["id"] != cur["id"] and not linked(c, cur)]
        if not cands:
            continue
        target = pick_target(cands, v["latitude"], v["longitude"])
        if target["ccountry"] != cur["ccountry"]:
            why = f"{cur['ccountry']} → {target['ccountry']}"
        else:
            if not geo:
                continue
            d = km(cur["latitude"], cur["longitude"], target["latitude"], target["longitude"])
            if d is None or d <= FAR_KM:
                continue
            why = f"{round(d)} km"
        row = {"venue_id": v["id"], "venue": v["name"], "from": cur, "to": target, "upcoming": n_up,
               "sources": dict(up.most_common(3)), "why": why,
               "physical": f"{v['physical_city']}, {v['physical_country']}"}
        existing = names_in_city.get(target["id"], {}).get(norm(v["name"]))
        if existing is not None:
            row["merge_into"] = existing
            collisions.append(row)
        else:
            moves.append(row)
    # A merge target can itself be a mis-homed venue that merges onward
    # (A → B → C); follow the chain so no event is left pointing at a row
    # we delete. Cycles (A ↔ B) are left unmerged.
    onward = {m["venue_id"]: m["merge_into"] for m in collisions}
    kept = []
    for m in collisions:
        seen, keep = {m["venue_id"]}, m["merge_into"]
        while keep in onward and keep not in seen:
            seen.add(keep)
            keep = onward[keep]
        if keep in seen:
            continue
        m["merge_into"] = keep
        kept.append(m)
    collisions = kept
    moves.sort(key=lambda r: (-r["upcoming"], r["venue_id"]))
    collisions.sort(key=lambda r: (-r["upcoming"], r["venue_id"]))
    return moves, collisions


def fmt_city(c: dict) -> str:
    st = f", {c['state']}" if c.get("state") else ""
    return f"{c['name']}{st}, {c['country']} #{c['id']}"


def apply(db, moves: list[dict], collisions: list[dict] | None = None) -> dict:
    n = n_merged = n_events = 0
    for m in moves:
        r = db.execute(text("UPDATE venues SET city_id = :t WHERE id = :v AND city_id = :f"),
                       {"t": m["to"]["id"], "v": m["venue_id"], "f": m["from"]["id"]})
        n += r.rowcount or 0
    for m in collisions or []:
        r = db.execute(text("UPDATE events SET venue_id = :keep WHERE venue_id = :gone"),
                       {"keep": m["merge_into"], "gone": m["venue_id"]})
        n_events += r.rowcount or 0
        db.execute(text("DELETE FROM venues WHERE id = :gone AND city_id = :f"),
                   {"gone": m["venue_id"], "f": m["from"]["id"]})
        n_merged += 1
    db.commit()
    return {"venues_rehomed": n, "venues_merged": n_merged, "events_repointed": n_events}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--limit", type=int, default=None, help="only the first N moves (by upcoming events)")
    ap.add_argument("--source", type=str, default=None, help="only venues with upcoming events from this scrape_source")
    ap.add_argument("--min-upcoming", type=int, default=0, help="only venues with at least N upcoming events")
    ap.add_argument("--venue-id", type=int, default=None, help="only this venue")
    ap.add_argument("--geo", action="store_true",
                    help="also propose same-country targets > 150 km away (see docstring for why this is opt-in)")
    ap.add_argument("--merge-collisions", action="store_true",
                    help="when the target city already has a same-name venue, repoint events to it and drop the mis-homed row")
    ap.add_argument("--show", type=int, default=80, help="rows to print per list")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        moves, collisions = plan(*load(db), source=args.source, min_upcoming=args.min_upcoming,
                                 venue_id=args.venue_id, geo=args.geo)
        if args.limit:
            moves = moves[: args.limit]
        tot_up = sum(m["upcoming"] for m in moves)
        by_from = Counter(fmt_city(m["from"]) for m in moves)
        by_src = Counter()
        for m in moves:
            for s in m["sources"]:
                by_src[s] += 1
        log.info("%d venues to re-home (%d upcoming events); %d collisions (%d upcoming events) %s",
                 len(moves), tot_up, len(collisions), sum(m["upcoming"] for m in collisions),
                 "to merge into the existing same-name venue" if args.merge_collisions
                 else "listed only (re-run with --merge-collisions to merge them)")
        log.info("by current (wrong) city: %s", ", ".join(f"{c}×{n}" for c, n in by_from.most_common(12)))
        log.info("by source of upcoming events: %s", ", ".join(f"{s}×{n}" for s, n in by_src.most_common(12)))
        for m in moves[: args.show]:
            log.info("  #%-7d %-42s %-34s → %-34s %3de  [%s; %s]", m["venue_id"], (m["venue"] or "")[:42],
                     fmt_city(m["from"])[:34], fmt_city(m["to"])[:34], m["upcoming"], m["why"], m["physical"])
        if len(moves) > args.show:
            log.info("  … %d more", len(moves) - args.show)
        if collisions:
            log.info("collisions (same-name venue already in the target city):")
            for m in collisions[: args.show]:
                log.info("  #%-7d %-42s %-34s → %-34s %3de  [merge into venue #%d]", m["venue_id"],
                         (m["venue"] or "")[:42], fmt_city(m["from"])[:34], fmt_city(m["to"])[:34],
                         m["upcoming"], m["merge_into"])
            if len(collisions) > args.show:
                log.info("  … %d more", len(collisions) - args.show)
        if not args.apply:
            log.info("DRY RUN — nothing written. Re-run with --apply.")
            return
        res = apply(db, moves, collisions if args.merge_collisions else None)
        log.info("APPLIED: %s", res)
    finally:
        db.close()


if __name__ == "__main__":
    main()
