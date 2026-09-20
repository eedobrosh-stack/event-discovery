#!/usr/bin/env python3
"""Fill cities.latitude/longitude where missing.

Why (2026-09-21): 885 City rows that own venues had no coordinates —
Jerusalem, Haifa, Ashdod, Beersheba… every Israeli city but Tel Aviv. The
homepage's browser-position → /api/geo/nearest lookup, the venue↔city
sanity checks and any distance logic silently skip such rows, so a user in
Haifa was resolved to a stray Hebrew-named Tel Aviv row 80 km away.

Sources, in order, per city:
  1. Nominatim (OpenStreetMap) — free, 1 request/second, UA required.
     Query = city name + country (canon_country), administrative/place hit.
  2. Centroid of the city's own geocoded venues when ≥ MIN_VENUES have
     coordinates and they agree (spread < 40 km).
Dry-run by default. Only rows with NULL/0,0 coordinates are touched.

    PYTHONPATH=. python3 scripts/backfill_city_coords.py --country Israel        # dry run
    PYTHONPATH=. python3 scripts/backfill_city_coords.py --country Israel --apply
    PYTHONPATH=. python3 scripts/backfill_city_coords.py --apply                 # everything with venues (~15 min)
"""
from __future__ import annotations

import argparse
import logging
import math
import sys
import time
import urllib.parse
import urllib.request
import json
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
load_dotenv(str(ROOT / ".env"))
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("backfill_city_coords")

from sqlalchemy import text  # noqa: E402
from app.database import SessionLocal  # noqa: E402
from app.services.recipes.countries import canon_country  # noqa: E402

UA = "supercaly-geocoder/1.0 (https://superca.ly; eedo.b@taboola.com)"
NOMINATIM = "https://nominatim.openstreetmap.org/search"
MIN_VENUES = 3
MAX_SPREAD_KM = 40.0


def hav(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(a))


def nominatim(name: str, country: str | None):
    params = {"format": "jsonv2", "limit": 1, "city": name}
    if country:
        params["country"] = country
    for attempt, q in enumerate((params, {"format": "jsonv2", "limit": 1, "q": f"{name}, {country or ''}".strip(", ")})):
        url = NOMINATIM + "?" + urllib.parse.urlencode(q)
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=20) as r:
                hits = json.load(r)
        except Exception as e:
            log.warning("  nominatim %s: %s", name, e)
            hits = []
        time.sleep(1.1)                       # usage policy: max 1 req/s
        if hits:
            h = hits[0]
            return float(h["lat"]), float(h["lon"]), f"nominatim:{h.get('type')}:{h.get('display_name', '')[:60]}"
    return None


def venue_centroid(db, city_id: int):
    pts = db.execute(text("select latitude, longitude from venues where city_id=:c and latitude is not null "
                          "and longitude is not null and not (abs(latitude)<0.01 and abs(longitude)<0.01)"),
                     {"c": city_id}).all()
    if len(pts) < MIN_VENUES:
        return None
    lat = sum(p[0] for p in pts) / len(pts); lon = sum(p[1] for p in pts) / len(pts)
    if max(hav(lat, lon, p[0], p[1]) for p in pts) > MAX_SPREAD_KM:
        return None
    return lat, lon, f"venue_centroid:{len(pts)}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--country", help="only this country (City.country string, e.g. Israel)")
    ap.add_argument("--all-cities", action="store_true", help="also rows without any venue (default: rows that own venues)")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--no-nominatim", action="store_true", help="venue centroids only (offline)")
    args = ap.parse_args()

    db = SessionLocal()
    try:
        sql = """select c.id, c.name, c.country, count(v.id) venues from cities c left join venues v on v.city_id=c.id
                 where (c.latitude is null or c.longitude is null or (abs(c.latitude)<0.01 and abs(c.longitude)<0.01))
                 and c.canonical_city_id is null"""
        params = {}
        if args.country:
            sql += " and c.country = :country"; params["country"] = args.country
        sql += " group by c.id"
        if not args.all_cities:
            sql += " having count(v.id) > 0"
        sql += " order by venues desc"
        rows = db.execute(text(sql), params).all()
        if args.limit:
            rows = rows[: args.limit]
        log.info("%d cities without coordinates%s", len(rows), f" in {args.country}" if args.country else "")
        done = failed = 0
        for cid, name, country, venues in rows:
            hit = None if args.no_nominatim else nominatim(name, canon_country(country) or country)
            hit = hit or venue_centroid(db, cid)
            if not hit:
                failed += 1
                log.info("  #%-6d %-28s %-16s %3dv  → no fix", cid, name[:28], (country or "")[:16], venues)
                continue
            lat, lon, how = hit
            log.info("  #%-6d %-28s %-16s %3dv  → %.4f, %.4f  [%s]", cid, name[:28], (country or "")[:16], venues, lat, lon, how)
            if args.apply:
                db.execute(text("update cities set latitude=:la, longitude=:lo where id=:c"), {"la": lat, "lo": lon, "c": cid})
                db.commit()
            done += 1
        log.info("%s: %d resolved, %d unresolved", "APPLIED" if args.apply else "DRY RUN — nothing written", done, failed)
    finally:
        db.close()


if __name__ == "__main__":
    main()
