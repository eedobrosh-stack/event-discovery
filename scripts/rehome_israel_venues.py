"""Re-home Israeli venues attached to the wrong City row.

~250 Israeli venues sit under a City they are not in — almost all under
Tel Aviv, because recipes and collectors scoped to Tel Aviv fell back to
their own city when the venue's city was not looked up (Kfar Etzion,
Kiryat Shmona, kibbutzim, even Houston / Dallas / Boston).
``venues.physical_city`` still says where each one is; this script moves
``venues.city_id`` to match it.

Target, per venue (via ``app.services.il_places.canon_place``):
  * an Israeli City row with that canonical name (alias rows resolve to
    their canonical) → move there;
  * an Israeli place with no City row (small moshav / kibbutz / regional
    council) → the "Israel - Other" City, never left under Tel Aviv;
  * a place outside Israel → the one City row of that name outside
    Israel, when exactly one exists; otherwise reported and left;
  * "Online" → left alone.

Aliases: the venue's name and every alias it owns are ADDED under the
target city. The old-city aliases are kept on purpose — a collector
still scoped to Tel Aviv then resolves the next scrape of the same
spelling onto the moved venue instead of re-creating it in Tel Aviv.

Usage (dry run first, then --apply; audit JSON in data/):
    PYTHONPATH=. python3 scripts/rehome_israel_venues.py
    PYTHONPATH=. python3 scripts/rehome_israel_venues.py --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
from app.services.il_places import PLACES, canon_place  # noqa: E402
from app.services.venue_aliases import normalize_venue_name  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("rehome")

OTHER = "Israel - Other"
IL_PLACES = {v for v in PLACES.values() if not v.startswith(("Outside Israel", "Online"))}


def _key(name: str | None) -> str:
    """Case / hyphen / space-insensitive city key: "Tel-Aviv" = "tel aviv"."""
    return re.sub(r"[^\w]+", "", (name or "").lower())


def plan(db) -> tuple[list[dict], list[dict]]:
    cities = db.execute(text(
        "SELECT id, name, country, canonical_city_id FROM cities")).fetchall()
    by_id = {c.id: c for c in cities}

    def canon_row(cid: int):
        c = by_id[cid]
        seen = set()
        while c.canonical_city_id and c.canonical_city_id in by_id and c.id not in seen:
            seen.add(c.id)
            c = by_id[c.canonical_city_id]
        return c

    il_by_name: dict[str, int] = {}
    abroad_by_name: dict[str, list[int]] = defaultdict(list)
    for c in cities:
        target = canon_row(c.id)
        if c.country == "Israel":
            il_by_name.setdefault(_key(c.name), target.id)
        else:
            abroad_by_name[_key(c.name)].append(target.id)
    other_id = il_by_name.get(_key(OTHER))

    venues = db.execute(text("""
        SELECT v.id, v.name, v.city_id, v.physical_city, v.physical_country,
               (SELECT COUNT(*) FROM events e WHERE e.venue_id = v.id
                  AND e.start_date >= date('now')) AS upcoming
        FROM venues v JOIN cities c ON c.id = v.city_id
        WHERE c.country = 'Israel'
    """)).fetchall()

    moves, skipped = [], []
    for v in venues:
        current = canon_row(v.city_id)
        place = canon_place(v.physical_city, current.name)
        if place == current.name or place == "Online":
            continue
        if _key(place) == _key(current.name):
            continue
        target_id = il_by_name.get(_key(place))
        reason = "city_row"
        # Not an Israeli city: a place the map marks as abroad, or a
        # Latin-script name with no Israeli City row ("Da Nang, Vietnam")
        # goes to its City row abroad — never into "Israel - Other".
        abroad = place.startswith("Outside Israel") or (
            target_id is None and place not in IL_PLACES
            and not re.search("[\u0590-\u05ff]", place))
        if abroad:
            raw = (v.physical_city or "").split(",")[0]
            ids = sorted(set(abroad_by_name.get(_key(raw), [])))
            if len(ids) == 1:
                target_id, reason = ids[0], "outside_israel"
            else:
                skipped.append({"id": v.id, "name": v.name, "physical_city": v.physical_city,
                                "reason": f"not an Israeli city, {len(ids)} City rows abroad match"})
                continue
        elif target_id is None:
            target_id, reason = other_id, "no_city_row"
        if target_id is None or target_id == current.id:
            continue
        moves.append({"id": v.id, "name": v.name, "physical_city": v.physical_city,
                      "place": place, "from_city": current.name, "from_city_id": v.city_id,
                      "to_city": by_id[target_id].name, "to_city_id": target_id,
                      "reason": reason, "upcoming": v.upcoming})
    return moves, skipped


def apply_moves(db, moves: list[dict]) -> int:
    aliases_added = 0
    for m in moves:
        names = [m["name"]] + [r[0] for r in db.execute(
            text("SELECT alias FROM venue_aliases WHERE venue_id = :v"), {"v": m["id"]})]
        for alias in dict.fromkeys(names):
            key = normalize_venue_name(alias)
            if not key:
                continue
            res = db.execute(text("""
                INSERT INTO venue_aliases (venue_id, city_id, alias, normalized_alias,
                                           source, confidence, created_at)
                VALUES (:v, :c, :a, :k, 'rehome_israel_venues', 1.0, CURRENT_TIMESTAMP)
                ON CONFLICT (city_id, normalized_alias) DO NOTHING
            """), {"v": m["id"], "c": m["to_city_id"], "a": alias, "k": key})
            aliases_added += res.rowcount or 0
        db.execute(text("UPDATE venues SET city_id = :c WHERE id = :v"),
                   {"c": m["to_city_id"], "v": m["id"]})
    db.commit()
    return aliases_added


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry run).")
    args = ap.parse_args()
    db = SessionLocal()
    try:
        moves, skipped = plan(db)
        by_reason = Counter(m["reason"] for m in moves)
        log.info(f"venues to re-home: {len(moves)} ({dict(by_reason)}), "
                 f"upcoming events moved with them: {sum(m['upcoming'] for m in moves)}, "
                 f"skipped: {len(skipped)}")
        top = Counter((m["from_city"], m["to_city"]) for m in moves).most_common(12)
        log.info("top moves: " + "; ".join(f"{a} → {b}: {n}" for (a, b), n in top))
        no_row = Counter(m["place"] for m in moves if m["reason"] == "no_city_row")
        if no_row:
            log.info(f"places with no City row (→ {OTHER}): {len(no_row)} "
                     f"({', '.join(p for p, _ in no_row.most_common(15))}…)")
        aliases_added = apply_moves(db, moves) if args.apply else 0
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = ROOT / "data" / f"rehome_israel_venues_{ts}_{'apply' if args.apply else 'dryrun'}.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"moves": moves, "skipped": skipped,
                                   "aliases_added": aliases_added},
                                  ensure_ascii=False, indent=2))
        log.info(f"aliases added under target cities: {aliases_added}")
        log.info(f"audit written: {out}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
