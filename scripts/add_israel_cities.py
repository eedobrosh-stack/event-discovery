"""Promote the busiest places inside "Israel - Other" to City rows.

After scripts/rehome_israel_venues.py, ~220 venues whose place has no
City row sit under "Israel - Other" (Kfar Etzion, Oranit, Kfar Yona …),
so they can't be picked in the location box. This adds a City row for
each place with at least 5 upcoming events (2026-09-26 snapshot) with an
approximate centre point (for /api/geo "near me"), then re-runs the
re-home so their venues move in. Places below the bar stay in
"Israel - Other"; venues with no place at all (≈190) can't be placed.

    PYTHONPATH=. python3 scripts/add_israel_cities.py            # dry run
    PYTHONPATH=. python3 scripts/add_israel_cities.py --apply
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

import sys  # noqa: E402
sys.path.insert(0, str(ROOT / "scripts"))

from sqlalchemy import text  # noqa: E402

from app.database import SessionLocal  # noqa: E402
import rehome_israel_venues as RH  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("add_cities")

# name (as il_places canonicalises it) → approximate centre (lat, lon)
NEW_CITIES = {
    "Kfar Etzion": (31.651, 35.117), "Oranit": (32.131, 34.990), "Kfar Yona": (32.317, 34.935),
    "Neve Yarak": (32.140, 34.917), "Ramat Yishai": (32.705, 35.170), "Pardesiya": (32.306, 34.908),
    "Abu Ghosh": (31.806, 35.110), "Hamadia": (32.520, 35.520), "Mateh Yehuda": (31.770, 35.050),
    "Kochav Yair": (32.223, 34.998), "Shavei Zion": (32.983, 35.090), "Harish": (32.460, 35.040),
    "Givat Avni": (32.772, 35.470), "Savyon": (32.049, 34.874), "Kfar Vradim": (33.000, 35.275),
    "Caesarea": (32.500, 34.904), "Beit Jimal": (31.735, 34.975), "Mevaseret Zion": (31.800, 35.150),
    "Latrun": (31.838, 34.980), "Na'an": (31.880, 34.855), "Nahal Tzippori": (32.760, 35.270),
    "Mazkeret Batya": (31.853, 34.847), "Be'er Tuvia": (31.740, 34.720),
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    db = SessionLocal()
    try:
        existing = {r[0].lower() for r in db.execute(text("SELECT name FROM cities WHERE country = 'Israel'"))}
        todo = {n: ll for n, ll in NEW_CITIES.items() if n.lower() not in existing}
        log.info(f"cities to add: {len(todo)} of {len(NEW_CITIES)} ({', '.join(todo)})")
        if args.apply:
            for name, (lat, lon) in todo.items():
                db.execute(text("""INSERT INTO cities (name, country, timezone, latitude, longitude)
                                   VALUES (:n, 'Israel', 'Asia/Jerusalem', :lat, :lon)"""),
                           {"n": name, "lat": lat, "lon": lon})
            db.commit()
        moves, skipped = RH.plan(db)
        targets = {n.lower() for n in NEW_CITIES}
        new_moves = [m for m in moves if m["to_city"].lower() in targets or m["place"].lower() in targets]
        log.info(f"re-home after adding: {len(moves)} moves "
                 f"({len(new_moves)} into the new cities, "
                 f"{sum(m['upcoming'] for m in new_moves)} upcoming events)")
        if args.apply and moves:
            added = RH.apply_moves(db, moves)
            log.info(f"moved {len(moves)} venues; aliases added {added}")
        if not args.apply:
            log.info("DRY-RUN — re-run with --apply to write. (Without --apply the new cities don't "
                     "exist yet, so the move count above only shows venues that already had a target.)")
    finally:
        db.close()


if __name__ == "__main__":
    main()
