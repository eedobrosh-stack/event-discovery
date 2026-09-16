#!/usr/bin/env python3
"""Route 3 recipe CLI — author, test, and publish recipes.

Recipes live in git under recipes/<domain>.json (source of truth) and in
the `source_recipes` table (runtime copy). This script bridges the two.

    # validate + fetch + parse, print what the selectors matched.
    # Touches NO database. Safe anywhere.
    PYTHONPATH=. python3 scripts/recipe_run.py recipes/icm.org.il.json --dry-run
    PYTHONPATH=. python3 scripts/recipe_run.py recipes/icm.org.il.json --dry-run --max-requests 5 --show 20

    # upsert the recipe row (bumps recipe_version, resets next_run_at so the
    # next nightly run picks it up, graduates matching LLMSource rows).
    # Run on the Render box over SSH (prod DB) — Claude does this, not the user.
    PYTHONPATH=. python3 scripts/recipe_run.py recipes/icm.org.il.json --upsert
    PYTHONPATH=. python3 scripts/recipe_run.py --sync-all            # every recipes/*.json

    # run one recipe end-to-end against the DB it is pointed at
    # (persist events, update health) — what the nightly job does.
    PYTHONPATH=. python3 scripts/recipe_run.py --execute icm.org.il

    # housekeeping
    PYTHONPATH=. python3 scripts/recipe_run.py --list
    PYTHONPATH=. python3 scripts/recipe_run.py --disable icm.org.il
    PYTHONPATH=. python3 scripts/recipe_run.py --enable  icm.org.il
    PYTHONPATH=. python3 scripts/recipe_run.py --validate recipes/*.json
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv(os.environ.get("SUPERCALY_ENV", "/Users/eedo.b/supercaly/.env"))
logging.basicConfig(level=os.environ.get("LOGLEVEL", "WARNING"),
                    format="%(levelname)s %(name)s: %(message)s")

from app.services.recipes.schema import validate_recipe  # noqa: E402
from app.services.recipes.runner import run_recipe  # noqa: E402


def _load(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _print_rows(rows: list[dict], n: int) -> None:
    for i, r in enumerate(rows[:n], 1):
        if "_jsonld" in r:
            ev = r["_jsonld"]
            print(f"  {i:>3}. [jsonld] {ev.get('name')!r} start={ev.get('startDate')} "
                  f"loc={(ev.get('location') or {}).get('name') if isinstance(ev.get('location'), dict) else ev.get('location')}")
            continue
        vis = {k: (v if len(str(v)) <= 70 else str(v)[:67] + "...")
               for k, v in r.items() if not k.startswith("_")}
        print(f"  {i:>3}. {vis}")


def cmd_dry_run(path: str, max_requests: int, show: int) -> int:
    doc = _load(path)
    probs = validate_recipe(doc)
    print(f"== {doc.get('domain')}  ({path})")
    if probs:
        print("INVALID:")
        for p in probs:
            print("  -", p)
        return 2
    print("schema: ok")
    res = run_recipe(doc, max_requests=max_requests)
    s = res.summary()
    print(f"pages fetched : {s['pages']}   requests: {s['requests']}   {s['duration_s']}s")
    for u in res.pages[:10]:
        print("   ", u)
    if len(res.pages) > 10:
        print(f"    … +{len(res.pages) - 10} more")
    print(f"rows parsed   : {s['rows']}   detail fetched: {s['detail_fetched']}")
    print(f"events built  : {s['events']}   dropped: {s['dropped']}")
    if res.samples:
        print("drop samples  :")
        for k, v in res.samples.items():
            print(f"   {k}: {str(v)[:220]}")
    if res.errors:
        print(f"errors ({len(res.errors)}):")
        for e in res.errors[:8]:
            print("   -", e[:220])
    if res.fatal:
        print("FATAL:", res.fatal)

    if res.rows:
        print(f"\nfirst {min(show, len(res.rows))} parsed rows:")
        _print_rows(res.rows, show)
    if res.events:
        print(f"\nfirst {min(show, len(res.events))} normalised events:")
        for i, ev in enumerate(res.events[:show], 1):
            print(f"  {i:>3}. {ev.start_date} {ev.start_time or '     '}  {ev.name[:60]!r}"
                  f"  @ {ev.venue_name or '-'}  [{ev.venue_city or '-'}]  id={ev.source_id}"
                  f"{'  ' + str(ev.price) + ' ' + ev.price_currency if ev.price is not None else ''}")
        # field fill-rate — the quickest smell test for a weak selector
        fields = ["start_time", "artist_name", "venue_name", "venue_city",
                  "purchase_link", "image_url", "price", "description"]
        n = len(res.events)
        fill = {f: sum(1 for e in res.events if getattr(e, f) not in (None, "", [])) for f in fields}
        print("\nfill rate     : " + "  ".join(f"{f}={100 * v // n}%" for f, v in fill.items()))
        dates = sorted(e.start_date for e in res.events)
        print(f"date range    : {dates[0]} → {dates[-1]}   unique ids: {len({e.source_id for e in res.events})}")
    return 0 if (res.events and not res.fatal) else 1


def _db():
    from app.database import SessionLocal, engine, Base
    from app.models import SourceRecipe  # noqa: F401  (ensure table registered)
    Base.metadata.create_all(bind=engine, tables=[SourceRecipe.__table__])
    return SessionLocal()


def cmd_upsert(path: str, *, written_by: str, priority: int | None,
               cadence: int | None, enable: bool | None) -> int:
    from app.services.recipes.sync import upsert_recipe
    doc = _load(path)
    db = _db()
    try:
        res = upsert_recipe(db, doc, written_by=written_by, priority=priority,
                            cadence=cadence, enable=enable)
    finally:
        db.close()
    if res["verb"] in ("invalid", "clash"):
        print(f"{path}: {res['verb'].upper()}")
        for pr in res["problems"]:
            print("  -", pr)
        return 2
    print(f"{res['domain']}: {res['verb']} (v{res['version']}, enabled={res['enabled']}, "
          f"priority={res['priority']}, cadence={res['cadence']}h, "
          f"llm_sources graduated={res['graduated']})")
    return 0


def cmd_list() -> int:
    from app.models import SourceRecipe
    db = _db()
    try:
        rows = db.query(SourceRecipe).order_by(
            SourceRecipe.drift_flag.desc(), SourceRecipe.priority.desc()).all()
        if not rows:
            print("(no recipes)")
            return 0
        print(f"{'domain':32} {'kind':6} {'on':3} {'v':>2} {'prio':>4} {'status':8} "
              f"{'drift':5} {'last_saved':>10} {'saved_total':>11} {'last_run':16} next_run")
        for r in rows:
            kind = ((r.recipe or {}).get("parse") or {}).get("kind", "?")
            print(f"{r.domain:32} {kind:6} {'y' if r.enabled else 'n':3} {r.recipe_version:>2} "
                  f"{r.priority:>4} {r.last_status:8} {'YES' if r.drift_flag else '':5} "
                  f"{r.last_saved or 0:>10} {r.saved_total or 0:>11} "
                  f"{r.last_run_at.strftime('%Y-%m-%d %H:%M') if r.last_run_at else '-':16} "
                  f"{r.next_run_at.strftime('%Y-%m-%d %H:%M') if r.next_run_at else '-'}")
            if r.drift_flag and r.last_error:
                print(f"    ↳ {r.last_error.splitlines()[0][:110]}")
        return 0
    finally:
        db.close()


def cmd_toggle(domain: str, enabled: bool) -> int:
    from app.models import SourceRecipe
    db = _db()
    try:
        r = db.query(SourceRecipe).filter(SourceRecipe.domain == domain).first()
        if r is None:
            print(f"{domain}: not found")
            return 1
        r.enabled = enabled
        if enabled:
            r.next_run_at = datetime.utcnow()
        db.commit()
        print(f"{domain}: enabled={enabled}")
        return 0
    finally:
        db.close()


def cmd_execute(domain: str, dry: bool) -> int:
    from app.models import SourceRecipe
    from app.services.recipes.runner import execute_recipe_row
    db = _db()
    try:
        r = db.query(SourceRecipe).filter(SourceRecipe.domain == domain).first()
        if r is None:
            print(f"{domain}: not found")
            return 1
        rid = r.id
    finally:
        db.close()
    summary = execute_recipe_row(rid, dry_run=dry)
    print(json.dumps(summary, indent=2, default=str))
    return 0 if not summary.get("fatal") else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="*", help="recipe json file(s)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", help="fetch+parse, no DB")
    g.add_argument("--validate", action="store_true", help="schema only")
    g.add_argument("--upsert", action="store_true", help="write recipe row(s) to the DB")
    g.add_argument("--sync-all", action="store_true", help="upsert every recipes/*.json")
    g.add_argument("--list", action="store_true")
    g.add_argument("--enable", metavar="DOMAIN")
    g.add_argument("--disable", metavar="DOMAIN")
    g.add_argument("--execute", metavar="DOMAIN", help="run + persist one recipe now")
    g.add_argument("--execute-dry", metavar="DOMAIN", help="run one stored recipe, no persist")
    ap.add_argument("--max-requests", type=int, default=25, help="dry-run request cap (default 25)")
    ap.add_argument("--show", type=int, default=10, help="rows/events to print")
    ap.add_argument("--priority", type=int)
    ap.add_argument("--cadence-hours", type=int)
    ap.add_argument("--written-by", default="claude-session")
    ap.add_argument("--set-enabled", choices=["true", "false"])
    a = ap.parse_args(argv)

    if a.list:
        return cmd_list()
    if a.enable:
        return cmd_toggle(a.enable, True)
    if a.disable:
        return cmd_toggle(a.disable, False)
    if a.execute:
        return cmd_execute(a.execute, dry=False)
    if a.execute_dry:
        return cmd_execute(a.execute_dry, dry=True)

    paths = a.paths
    if a.sync_all:
        root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "recipes")
        paths = sorted(glob.glob(os.path.join(root, "*.json")))
        a.upsert = True
    if not paths:
        ap.error("give at least one recipe path")

    rc = 0
    enable = None if a.set_enabled is None else (a.set_enabled == "true")
    for p in paths:
        if a.validate:
            probs = validate_recipe(_load(p))
            print(f"{p}: {'ok' if not probs else 'INVALID'}")
            for pr in probs:
                print("  -", pr)
            rc |= 2 if probs else 0
        elif a.upsert:
            rc |= cmd_upsert(p, written_by=a.written_by, priority=a.priority,
                             cadence=a.cadence_hours, enable=enable)
        else:  # default: dry-run
            rc |= cmd_dry_run(p, a.max_requests, a.show)
    return rc


if __name__ == "__main__":
    sys.exit(main())
