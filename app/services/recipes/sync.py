"""Sync recipes/*.json (git = source of truth) into the source_recipes table.

Called from two places with identical semantics:
  • app startup (`_deferred_seed` in main.py) — every deploy upserts the
    committed recipes, so pushing to main IS the publish step. No SSH,
    no admin token needed.
  • scripts/recipe_run.py --upsert / --sync-all — the same code path for
    a manual push from a shell.

Upsert rules:
  • row keyed by `domain`; `source_name` must be unique across rows.
  • document changed  → recipe_version += 1, next_run_at = now, drift
    flag + consecutive counters cleared (a repaired recipe runs tonight
    and leaves the repair queue).
  • unchanged         → nothing touched (so prod health columns survive
    every redeploy).
  • optional top-level keys `priority`, `cadence_hours`, `enabled` in
    the JSON control the scheduling columns from git. When absent, the
    existing row values are kept (defaults 0 / 24 / true on create).
  • LLMSource rows on the same registered domain are marked
    `graduated` in a separate transaction — a schema mismatch there
    (stale local DB) must never undo the recipe upsert.
"""
from __future__ import annotations

import glob
import json
import logging
import os
from datetime import datetime
from typing import Optional

from app.services.recipes.schema import validate_recipe, registered_domain

logger = logging.getLogger(__name__)

DEFAULT_RECIPES_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "recipes",
)


def _canon(doc: dict) -> str:
    return json.dumps(doc, sort_keys=True, ensure_ascii=False)


def upsert_recipe(db, doc: dict, *, written_by: str,
                  priority: Optional[int] = None, cadence: Optional[int] = None,
                  enable: Optional[bool] = None, now: Optional[datetime] = None) -> dict:
    """Upsert one validated recipe document. Returns a summary dict:
    {domain, verb: created|updated|unchanged|invalid|clash, version,
     enabled, priority, cadence, graduated}."""
    from app.models import SourceRecipe, LLMSource

    problems = validate_recipe(doc)
    if problems:
        return {"domain": doc.get("domain"), "verb": "invalid", "problems": problems}
    now = now or datetime.utcnow()
    dom = doc["domain"]

    clash = db.query(SourceRecipe).filter(
        SourceRecipe.source_name == doc["source_name"],
        SourceRecipe.domain != dom).first()
    if clash:
        return {"domain": dom, "verb": "clash",
                "problems": [f"source_name {doc['source_name']!r} already used by {clash.domain}"]}

    # git-controlled scheduling knobs (explicit args win over the file)
    if priority is None and isinstance(doc.get("priority"), int):
        priority = doc["priority"]
    if cadence is None and isinstance(doc.get("cadence_hours"), int):
        cadence = doc["cadence_hours"]
    if enable is None and isinstance(doc.get("enabled"), bool):
        enable = doc["enabled"]

    row = db.query(SourceRecipe).filter(SourceRecipe.domain == dom).first()
    if row is None:
        row = SourceRecipe(domain=dom, source_name=doc["source_name"], recipe=doc,
                           recipe_version=1, enabled=True if enable is None else enable,
                           priority=priority or 0, cadence_hours=cadence or 24,
                           written_by=written_by, next_run_at=now)
        db.add(row)
        verb = "created"
    else:
        changed = _canon(row.recipe or {}) != _canon(doc)
        if changed:
            row.recipe = doc
            row.source_name = doc["source_name"]
            row.recipe_version = (row.recipe_version or 0) + 1
            row.next_run_at = now
            row.drift_flag = False
            row.consecutive_errors = 0
            row.consecutive_zero_fetch = 0
            row.written_by = written_by
            verb = "updated"
        else:
            verb = "unchanged"
        if priority is not None and row.priority != priority:
            row.priority = priority
            verb = "updated" if verb == "unchanged" else verb
        if cadence is not None and row.cadence_hours != cadence:
            row.cadence_hours = cadence
            verb = "updated" if verb == "unchanged" else verb
        if enable is not None and bool(row.enabled) != enable:
            row.enabled = enable
            if enable:
                row.next_run_at = now
            verb = "updated" if verb == "unchanged" else verb
    row.country = doc.get("country")
    row.city_name = doc.get("city_name")
    row.notes = doc.get("notes")
    db.commit()

    graduated: object = 0
    try:
        n = 0
        for src in db.query(LLMSource).filter(LLMSource.url.ilike(f"%{dom}%")).all():
            if registered_domain(src.url) == dom and src.state != "graduated":
                src.state = "graduated"
                src.notes = ((src.notes or "") +
                             f"\n[{now:%Y-%m-%d}] graduated → recipe {doc['source_name']}").strip()
                n += 1
        db.commit()
        graduated = n
    except Exception as e:  # stale schema etc. — never fatal
        db.rollback()
        graduated = f"skipped ({type(e).__name__})"

    return {"domain": dom, "verb": verb, "version": row.recipe_version,
            "enabled": bool(row.enabled), "priority": row.priority,
            "cadence": row.cadence_hours, "graduated": graduated}


def sync_recipes_from_dir(db, directory: str = DEFAULT_RECIPES_DIR, *,
                          written_by: str = "git-deploy") -> dict:
    """Upsert every recipes/*.json. Never raises for a bad file — it is
    reported in the summary and the rest still sync."""
    summary = {"dir": directory, "files": 0, "created": 0, "updated": 0,
               "unchanged": 0, "invalid": [], "clash": [], "results": []}
    for path in sorted(glob.glob(os.path.join(directory, "*.json"))):
        summary["files"] += 1
        try:
            with open(path, encoding="utf-8") as f:
                doc = json.load(f)
        except Exception as e:
            summary["invalid"].append(f"{os.path.basename(path)}: {e}")
            continue
        try:
            res = upsert_recipe(db, doc, written_by=written_by)
        except Exception as e:  # pragma: no cover — defensive
            db.rollback()
            summary["invalid"].append(f"{os.path.basename(path)}: {type(e).__name__}: {e}")
            continue
        summary["results"].append(res)
        verb = res["verb"]
        if verb in ("created", "updated", "unchanged"):
            summary[verb] += 1
        elif verb == "invalid":
            summary["invalid"].append(f"{os.path.basename(path)}: " + "; ".join(res["problems"][:3]))
        elif verb == "clash":
            summary["clash"].append(f"{os.path.basename(path)}: " + "; ".join(res["problems"]))
    return summary


def seed_recipes_at_startup() -> None:
    """Entry point for main.py's deferred seed. Own session; logs a
    one-line summary; swallows everything (startup must not fail)."""
    from app.database import SessionLocal
    db = SessionLocal()
    try:
        s = sync_recipes_from_dir(db)
        logger.info(
            f"recipes sync: {s['files']} file(s) — created={s['created']} "
            f"updated={s['updated']} unchanged={s['unchanged']} "
            f"invalid={len(s['invalid'])} clash={len(s['clash'])}"
        )
        for bad in s["invalid"] + s["clash"]:
            logger.warning(f"recipes sync: {bad}")
    except Exception as e:
        logger.warning(f"recipes sync failed: {e}")
    finally:
        db.close()
