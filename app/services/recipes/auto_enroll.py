"""Step 1 of the Route 3 scaling plan: auto-enroll JSON-LD sources.

Cadence A recorded `last_method='jsonld'` on every LLMSource page whose
events came straight out of schema.org JSON-LD — no LLM was involved.
Those pages (≈2.7k across a few hundred domains on 2026-09-16) need no
authoring at all: a generic `{"parse": {"kind": "jsonld"}}` recipe per
domain, with the domain's JSON-LD pages as entry URLs, reproduces the
extraction with zero Gemini spend.

Rules:
  • one recipe per registered domain, source_name "ld_<domain-slug>",
    written_by='auto-jsonld'.
  • a domain that already has ANY SourceRecipe row (hand-written from
    git or an earlier auto row) is left alone — git always wins.
  • reserved domains (Route 2 collector covers them) and blocked
    LLMSources are skipped.
  • country is required by the recipe schema (City fallback); domains
    where no LLMSource row carries a country are skipped and counted.
  • entry.urls = the domain's JSON-LD pages ordered by yield
    (events_saved_total desc, then last_event_count), capped.
  • priority = min(30, total yield / 10) so hand-written recipes
    (40–50) always run first in the nightly loop.
  • upsert_recipe() then graduates the domain's LLMSource rows.

Idempotent: safe to run at every startup and weekly.
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Optional

from app.services.recipes.schema import registered_domain
from app.services.recipes.sync import upsert_recipe

logger = logging.getLogger(__name__)

WRITTEN_BY = "auto-jsonld"
DEFAULT_MAX_URLS = 40
DEFAULT_CADENCE_HOURS = 48   # JSON-LD calendars rarely change hourly; halve the nightly load


def _slug(domain: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", domain.lower()).strip("_")
    return ("ld_" + s)[:59]


def _is_reserved(url: str) -> bool:
    try:
        from app.scheduler.jobs import _is_reserved_discovery_url
        return bool(_is_reserved_discovery_url(url))
    except Exception:  # jobs.py not importable in this interpreter (tests on py3.9)
        return False


def plan_jsonld_recipes(db, *, max_urls: int = DEFAULT_MAX_URLS) -> dict:
    """Pure planning step: returns {"recipes": [doc,...], "skipped": {...}}
    without writing anything."""
    from app.models import LLMSource, SourceRecipe

    existing = {d for (d,) in db.query(SourceRecipe.domain).all()}
    rows = (
        db.query(LLMSource)
        .filter(LLMSource.last_method == "jsonld")
        .filter(LLMSource.state != "blocked")
        .all()
    )
    by_domain: dict = defaultdict(list)
    skipped = {"reserved": 0, "has_recipe": 0, "no_country": 0, "bad_domain": 0}
    for src in rows:
        dom = registered_domain(src.url)
        if not dom or "." not in dom:
            skipped["bad_domain"] += 1
            continue
        if dom in existing:
            skipped["has_recipe"] += 1
            continue
        if _is_reserved(src.url):
            skipped["reserved"] += 1
            continue
        by_domain[dom].append(src)

    docs = []
    for dom, srcs in sorted(by_domain.items()):
        srcs.sort(key=lambda s: ((s.events_saved_total or 0), (s.last_event_count or 0)), reverse=True)
        countries = [s.country for s in srcs if s.country]
        if not countries:
            skipped["no_country"] += len(srcs)
            continue
        country = max(set(countries), key=countries.count)
        cities = [s.city_name for s in srcs if s.city_name and s.country == country]
        city = max(set(cities), key=cities.count) if cities else None
        total_yield = sum(s.events_saved_total or 0 for s in srcs)
        urls = []
        for s in srcs:
            if s.url not in urls:
                urls.append(s.url)
            if len(urls) >= max_urls:
                break
        docs.append({
            "recipe_version": 1,
            "domain": dom,
            "source_name": _slug(dom),
            "country": country,
            **({"city_name": city} if city else {}),
            "priority": int(min(30, total_yield // 10)),
            "cadence_hours": DEFAULT_CADENCE_HOURS,
            "entry": {"urls": urls, "paginate": {"mode": "none"}},
            "fetch": {"delay_seconds": 1.5, "timeout": 20},
            "parse": {"kind": "jsonld"},
            "notes": (f"Auto-enrolled from {len(srcs)} JSON-LD LLMSource page(s) "
                      f"(Cadence A yield {total_yield}); {len(urls)} URL(s) kept. "
                      f"Replace with a hand-written recipes/{dom}.json to take over."),
        })
    return {"recipes": docs, "skipped": skipped, "pages_considered": len(rows)}


def auto_enroll_jsonld(db, *, max_urls: int = DEFAULT_MAX_URLS, dry_run: bool = False,
                       limit: Optional[int] = None) -> dict:
    plan = plan_jsonld_recipes(db, max_urls=max_urls)
    docs = plan["recipes"][:limit] if limit else plan["recipes"]
    summary = {"pages_considered": plan["pages_considered"], "domains_planned": len(plan["recipes"]),
               "created": 0, "updated": 0, "unchanged": 0, "invalid": [], "skipped": plan["skipped"],
               "dry_run": dry_run, "sample": [d["domain"] for d in docs[:10]]}
    if dry_run:
        return summary
    for doc in docs:
        try:
            res = upsert_recipe(db, doc, written_by=WRITTEN_BY)
        except Exception as e:  # pragma: no cover — defensive
            db.rollback()
            summary["invalid"].append(f"{doc['domain']}: {type(e).__name__}: {e}")
            continue
        v = res["verb"]
        if v in ("created", "updated", "unchanged"):
            summary[v] += 1
        else:
            summary["invalid"].append(f"{doc['domain']}: " + "; ".join(res.get("problems", [])[:2]))
    return summary


def auto_enroll_at_startup() -> None:
    """Deferred-seed hook: own session, logs one line, never raises."""
    from app.database import SessionLocal
    db = SessionLocal()
    try:
        s = auto_enroll_jsonld(db)
        logger.info(
            f"recipes auto-enroll(jsonld): pages={s['pages_considered']} "
            f"domains={s['domains_planned']} created={s['created']} updated={s['updated']} "
            f"unchanged={s['unchanged']} invalid={len(s['invalid'])} skipped={s['skipped']}"
        )
        for bad in s["invalid"][:10]:
            logger.warning(f"recipes auto-enroll: {bad}")
    except Exception as e:
        logger.warning(f"recipes auto-enroll failed: {e}")
    finally:
        db.close()
