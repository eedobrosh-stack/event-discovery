"""Live lookup for a search that found nothing (2026-09-26).

Supercaly answers searches from its own table; a source is only there
once a crawl has run. When a search comes back empty, the results page
asks this module to query — right then — the sources that can SEARCH:

  * recipes with ``entry.search.url`` (a ``{q}`` URL, e.g. kupat.co.il's
    Strapi filter) — one request each;
  * any recipe whose entry is The Events Calendar REST API
    (``/wp-json/tribe/events/v1/events``) — ``&search={q}`` is derived, no
    recipe edit needed;
  * Ticketmaster's Discovery API ``keyword`` search.

Whatever comes back is saved through the normal ingest path
(CollectorRegistry._save_events via runner.persist_result /
group_events_by_city), so dedupe, the Israeli artist rule and the
time-conflict rule all apply; the page then re-runs the search.

Guard rails: one job per (term, country) per 15 min, 2 jobs at a time,
≤ MAX_RECIPES recipes per job (country-matched first), each recipe capped
at 2 requests, ~25 s wall-clock budget, geo-walled (Mac-relay) recipes
skipped. robots.txt is honoured by the recipe Fetcher.
"""
from __future__ import annotations

import asyncio
import copy
import logging
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, wait
from urllib.parse import quote

log = logging.getLogger(__name__)

MAX_RECIPES = 16
BUDGET_S = 25
DEDUPE_S = 15 * 60
_HEBREW = re.compile(r"[֐-׿]")
_TRIBE = "/wp-json/tribe/events/v1/events"

_jobs: dict[str, dict] = {}
_by_key: dict[tuple, tuple[str, float]] = {}
_lock = threading.Lock()
_slots = threading.Semaphore(2)


def _search_url(doc: dict, q: str) -> str | None:
    entry = doc.get("entry") or {}
    s = (entry.get("search") or {}).get("url")
    if s:
        from datetime import date
        return s.replace("{q}", quote(q)).replace("{today}", date.today().isoformat())
    for u in entry.get("urls") or []:
        if _TRIBE in u:
            sep = "&" if "?" in u else "?"
            return f"{u}{sep}search={quote(q)}"
    return None


def _pick_recipes(db, q: str, country: str | None) -> list:
    from app.models.source_recipe import SourceRecipe
    rows = db.query(SourceRecipe).filter(SourceRecipe.enabled.is_(True)).all()
    cands = []
    for r in rows:
        doc = r.recipe or {}
        if doc.get("relay") == "mac":
            continue
        url = _search_url(doc, q)
        if url:
            cands.append((r, url))
    # country first ("פאר טסי" → Israeli sources), then the most productive
    want = country or ("Israel" if _HEBREW.search(q) else None)
    cands.sort(key=lambda t: (0 if want and (t[0].country == want) else 1, -(t[0].saved_total or 0)))
    if want and _HEBREW.search(q):
        cands = [c for c in cands if c[0].country == want]
    return cands[:MAX_RECIPES]


def _run(job: dict) -> None:
    from app.database import SessionLocal
    from app.services.recipes.runner import group_events_by_city, persist_result, run_recipe
    from app.services.collectors.registry import CollectorRegistry
    t0 = time.monotonic()
    db = SessionLocal()
    try:
        cands = _pick_recipes(db, job["q"], job["country"])
        job["sources_tried"] = len(cands) + 1
        results = []
        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = {}
            for row, url in cands:
                doc = copy.deepcopy(row.recipe or {})
                doc["entry"] = {"urls": [url], "paginate": {"mode": "none"}}
                futs[pool.submit(run_recipe, doc, max_requests=2)] = row
            done, _ = wait(futs, timeout=max(1, BUDGET_S - (time.monotonic() - t0)))
            for f in done:
                try:
                    results.append((futs[f], f.result()))
                except Exception as e:
                    log.warning("live lookup %s: %s", futs[f].domain, e)
        saved = 0
        for row, res in results:
            if res.events:
                persist_result(db, row, res)
                saved += res.saved or 0
                if res.saved:
                    job["hits"].append({"source": row.domain, "saved": res.saved})
        # Ticketmaster keyword search (not a recipe)
        try:
            from app.services.collectors.api.ticketmaster import TicketmasterCollector
            tm = TicketmasterCollector()
            if tm.is_configured() and time.monotonic() - t0 < BUDGET_S:
                evs = asyncio.run(tm.search(job["q"], job["country_iso"]))
                if evs:
                    groups, _, _ = group_events_by_city(db, evs, None, None)
                    reg = CollectorRegistry()
                    n = sum(reg._save_events(e, c, db) for c, e in groups)
                    db.commit()
                    saved += n
                    if n:
                        job["hits"].append({"source": "ticketmaster", "saved": n})
        except Exception as e:
            db.rollback()
            log.warning("live lookup ticketmaster: %s", e)
        job["saved"] = saved
        job["state"] = "done"
    except Exception as e:
        db.rollback()
        job["state"] = "error"
        job["error"] = f"{type(e).__name__}: {e}"[:300]
        log.exception("live lookup failed")
    finally:
        job["duration_s"] = round(time.monotonic() - t0, 1)
        db.close()
        _slots.release()


def start(q: str, *, country: str | None = None, country_iso: str | None = None) -> dict:
    q = (q or "").strip()[:80]
    if len(q) < 2:
        return {"state": "skipped", "reason": "term too short"}
    key = (q.lower(), country)
    now = time.time()
    with _lock:
        prev = _by_key.get(key)
        if prev and now - prev[1] < DEDUPE_S and prev[0] in _jobs:
            return _jobs[prev[0]]
        if not _slots.acquire(blocking=False):
            return {"state": "busy"}
        jid = uuid.uuid4().hex[:12]
        job = {"id": jid, "q": q, "country": country, "country_iso": country_iso, "state": "running",
               "started": now, "sources_tried": 0, "saved": 0, "hits": []}
        _jobs[jid] = job
        _by_key[key] = (jid, now)
        for k, (j, ts) in list(_by_key.items()):          # forget old jobs
            if now - ts > DEDUPE_S:
                _by_key.pop(k, None); _jobs.pop(j, None)
    threading.Thread(target=_run, args=(job,), daemon=True, name=f"live-lookup-{jid}").start()
    return job


def status(jid: str) -> dict | None:
    return _jobs.get(jid)
