"""Run one recipe: enumerate entry URLs → fetch (with pagination) → parse
→ optional detail hop → normalise → (persist).

Two entry points:

  run_recipe(doc, ...)            pure: returns RunResult, never touches
                                  the DB unless `existing_ids` is given.
                                  Used by scripts/recipe_run.py --dry-run
                                  and by tests.

  execute_recipe_row(row_id, ...) opens its own SessionLocal, runs the
                                  recipe stored on the SourceRecipe row,
                                  persists via CollectorRegistry._save_events
                                  (the same ingest path every collector
                                  uses), updates health/drift columns.
                                  Called by the nightly job in a thread.
  persist_result(db, row, result) the persist half of execute_recipe_row,
                                  also fed by POST /api/admin/recipes/{domain}/relay
                                  when a "relay": "mac" recipe ran off-box.
"""
from __future__ import annotations

import logging
import time
import traceback
from collections import Counter
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta
from typing import Callable, Optional

from app.services.recipes import parse as P
from app.services.recipes.fetch import (
    Fetcher, BudgetExhausted, RobotsDisallowed,
)
from app.services.recipes.normalize import normalize
from app.services.recipes.schema import validate_recipe

logger = logging.getLogger(__name__)

MAX_EVENTS_PER_RUN = 5000
DRIFT_WINDOW = 10
DRIFT_ZERO_RUNS = 2       # fetched == 0 this many runs in a row → drift
DRIFT_ERROR_RUNS = 2      # errors this many runs in a row → drift
EMPTY_RUNS_DEPRIORITISE = 3


@dataclass
class RunResult:
    domain: str
    source_name: str
    rows: list = field(default_factory=list)       # raw parsed dicts
    events: list = field(default_factory=list)     # RawEvents
    pages: list = field(default_factory=list)      # URLs fetched (listing)
    detail_fetched: int = 0
    requests: int = 0
    dropped: Counter = field(default_factory=Counter)
    samples: dict = field(default_factory=dict)
    errors: list = field(default_factory=list)     # non-fatal, per page
    fatal: Optional[str] = None
    duration_s: float = 0.0
    saved: int = 0
    budget_hit: bool = False
    followed: int = 0
    city_groups: int = 0

    @property
    def fetched(self) -> int:
        return len(self.rows)

    def summary(self) -> dict:
        return {
            "domain": self.domain, "source": self.source_name,
            "pages": len(self.pages), "requests": self.requests,
            "rows": self.fetched, "events": len(self.events),
            "saved": self.saved, "detail_fetched": self.detail_fetched,
            "followed": self.followed, "city_groups": self.city_groups,
            "dropped": dict(self.dropped), "errors": len(self.errors),
            "fatal": self.fatal, "duration_s": round(self.duration_s, 1),
        }


# ── URL enumeration ──────────────────────────────────────────────────────
def expand_entry_urls(entry: dict) -> list[tuple[str, dict]]:
    """→ [(url, template_values)] — values are what {value} expanded to,
    passed to POST-body templating."""
    from app.extractors.llm_extractor import resolve_template_urls
    out: list[tuple[str, dict]] = []
    seen = set()
    from datetime import date as _date
    for u in entry.get("urls") or []:
        # "{today}" → ISO date, for APIs that must be filtered to upcoming
        # events server-side (kupat.co.il pages over past rows otherwise).
        u = u.replace("{today}", _date.today().isoformat())
        if u not in seen:
            out.append((u, {}))
            seen.add(u)
    tpl = entry.get("template")
    if tpl:
        base = (entry.get("urls") or [None])[0]
        if entry.get("range_months"):
            urls = resolve_template_urls(base or tpl, tpl, entry["range_months"], None)
            for u in urls:
                if u not in seen and u != base:
                    out.append((u, {}))
                    seen.add(u)
        elif entry.get("values"):
            for v in entry["values"]:
                try:
                    u = tpl.format(value=v)
                except (KeyError, IndexError):
                    break
                if u not in seen:
                    out.append((u, {"value": v}))
                    seen.add(u)
    return out


# ── one listing URL with pagination ──────────────────────────────────────
def _parse_body(kind: str, resp, parse_cfg: dict):
    """→ (rows, decoded_json_or_None)"""
    if kind == "api":
        body = P.decode_json(resp.text)
        return P.parse_api(body, parse_cfg, resp.url), body
    if kind == "html":
        return P.parse_html(resp.text, parse_cfg, resp.url), None
    if kind == "jsonld":
        return P.parse_jsonld(resp.text, resp.url), None
    if kind == "ics":
        return P.parse_ics(resp.text, resp.url), None
    raise ValueError(f"unknown parse kind {kind}")


def _walk_pages(fetcher: Fetcher, start_url: str, values: dict, doc: dict,
                result: RunResult, *, collect: str = "items",
                paginate: bool = True) -> list:
    """Walk one listing URL (with pagination). collect="items" → parsed
    event rows; collect="links" → detail URLs per entry.follow."""
    entry = doc.get("entry") or {}
    pag = (entry.get("paginate") or {}) if paginate else {}
    mode = pag.get("mode", "none")
    max_pages = int(pag.get("max_pages", 20))
    kind = doc["parse"]["kind"]
    rows_all: list[dict] = []
    url = start_url
    page_no = int(pag.get("start", 1))
    if mode == "page_param":
        url = P.set_query_param(start_url, pag["param"], page_no)
    seen_urls = set()

    for _ in range(max_pages):
        if url in seen_urls:
            break
        seen_urls.add(url)
        try:
            # page_param also exposes the page number to POST-body
            # templating ({"page": "{page}"}): goshow.co.il's category
            # load-more reads the page from the form body, not the URL.
            vals = {**values, "page": page_no} if mode == "page_param" else values
            resp = fetcher.get(url, values=vals)
        except BudgetExhausted as e:
            # keep everything parsed so far — a capped run is a partial
            # run, not a failed one
            result.errors.append(f"request budget exhausted ({e}) at {url}; partial run")
            result.budget_hit = True
            break
        except RobotsDisallowed:
            raise
        except Exception as e:
            result.errors.append(f"{url}: fetch {type(e).__name__}: {e}")
            break
        if resp.status >= 400:
            result.errors.append(f"{url}: HTTP {resp.status}")
            break
        result.pages.append(url)
        try:
            if collect == "links":
                rows = P.extract_links(resp.text, entry["follow"], resp.url, doc["domain"])
                body = None
            else:
                rows, body = _parse_body(kind, resp, doc["parse"])
        except Exception as e:
            result.errors.append(f"{url}: parse {type(e).__name__}: {e}")
            break
        rows_all.extend(rows)
        if len(rows_all) >= MAX_EVENTS_PER_RUN:
            break

        # next page?
        if mode == "none":
            break
        if mode == "next_link":
            nxt = P.next_link(resp.text, pag["selector"], resp.url)
            if not nxt:
                break
            url = nxt
        elif mode == "page_param":
            if not rows:
                break
            page_no += 1
            url = P.set_query_param(start_url, pag["param"], page_no)
        elif mode == "cursor":
            cur = P.get_path(body, pag["cursor_path"]) if body is not None else None
            if not cur or not rows:
                break
            url = P.set_query_param(start_url, pag["param"], cur)
        elif mode == "next_url":
            # APIs that hand back the full URL of the next page
            # (WordPress The Events Calendar: next_rest_url)
            nxt = P.get_path(body, pag["path"]) if body is not None else None
            if not isinstance(nxt, str) or not nxt.startswith("http") or not rows:
                break
            url = nxt
    return rows_all


# ── detail hop ───────────────────────────────────────────────────────────
def _detail_hop(fetcher: Fetcher, rows: list[dict], doc: dict, result: RunResult,
                is_new: Optional[Callable[[str], bool]]):
    det = doc.get("detail")
    if not det:
        return
    url_field = det.get("url_field", "purchase_link")
    budget = int(det.get("max_per_run", 60))
    only_new = bool(det.get("only_new", True))
    dp = det.get("parse") or {}
    kind = dp.get("kind", "html")
    want = set((dp.get("fields") or {}).keys())
    # detail.overwrite: fields the detail page REPLACES even when the listing
    # filled them (muzi.co.il: the listing link is the muzi page — needed as
    # url_field to reach the detail page — but the seller's outbound ticket
    # URL on that page is the purchase_link we want).
    overwrite = set(det.get("overwrite") or [])
    done = 0
    for row in rows:
        if done >= budget:
            break
        url = row.get(url_field)
        if not isinstance(url, str) or not url.startswith("http"):
            continue
        # skip when the listing already provided every detail field
        pending = {f for f in want if f in overwrite or row.get(f) in (None, "", [])}
        if want and not pending:
            continue
        if only_new and is_new is not None:
            sid = row.get("source_id")
            if sid and not is_new(str(sid)):
                continue
        try:
            resp = fetcher.get(url, method="GET")   # detail pages are plain pages even when the listing is a POST
        except BudgetExhausted as e:
            result.errors.append(f"request budget exhausted ({e}) during detail hop; partial")
            result.budget_hit = True
            break
        except RobotsDisallowed:
            raise
        except Exception as e:
            result.errors.append(f"detail {url}: {type(e).__name__}: {e}")
            continue
        done += 1
        result.detail_fetched += 1
        if resp.status >= 400:
            result.errors.append(f"detail {url}: HTTP {resp.status}")
            continue
        try:
            if kind == "jsonld":
                sub = P.parse_jsonld(resp.text, resp.url)
                if sub:
                    row["_jsonld_detail"] = sub[0]["_jsonld"]
                continue
            sub_rows, _ = _parse_body(kind, resp, {**dp, "item": dp.get("item")})
        except Exception as e:
            result.errors.append(f"detail {url}: parse {e}")
            continue
        if sub_rows:
            for k, v in sub_rows[0].items():
                if k.startswith("_"):
                    continue
                if (k in overwrite or row.get(k) in (None, "", [])) and v not in (None, "", []):
                    row[k] = v


# ── public: pure run ─────────────────────────────────────────────────────
def run_recipe(doc: dict, *, fetcher: Optional[Fetcher] = None,
               is_new: Optional[Callable[[str], bool]] = None,
               max_requests: Optional[int] = None) -> RunResult:
    t0 = time.monotonic()
    result = RunResult(domain=doc.get("domain", "?"),
                       source_name=doc.get("source_name", "?"))
    problems = validate_recipe(doc)
    if problems:
        result.fatal = "invalid recipe: " + "; ".join(problems[:5])
        result.duration_s = time.monotonic() - t0
        return result

    own = fetcher is None
    if own:
        kw = {}
        if max_requests:
            kw["max_requests"] = max_requests
        fetcher = Fetcher(doc.get("fetch"), **kw)
    try:
        rows: list[dict] = []
        try:
            follow = (doc.get("entry") or {}).get("follow")
            if follow:
                # two-level crawl: listing pages → detail pages → items
                links: list[str] = []
                for url, values in expand_entry_urls(doc["entry"]):
                    for l in _walk_pages(fetcher, url, values, doc, result, collect="links"):
                        if l not in links:
                            links.append(l)
                    if result.budget_hit:
                        break
                cap = int(follow.get("max_links", 100))
                if len(links) > cap:
                    result.errors.append(f"follow: {len(links)} links found, capped at {cap}")
                    links = links[:cap]
                result.followed = len(links)
                for l in links:
                    if result.budget_hit:
                        break
                    rows.extend(_walk_pages(fetcher, l, {}, doc, result, paginate=False))
                    if len(rows) >= MAX_EVENTS_PER_RUN:
                        result.errors.append(f"cap {MAX_EVENTS_PER_RUN} rows reached")
                        break
            else:
                for url, values in expand_entry_urls(doc["entry"]):
                    rows.extend(_walk_pages(fetcher, url, values, doc, result))
                    if result.budget_hit:
                        break
                    if len(rows) >= MAX_EVENTS_PER_RUN:
                        result.errors.append(f"cap {MAX_EVENTS_PER_RUN} rows reached")
                        break
            if not result.budget_hit:
                _detail_hop(fetcher, rows, doc, result, is_new)
        except RobotsDisallowed as e:
            result.fatal = f"robots.txt disallows {e}"
        # In-run dedupe on source_id: the same event often appears on
        # several listing pages of one site (category page + series
        # page). _save_events dedupes against the DB, not within a
        # batch, and the unique (scrape_source, source_id) index would
        # reject the second insert.
        seen_ids: set = set()
        deduped: list[dict] = []
        for r in rows:
            sid = r.get("source_id") if "_jsonld" not in r else (r["_jsonld"].get("url") or r["_jsonld"].get("@id"))
            if sid not in (None, ""):
                sid = str(sid)
                if sid in seen_ids:
                    result.dropped["dup_in_run"] += 1
                    continue
                seen_ids.add(sid)
            deduped.append(r)
        rows = deduped
        result.rows = rows
        result.requests = fetcher.requests_made
        if getattr(fetcher, "switched_to_impersonation", False):
            result.errors.append("plain client got 403/429 → switched to Chrome impersonation for this run "
                                 "(consider fetch.impersonate=true in the recipe)")

        # JSON-LD detail merges: prefer detail block if listing lacked one
        for r in rows:
            if "_jsonld_detail" in r and "_jsonld" not in r:
                r["_jsonld"] = r.pop("_jsonld_detail")

        norm = normalize(rows, doc)
        result.events = norm.events
        result.dropped = norm.dropped
        result.samples = norm.samples
        if not result.pages and not result.fatal:
            result.fatal = result.errors[0] if result.errors else "no pages fetched"
    except Exception as e:  # anything else is a runner bug → fatal, logged
        result.fatal = f"{type(e).__name__}: {e}"
        logger.error("run_recipe(%s) crashed:\n%s", result.domain, traceback.format_exc())
    finally:
        if own:
            fetcher.close()
        result.duration_s = time.monotonic() - t0
    return result


# ── public: run + persist one SourceRecipe row ───────────────────────────
def _resolve_city(db, country: Optional[str], city_name: Optional[str]):
    """Same rule as Cadence A: named city in country → any city in
    country (venue_city per event drives the final assignment) → None."""
    from app.models import City
    from app.services.recipes.countries import canon_country
    country = canon_country(country)
    city = None
    if city_name:
        q = db.query(City).filter(City.name == city_name)
        if country:
            q = q.filter(City.country == country)
        city = q.first()
        if city is None and country:
            # LLM-written city names drift ("München" vs "Munich"): any city
            # in the country beats failing the whole recipe — venue_city per
            # event drives the final assignment anyway.
            city = db.query(City).filter(City.country == country).first()
    if city is None and country:
        city = db.query(City).filter(City.country == country).first()
    return city


def group_events_by_city(db, events: list, country: Optional[str], default_city):
    """_save_events assigns every event of a batch to ONE City (venue_city
    only lands in Venue.physical_city), so a multi-city recipe (Bravo:
    Beersheba / Modi'in / Kfar Saba …) must be persisted per resolved
    city. → ([(City, [events])], unresolved, skipped).

    Resolution order per event (2026-09-20):
      1. venue_city inside canon_country(venue_country) when the event names
         a country other than the recipe's. Nationwide / foreign listings
         (livenation.com under country=Israel emitted "Houston", "US") used
         to miss the Israel-only lookup and fall back to the recipe city,
         which is how "House of Blues (Houston)" ended up in Tel Aviv.
      2. venue_city inside the recipe country (the original rule).
      3. default_city — unless the event's venue_country is a KNOWN other
         country (some City row carries it): then the city is simply one we
         don't track yet, and homing it under Tel Aviv would be wrong, so
         the event is skipped and counted in `skipped`. Unknown country
         strings and same-country misses still fall back to default_city
         and are counted in `unresolved` (unchanged behaviour)."""
    from app.models import City
    from collections import Counter
    from app.services.recipes.countries import canon_country
    country = canon_country(country)
    home_country = canon_country(getattr(default_city, "country", None)) or country

    # canonical country → raw City.country spellings ("Czechia" and "Czech
    # Republic" both resolve); doubles as the set of KNOWN countries.
    raw_by_canon: dict = {}
    for (raw,) in db.query(City.country).distinct():
        c = canon_country(raw)
        if c:
            raw_by_canon.setdefault(c, set()).add(raw)

    def lookup(name: str, ctry: Optional[str]):
        q = db.query(City).filter(City.name == name)
        if ctry:
            raws = raw_by_canon.get(ctry)
            if not raws:
                return None
            q = q.filter(City.country.in_(sorted(raws)))
        # prefer canonical rows over aliases (canonical_city_id set)
        return q.order_by(City.canonical_city_id.isnot(None), City.id).first()

    groups: dict = {}
    cache: dict = {}
    unresolved: Counter = Counter()
    skipped: Counter = Counter()
    for ev in events:
        name = (ev.venue_city or "").strip()
        ev_country = canon_country(getattr(ev, "venue_country", None))
        foreign = bool(ev_country and ev_country != home_country and ev_country != country)
        city = None
        if name:
            key = (name, ev_country if foreign else None)
            if key not in cache:
                hit = None
                if foreign:
                    hit = lookup(name, ev_country)
                if hit is None:
                    hit = lookup(name, country)
                cache[key] = hit
            city = cache[key]
        if city is None:
            if foreign and ev_country in raw_by_canon:
                skipped[f"{name or '?'} ({ev_country})"] += 1
                continue
            if name:
                unresolved[name] += 1
        city = city or default_city
        if city is None:
            continue
        groups.setdefault(city.id, (city, []))[1].append(ev)
    return [v for v in groups.values()], unresolved, skipped


def _update_health(row, result: RunResult, now: datetime) -> None:
    row.last_run_at = now
    row.runs_total = (row.runs_total or 0) + 1
    row.last_fetched = result.fetched
    row.last_saved = result.saved
    row.last_requests = result.requests
    row.last_duration_s = int(result.duration_s)
    row.fetched_total = (row.fetched_total or 0) + result.fetched
    row.saved_total = (row.saved_total or 0) + result.saved

    hist = list(row.recent_fetched_counts or [])
    hist.append(int(result.fetched))
    row.recent_fetched_counts = hist[-DRIFT_WINDOW:]

    cadence = int(row.cadence_hours or 24)
    if result.fatal:
        row.last_status = "error"
        row.last_error = (result.fatal + ("\n" + "\n".join(result.errors[:3]) if result.errors else ""))[:2000]
        row.consecutive_errors = (row.consecutive_errors or 0) + 1
        row.consecutive_zero_fetch = (row.consecutive_zero_fetch or 0) + 1
    elif result.fetched == 0:
        row.last_status = "drift" if (row.consecutive_zero_fetch or 0) + 1 >= DRIFT_ZERO_RUNS else "empty"
        row.last_error = "\n".join(result.errors[:3])[:2000] or None
        row.consecutive_errors = 0
        row.consecutive_zero_fetch = (row.consecutive_zero_fetch or 0) + 1
    else:
        row.last_status = "ok" if result.saved > 0 else "empty"
        row.last_error = "\n".join(result.errors[:3])[:2000] or None
        row.consecutive_errors = 0
        row.consecutive_zero_fetch = 0

    row.drift_flag = bool(
        (row.consecutive_zero_fetch or 0) >= DRIFT_ZERO_RUNS
        or (row.consecutive_errors or 0) >= DRIFT_ERROR_RUNS
    )
    # back off broken recipes so they don't burn the nightly budget;
    # a repaired --upsert resets next_run_at to now.
    mult = 2 if row.drift_flag else 1
    row.next_run_at = now + timedelta(hours=cadence * mult)


def execute_recipe_row(row_id: int, *, dry_run: bool = False,
                       max_requests: Optional[int] = None) -> dict:
    """Thread-safe unit of work for the scheduler: own session, own
    fetcher. Returns result.summary()."""
    from app.database import SessionLocal
    from app.models import SourceRecipe, Event
    from app.services.collectors.registry import CollectorRegistry

    db = SessionLocal()
    try:
        row = db.query(SourceRecipe).get(row_id)
        if row is None:
            return {"fatal": f"recipe id {row_id} missing"}
        doc = dict(row.recipe or {})
        source = row.source_name

        def is_new(sid: str) -> bool:
            return not db.query(Event.id).filter(
                Event.scrape_source == source, Event.source_id == sid
            ).first()

        result = run_recipe(doc, is_new=is_new, max_requests=max_requests)

        if not dry_run:
            persist_result(db, row, result)
            row = db.query(SourceRecipe).get(row_id)  # re-attach after commit
            _update_health(row, result, datetime.utcnow())
            db.commit()
        return result.summary()
    finally:
        db.close()


def persist_result(db, row, result: RunResult) -> None:
    """Persist a RunResult's events under the recipe row's default city via
    CollectorRegistry._save_events (the ingest path every collector uses).
    Shared by execute_recipe_row (on-box run) and the relay endpoint
    (events fetched+parsed off-box). Sets result.saved / city_groups /
    errors / fatal in place; never raises."""
    from app.services.collectors.registry import CollectorRegistry
    if not result.events or result.fatal:
        return
    doc = dict(row.recipe or {})
    try:
        city = _resolve_city(db, row.country or doc.get("country"),
                             row.city_name or doc.get("city_name"))
        if city is None:
            result.fatal = (f"no City resolvable for country={row.country!r} "
                            f"city={row.city_name!r}; events not persisted")
            return
        reg = CollectorRegistry()
        groups, unresolved, skipped = group_events_by_city(
            db, result.events, row.country or doc.get("country"), city)
        saved = 0
        for grp_city, grp_events in groups:
            saved += reg._save_events(grp_events, grp_city, db)
        db.commit()
        result.saved = saved
        result.city_groups = len(groups)
        if unresolved:
            result.errors.append(
                "unresolved venue_city → default city: "
                + ", ".join(f"{k}×{v}" for k, v in unresolved.most_common(8)))
        if skipped:
            result.errors.append(
                "skipped, foreign venue_country but city unknown: "
                + ", ".join(f"{k}×{v}" for k, v in skipped.most_common(8)))
    except Exception as e:
        db.rollback()
        result.fatal = f"persist failed: {type(e).__name__}: {str(e)[:300]}"
        logger.error("recipe %s persist failed:\n%s", row.domain, traceback.format_exc())


# ── relay transport: RunResult ⇄ JSON ────────────────────────────────────
_RAW_DATE_FIELDS = ("start_date", "end_date")


def raw_event_to_dict(ev) -> dict:
    d = asdict(ev)
    for k in _RAW_DATE_FIELDS:
        if d.get(k) is not None:
            d[k] = d[k].isoformat()
    return d


def raw_event_from_dict(d: dict):
    from datetime import date as _date
    from app.services.collectors.base import RawEvent
    allowed = {f.name for f in fields(RawEvent)}
    kw = {k: v for k, v in d.items() if k in allowed}
    for k in _RAW_DATE_FIELDS:
        v = kw.get(k)
        if isinstance(v, str):
            kw[k] = _date.fromisoformat(v[:10])
    if kw.get("raw_categories") is None:
        kw["raw_categories"] = []
    return RawEvent(**kw)


def result_to_payload(result: RunResult) -> dict:
    """What the Mac relay POSTs: everything the health bookkeeping and the
    persist step need, events as plain dicts (dates ISO)."""
    return {
        "domain": result.domain, "source_name": result.source_name,
        "events": [raw_event_to_dict(e) for e in result.events],
        "rows": result.fetched, "pages": list(result.pages),
        "detail_fetched": result.detail_fetched, "requests": result.requests,
        "dropped": dict(result.dropped), "samples": {k: str(v)[:300] for k, v in result.samples.items()},
        "errors": list(result.errors), "fatal": result.fatal,
        "duration_s": round(result.duration_s, 1), "budget_hit": result.budget_hit,
        "followed": result.followed,
    }


def result_from_payload(payload: dict, *, domain: str, source_name: str) -> RunResult:
    r = RunResult(domain=domain, source_name=source_name)
    r.events = [raw_event_from_dict(e) for e in (payload.get("events") or [])]
    # rows drives .fetched (health: drift on zero fetch) — keep the remote count
    r.rows = [None] * max(int(payload.get("rows") or 0), len(r.events))
    r.pages = list(payload.get("pages") or [])
    r.detail_fetched = int(payload.get("detail_fetched") or 0)
    r.requests = int(payload.get("requests") or 0)
    r.dropped = Counter(payload.get("dropped") or {})
    r.samples = dict(payload.get("samples") or {})
    r.errors = [str(x) for x in (payload.get("errors") or [])]
    r.fatal = payload.get("fatal") or None
    r.duration_s = float(payload.get("duration_s") or 0.0)
    r.budget_hit = bool(payload.get("budget_hit"))
    r.followed = int(payload.get("followed") or 0)
    return r
