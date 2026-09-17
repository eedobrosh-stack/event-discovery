"""Route 3 prober — crack never-recipe'd LLMSource domains automatically.

The LLMSource pool is ~5k domains; only the JSON-LD ones were enrolled
by auto_enroll.py. This module walks the rest, around the clock, one
domain at a time, and tries a cascade of FREE deterministic paths:

    1. jsonld      the page carries schema.org/Event now (sites change,
                   and Cadence A only tagged what it saw months ago)
    2. ics         an iCalendar feed is linked (<link type=text/calendar>,
                   *.ics, ?ical=1) or the WordPress Events Calendar feed
                   URLs answer
    3. tribe_rest  WordPress "The Events Calendar" REST API
                   /wp-json/tribe/events/v1/events

The first detector that yields ≥ MIN_EVENTS *future* events wins and a
recipe is created through the same upsert_recipe path (written_by=
'auto-probe', cadence 48h). Every attempt is recorded in source_probes
so a domain is never probed twice (outcome 'none' domains are retried
after REPROBE_DAYS — a site may adopt JSON-LD later).

No LLM, no Brave. Budget ≤ 8 requests per domain.
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urljoin, urlsplit

from app.services.recipes import parse as P
from app.services.recipes.fetch import Fetcher, BudgetExhausted, RobotsDisallowed
from app.services.recipes.normalize import normalize
from app.services.recipes.schema import registered_domain
from app.services.recipes.sync import upsert_recipe

logger = logging.getLogger(__name__)

WRITTEN_BY = "auto-probe"
MIN_EVENTS = 3
MAX_PAGES_PER_DOMAIN = 2
REQUESTS_PER_DOMAIN = 8
REPROBE_DAYS = 45
CADENCE_HOURS = 48

TRIBE_PATH = "/wp-json/tribe/events/v1/events?per_page=50&start_date=now"
TRIBE_FIELDS = {
    "source_id": "id", "name": "title",
    "start_datetime": "start_date", "end_datetime": "end_date",
    "venue_name": "venue.venue", "venue_address": "venue.address",
    "venue_city": "venue.city", "venue_country": "venue.country",
    "purchase_link": "url", "image_url": "image.url", "price": "cost",
    "raw_categories": "categories[*].name",
    "description": {"path": "description", "strip_html": True},
}
_WP_HINTS = ("wp-content", "wp-json", "tribe-events", "tribe_events")
_ICS_HREF = re.compile(r"\.ics(\?|$)|[?&]ical=1|/ical/?$|/feed/ical|format=ical", re.I)


def _slug(prefix: str, domain: str) -> str:
    return (prefix + "_" + re.sub(r"[^a-z0-9]+", "_", domain.lower()).strip("_"))[:59]


def _origin(url: str) -> str:
    p = urlsplit(url)
    return f"{p.scheme}://{p.netloc}"


def _count_future(rows: list, doc_stub: dict) -> int:
    """Normalise parsed rows with a throwaway recipe stub and count survivors
    (future-dated, named). This is exactly what the nightly runner would
    keep, so a probe 'hit' means real events."""
    res = normalize(rows, doc_stub)
    return len(res.events)


def _stub(domain: str, kind: str, country: Optional[str]) -> dict:
    return {"source_name": "probe", "domain": domain, "country": country or "?",
            "parse": {"kind": kind}, "language": None, "timezone": None}


# ── detectors ────────────────────────────────────────────────────────────
def detect_jsonld(html: str, page_url: str, domain: str, country) -> Optional[dict]:
    rows = P.parse_jsonld(html, page_url)
    n = _count_future(rows, _stub(domain, "jsonld", country))
    return {"detector": "jsonld", "events": n, "evidence": f"jsonld on {page_url}"} if n >= MIN_EVENTS else None


def find_ics_links(html: str, page_url: str) -> list[str]:
    doc = P.soup(html)
    out: list[str] = []
    for l in doc.find_all("link", href=True):
        if (l.get("type") or "").lower() in ("text/calendar", "application/ics"):
            out.append(urljoin(page_url, l["href"]))
    for a in doc.find_all("a", href=True):
        h = a["href"]
        if _ICS_HREF.search(h) and not h.lower().startswith(("mailto:", "javascript:")):
            out.append(urljoin(page_url, h))
    if any(h in html for h in _WP_HINTS):
        o = _origin(page_url)
        out += [o + "/events/?ical=1", o + "/?post_type=tribe_events&ical=1"]
    seen: list[str] = []
    for u in out:
        if u not in seen:
            seen.append(u)
    return seen[:4]


def detect_ics(fetcher: Fetcher, html: str, page_url: str, domain: str, country, trail: list) -> Optional[dict]:
    for feed in find_ics_links(html, page_url):
        try:
            r = fetcher.get(feed)
        except (BudgetExhausted, RobotsDisallowed):
            raise
        except Exception as e:
            trail.append(f"ics {feed}: {type(e).__name__}")
            continue
        trail.append(f"ics {feed}: HTTP {r.status}")
        if r.status >= 400 or "BEGIN:VCALENDAR" not in r.text[:2000]:
            continue
        try:
            rows = P.parse_ics(r.text, feed)
        except Exception as e:
            trail.append(f"ics parse: {type(e).__name__}")
            continue
        n = _count_future(rows, _stub(domain, "ics", country))
        if n >= MIN_EVENTS:
            return {"detector": "ics", "events": n, "feed": feed, "evidence": f"ics feed {feed} → {n} future"}
    return None


def detect_tribe(fetcher: Fetcher, html: str, page_url: str, domain: str, country, trail: list) -> Optional[dict]:
    if not any(h in html for h in _WP_HINTS):
        return None
    api = _origin(page_url) + TRIBE_PATH
    try:
        r = fetcher.get(api)
    except (BudgetExhausted, RobotsDisallowed):
        raise
    except Exception as e:
        trail.append(f"tribe: {type(e).__name__}")
        return None
    trail.append(f"tribe {api}: HTTP {r.status}")
    if r.status >= 400:
        return None
    try:
        body = P.decode_json(r.text)
        rows = P.parse_api(body, {"items": "events", "fields": TRIBE_FIELDS}, api)
    except Exception as e:
        trail.append(f"tribe parse: {type(e).__name__}")
        return None
    n = _count_future(rows, _stub(domain, "api", country))
    if n >= MIN_EVENTS:
        return {"detector": "tribe_rest", "events": n, "api": api, "evidence": f"tribe REST → {n} future"}
    return None


# ── recipe builders ──────────────────────────────────────────────────────
def build_recipe(domain: str, hit: dict, page_urls: list[str], country: str, city: Optional[str], prior_yield: int) -> dict:
    base = {
        "recipe_version": 1, "domain": domain, "country": country,
        **({"city_name": city} if city else {}),
        "priority": int(min(30, max(prior_yield // 10, hit["events"] // 5))),
        "cadence_hours": CADENCE_HOURS,
        "fetch": {"delay_seconds": 1.5, "timeout": 20},
    }
    if hit["detector"] == "jsonld":
        base.update(source_name=_slug("ld", domain),
                    entry={"urls": page_urls[:40], "paginate": {"mode": "none"}},
                    parse={"kind": "jsonld"})
    elif hit["detector"] == "ics":
        base.update(source_name=_slug("ics", domain),
                    entry={"urls": [hit["feed"]], "paginate": {"mode": "none"}},
                    parse={"kind": "ics"})
    else:
        base.update(source_name=_slug("tec", domain),
                    entry={"urls": [hit["api"]],
                           "paginate": {"mode": "next_url", "path": "next_rest_url", "max_pages": 10}},
                    fetch={"delay_seconds": 1.5, "timeout": 25, "headers": {"Accept": "application/json"}},
                    parse={"kind": "api", "items": "events", "fields": TRIBE_FIELDS})
    base["notes"] = (f"Auto-probed {datetime.utcnow():%Y-%m-%d}: {hit['evidence']} "
                     f"(Cadence A yield {prior_yield}). Replace with recipes/{domain}.json to take over.")
    return base


# ── one domain ───────────────────────────────────────────────────────────
def probe_domain(domain: str, page_urls: list[str], country: Optional[str], *,
                 fetcher: Optional[Fetcher] = None) -> dict:
    """→ {"hit": dict|None, "trail": [...], "pages_checked": n, "requests": n, "error": str|None}"""
    own = fetcher is None
    fetcher = fetcher or Fetcher({"delay_seconds": 1.0, "timeout": 20}, max_requests=REQUESTS_PER_DOMAIN)
    trail: list = []
    hit = None
    pages_checked = 0
    error = None
    try:
        for url in page_urls[:MAX_PAGES_PER_DOMAIN]:
            try:
                r = fetcher.get(url)
            except BudgetExhausted:
                trail.append("budget")
                break
            except RobotsDisallowed:
                trail.append(f"robots disallows {url}")
                continue
            except Exception as e:
                trail.append(f"{url}: {type(e).__name__}")
                continue
            pages_checked += 1
            trail.append(f"{url}: HTTP {r.status}")
            if r.status >= 400 or not r.text:
                continue
            html = r.text
            hit = detect_jsonld(html, r.url, domain, country)
            if hit:
                break
            hit = detect_ics(fetcher, html, r.url, domain, country, trail)
            if hit:
                break
            hit = detect_tribe(fetcher, html, r.url, domain, country, trail)
            if hit:
                break
    except BudgetExhausted:
        trail.append("budget")
    except Exception as e:  # runner bug → record, never crash the batch
        error = f"{type(e).__name__}: {e}"
    finally:
        reqs = fetcher.requests_made
        if own:
            fetcher.close()
    return {"hit": hit, "trail": trail, "pages_checked": pages_checked, "requests": reqs, "error": error}


# ── batch over the pool ──────────────────────────────────────────────────
def _is_reserved(url: str) -> bool:
    try:
        from app.scheduler.jobs import _is_reserved_discovery_url
        return bool(_is_reserved_discovery_url(url))
    except Exception:
        return False


def select_candidates(db, limit: int) -> list[dict]:
    """Domains with LLMSource pages, no SourceRecipe, and no fresh probe.
    Ordered by Cadence A yield desc so domains that demonstrably carried
    events are cracked first."""
    from app.models import LLMSource, SourceRecipe, SourceProbe
    have_recipe = {d for (d,) in db.query(SourceRecipe.domain).all()}
    cutoff = datetime.utcnow() - timedelta(days=REPROBE_DAYS)
    fresh = {}
    for pr in db.query(SourceProbe).all():
        # skip: recipe already made, or probed recently (any outcome), or
        # permanently reserved
        if pr.outcome in ("recipe", "reserved") or (pr.last_probed_at and pr.last_probed_at >= cutoff):
            fresh[pr.domain] = pr
    rows = (db.query(LLMSource.url, LLMSource.country, LLMSource.city_name,
                     LLMSource.events_saved_total, LLMSource.last_event_count, LLMSource.state)
            .filter(LLMSource.state != "blocked").all())
    by_dom: dict = defaultdict(list)
    for url, country, city, saved, last, state in rows:
        dom = registered_domain(url)
        if not dom or "." not in dom or dom in have_recipe or dom in fresh:
            continue
        by_dom[dom].append((url, country, city, saved or 0, last or 0))
    cands = []
    for dom, lst in by_dom.items():
        lst.sort(key=lambda t: (-t[3], -t[4], t[0]))
        countries = [c for _, c, _, _, _ in lst if c]
        country = max(set(countries), key=countries.count) if countries else None
        cities = [ci for _, c, ci, _, _ in lst if ci and c == country]
        city = max(set(cities), key=cities.count) if cities else None
        cands.append({"domain": dom, "urls": [u for u, *_ in lst], "country": country, "city": city,
                      "prior_yield": sum(t[3] for t in lst), "pages": len(lst)})
    cands.sort(key=lambda c: (-c["prior_yield"], -c["pages"], c["domain"]))
    return cands[:limit]


def run_probe_batch(db, *, limit: int = 60, dry_run: bool = False,
                    wall_clock_s: int = 40 * 60) -> dict:
    from app.models import SourceProbe
    t0 = datetime.utcnow()
    cands = select_candidates(db, limit)
    summary = {"candidates": len(cands), "probed": 0, "recipes": 0, "none": 0, "error": 0,
               "no_country": 0, "reserved": 0, "requests": 0, "by_detector": {}, "hits": [], "dry_run": dry_run}
    if dry_run:
        summary["sample"] = [(c["domain"], c["prior_yield"], c["pages"]) for c in cands[:20]]
        return summary
    for c in cands:
        if (datetime.utcnow() - t0).total_seconds() > wall_clock_s:
            summary["stopped"] = "wall clock"
            break
        dom = c["domain"]
        pr = db.query(SourceProbe).filter(SourceProbe.domain == dom).first()
        if pr is None:
            pr = SourceProbe(domain=dom)
            db.add(pr)
        pr.attempts = (pr.attempts or 0) + 1
        pr.last_probed_at = datetime.utcnow()
        pr.prior_yield = c["prior_yield"]
        if _is_reserved(c["urls"][0]):
            pr.outcome = "reserved"
            pr.evidence = "Route 2 collector covers this domain"
            summary["reserved"] += 1
            db.commit()
            continue
        res = probe_domain(dom, c["urls"], c["country"])
        summary["probed"] += 1
        summary["requests"] += res["requests"]
        pr.pages_checked = res["pages_checked"]
        pr.requests = res["requests"]
        pr.evidence = " | ".join(res["trail"])[:1500]
        hit = res["hit"]
        if res["error"]:
            pr.outcome = "error"
            pr.evidence = (res["error"] + " | " + pr.evidence)[:1500]
            summary["error"] += 1
        elif hit is None:
            pr.outcome = "none"
            pr.detector = None
            pr.events_found = 0
            summary["none"] += 1
        elif not c["country"]:
            pr.outcome = "no_country"
            pr.detector = hit["detector"]
            pr.events_found = hit["events"]
            summary["no_country"] += 1
        else:
            doc = build_recipe(dom, hit, c["urls"], c["country"], c["city"], c["prior_yield"])
            up = upsert_recipe(db, doc, written_by=WRITTEN_BY)
            if up["verb"] in ("created", "updated", "unchanged"):
                pr.outcome = "recipe"
                pr.detector = hit["detector"]
                pr.events_found = hit["events"]
                summary["recipes"] += 1
                summary["by_detector"][hit["detector"]] = summary["by_detector"].get(hit["detector"], 0) + 1
                summary["hits"].append((dom, hit["detector"], hit["events"]))
            else:
                pr.outcome = "error"
                pr.evidence = ("recipe " + up["verb"] + ": " + "; ".join(up.get("problems", [])[:2]) + " | " + pr.evidence)[:1500]
                summary["error"] += 1
        db.commit()
    summary["duration_s"] = int((datetime.utcnow() - t0).total_seconds())
    return summary


def probe_at_job() -> dict:
    from app.database import SessionLocal
    db = SessionLocal()
    try:
        s = run_probe_batch(db)
        logger.info(
            f"recipe_probe: candidates={s['candidates']} probed={s['probed']} recipes={s['recipes']} "
            f"{s['by_detector']} none={s['none']} error={s['error']} no_country={s['no_country']} "
            f"reserved={s['reserved']} requests={s['requests']} {s.get('stopped', '')}"
        )
        return s
    finally:
        db.close()
