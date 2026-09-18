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

No LLM, no Brave. Budget ≤ 8 requests per domain. Country for the recipe: the
LLMSource rows' country, else inferred from the detected events' own
addresses (ISO codes mapped), else the domain's country-code TLD.
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
from app.services.recipes.countries import canon_country
from app.services.recipes.schema import registered_domain
from app.services.recipes.sync import upsert_recipe

logger = logging.getLogger(__name__)

WRITTEN_BY = "auto-probe"
MIN_EVENTS = 3
MAX_PAGES_PER_DOMAIN = 2
REQUESTS_PER_DOMAIN = 8
REPROBE_DAYS = 45
# 'no_country' = detector HIT but no country to file the recipe under.
# Until 2026-09-18 these were re-eligible immediately, so the same ~86
# domains ate 70% of every hourly batch (probed=120, no_country=86).
# Now they wait like everyone else (shorter, since a pin with a country
# or a better inference can still rescue them), and the queue page lists
# them under "needs country" for a human to pin with one.
NO_COUNTRY_REPROBE_DAYS = 7
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


def _future_events(rows: list, doc_stub: dict) -> list:
    """Normalise parsed rows with a throwaway recipe stub and return the
    survivors (future-dated, named). This is exactly what the nightly
    runner would keep, so a probe 'hit' means real events."""
    return normalize(rows, doc_stub).events


def _count_future(rows: list, doc_stub: dict) -> int:
    return len(_future_events(rows, doc_stub))


# ISO-3166 alpha-2 / common variants → the country names City.country uses.
_ISO2 = {
    "IL": "Israel", "US": "United States", "USA": "United States", "GB": "United Kingdom", "UK": "United Kingdom",
    "DE": "Germany", "FR": "France", "ES": "Spain", "IT": "Italy", "PT": "Portugal", "NL": "Netherlands",
    "BE": "Belgium", "AT": "Austria", "CH": "Switzerland", "IE": "Ireland", "SE": "Sweden", "NO": "Norway",
    "DK": "Denmark", "FI": "Finland", "PL": "Poland", "CZ": "Czech Republic", "HU": "Hungary", "GR": "Greece",
    "TR": "Turkey", "CA": "Canada", "MX": "Mexico", "BR": "Brazil", "AR": "Argentina", "CL": "Chile",
    "AU": "Australia", "NZ": "New Zealand", "JP": "Japan", "KR": "South Korea", "TH": "Thailand",
    "SG": "Singapore", "IN": "India", "ZA": "South Africa", "AE": "United Arab Emirates", "CY": "Cyprus",
}
_TLD = {
    "il": "Israel", "co.il": "Israel", "org.il": "Israel", "de": "Germany", "fr": "France", "es": "Spain",
    "it": "Italy", "pt": "Portugal", "nl": "Netherlands", "be": "Belgium", "at": "Austria", "ch": "Switzerland",
    "ie": "Ireland", "se": "Sweden", "no": "Norway", "dk": "Denmark", "fi": "Finland", "pl": "Poland",
    "cz": "Czech Republic", "hu": "Hungary", "gr": "Greece", "tr": "Turkey", "ca": "Canada", "mx": "Mexico",
    "br": "Brazil", "com.br": "Brazil", "ar": "Argentina", "cl": "Chile", "au": "Australia", "com.au": "Australia",
    "nz": "New Zealand", "jp": "Japan", "kr": "South Korea", "th": "Thailand", "sg": "Singapore", "in": "India",
    "za": "South Africa", "co.za": "South Africa", "uk": "United Kingdom", "co.uk": "United Kingdom", "cy": "Cyprus",
}


def infer_country(events: list, domain: str) -> Optional[str]:
    """Most common venue_country among the detected events (ISO codes
    mapped to City.country names), else the domain's country-code TLD."""
    votes: dict = {}
    for ev in events:
        c = (getattr(ev, "venue_country", None) or "").strip()
        if not c:
            continue
        name = _ISO2.get(c.upper(), None) if len(c) <= 3 else canon_country(c)
        if name:
            votes[name] = votes.get(name, 0) + 1
    if votes:
        return max(votes, key=votes.get)
    parts = domain.lower().split(".")
    for n in (2, 1):
        tld = ".".join(parts[-n:])
        if tld in _TLD:
            return _TLD[tld]
    return None


def _stub(domain: str, kind: str, country: Optional[str]) -> dict:
    return {"source_name": "probe", "domain": domain, "country": country or "?",
            "parse": {"kind": kind}, "language": None, "timezone": None}


# ── detectors ────────────────────────────────────────────────────────────
def detect_jsonld(html: str, page_url: str, domain: str, country) -> Optional[dict]:
    rows = P.parse_jsonld(html, page_url)
    evs = _future_events(rows, _stub(domain, "jsonld", country))
    if len(evs) < MIN_EVENTS:
        return None
    return {"detector": "jsonld", "events": len(evs), "evidence": f"jsonld on {page_url}",
            "country": country or infer_country(evs, domain)}


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
        evs = _future_events(rows, _stub(domain, "ics", country))
        if len(evs) >= MIN_EVENTS:
            return {"detector": "ics", "events": len(evs), "feed": feed,
                    "evidence": f"ics feed {feed} → {len(evs)} future",
                    "country": country or infer_country(evs, domain)}
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
    evs = _future_events(rows, _stub(domain, "api", country))
    if len(evs) >= MIN_EVENTS:
        return {"detector": "tribe_rest", "events": len(evs), "api": api,
                "evidence": f"tribe REST → {len(evs)} future",
                "country": country or infer_country(evs, domain)}
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
    events are cracked first.

    Human pins (QueuePin, superca.ly/queue.html) come first in rank order,
    regardless of probe history, and may name domains that are not in the
    LLMSource pool at all (the homepage becomes the entry URL)."""
    from app.models import LLMSource, SourceRecipe, SourceProbe, QueuePin, City
    have_recipe = {d for (d,) in db.query(SourceRecipe.domain).all()}
    # city → country when the city name is unambiguous in our City table.
    # Most pool rows have a city but no country (Cadence B stored the
    # query city only), which is what produced the 'no_country' pile.
    city_country: dict = {}
    for name, country in db.query(City.name, City.country).all():
        if not name or not country:
            continue
        if name in city_country and city_country[name] != country:
            city_country[name] = None            # ambiguous (Dublin IE / Dublin OH)
        else:
            city_country.setdefault(name, country)
    pins = {p.domain: p for p in db.query(QueuePin).filter(QueuePin.status == "queued")
            .order_by(QueuePin.rank.asc(), QueuePin.id.asc()).all()}
    now = datetime.utcnow()
    cutoff = now - timedelta(days=REPROBE_DAYS)
    cutoff_nc = now - timedelta(days=NO_COUNTRY_REPROBE_DAYS)
    fresh = {}
    nc_fresh: set = set()                 # no_country hits still cooling down
    for pr in db.query(SourceProbe).all():
        if pr.domain in pins:
            continue                      # a pin overrides any history
        if pr.outcome in ("recipe", "reserved"):
            fresh[pr.domain] = pr
        elif pr.outcome == "no_country":
            if pr.last_probed_at and pr.last_probed_at >= cutoff_nc:
                nc_fresh.add(pr.domain)   # decided below: eligible iff a country is now known
        elif pr.last_probed_at and pr.last_probed_at >= cutoff:
            fresh[pr.domain] = pr
    rows = (db.query(LLMSource.url, LLMSource.country, LLMSource.city_name,
                     LLMSource.events_saved_total, LLMSource.last_event_count, LLMSource.state)
            .filter(LLMSource.state != "blocked").all())
    by_dom: dict = defaultdict(list)
    for url, country, city, saved, last, state in rows:
        dom = registered_domain(url)
        if not dom or "." not in dom or dom in have_recipe or dom in fresh:
            continue
        by_dom[dom].append((url, canon_country(country), city, saved or 0, last or 0))
    cands = []
    for dom, lst in by_dom.items():
        lst.sort(key=lambda t: (-t[3], -t[4], t[0]))
        countries = [c for _, c, _, _, _ in lst if c]
        country = max(set(countries), key=countries.count) if countries else None
        cities = [ci for _, c, ci, _, _ in lst if ci and c == country]
        city = max(set(cities), key=cities.count) if cities else None
        if country is None and city and city_country.get(city):
            country = city_country[city]
        if dom in nc_fresh and country is None:
            continue                      # still nothing to file it under
        pin = pins.get(dom)
        cands.append({"domain": dom, "urls": [u for u, *_ in lst],
                      "country": canon_country(pin.country) if (pin and pin.country) else country,
                      "city": city, "prior_yield": sum(t[3] for t in lst), "pages": len(lst),
                      "pin_rank": pin.rank if pin else None})
    # pinned domains outside the pool: probe the homepage
    for dom, pin in pins.items():
        if dom in by_dom or dom in have_recipe:
            continue
        cands.append({"domain": dom, "urls": [f"https://{dom}/", f"https://www.{dom}/"],
                      "country": canon_country(pin.country), "city": None,
                      "prior_yield": 0, "pages": 0, "pin_rank": pin.rank})
    cands.sort(key=lambda c: (0 if c["pin_rank"] is not None else 1, c["pin_rank"] or 0,
                              -c["prior_yield"], -c["pages"], c["domain"]))
    return cands[:limit]


def _resolve_pin(db, domain: str, outcome: str) -> None:
    from app.models import QueuePin
    pin = db.query(QueuePin).filter(QueuePin.domain == domain, QueuePin.status == "queued").first()
    if pin is None:
        return
    pin.status = "recipe" if outcome == "recipe" else "probed"
    pin.outcome = outcome
    pin.resolved_at = datetime.utcnow()


def run_probe_batch(db, *, limit: int = 120, dry_run: bool = False,
                    wall_clock_s: int = 40 * 60) -> dict:
    from app.models import SourceProbe
    t0 = datetime.utcnow()
    cands = select_candidates(db, limit)
    summary = {"candidates": len(cands), "probed": 0, "recipes": 0, "none": 0, "error": 0,
               "no_country": 0, "reserved": 0, "requests": 0, "by_detector": {}, "hits": [], "dry_run": dry_run,
               "pinned": sum(1 for c in cands if c.get("pin_rank") is not None)}
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
            _resolve_pin(db, dom, "reserved")
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
        elif not (c["country"] or hit.get("country")):
            pr.outcome = "no_country"
            pr.detector = hit["detector"]
            pr.events_found = hit["events"]
            summary["no_country"] += 1
        else:
            country = c["country"] or hit["country"]
            if not c["country"]:
                res["trail"].append(f"country inferred: {country}")
                pr.evidence = " | ".join(res["trail"])[:1500]
            doc = build_recipe(dom, hit, c["urls"], country, c["city"], c["prior_yield"])
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
        _resolve_pin(db, dom, pr.outcome)
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
            f"reserved={s['reserved']} pinned={s.get('pinned', 0)} requests={s['requests']} {s.get('stopped', '')}"
        )
        return s
    finally:
        db.close()
